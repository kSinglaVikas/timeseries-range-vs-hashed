#!/bin/bash

# Install MongoDB Kafka Connector
# This downloads and installs the MongoDB connector to the Kafka Connect container

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

KAFKA_CONNECT_HOST=${KAFKA_CONNECT_HOST:-localhost}
KAFKA_CONNECT_PORT=${KAFKA_CONNECT_PORT:-8083}

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}Installing MongoDB Kafka Connector${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""

# Check if Kafka Connect is running
echo -e "${YELLOW}Checking Kafka Connect...${NC}"
if ! curl -s "http://$KAFKA_CONNECT_HOST:$KAFKA_CONNECT_PORT/" > /dev/null 2>&1; then
    echo -e "${RED}Error: Kafka Connect is not accessible at http://$KAFKA_CONNECT_HOST:$KAFKA_CONNECT_PORT/${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Kafka Connect is running${NC}"
echo ""

# Auto-detect Kafka Connect container name
echo -e "${YELLOW}Discovering Kafka Connect container...${NC}"
KAFKA_CONNECT_CONTAINER=$(docker ps --filter "label=com.docker.compose.service=kafka-connect" --format "{{.Names}}" | head -1)

# If not found by label, try to find by image name
if [ -z "$KAFKA_CONNECT_CONTAINER" ]; then
    KAFKA_CONNECT_CONTAINER=$(docker ps --filter "ancestor=confluentinc/cp-kafka-connect:*" --format "{{.Names}}" | head -1)
fi

# Fallback to env var or error
if [ -z "$KAFKA_CONNECT_CONTAINER" ]; then
    KAFKA_CONNECT_CONTAINER="${KAFKA_CONNECT_CONTAINER_OVERRIDE:-}"
    if [ -z "$KAFKA_CONNECT_CONTAINER" ]; then
        echo -e "${RED}Error: Could not find Kafka Connect container. Please ensure Docker services are running.${NC}"
        echo "Running containers:"
        docker ps --format "table {{.Names}}\t{{.Image}}"
        exit 1
    fi
fi

echo -e "${GREEN}✓ Found Kafka Connect container: $KAFKA_CONNECT_CONTAINER${NC}"
echo ""

# Create lib directory in the container
echo -e "${YELLOW}Creating connector lib directory...${NC}"
docker exec $KAFKA_CONNECT_CONTAINER mkdir -p /usr/share/confluent-hub-components/mongodb-kafka-connect-mongodb-latest/lib


# Download the MongoDB connector JAR
echo -e "${YELLOW}Downloading MongoDB Kafka Connector...${NC}"
CONNECTOR_VERSION="1.13.0"
CONNECTOR_JAR="mongo-kafka-connect-$CONNECTOR_VERSION-all.jar"

# Download from the correct Maven Central URL (org/mongodb)
echo "Downloading from Maven Central (org/mongodb)..."
docker exec $KAFKA_CONNECT_CONTAINER bash -c "
  mkdir -p /usr/share/confluent-hub-components/mongodb-kafka-connect-mongodb-latest/lib
  cd /usr/share/confluent-hub-components/mongodb-kafka-connect-mongodb-latest/lib
  curl -L -f -o $CONNECTOR_JAR 'https://repo1.maven.org/maven2/org/mongodb/kafka/mongo-kafka-connect/$CONNECTOR_VERSION/$CONNECTOR_JAR'
  # Verify download was successful (file should be > 50MB)
  if [ -f $CONNECTOR_JAR ]; then
    SIZE=\$(du -k $CONNECTOR_JAR | cut -f1)
    if [ \$SIZE -lt 100 ]; then
      echo 'ERROR: JAR file appears to be too small or empty'
      rm -f $CONNECTOR_JAR
      exit 1
    fi
    # Copy JAR to parent directory for plugin discovery compatibility
    cp $CONNECTOR_JAR ../$CONNECTOR_JAR
  fi
" || {
    echo -e "${RED}Failed to download connector from Maven Central (org/mongodb)${NC}"
    echo -e "${RED}Trying confluent-hub install method...${NC}"
    # Try using confluent-hub
    docker exec $KAFKA_CONNECT_CONTAINER confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:$CONNECTOR_VERSION || {
        echo -e "${RED}All download methods failed.${NC}"
        echo "Please download the MongoDB Kafka Connector manually from:"
        echo "https://repo1.maven.org/maven2/org/mongodb/kafka/mongo-kafka-connect/"
        exit 1
    }
}

echo -e "${GREEN}✓ Connector JAR downloaded${NC}"

# Create manifest file
echo -e "${YELLOW}Creating manifest file...${NC}"
docker exec $KAFKA_CONNECT_CONTAINER bash -c "
  mkdir -p /usr/share/confluent-hub-components/mongodb-kafka-connect-mongodb-latest
  cat > /usr/share/confluent-hub-components/mongodb-kafka-connect-mongodb-latest/manifest.json << 'EOF'
{
  \"name\": \"MongoDB Connector for Kafka\",
  \"owner\": {
    \"username\": \"mongodb\",
    \"type\": \"organization\",
    \"url\": \"https://www.mongodb.com\",
    \"logo\": \"\"
  },
  \"version\": \"$CONNECTOR_VERSION\",
  \"release_date\": \"2024-01-01\",
  \"documentation_url\": \"https://docs.mongodb.com/kafka-connector/\",
  \"source_url\": \"https://github.com/mongodb-labs/mongo-kafka\",
  \"issues_url\": \"https://github.com/mongodb-labs/mongo-kafka/issues\",
  \"support\": {
    \"provided_by\": \"MongoDB\",
    \"support_url\": \"https://www.mongodb.com/support\",
    \"logo\": \"\"
  },
  \"component_types\": [
    \"sink\",
    \"source\"
  ],
  \"description\": \"MongoDB Kafka Connector for Kafka Connect\",
  \"details_url\": \"https://www.mongodb.com/docs/kafka-connector/\",
  \"tags\": [
    \"MongoDB\",
    \"database\",
    \"sink\",
    \"source\"
  ],
  \"requirements\": [],
  \"owner_username\": \"mongodb\",
  \"owner_type\": \"organization\",
  \"owner_name\": \"MongoDB\",
  \"owner_url\": \"https://www.mongodb.com\",
  \"owner_logo\": \"\",
  \"component\": {
    \"type\": \"sink\",
    \"name\": \"MongoSinkConnector\",
    \"title\": \"MongoDB Sink\",
    \"description\": \"MongoDB Sink Connector\",
    \"documentation_url\": \"https://docs.mongodb.com/kafka-connector/sink/\",
    \"source_url\": \"https://github.com/mongodb-labs/mongo-kafka\",
    \"docker_image\": \"\",
    \"docker_tag\": \"\",
    \"jar_url\": \"\",
    \"license\": [
      {
        \"name\": \"Server Side Public License v1\",
        \"url\": \"https://www.mongodb.com/licensing/server-side-public-license\"
      }
    ],
    \"author\": \"MongoDB\",
    \"preview\": false,
    \"features\": []
  }
}
EOF
"

echo -e "${GREEN}✓ Manifest file created${NC}"

# Restart Kafka Connect to reload plugins
echo -e "${YELLOW}Restarting Kafka Connect to load the connector...${NC}"
docker restart $KAFKA_CONNECT_CONTAINER > /dev/null 2>&1
sleep 10

# Verify connector is installed
echo -e "${YELLOW}Verifying connector installation...${NC}"
if curl -s "http://$KAFKA_CONNECT_HOST:$KAFKA_CONNECT_PORT/connector-plugins" | grep -q "MongoSinkConnector"; then
    echo -e "${GREEN}✓ MongoDB Connector successfully installed!${NC}"
    echo ""
    echo -e "${YELLOW}Installed connectors:${NC}"
    curl -s "http://$KAFKA_CONNECT_HOST:$KAFKA_CONNECT_PORT/connector-plugins" | grep -o '"class":"[^"]*"' | head -10
    echo ""
    echo -e "${GREEN}You can now deploy your connector using:${NC}"
    echo "./deploy-connector.sh"
else
    echo -e "${YELLOW}⚠ Connector may not have been installed correctly.${NC}"
    echo -e "${YELLOW}Check Docker logs for more details:${NC}"
    echo "docker logs $KAFKA_CONNECT_CONTAINER"
fi

echo ""
echo -e "${GREEN}Done!${NC}"
