#!/bin/bash

# Deploy MongoDB Atlas Sink Connectors (time-series range and time-series hash) to Kafka Connect
# This script reads both connector configurations and deploys them to the running Kafka Connect instance

set -e

# Configuration
KAFKA_CONNECT_HOST=${KAFKA_CONNECT_HOST:-localhost}
KAFKA_CONNECT_PORT=${KAFKA_CONNECT_PORT:-8083}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}===============================================${NC}"
echo -e "${YELLOW}MongoDB Atlas Kafka Connect Sink Deployment${NC}"
echo -e "${YELLOW}===============================================${NC}"
echo ""

# Check .env file
if [ ! -f ".env" ]; then
    echo -e "${RED}Error: .env file not found. Please copy .env.example to .env and update with your credentials${NC}"
    exit 1
fi

# Source environment variables
source .env

ATLAS_TIME_SERIES_RANGE_COLLECTION=${ATLAS_TIME_SERIES_RANGE_COLLECTION:-events_ts_range}
ATLAS_TIME_SERIES_HASH_COLLECTION=${ATLAS_TIME_SERIES_HASH_COLLECTION:-events_ts_hash}
DEPLOY_TS_RANGE_CONNECTOR=${DEPLOY_TS_RANGE_CONNECTOR:-true}
DEPLOY_TS_HASH_CONNECTOR=${DEPLOY_TS_HASH_CONNECTOR:-true}

# Validate required environment variables
if [ -z "$ATLAS_CONNECTION_STRING" ]; then
    echo -e "${RED}Error: ATLAS_CONNECTION_STRING not set in .env file${NC}"
    exit 1
fi

echo "Kafka Connect Host: $KAFKA_CONNECT_HOST:$KAFKA_CONNECT_PORT"
echo ""

# Function to deploy a connector
deploy_connector() {
    local CONFIG_FILE=$1
    local CONNECTOR_NAME=$2

    echo ""
    echo -e "${YELLOW}Deploying connector: $CONNECTOR_NAME using $CONFIG_FILE...${NC}"

    if [ ! -f "$CONFIG_FILE" ]; then
        echo -e "${RED}Error: Configuration file '$CONFIG_FILE' not found${NC}"
        return 1
    fi

    # Resolve collection name per connector type.
    if [[ "$CONFIG_FILE" == *"range"* ]]; then
        COLLECTION_NAME="$ATLAS_TIME_SERIES_RANGE_COLLECTION"
    else
        COLLECTION_NAME="$ATLAS_TIME_SERIES_HASH_COLLECTION"
    fi

    # Substitute environment variables in config file
    CONNECTOR_JSON=$(cat "$CONFIG_FILE" | \
        sed "s|ATLAS_CONNECTION_STRING|$ATLAS_CONNECTION_STRING|g" | \
        sed "s|ATLAS_DATABASE|${ATLAS_DATABASE:-kafka_data}|g" | \
        sed "s|ATLAS_TIME_SERIES_RANGE_COLLECTION|$ATLAS_TIME_SERIES_RANGE_COLLECTION|g" | \
        sed "s|ATLAS_TIME_SERIES_HASH_COLLECTION|$ATLAS_TIME_SERIES_HASH_COLLECTION|g" | \
        sed "s|ATLAS_COLLECTION|$COLLECTION_NAME|g" | \
        sed "s|MAX_TASKS|${MAX_TASKS:-1}|g")

    # Check if connector already exists
    EXISTING=$(curl -s -X GET "http://$KAFKA_CONNECT_HOST:$KAFKA_CONNECT_PORT/connectors/$CONNECTOR_NAME" 2>/dev/null || echo "")
    if [ ! -z "$EXISTING" ]; then
        echo -e "${YELLOW}Connector '$CONNECTOR_NAME' already exists. Deleting it first...${NC}"
        curl -s -X DELETE "http://$KAFKA_CONNECT_HOST:$KAFKA_CONNECT_PORT/connectors/$CONNECTOR_NAME"
        sleep 2
    fi

    # Deploy the connector
    RESPONSE=$(curl -s -X POST "http://$KAFKA_CONNECT_HOST:$KAFKA_CONNECT_PORT/connectors" \
        -H "Content-Type: application/json" \
        -d "$CONNECTOR_JSON")

    # Check if deployment was successful
    if echo "$RESPONSE" | grep -q '"name":"'$CONNECTOR_NAME'"'; then
        echo -e "${GREEN}✓ Connector '$CONNECTOR_NAME' deployed successfully!${NC}"
        echo "$RESPONSE" | python3 -m json.tool
    else
        echo -e "${RED}✗ Failed to deploy connector '$CONNECTOR_NAME'${NC}"
        echo "Response: $RESPONSE"
        return 1
    fi

    sleep 2
    STATUS=$(curl -s -X GET "http://$KAFKA_CONNECT_HOST:$KAFKA_CONNECT_PORT/connectors/$CONNECTOR_NAME/status")
    echo -e "${YELLOW}Initial Status for $CONNECTOR_NAME:${NC}"
    echo "$STATUS" | python3 -m json.tool
}

# Deploy connectors based on selected mode.
if [ "$DEPLOY_TS_RANGE_CONNECTOR" = "true" ]; then
    deploy_connector "atlas-sink-timeseries-range-connector.json" "atlas-sink-timeseries-range-connector"
else
    echo -e "${YELLOW}Skipping range time-series connector (DEPLOY_TS_RANGE_CONNECTOR=false)${NC}"
fi

if [ "$DEPLOY_TS_HASH_CONNECTOR" = "true" ]; then
    deploy_connector "atlas-sink-timeseries-hash-connector.json" "atlas-sink-timeseries-hash-connector"
else
    echo -e "${YELLOW}Skipping hash time-series connector (DEPLOY_TS_HASH_CONNECTOR=false)${NC}"
fi

echo ""
echo -e "${GREEN}Done! Monitor the connector status with:${NC}"
echo "curl http://$KAFKA_CONNECT_HOST:$KAFKA_CONNECT_PORT/connectors/atlas-sink-timeseries-range-connector/status | jq"
echo "curl http://$KAFKA_CONNECT_HOST:$KAFKA_CONNECT_PORT/connectors/atlas-sink-timeseries-hash-connector/status | jq"