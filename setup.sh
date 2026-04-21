#!/bin/bash

# Complete setup script for Python Kafka to MongoDB Atlas pipeline
# This script sets up everything in the correct order

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Helper functions
print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
}

print_step() {
    echo -e "${YELLOW}$1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Main setup
print_header "Python Kafka to MongoDB Atlas Setup"

# Step 1: Check prerequisites
print_step "Step 1: Checking prerequisites..."

# Check Docker
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed"
    exit 1
fi
print_success "Docker installed"

# Check Docker Compose
if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose is not installed"
    exit 1
fi
print_success "Docker Compose installed"

# Check Python3
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed"
    exit 1
fi
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
print_success "Python $PYTHON_VERSION installed"

# Step 2: Configure environment
print_step "Step 2: Checking environment configuration..."

if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        print_step "Creating .env from .env.example..."
        cp .env.example .env
        print_success ".env created"
        echo ""
        print_error "Please edit .env with your MongoDB Atlas credentials:"
        echo "  - ATLAS_CONNECTION_STRING"
        echo "  - ATLAS_DATABASE"
        echo "  - ATLAS_TIME_SERIES_RANGE_COLLECTION"
        echo "  - ATLAS_TIME_SERIES_HASH_COLLECTION"
        echo ""
        exit 0
    fi
fi
print_success ".env file exists"

echo ""

# Step 3: Setup Python virtual environment
print_step "Step 3: Setting up Python virtual environment..."

if [ ! -d "venv" ]; then
    python3 -m venv venv
    print_success "Virtual environment created"
else
    print_success "Virtual environment already exists"
fi

# Activate venv
source venv/bin/activate
print_success "Virtual environment activated"

# Upgrade pip
pip install --upgrade pip > /dev/null 2>&1
print_success "pip upgraded"

# Install requirements
pip install -r requirements.txt > /dev/null 2>&1
print_success "Dependencies installed"

echo ""

# Step 4: Start Docker services
print_step "Step 4: Starting Docker services..."

if docker ps -a --format '{{.Names}}' | grep -q "python-kafka-atlas-sink"; then
    print_step "Containers already exist. Removing..."
    docker-compose down > /dev/null 2>&1 || true
fi

docker-compose up -d
sleep 10

# Verify services are running
if docker ps --format '{{.Names}}' | grep -q "kafka-1"; then
    print_success "Kafka started"
else
    print_error "Kafka failed to start"
    docker-compose logs kafka | tail -20
    exit 1
fi

if docker ps --format '{{.Names}}' | grep -q "zookeeper-1"; then
    print_success "Zookeeper started"
else
    print_error "Zookeeper failed to start"
    exit 1
fi

if docker ps --format '{{.Names}}' | grep -q "kafka-connect-1"; then
    print_success "Kafka Connect started"
else
    print_error "Kafka Connect failed to start"
    exit 1
fi

print_step "Creating Kafka topic with configured partition count..."
source .env
TOPIC_NAME=${KAFKA_TOPIC:-events}
TOPIC_PARTITIONS=${KAFKA_TOPIC_PARTITIONS:-10}
docker compose exec -T kafka kafka-topics \
    --bootstrap-server kafka:29092 \
    --create \
    --if-not-exists \
    --topic "$TOPIC_NAME" \
    --partitions "$TOPIC_PARTITIONS" \
    --replication-factor 1 > /dev/null
print_success "Topic '$TOPIC_NAME' ready with $TOPIC_PARTITIONS partitions"

echo ""

# Step 5: Install MongoDB Connector
print_step "Step 5: Installing MongoDB Connector..."

if ./install-connector.sh > /dev/null 2>&1; then
    print_success "MongoDB Connector installed"
else
    print_error "MongoDB Connector installation failed"
    ./install-connector.sh
    exit 1
fi

echo ""

# Step 6: Prepare MongoDB Atlas collections
print_step "Step 6: Preparing MongoDB Atlas collections..."

if python mongo_setup.py > /dev/null 2>&1; then
    print_success "MongoDB Atlas collections prepared"
else
    print_error "Failed to prepare MongoDB Atlas collections"
    python mongo_setup.py
    exit 1
fi

echo ""

# Step 7: Deploy sink connectors
print_step "Step 7: Deploying MongoDB sink connectors..."

if ./deploy-connector.sh > /dev/null 2>&1; then
    print_success "MongoDB sink connector deployed"
else
    print_error "MongoDB sink connector deployment failed"
    echo "Please run: ./deploy-connector.sh"
fi

echo ""

# Final summary
print_header "Setup Complete!"

echo -e "${YELLOW}Summary:${NC}"
echo "  Docker services:         Running"
echo "  MongoDB Connector:       Installed"
echo "  Sink Connector:          Deployed"
echo "  Python venv:             Active (venv/bin/activate)"
echo ""

echo -e "${YELLOW}Next steps:${NC}"
echo ""
echo "  1. Run the producer in the activated venv:"
echo "     python producer.py"
echo ""
echo "  2. Monitor data in MongoDB Atlas:"
echo "     use kafka_data"
echo "     db.events_ts_range.estimatedDocumentCount()"
echo "     db.events_ts_hash.estimatedDocumentCount()"
echo ""

echo -e "${YELLOW}Useful commands:${NC}"
echo "  View Kafka topics:        docker exec python-kafka-atlas-sink-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list"
echo "  Check connector status:   curl http://localhost:8083/connectors/atlas-sink-timeseries-range-connector/status"
echo "  Check lag (range):        docker exec python-kafka-atlas-sink-kafka-1 bash -c \"kafka-consumer-groups --bootstrap-server localhost:9092 --group connect-atlas-sink-timeseries-range-connector --describe\""
echo "  Check lag (hash):         docker exec python-kafka-atlas-sink-kafka-1 bash -c \"kafka-consumer-groups --bootstrap-server localhost:9092 --group connect-atlas-sink-timeseries-hash-connector --describe\""
echo "  View Docker logs:         docker-compose logs -f [service]"
echo "  Stop services:            docker-compose down"
echo ""

print_success "All systems ready!"
