#!/bin/bash

# Performance test script for comparing MongoDB time series range-sharded vs hash-sharded insert performance
# This script pauses connectors, fills Kafka queue, then runs connectors sequentially to measure performance

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

KAFKA_CONNECT_HOST=${KAFKA_CONNECT_HOST:-localhost}
KAFKA_CONNECT_PORT=${KAFKA_CONNECT_PORT:-8083}
KAFKA_TOPIC=${KAFKA_TOPIC:-events}
KAFKA_CONTAINER="python-kafka-atlas-sink-kafka-1"

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

# Function to pause a connector
pause_connector() {
    local CONNECTOR_NAME=$1
    echo -e "${YELLOW}Pausing connector: $CONNECTOR_NAME${NC}"
    curl -s -X PUT "http://$KAFKA_CONNECT_HOST:$KAFKA_CONNECT_PORT/connectors/$CONNECTOR_NAME/pause" > /dev/null
    sleep 2
    print_success "Connector $CONNECTOR_NAME paused"
}

# Function to resume a connector
resume_connector() {
    local CONNECTOR_NAME=$1
    echo -e "${YELLOW}Resuming connector: $CONNECTOR_NAME${NC}"
    curl -s -X PUT "http://$KAFKA_CONNECT_HOST:$KAFKA_CONNECT_PORT/connectors/$CONNECTOR_NAME/resume" > /dev/null
    sleep 2
    print_success "Connector $CONNECTOR_NAME resumed"
}

# Function to get consumer group lag
get_lag() {
    local CONSUMER_GROUP=$1
    # Get lag, handling '-' (uninitialized offset) by calculating from log-end-offset
    docker exec $KAFKA_CONTAINER kafka-consumer-groups \
        --bootstrap-server localhost:9092 \
        --group "$CONSUMER_GROUP" \
        --describe 2>/dev/null | grep "$KAFKA_TOPIC" | awk '
        {
            current = $4;
            logend = $5;
            lag = $6;
            
            # If LAG is "-" (uninitialized), calculate it
            if (lag == "-") {
                if (current == "-") {
                    # No offset committed yet, lag = all messages
                    lag = logend;
                } else {
                    # Calculate lag manually
                    lag = logend - current;
                }
            }
            sum += lag;
        }
        END {print sum}'
}

# Function to wait for lag to reach zero
wait_for_completion() {
    local CONSUMER_GROUP=$1
    local CONNECTOR_NAME=$2
    local START_TIME=$(date +%s)
    local MAX_WAIT=3600  # 1 hour max wait time
    
    echo -e "${YELLOW}Waiting for $CONNECTOR_NAME to process all messages...${NC}" >&2
    
    # Wait for consumer group offsets to appear and lag to become measurable.
    # A zero/empty lag immediately after resume can be transient while Connect tasks start.
    echo "Waiting for consumer group to initialize..." >&2
    sleep 30

    local INITIAL_LAG=""
    local INIT_WAIT=0
    local INIT_TIMEOUT=300
    while [ $INIT_WAIT -lt $INIT_TIMEOUT ]; do
        INITIAL_LAG=$(get_lag "$CONSUMER_GROUP")

        if [ -n "$INITIAL_LAG" ] && [ "$INITIAL_LAG" -gt 0 ] 2>/dev/null; then
            break
        fi

        echo "Consumer group not ready or lag is 0 yet... (waited: ${INIT_WAIT}s/${INIT_TIMEOUT}s)" >&2
        sleep 15
        INIT_WAIT=$((INIT_WAIT + 15))
    done

    if [ -z "$INITIAL_LAG" ] || [ "$INITIAL_LAG" = "0" ]; then
        echo -e "${RED}✗ Initial lag stayed at 0 after ${INIT_TIMEOUT}s. Connector may not be consuming backlog.${NC}" >&2
        echo -e "${YELLOW}  Hint: check connector status and consider resetting connector offsets before rerun.${NC}" >&2
        return 1
    fi
    echo "Initial lag: $INITIAL_LAG messages" >&2
    
    while true; do
        LAG=$(get_lag "$CONSUMER_GROUP")
        
        # Handle empty lag (consumer group might not exist yet or no data)
        if [ -z "$LAG" ]; then
            LAG=0
        fi
        
        # Check if lag is zero
        if [ "$LAG" = "0" ]; then
            # Double check - wait a bit and verify it's still zero
            sleep 5
            LAG=$(get_lag "$CONSUMER_GROUP")
            if [ -z "$LAG" ] || [ "$LAG" = "0" ]; then
                break
            fi
        fi
        
        # Check timeout
        local CURRENT_TIME=$(date +%s)
        local ELAPSED=$((CURRENT_TIME - START_TIME))
        if [ $ELAPSED -gt $MAX_WAIT ]; then
            echo -e "${RED}✗ Timeout reached after $MAX_WAIT seconds${NC}" >&2
            echo "$ELAPSED"
            return 1
        fi
        
        echo -e "Lag: $LAG messages remaining... (elapsed: ${ELAPSED}s)" >&2
        sleep 30
    done
    
    local END_TIME=$(date +%s)
    local DURATION=$((END_TIME - START_TIME))
    
    echo -e "${GREEN}✓ $CONNECTOR_NAME completed in $DURATION seconds${NC}" >&2
    echo "$DURATION"
}

# Function to get message count in topic
get_topic_message_count() {
    docker exec $KAFKA_CONTAINER kafka-get-offsets \
        --bootstrap-server localhost:9092 \
        --topic "$KAFKA_TOPIC" \
        --time -1 2>/dev/null | awk -F: '{sum+=$3} END {print sum}'
}

print_header "MongoDB Sink Performance Test"

# Step 1: Pause both connectors
print_step "Step 1: Pausing both connectors..."
pause_connector "atlas-sink-timeseries-range-connector"
pause_connector "atlas-sink-timeseries-hash-connector"
echo ""

# Step 2: Clear Kafka topic (optional, uncomment if you want to start fresh)
print_step "Step 2: Clearing and recreating Kafka topic '$KAFKA_TOPIC'..."
docker exec $KAFKA_CONTAINER kafka-topics --bootstrap-server localhost:9092 --topic "$KAFKA_TOPIC" --delete
sleep 5
# Recreate topic with 10 partitions
docker exec $KAFKA_CONTAINER kafka-topics --bootstrap-server localhost:9092 --topic "$KAFKA_TOPIC" --create --partitions 10 --replication-factor 1
sleep 2
INITIAL_MESSAGES=$(get_topic_message_count)
echo "Messages in topic: $INITIAL_MESSAGES"
echo ""

# Step 3: Clear MongoDB collections
print_step "Step 3: Clearing MongoDB collections..."
source venv/bin/activate
python mongo_setup.py
echo ""

# Step 4: Run producer
print_step "Step 4: Running producer to fill Kafka queue..."
echo "Starting producer (this will run based on PRODUCER_TOTAL_MESSAGES in .env)..."
python producer.py
print_success "Producer completed"
echo ""

# Step 5: Check new message count
print_step "Step 5: Verifying messages in Kafka..."
TOTAL_MESSAGES=$(get_topic_message_count)
NEW_MESSAGES=$((TOTAL_MESSAGES - INITIAL_MESSAGES))
print_success "Total messages to process: $NEW_MESSAGES"
echo ""

# Step 6: Test Time Series Range Connector
print_step "Step 6: Testing Time Series Range Connector (atlas-sink-timeseries-range-connector)..."
resume_connector "atlas-sink-timeseries-range-connector"
RANGE_DURATION=$(wait_for_completion "connect-atlas-sink-timeseries-range-connector" "atlas-sink-timeseries-range-connector")
pause_connector "atlas-sink-timeseries-range-connector"
echo ""

echo " Waiting for 5 minutes before starting the next connector..." >&2
sleep 300

# Step 7: Test Time Series Hash Connector
print_step "Step 7: Testing Time Series Hash Connector (atlas-sink-timeseries-hash-connector)..."
resume_connector "atlas-sink-timeseries-hash-connector"
HASH_DURATION=$(wait_for_completion "connect-atlas-sink-timeseries-hash-connector" "atlas-sink-timeseries-hash-connector")
pause_connector "atlas-sink-timeseries-hash-connector"
echo ""

# Final Results
print_header "Performance Test Results"
echo -e "${BLUE}Messages processed: ${GREEN}$NEW_MESSAGES${NC}"
echo ""
echo -e "${YELLOW}Time Series Range Connector (events_ts_range):${NC}"
echo -e "  Duration: ${GREEN}${RANGE_DURATION} seconds${NC}"
if [ "$NEW_MESSAGES" -gt 0 ] && [ "$RANGE_DURATION" -gt 0 ] 2>/dev/null; then
    THROUGHPUT=$(echo "scale=2; $NEW_MESSAGES / $RANGE_DURATION" | bc 2>/dev/null || echo "N/A")
    echo -e "  Throughput: ${GREEN}${THROUGHPUT} messages/second${NC}"
fi
echo ""
echo -e "${YELLOW}Time Series Hash Connector (events_ts_hash):${NC}"
echo -e "  Duration: ${GREEN}${HASH_DURATION} seconds${NC}"
if [ "$NEW_MESSAGES" -gt 0 ] && [ "$HASH_DURATION" -gt 0 ] 2>/dev/null; then
    THROUGHPUT=$(echo "scale=2; $NEW_MESSAGES / $HASH_DURATION" | bc 2>/dev/null || echo "N/A")
    echo -e "  Throughput: ${GREEN}${THROUGHPUT} messages/second${NC}"
fi
echo ""

# Calculate performance difference
if [ "$RANGE_DURATION" -lt "$HASH_DURATION" ] 2>/dev/null; then
    IMPROVEMENT=$(echo "scale=2; ($HASH_DURATION - $RANGE_DURATION) / $HASH_DURATION * 100" | bc 2>/dev/null || echo "N/A")
    echo -e "${GREEN}Range sharding is ${IMPROVEMENT}% faster than hash sharding${NC}"
elif [ "$RANGE_DURATION" -gt "$HASH_DURATION" ] 2>/dev/null; then
    IMPROVEMENT=$(echo "scale=2; ($RANGE_DURATION - $HASH_DURATION) / $RANGE_DURATION * 100" | bc 2>/dev/null || echo "N/A")
    echo -e "${GREEN}Hash sharding is ${IMPROVEMENT}% faster than range sharding${NC}"
else
    echo -e "${YELLOW}Both connectors performed equally${NC}"
fi
echo ""

# Step 8: Resume both connectors
print_step "Resuming both connectors for normal operation..."
resume_connector "atlas-sink-timeseries-range-connector"
resume_connector "atlas-sink-timeseries-hash-connector"
echo ""

print_success "Performance test complete!"
