# Python to Kafka to MongoDB Atlas Sink

This project demonstrates a data pipeline where a Python producer writes messages to Kafka, and Kafka Connect with the MongoDB connector sinks the data to MongoDB Atlas.

## Architecture

```
Python Producer → Kafka Topic → Kafka Connect → MongoDB Atlas
                                      ├─→ Time Series Range-Sharded Collection (events_ts_range)
                                      └─→ Time Series Hash-Sharded Collection (events_ts_hash)
```

This pipeline deploys two MongoDB sink connectors:
- **atlas-sink-timeseries-range-connector**: Writes to time series collection sharded by range on `user_id` and `timestamp`
- **atlas-sink-timeseries-hash-connector**: Writes to time series collection sharded by hashed `user_id` plus `timestamp`

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- MongoDB Atlas cluster (with appropriate credentials)
- Apache Kafka (managed by Docker Compose)

## Setup

### ⚡ Quick Start

**For the fastest setup, see [QUICKSTART.md](QUICKSTART.md)** — 3 simple steps to get running in minutes.

### Detailed Setup (Step-by-Step)

If you prefer to set up manually or troubleshoot issues, follow these steps in order:

### 1. Configure MongoDB Atlas

1. Create a MongoDB Atlas cluster or use an existing one
2. Create a database user with appropriate permissions
3. Obtain the connection string in the format: `mongodb+srv://username:password@cluster.mongodb.net/`
4. Create an IP whitelist entry for your network (or 0.0.0.0/0 for testing)

### 2. Environment Variables

Copy the example environment file and update with your Atlas credentials:

```bash
cp .env.example .env
```

Edit `.env` and set:
- `ATLAS_CONNECTION_STRING` - Your MongoDB Atlas connection string
- `ATLAS_DATABASE` - Target database name (default: `kafka_data`)
- `ATLAS_TIME_SERIES_RANGE_COLLECTION` - Range-sharded time series collection name (default: `events_ts_range`)
- `ATLAS_TIME_SERIES_HASH_COLLECTION` - Hash-sharded time series collection name (default: `events_ts_hash`)
- `MAX_TASKS` - Number of parallel connector tasks (default: `5`)

### 3. Install Python Dependencies

Set up a Python virtual environment and install dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Start Infrastructure

```bash
docker-compose up -d
```

This starts:
- Zookeeper (port 2181)
- Kafka broker (port 9092)
- MongoDB Kafka Connector service (port 8083)

### 5. Install MongoDB Connector

```bash
./install-connector.sh
```

This downloads and installs the MongoDB connector plugin to Kafka Connect.

### 6. Prepare MongoDB Collections

This step drops the database (if exists), creates the collections, and adds indexes:

```bash
python mongo_setup.py
```

This creates:
- **events_ts_range**: Time series collection sharded by range key `{ user_id: 1, timestamp: 1 }`
- **events_ts_hash**: Time series collection sharded by key `{ user_id: "hashed", timestamp: 1 }`

### 7. Deploy the MongoDB Sink Connectors

```bash
./deploy-connector.sh
```

This deploys both connectors:
- **atlas-sink-timeseries-range-connector**: Range-sharded time series collection sink
- **atlas-sink-timeseries-hash-connector**: Hash-sharded time series collection sink

### 8. Run the Producer

```bash
python producer.py
```

The producer will:
- Generate sample events (user actions, transactions, etc.)
- Write them to the Kafka topic `events`
- Continue running, sending events at regular intervals

## Verifying the Pipeline

### Check Kafka Messages

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic events \
  --from-beginning
```

### Check Connector Status

```bash
curl http://localhost:8083/connectors/atlas-sink-timeseries-range-connector/status | jq
curl http://localhost:8083/connectors/atlas-sink-timeseries-hash-connector/status | jq
```

### Query MongoDB Atlas

Connect to your Atlas cluster and query the database:

```javascript
use kafka_data

// Check time series collection
db.events_ts_range.estimatedDocumentCount()
db.events_ts_range.find({}).limit(5)

// Check hash-sharded time series collection
db.events_ts_hash.estimatedDocumentCount()
db.events_ts_hash.find({}).limit(5)
```

### Run Performance Comparison Test

Use the performance test script to compare ingestion speed of the two sink connectors under the same queued workload.

```bash
./performance-test.sh
```

The script pauses both connectors, fills the Kafka topic, runs each connector sequentially, and prints total duration and throughput for each.

## Project Structure

```
python-kafka-atlas-sink/
├── README.md                           # This file
├── QUICKSTART.md                       # Quick start guide
├── requirements.txt                    # Python dependencies
├── .env.example                        # Example environment variables
├── docker-compose.yml                  # Docker Compose configuration
├── atlas-sink-timeseries-range-connector.json # Range-sharded time series sink connector config
├── atlas-sink-timeseries-hash-connector.json  # Hash-sharded time series sink connector config
├── install-connector.sh                # Script to install MongoDB connector
├── deploy-connector.sh                 # Script to deploy both sink connectors
├── setup.sh                            # Complete automated setup script
├── setup-venv.sh                       # Script to setup Python virtual environment
├── mongo_setup.py                      # MongoDB collection setup script
├── producer.py                         # Python Kafka producer
├── utils/
│   └── __init__.py                     # Data generation utilities
└── configs/
    └── connector-base.json             # Base connector configuration template
```

## Configuration Files

### atlas-sink-timeseries-range-connector.json (Time Series Range-Sharded)

The Kafka Connect sink connector for time series collection:

- **connection.uri**: MongoDB Atlas connection string
- **database**: Target database
- **collection**: Range-sharded time series collection (ATLAS_TIME_SERIES_RANGE_COLLECTION)
- **topics**: Kafka topics to sink
- **tasks.max**: Number of parallel tasks (MAX_TASKS)
- **timeseries.timefield**: Field containing timestamp
- **timeseries.timefield.auto.convert**: Auto-convert timestamp format
- **write.method**: insert (for time series)

### atlas-sink-timeseries-hash-connector.json (Time Series Hash-Sharded)

The Kafka Connect sink connector for the hash-sharded time series collection:

- **connection.uri**: MongoDB Atlas connection string
- **database**: Target database
- **collection**: Hash-sharded time series collection (ATLAS_TIME_SERIES_HASH_COLLECTION)
- **topics**: Kafka topics to sink
- **tasks.max**: Number of parallel tasks (MAX_TASKS)
- **timeseries.metadata.field**: `user_id`
- **write.method**: `insert`

### Sharding Strategy

`mongo_setup.py` creates both time series collections and applies:

- Range sharding key: `{ user_id: 1, timestamp: 1 }`
- Hash sharding key: `{ user_id: "hashed", timestamp: 1 }`

## Producer Configuration

The Python producer generates events with the following fields:

- `timestamp` - Event timestamp
- `event_type` - Type of event (purchase, login, signup, etc.)
- `user_id` - User identifier
- `data` - Event-specific data payload

## Troubleshooting

### Connector fails to deploy

1. Check connector logs:
   ```bash
  curl http://localhost:8083/connectors/atlas-sink-timeseries-range-connector/status
   ```

2. Verify MongoDB Atlas credentials in `.env`

3. Ensure Atlas cluster has correct firewall rules

### No data appearing in Atlas

1. Verify Kafka topic has messages:
   ```bash
   docker exec kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --group connect-cluster \
     --describe
   ```

2. Check connector logs in Kafka Connect UI or via API

3. Verify database and collection names match configuration

### Connection timeout errors

- Check MongoDB Atlas network access settings
- Verify connection string is correct
- Ensure firewall allows outbound connections to Atlas

## Production Considerations

- Use secrets management for credentials (HashiCorp Vault, AWS Secrets Manager, etc.)
- Configure proper error handling and dead letter topics
- Implement monitoring and alerting for the pipeline
- Use connection pooling and tuning parameters
- Implement schema validation (JSON Schema or Avro)
- Set up proper authentication and encryption (TLS)
- Configure appropriate retention policies

## References

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [MongoDB Kafka Connector](https://www.mongodb.com/docs/kafka-connector/current/)
- [MongoDB Atlas](https://docs.atlas.mongodb.com/)
- [Kafka Connect](https://kafka.apache.org/documentation/#connect)
