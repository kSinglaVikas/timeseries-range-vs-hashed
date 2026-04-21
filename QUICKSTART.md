# Quick Start Guide

Get up and running in 3 steps.

## Prerequisites (5 minutes)

### 1. MongoDB Atlas Setup
- Create a [MongoDB Atlas cluster](https://www.mongodb.com/cloud/atlas)
- Create a database user: **Database Access** → **Add Database User**
- Configure network access: **Network Access** → **Add IP Address** (use `0.0.0.0/0` for testing)
- Copy the connection string: **Databases** → **Connect** → **Drivers**

### 2. Local Prerequisites
- Docker & Docker Compose
- Python 3.8+

## Quick Setup (2 commands)

### Step 1: Configure Credentials
```bash
cp .env.example .env
# Edit .env and add your MongoDB Atlas connection string
nano .env
```

**Required fields to update:**
```env
ATLAS_CONNECTION_STRING=mongodb+srv://user:password@cluster.mongodb.net/
ATLAS_DATABASE=kafka_data
ATLAS_TIME_SERIES_RANGE_COLLECTION=events_ts_range
ATLAS_TIME_SERIES_HASH_COLLECTION=events_ts_hash
MAX_TASKS=5
```

### Step 2: Run Automated Setup
```bash
./setup.sh
```

This handles everything:
- ✓ Validates prerequisites
- ✓ Starts Docker services (Kafka, Zookeeper, Kafka Connect)
- ✓ Installs MongoDB Connector plugin
- ✓ Creates MongoDB collections & sharding keys
- ✓ Deploys both sink connectors
- ✓ Sets up Python virtual environment

### Step 3: Run Producer
```bash
python producer.py
```

You should see batches of events being sent to Kafka:
```
Batch 1: Sent 100 events. Total: 100
Batch 2: Sent 100 events. Total: 200
...
```

---

## Verify Data in MongoDB Atlas

### Via MongoDB Atlas Console
1. Go to **Collections**
2. Select `kafka_data` database
3. View real-time data in:
  - `events_ts_range` (time series with range sharding)
  - `events_ts_hash` (time series with hash sharding)

### Via MongoDB Shell
```javascript
use kafka_data

// Check counts
db.events_ts_range.estimatedDocumentCount()
db.events_ts_hash.estimatedDocumentCount()

// View sample documents
db.events_ts_range.findOne()
db.events_ts_hash.findOne()
```

---

## Monitor Connectors

### Check Connector Status
```bash
curl http://localhost:8083/connectors/atlas-sink-timeseries-range-connector/status | jq
curl http://localhost:8083/connectors/atlas-sink-timeseries-hash-connector/status | jq
```

### Check Lag (Processed Messages)
```bash
# Range connector
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group connect-atlas-sink-timeseries-range-connector --describe

# Hash connector
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group connect-atlas-sink-timeseries-hash-connector --describe
```

### View Logs
```bash
docker-compose logs -f kafka-connect
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| "Connection refused" | Check `.env` values; verify MongoDB Atlas IP whitelist includes your IP |
| "Connector fails to deploy" | Run `docker-compose logs kafka-connect` to see error details |
| "No data in MongoDB" | Check if producer is running; verify Kafka topic has messages |

---

## Cleanup

```bash
# Stop containers
docker-compose down

# Stop producer (Ctrl+C)

# Deactivate Python environment
deactivate
```

---

## Next Steps

- **Custom events**: Edit `producer.py` to generate your own data
- **Detailed setup**: See [README.md](README.md) for comprehensive guide
- **Manual steps**: See [README.md](README.md) section "Manual Setup (Step-by-Step)"
- **Sharding details**: See [README.md](README.md) section "Sharding Strategy"

---

For detailed information, architecture overview, and advanced configuration, see [README.md](README.md).
