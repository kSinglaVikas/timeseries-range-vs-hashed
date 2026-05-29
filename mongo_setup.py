#!/usr/bin/env python3
"""
    MongoDB Atlas Setup Script
"""
import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

uri = os.environ.get('ATLAS_CONNECTION_STRING')
db_name = os.environ.get('ATLAS_DATABASE', 'kafka_data')
ts_range_collection = os.environ.get('ATLAS_TIME_SERIES_RANGE_COLLECTION', 'events_ts_range')
ts_hash_collection = os.environ.get('ATLAS_TIME_SERIES_HASH_COLLECTION', 'events_ts_hash')

if not uri:
    print('ATLAS_CONNECTION_STRING not set. Skipping MongoDB setup.')
    exit(0)

client = MongoClient(uri)
client.drop_database(db_name)
print(f"Dropped database: {db_name}")

# Enable Sharding and create time series collections with appropriate shard keys


def cluster_supports_sharding(mongo_client: MongoClient) -> bool:
    """Return True when connected through mongos (required for sharding commands)."""
    try:
        hello = mongo_client.admin.command('hello')
        return hello.get('msg') == 'isdbgrid'
    except Exception:
        return False

db = client[db_name]
for collection_name in (ts_range_collection, ts_hash_collection):
    db.create_collection(
        collection_name,
        timeseries={
            "timeField": "timestamp",
            "metaField": "user_id",
        }
    )
    print(
        f"Created time series collection: {collection_name} "
        "with timeField='timestamp' and metaField='user_id'"
    )

if cluster_supports_sharding(client):
    try:
        client.admin.command({'enableSharding': db_name})
    except Exception:
        # Database may already be sharded, which is fine.
        pass

    range_ns = f"{db_name}.{ts_range_collection}"
    hash_ns = f"{db_name}.{ts_hash_collection}"

    client.admin.command({
        'shardCollection': range_ns,
        'key': {'user_id': 1},
    })
    print(f"Applied range shard key on {range_ns}: {{'user_id': 1}}")

    client.admin.command({
        'shardCollection': hash_ns,
        'key': {'user_id': 'hashed'},
    })
    print(f"Applied hash shard key on {hash_ns}: {{'user_id': 'hashed'}}")
else:
    print(
        'Cluster does not support sharding commands (not connected through mongos). '
        'Skipping enableSharding and shardCollection steps.'
    )
