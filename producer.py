#!/usr/bin/env python3
"""
Kafka Producer for MongoDB Atlas Sink

This producer generates sample events and writes them to Kafka using parallel threads.
The events are consumed by Kafka Connect and sinked to MongoDB Atlas.
"""

import json
import logging
import os
import sys
import time
import threading
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, Any
from queue import Queue, Empty

from dotenv import load_dotenv
from confluent_kafka import Producer, KafkaException

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'events')
PRODUCER_BATCH_SIZE = int(os.getenv('PRODUCER_BATCH_SIZE', '100'))
PRODUCER_INTERVAL_SECONDS = int(os.getenv('PRODUCER_INTERVAL_SECONDS', '2'))
PRODUCER_TOTAL_MESSAGES = int(os.getenv('PRODUCER_TOTAL_MESSAGES', '1000000'))
PRODUCER_PARTITIONS = int(os.getenv('PRODUCER_PARTITIONS', '1'))  # Number of parallel producer threads


def generate_user_event() -> Dict[str, Any]:
    """Generate a random user event using fast random generation instead of Faker."""
    event_types = ['login', 'logout', 'purchase', 'view_product', 'add_to_cart', 'checkout', 'signup']
    
    timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    user_id = random.randint(1, 25000)
    event_type = random.choice(event_types)
    
    # Much faster than Faker - use simple random generation
    event = {
        'timestamp': timestamp_ms,
        'event_id': f"{user_id}-{timestamp_ms}-{random.randint(0, 9999)}",
        'event_type': event_type,
        'user_id': user_id,
        'user_email': f"user{user_id}@example.com",
        'session_id': f"session-{random.getrandbits(64)}",
        'ip_address': f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}",
        'user_agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
        'data': {
            'page_url': f"https://example.com/page/{random.randint(1, 10000)}",
            'referrer': f"https://referrer.com/{random.randint(1, 100)}" if random.random() > 0.7 else None,
        }
    }
    
    # Add event-specific data
    if event_type == 'purchase':
        event['data'].update({
            'product_id': f"prod-{random.randint(1000, 99999)}",
            'product_name': random.choice(['Widget', 'Gadget', 'Phone', 'Laptop', 'Monitor']),
            'amount': round(random.uniform(10, 500), 2),
            'currency': 'USD'
        })
    elif event_type == 'view_product':
        event['data'].update({
            'product_id': f"prod-{random.randint(1000, 99999)}",
            'product_name': random.choice(['Widget', 'Gadget', 'Phone', 'Laptop', 'Monitor']),
            'category': random.choice(['Electronics', 'Clothing', 'Books', 'Home', 'Sports'])
        })
    elif event_type == 'add_to_cart':
        event['data'].update({
            'product_id': f"prod-{random.randint(1000, 99999)}",
            'quantity': random.randint(1, 10),
            'price': round(random.uniform(5, 200), 2)
        })
    elif event_type == 'checkout':
        event['data'].update({
            'item_count': random.randint(1, 20),
            'total_amount': round(random.uniform(50, 5000), 2),
            'currency': 'USD'
        })
    
    return event


def create_producer() -> Producer:
    """Create and configure Kafka producer."""
    try:
        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': f'python-kafka-producer-{threading.current_thread().ident}',
            'acks': 'all',
            'retries': 3,
            'compression.type': 'gzip',
            'linger.ms': 10,  # Wait 10ms before sending to batch messages
            'batch.size': 32768  # 32KB batch size for better parallelism
        }
        producer = Producer(conf)
        logger.info(f"Kafka producer created in thread {threading.current_thread().name}")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise


def delivery_callback(err, msg):
    """Callback for message delivery reports."""
    if err:
        logger.error(f"Failed to deliver message: {err}")


def produce_batch(thread_id: int, message_queue: Queue, start_offset: int, messages_to_send: int, stop_event: threading.Event):
    """Producer thread function - consumes messages from queue and sends to Kafka."""
    producer = create_producer()
    sent_count = 0
    
    try:
        while not stop_event.is_set():
            try:
                # Block indefinitely for next message (sentinel signals end)
                event = message_queue.get(timeout=None)
                
                if event is None:  # Sentinel value to stop
                    logger.debug(f"Thread {thread_id}: Received sentinel, stopping.")
                    break
                
                value = json.dumps(event).encode('utf-8')
                
                producer.produce(
                    KAFKA_TOPIC,
                    value=value,
                    callback=delivery_callback,
                    key=str(event.get('user_id')).encode('utf-8')
                )
                
                sent_count += 1
                producer.poll(0)  # Non-blocking poll for callbacks
                
                if sent_count % 50000 == 0:
                    logger.info(f"Thread {thread_id}: Sent {sent_count} messages")
                
            except Exception as e:
                logger.error(f"Thread {thread_id} error: {type(e).__name__}: {e}", exc_info=True)
                break
    
    finally:
        producer.flush()
        logger.info(f"Thread {thread_id}: Completed. Sent {sent_count} messages")


def generate_events_parallel(message_queue: Queue, total_messages: int, num_threads: int):
    """Generate events in parallel and put them in queue for parallel producers."""
    messages_per_thread = total_messages // (num_threads * 2)  # Use 2x threads for generation
    num_gen_threads = min(num_threads * 2, 10)  # Cap at 10 generation threads
    
    logger.info(f"Starting parallel event generation with {num_gen_threads} threads")
    
    def gen_worker(thread_id: int, count: int):
        """Worker thread to generate events."""
        gen_count = 0
        for _ in range(count):
            event = generate_user_event()
            message_queue.put(event)
            gen_count += 1
        logger.debug(f"Generator thread {thread_id}: Generated {gen_count} events")
        return gen_count
    
    # Use ThreadPoolExecutor for parallel event generation
    total_generated = 0
    with ThreadPoolExecutor(max_workers=num_gen_threads) as executor:
        futures = []
        for i in range(num_gen_threads):
            msgs = messages_per_thread
            if i == num_gen_threads - 1:  # Last thread gets remainder
                msgs = total_messages - (messages_per_thread * (num_gen_threads - 1))
            future = executor.submit(gen_worker, i, msgs)
            futures.append(future)
        
        # Wait for all generators to complete
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            total_generated += result
            logger.info(f"Generator {i}: Complete. Total generated: {total_generated}")
    
    logger.info(f"Finished generating {total_generated} events")


def produce_events(batch_size: int = 100):
    """Produce events to Kafka using parallel threads."""
    try:
        # Message queue for thread-safe communication (larger buffer for throughput)
        message_queue = Queue(maxsize=50000)
        stop_event = threading.Event()
        
        num_producer_threads = PRODUCER_PARTITIONS
        logger.info(f"Starting production with {num_producer_threads} parallel producer threads")
        
        # Start generation thread
        gen_thread = threading.Thread(
            target=generate_events_parallel,
            args=(message_queue, PRODUCER_TOTAL_MESSAGES, num_producer_threads),
            daemon=False
        )
        gen_thread.start()
        
        # Start producer threads
        producer_threads = []
        messages_per_thread = PRODUCER_TOTAL_MESSAGES // num_producer_threads
        
        for i in range(num_producer_threads):
            thread = threading.Thread(
                target=produce_batch,
                args=(i, message_queue, i * messages_per_thread, messages_per_thread, stop_event),
                name=f"Producer-{i}",
                daemon=False
            )
            thread.start()
            producer_threads.append(thread)
        
        # Wait for generation to complete
        gen_thread.join()
        logger.info("Event generation complete. Sending sentinels to stop producers...")
        
        # Push sentinel values to signal producers to stop
        for _ in range(num_producer_threads):
            message_queue.put(None)
        
        # Wait for all producer threads to complete
        for thread in producer_threads:
            thread.join()
        
        logger.info("All producer threads completed")
    
    except KeyboardInterrupt:
        logger.info("Stopping producers...")
        stop_event.set()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        stop_event.set()
        raise


def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("Kafka Producer for MongoDB Atlas Sink (Parallel)")
    logger.info("=" * 60)
    logger.info(f"Topic: {KAFKA_TOPIC}")
    logger.info(f"Batch size: {PRODUCER_BATCH_SIZE}")
    logger.info(f"Interval: {PRODUCER_INTERVAL_SECONDS}s")
    logger.info(f"Parallel producer threads: {PRODUCER_PARTITIONS}")
    logger.info(f"Total messages: {PRODUCER_TOTAL_MESSAGES}")
    logger.info("=" * 60)
    
    start_time = time.time()
    produce_events(PRODUCER_BATCH_SIZE)
    end_time = time.time()
    
    elapsed = end_time - start_time
    throughput = PRODUCER_TOTAL_MESSAGES / elapsed if elapsed > 0 else 0
    
    logger.info("=" * 60)
    logger.info(f"Production complete in {elapsed:.2f} seconds")
    logger.info(f"Throughput: {throughput:.2f} messages/second")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
