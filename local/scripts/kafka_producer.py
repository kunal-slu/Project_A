#!/usr/bin/env python3
"""
Kafka Producer for Local Development

Reads CSV seed file and produces events to local Kafka topic.
This simulates a real-time streaming source for local ETL testing.

Usage:
    python local/scripts/kafka_producer.py --config local/config/local.yaml
"""
import sys
import os
import time
import csv
import json
import argparse
import logging
from pathlib import Path
from typing import Dict, Any

# Add src to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    print("ERROR: kafka-python not installed. Install with: pip install kafka-python")
    sys.exit(1)

from project_a.config_loader import load_config_resolved

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def parse_kafka_csv_row(row: Dict[str, str]) -> Dict[str, Any]:
    """
    Parse a CSV row from kafka seed file into event payload.
    
    The CSV has columns: event_id, topic, partition, offset, timestamp, key, value, headers
    We extract the 'value' JSON and send it as the event payload.
    """
    try:
        # The 'value' column contains JSON string
        value_json = json.loads(row['value'])
        return value_json
    except (json.JSONDecodeError, KeyError) as e:
        logger.warning(f"Failed to parse row value: {e}")
        return None


def produce_events(
    seed_file: str,
    bootstrap_servers: str,
    topic: str,
    delay_seconds: float = 1.0,
    max_events: int = None
) -> None:
    """
    Read CSV seed file and produce events to Kafka.
    
    Args:
        seed_file: Path to CSV seed file
        bootstrap_servers: Kafka bootstrap servers (e.g., 'localhost:9092')
        topic: Kafka topic name
        delay_seconds: Delay between events (simulates streaming)
        max_events: Maximum number of events to produce (None = all)
    """
    logger.info(f"Starting Kafka producer")
    logger.info(f"  Seed file: {seed_file}")
    logger.info(f"  Bootstrap servers: {bootstrap_servers}")
    logger.info(f"  Topic: {topic}")
    logger.info(f"  Delay: {delay_seconds}s per event")
    
    # Create producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        logger.info("✅ Kafka producer created")
    except Exception as e:
        logger.error(f"❌ Failed to create Kafka producer: {e}")
        logger.error("   Make sure Kafka is running: docker-compose up kafka")
        raise
    
    # Read CSV and produce events
    seed_path = Path(seed_file)
    if not seed_path.exists():
        raise FileNotFoundError(f"Seed file not found: {seed_file}")
    
    events_sent = 0
    events_failed = 0
    
    try:
        with open(seed_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row_num, row in enumerate(reader, 1):
                if max_events and events_sent >= max_events:
                    logger.info(f"Reached max_events limit ({max_events})")
                    break
                
                # Parse event payload
                event_payload = parse_kafka_csv_row(row)
                if not event_payload:
                    events_failed += 1
                    continue
                
                # Get key if available
                event_key = row.get('key', None)
                
                try:
                    # Send event
                    future = producer.send(
                        topic,
                        value=event_payload,
                        key=event_key
                    )
                    
                    # Wait for send to complete (optional, for reliability)
                    record_metadata = future.get(timeout=10)
                    
                    events_sent += 1
                    if events_sent % 100 == 0:
                        logger.info(f"Sent {events_sent} events...")
                    
                    # Simulate streaming delay
                    if delay_seconds > 0:
                        time.sleep(delay_seconds)
                        
                except KafkaError as e:
                    logger.error(f"Failed to send event {row_num}: {e}")
                    events_failed += 1
                    continue
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        producer.flush()
        producer.close()
        logger.info("=" * 60)
        logger.info(f"✅ Producer finished")
        logger.info(f"   Events sent: {events_sent}")
        logger.info(f"   Events failed: {events_failed}")
        logger.info("=" * 60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Kafka Producer for Local Development")
    parser.add_argument("--config", default="local/config/local.yaml", help="Config file path")
    parser.add_argument("--topic", help="Kafka topic (overrides config)")
    parser.add_argument("--bootstrap-servers", help="Kafka bootstrap servers (overrides config)")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between events (seconds)")
    parser.add_argument("--max-events", type=int, help="Maximum number of events to produce")
    parser.add_argument("--seed-file", help="CSV seed file path (overrides config)")
    args = parser.parse_args()
    
    # Load config
    config_path = PROJECT_ROOT / args.config
    config = load_config_resolved(str(config_path))
    
    # Get Kafka config
    kafka_cfg = config.get("sources", {}).get("kafka", {})
    
    bootstrap_servers = args.bootstrap_servers or kafka_cfg.get("local_bootstrap_servers", "localhost:9092")
    topic = args.topic or kafka_cfg.get("topic", "orders_events")
    seed_file = args.seed_file or kafka_cfg.get("seed_file", "")
    
    if not seed_file:
        # Fallback to kafka_sim seed file
        kafka_sim = config.get("sources", {}).get("kafka_sim", {})
        base_path = kafka_sim.get("base_path", "")
        files = kafka_sim.get("files", {})
        orders_seed = files.get("orders_seed", "")
        if base_path and orders_seed:
            seed_file = f"{base_path}/{orders_seed}"
    
    if not seed_file:
        raise ValueError("Kafka seed file not found in config. Set sources.kafka.seed_file or sources.kafka_sim")
    
    # Resolve relative paths
    if not seed_file.startswith(("s3://", "file://", "/")):
        seed_file = str(PROJECT_ROOT / seed_file)
    
    # Produce events
    produce_events(
        seed_file=seed_file,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        delay_seconds=args.delay,
        max_events=args.max_events
    )


if __name__ == "__main__":
    main()

