#!/usr/bin/env python3
"""
Produce Kafka order events from a CSV seed file.

Usage:
  python3 scripts/produce_kafka_events.py \
    --bootstrap localhost:9092 \
    --topic orders_events \
    --seed-file data/bronze/kafka/stream_kafka_events_100000.csv \
    --limit 1000
"""

from __future__ import annotations

import argparse
import csv
import json
import time

from kafka import KafkaProducer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce Kafka events from seed CSV")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="orders_events")
    parser.add_argument("--seed-file", required=True)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--sleep-ms", type=int, default=0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: str(v).encode("utf-8") if v else None,
    )

    sent = 0
    with open(args.seed_file, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if sent >= args.limit:
                break
            value = row.get("value")
            key = row.get("key")
            if not value:
                continue
            try:
                payload = json.loads(value)
            except json.JSONDecodeError:
                continue
            producer.send(args.topic, key=key, value=payload)
            sent += 1
            if args.sleep_ms:
                time.sleep(args.sleep_ms / 1000.0)

    producer.flush()
    print(f"Produced {sent} events to {args.topic}")


if __name__ == "__main__":
    main()
