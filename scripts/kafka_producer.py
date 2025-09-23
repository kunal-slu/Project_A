#!/usr/bin/env python3
"""
Kafka producer script for testing orders events.
Produces sample order events to the orders_events topic.
"""

import os
import sys
import json
import time
import random
from datetime import datetime, timedelta
from typing import Dict, Any

try:
    from confluent_kafka import Producer
except ImportError:
    print("Error: confluent-kafka not installed. Run: pip install confluent-kafka")
    sys.exit(1)


def create_sample_order(order_id: str) -> Dict[str, Any]:
    """
    Create a sample order event.
    
    Args:
        order_id: Order ID
        
    Returns:
        Order event dictionary
    """
    statuses = ["PLACED", "PAID", "SHIPPED", "CANCELLED"]
    currencies = ["USD", "EUR", "GBP", "CAD", "AUD"]
    
    return {
        "order_id": order_id,
        "customer_id": f"C{random.randint(10000, 99999)}",
        "event_ts": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat(),
        "currency": random.choice(currencies),
        "amount": round(random.uniform(10.0, 1000.0), 2),
        "status": random.choice(statuses)
    }


def produce_orders(
    bootstrap_servers: str,
    api_key: str,
    api_secret: str,
    topic: str,
    num_orders: int = 50
) -> None:
    """
    Produce sample order events to Kafka.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        api_key: Confluent API key
        api_secret: Confluent API secret
        topic: Kafka topic name
        num_orders: Number of orders to produce
    """
    # Configure producer
    producer_config = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': api_key,
        'sasl.password': api_secret,
    }
    
    producer = Producer(producer_config)
    
    print(f"Producing {num_orders} order events to topic '{topic}'...")
    
    try:
        for i in range(num_orders):
            order_id = f"O{random.randint(100000, 999999)}"
            order_event = create_sample_order(order_id)
            
            # Serialize to JSON
            message_value = json.dumps(order_event)
            
            # Produce message
            producer.produce(
                topic=topic,
                value=message_value,
                key=order_id,
                callback=lambda err, msg: print(f"Message delivered: {msg.key().decode('utf-8')}") if not err else print(f"Message failed: {err}")
            )
            
            # Flush every 10 messages
            if (i + 1) % 10 == 0:
                producer.flush()
                print(f"Produced {i + 1} messages...")
            
            # Small delay between messages
            time.sleep(0.1)
        
        # Flush remaining messages
        producer.flush()
        print(f"✅ Successfully produced {num_orders} order events!")
        
    except Exception as e:
        print(f"❌ Error producing messages: {e}")
        sys.exit(1)
    finally:
        producer.close()


def main():
    """Main function to run the Kafka producer."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Produce sample order events to Kafka")
    parser.add_argument("--bootstrap-servers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--api-key", required=True, help="Confluent API key")
    parser.add_argument("--api-secret", required=True, help="Confluent API secret")
    parser.add_argument("--topic", default="orders_events", help="Kafka topic name")
    parser.add_argument("--num-orders", type=int, default=50, help="Number of orders to produce")
    args = parser.parse_args()
    
    # Get configuration from environment variables if not provided
    bootstrap_servers = args.bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP")
    api_key = args.api_key or os.getenv("KAFKA_API_KEY")
    api_secret = args.api_secret or os.getenv("KAFKA_API_SECRET")
    topic = args.topic or os.getenv("KAFKA_TOPIC", "orders_events")
    
    if not all([bootstrap_servers, api_key, api_secret]):
        print("Error: Missing required configuration. Provide via arguments or environment variables:")
        print("  KAFKA_BOOTSTRAP, KAFKA_API_KEY, KAFKA_API_SECRET")
        sys.exit(1)
    
    produce_orders(bootstrap_servers, api_key, api_secret, topic, args.num_orders)


if __name__ == "__main__":
    main()