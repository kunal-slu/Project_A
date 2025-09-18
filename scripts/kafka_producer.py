#!/usr/bin/env python3
"""
Kafka Producer for Streaming Order Data
Generates realistic e-commerce order events for testing streaming pipelines.
"""

import json
import random
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any
import argparse
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OrderEventProducer:
    """Produces realistic order events to Kafka topic."""

    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas
            retries=3,
            batch_size=16384,
            linger_ms=10
        )

        # Sample data for realistic orders
        self.customer_ids = [f"CUST_{i:06d}" for i in range(1, 1001)]
        self.product_ids = [f"PROD_{i:06d}" for i in range(1, 501)]
        self.cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
                      "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
        self.states = ["NY", "CA", "IL", "TX", "AZ", "PA", "FL", "OH", "GA", "NC"]
        self.countries = ["USA", "Canada", "UK", "Germany", "France"]
        self.currencies = ["USD", "EUR", "GBP", "CAD"]
        self.payment_methods = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]

    def generate_order_event(self) -> Dict[str, Any]:
        """Generate a realistic order event."""
        now = datetime.now()

        # Random order date within last 30 days
        order_date = now - timedelta(days=random.randint(0, 30))

        # Random customer and product
        customer_id = random.choice(self.customer_ids)
        product_id = random.choice(self.product_ids)

        # Random quantities and amounts
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(10.0, 500.0), 2)
        total_amount = round(quantity * unit_price, 2)

        # Random shipping address
        shipping_city = random.choice(self.cities)
        shipping_state = random.choice(self.states)
        shipping_country = random.choice(self.countries)

        # Random payment info
        payment_method = random.choice(self.payment_methods)
        currency = random.choice(self.currencies)

        # Convert currency if not USD
        if currency != "USD":
            exchange_rates = {"EUR": 1.18, "GBP": 1.38, "CAD": 0.79}
            total_amount = round(total_amount * exchange_rates.get(currency, 1.0), 2)

        order_event = {
            "order_id": f"ORD_{int(time.time())}_{random.randint(1000, 9999)}",
            "customer_id": customer_id,
            "product_id": product_id,
            "order_date": order_date.isoformat(),
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "currency": currency,
            "shipping_address": f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Pine', 'Elm'])} St",
            "shipping_city": shipping_city,
            "shipping_state": shipping_state,
            "shipping_country": shipping_country,
            "payment": {
                "method": payment_method,
                "status": "completed",
                "transaction_id": f"TXN_{random.randint(100000, 999999)}"
            },
            "metadata": {
                "source": "kafka-stream",
                "timestamp": now.isoformat(),
                "version": "1.0"
            }
        }

        return order_event

    def produce_orders(self, num_orders: int, delay_seconds: float = 1.0):
        """Produce specified number of order events."""
        logger.info(f"Starting to produce {num_orders} order events to topic '{self.topic}'")

        successful = 0
        failed = 0

        for i in range(num_orders):
            try:
                order_event = self.generate_order_event()

                # Use order_id as key for partitioning
                future = self.producer.send(
                    self.topic,
                    key=order_event["order_id"],
                    value=order_event
                )

                # Wait for the send to complete
                record_metadata = future.get(timeout=10)

                successful += 1
                logger.info(f"✓ Order {i+1}/{num_orders} sent: {order_event['order_id']} "
                          f"to partition {record_metadata.partition} "
                          f"at offset {record_metadata.offset}")

                # Add delay between messages
                if delay_seconds > 0:
                    time.sleep(delay_seconds)

            except KafkaError as e:
                failed += 1
                logger.error(f"✗ Failed to send order {i+1}: {e}")
            except Exception as e:
                failed += 1
                logger.error(f"✗ Unexpected error sending order {i+1}: {e}")

        # Flush any remaining messages
        self.producer.flush()

        logger.info(f"Production complete: {successful} successful, {failed} failed")
        return successful, failed

    def produce_continuous(self, delay_seconds: float = 2.0, max_orders: int = None):
        """Continuously produce order events."""
        logger.info(f"Starting continuous production to topic '{self.topic}'")
        logger.info("Press Ctrl+C to stop")

        order_count = 0

        try:
            while True:
                if max_orders and order_count >= max_orders:
                    logger.info(f"Reached maximum orders limit: {max_orders}")
                    break

                order_event = self.generate_order_event()

                future = self.producer.send(
                    self.topic,
                    key=order_event["order_id"],
                    value=order_event
                )

                record_metadata = future.get(timeout=10)
                order_count += 1

                logger.info(f"✓ Order {order_count} sent: {order_event['order_id']} "
                          f"to partition {record_metadata.partition} "
                          f"at offset {record_metadata.offset}")

                time.sleep(delay_seconds)

        except KeyboardInterrupt:
            logger.info("Stopping continuous production...")
        except Exception as e:
            logger.error(f"Error in continuous production: {e}")
        finally:
            self.producer.flush()
            logger.info(f"Continuous production stopped. Total orders sent: {order_count}")

    def close(self):
        """Close the producer."""
        self.producer.close()
        logger.info("Kafka producer closed")

def main():
    parser = argparse.ArgumentParser(description='Kafka Order Event Producer')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                       help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--topic', default='orders-stream',
                       help='Kafka topic (default: orders-stream)')
    parser.add_argument('--num-orders', type=int, default=100,
                       help='Number of orders to produce (default: 100)')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='Delay between orders in seconds (default: 1.0)')
    parser.add_argument('--continuous', action='store_true',
                       help='Continuously produce orders')
    parser.add_argument('--max-orders', type=int,
                       help='Maximum orders for continuous mode')

    args = parser.parse_args()

    try:
        producer = OrderEventProducer(args.bootstrap_servers, args.topic)

        if args.continuous:
            producer.produce_continuous(args.delay, args.max_orders)
        else:
            producer.produce_orders(args.num_orders, args.delay)

    except Exception as e:
        logger.error(f"Failed to start producer: {e}")
        return 1
    finally:
        if 'producer' in locals():
            producer.close()

    return 0

if __name__ == "__main__":
    exit(main())
