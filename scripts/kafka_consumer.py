#!/usr/bin/env python3
"""
Kafka Consumer for Streaming Order Data
Consumes order events from Kafka topic for testing streaming pipelines.
"""

import json
import logging
import argparse
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OrderEventConsumer:
    """Consumes order events from Kafka topic."""

    def __init__(self, bootstrap_servers: str, topic: str, group_id: str = "test-consumer"):
        self.topic = topic
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',  # Start from beginning
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            consumer_timeout_ms=10000  # 10 second timeout
        )

        self.message_count = 0
        self.total_amount = 0.0

    def consume_messages(self, max_messages: int = None, timeout_seconds: int = 30):
        """Consume messages from Kafka topic."""
        logger.info(f"Starting to consume messages from topic '{self.topic}'")
        logger.info(f"Group ID: {self.consumer.config['group_id']}")

        start_time = time.time()

        try:
            for message in self.consumer:
                try:
                    # Parse the message
                    order_event = message.value
                    order_id = order_event.get('order_id', 'unknown')
                    customer_id = order_event.get('customer_id', 'unknown')
                    total_amount = order_event.get('total_amount', 0.0)
                    currency = order_event.get('currency', 'unknown')

                    self.message_count += 1
                    self.total_amount += total_amount

                    logger.info(f"✓ Message {self.message_count}: Order {order_id} "
                              f"from customer {customer_id} "
                              f"amount {total_amount} {currency} "
                              f"partition {message.partition} "
                              f"offset {message.offset}")

                    # Check if we've reached max messages
                    if max_messages and self.message_count >= max_messages:
                        logger.info(f"Reached maximum message limit: {max_messages}")
                        break

                    # Check timeout
                    if time.time() - start_time > timeout_seconds:
                        logger.info(f"Timeout reached: {timeout_seconds} seconds")
                        break

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
        finally:
            self.consumer.close()

        logger.info(f"Consumption complete: {self.message_count} messages, "
                   f"total amount: {self.total_amount:.2f}")

        return self.message_count, self.total_amount

    def consume_continuous(self, max_messages: int = None):
        """Continuously consume messages."""
        logger.info(f"Starting continuous consumption from topic '{self.topic}'")
        logger.info("Press Ctrl+C to stop")

        try:
            for message in self.consumer:
                try:
                    order_event = message.value
                    order_id = order_event.get('order_id', 'unknown')
                    customer_id = order_event.get('customer_id', 'unknown')
                    total_amount = order_event.get('total_amount', 0.0)
                    currency = order_event.get('currency', 'unknown')

                    self.message_count += 1
                    self.total_amount += total_amount

                    logger.info(f"✓ Message {self.message_count}: Order {order_id} "
                              f"from customer {customer_id} "
                              f"amount {total_amount} {currency} "
                              f"partition {message.partition} "
                              f"offset {message.offset}")

                    if max_messages and self.message_count >= max_messages:
                        logger.info(f"Reached maximum message limit: {max_messages}")
                        break

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue

        except KeyboardInterrupt:
            logger.info("Stopping continuous consumption...")
        except Exception as e:
            logger.error(f"Error in continuous consumption: {e}")
        finally:
            self.consumer.close()
            logger.info(f"Continuous consumption stopped. "
                       f"Total messages: {self.message_count}, "
                       f"total amount: {self.total_amount:.2f}")

    def get_topic_info(self):
        """Get information about the Kafka topic."""
        try:
            # Get topic partitions
            partitions = self.consumer.partitions_for_topic(self.topic)
            if partitions:
                logger.info(f"Topic '{self.topic}' has {len(partitions)} partitions: {list(partitions)}")

                # Get beginning and end offsets for each partition
                for partition in partitions:
                    beginning = self.consumer.beginning_offsets([(self.topic, partition)])
                    end = self.consumer.end_offsets([(self.topic, partition)])

                    beginning_offset = beginning.get((self.topic, partition), 0)
                    end_offset = end.get((self.topic, partition), 0)
                    message_count = end_offset - beginning_offset

                    logger.info(f"  Partition {partition}: "
                              f"offsets {beginning_offset} to {end_offset} "
                              f"({message_count} messages)")
            else:
                logger.warning(f"Topic '{self.topic}' not found or no partitions available")

        except Exception as e:
            logger.error(f"Error getting topic info: {e}")

    def close(self):
        """Close the consumer."""
        self.consumer.close()
        logger.info("Kafka consumer closed")

def main():

    parser = argparse.ArgumentParser(description='Kafka Order Event Consumer')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                       help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--topic', default='orders-stream',
                       help='Kafka topic (default: orders-stream)')
    parser.add_argument('--group-id', default='test-consumer',
                       help='Consumer group ID (default: test-consumer)')
    parser.add_argument('--max-messages', type=int,
                       help='Maximum messages to consume')
    parser.add_argument('--timeout', type=int, default=30,
                       help='Timeout in seconds (default: 30)')
    parser.add_argument('--continuous', action='store_true',
                       help='Continuously consume messages')
    parser.add_argument('--info', action='store_true',
                       help='Show topic information only')

    args = parser.parse_args()

    try:
        consumer = OrderEventConsumer(args.bootstrap_servers, args.topic, args.group_id)

        if args.info:
            consumer.get_topic_info()
        elif args.continuous:
            consumer.consume_continuous(args.max_messages)
        else:
            consumer.consume_messages(args.max_messages, args.timeout)

    except Exception as e:
        logger.error(f"Failed to start consumer: {e}")
        return 1
    finally:
        if 'consumer' in locals():
            consumer.close()

    return 0

if __name__ == "__main__":
    exit(main())
