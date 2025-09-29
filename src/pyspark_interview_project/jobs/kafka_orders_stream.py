#!/usr/bin/env python3
"""
Kafka orders streaming job with DLQ and schema validation.
Processes order events from Confluent Cloud Kafka and writes to Delta tables.
"""

import os
import sys
import json
import logging
from typing import Optional
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit, when, 
    to_timestamp, to_date, regexp_replace, split, expr
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.utils import AnalysisException

from pyspark_interview_project.utils.spark import get_spark_session
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.logging import setup_json_logging

logger = logging.getLogger(__name__)


def get_kafka_config() -> dict:
    """
    Get Kafka configuration from environment variables.
    
    Returns:
        Dictionary with Kafka configuration
    """
    return {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.jaas.config": f"""
            org.apache.kafka.common.security.plain.PlainLoginModule required
            username="{os.getenv('KAFKA_API_KEY')}"
            password="{os.getenv('KAFKA_API_SECRET')}";
        """,
        "group.id": os.getenv("KAFKA_GROUP_ID", "orders-stream-processor"),
        "auto.offset.reset": "latest",
        "enable.auto.commit": "false"
    }


def get_orders_schema() -> StructType:
    """
    Get the schema for orders events.
    
    Returns:
        Spark StructType for orders events
    """
    return StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("event_ts", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("status", StringType(), False)
    ])


def process_orders_stream(spark: SparkSession, config: dict) -> None:
    """
    Process the orders stream from Kafka.
    
    Args:
        spark: Spark session
        config: Configuration dictionary
    """
    logger.info("Starting Kafka orders stream processing")
    
    # Get Kafka configuration
    kafka_config = get_kafka_config()
    topic = os.getenv("KAFKA_TOPIC", "orders_events")
    
    # Get data lake configuration
    lake_root = config["lake"]["root"]
    checkpoint_location = f"{lake_root}/_checkpoints/orders_events"
    dlq_location = f"{lake_root}/_errors/kafka/orders_events"
    
    # Define the orders schema
    orders_schema = get_orders_schema()
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config["bootstrap.servers"]) \
        .option("kafka.security.protocol", kafka_config["security.protocol"]) \
        .option("kafka.sasl.mechanism", kafka_config["sasl.mechanism"]) \
        .option("kafka.sasl.jaas.config", kafka_config["sasl.jaas.config"]) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON and validate schema
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("json_value"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition"),
        col("offset")
    )
    
    # Parse JSON with schema validation
    orders_df = parsed_df.select(
        col("kafka_key"),
        col("kafka_timestamp"),
        col("partition"),
        col("offset"),
        from_json(col("json_value"), orders_schema).alias("parsed_data")
    )
    
    # Extract fields and add processing metadata
    processed_df = orders_df.select(
        col("kafka_key"),
        col("kafka_timestamp"),
        col("partition"),
        col("offset"),
        col("parsed_data.order_id"),
        col("parsed_data.customer_id"),
        col("parsed_data.event_ts"),
        col("parsed_data.currency"),
        col("parsed_data.amount"),
        col("parsed_data.status"),
        current_timestamp().alias("processed_at"),
        lit("kafka").alias("source")
    )
    
    # Add watermark for deduplication and add proc_date
    watermarked_df = processed_df.withWatermark("event_ts", "1 hour") \
        .withColumn("proc_date", to_date(col("event_ts")))
    
    # Separate valid and invalid records with business rules validation
    valid_df = watermarked_df.filter(
        col("order_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("event_ts").isNotNull() &
        col("currency").isNotNull() &
        col("amount").isNotNull() &
        col("status").isNotNull() &
        col("amount") >= 0.0 &  # Amount must be non-negative
        col("status").isin(["PLACED", "PAID", "SHIPPED", "CANCELLED"])  # Status must be valid
    )
    
    invalid_df = watermarked_df.filter(
        col("order_id").isNull() |
        col("customer_id").isNull() |
        col("event_ts").isNull() |
        col("currency").isNull() |
        col("amount").isNull() |
        col("status").isNull() |
        col("amount") < 0.0 |  # Amount must be non-negative
        ~col("status").isin(["PLACED", "PAID", "SHIPPED", "CANCELLED"])  # Status must be valid
    )
    
    # Write valid records to Delta table
    valid_query = valid_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_location}/valid") \
        .option("path", f"{lake_root}/silver/orders_events") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    # Write invalid records to DLQ
    dlq_query = invalid_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_location}/dlq") \
        .option("path", f"{dlq_location}") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    # Also write raw invalid records to S3 for debugging
    raw_invalid_df = parsed_df.filter(col("parsed_data").isNull())
    raw_dlq_query = raw_invalid_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_location}/raw_dlq") \
        .option("path", f"{dlq_location}/raw") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    logger.info("Kafka stream processing started")
    logger.info(f"Valid records: {lake_root}/silver/orders_events")
    logger.info(f"Invalid records: {dlq_location}")
    logger.info(f"Raw invalid records: {dlq_location}/raw")
    
    # Wait for termination
    try:
        valid_query.awaitTermination()
        dlq_query.awaitTermination()
        raw_dlq_query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping Kafka stream processing...")
        valid_query.stop()
        dlq_query.stop()
        raw_dlq_query.stop()


def main():
    """Main function to run the Kafka orders stream job."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Process Kafka orders stream")
    parser.add_argument("--config", required=True, help="Configuration file path")
    parser.add_argument("--checkpoint", help="Checkpoint location override")
    args = parser.parse_args()
    
    # Setup logging
    setup_json_logging()
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logging.getLogger().setLevel(getattr(logging, log_level.upper()))
    
    try:
        # Load configuration
        config = load_conf(args.config)
        
        # Create Spark session with Delta and Kafka support
        spark = get_spark_session(
            "KafkaOrdersStream",
            extra_conf={
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
                "spark.sql.streaming.checkpointLocation.enabled": "true"
            }
        )
        
        # Process the stream
        process_orders_stream(spark, config)
        
    except Exception as e:
        logger.error(f"Kafka stream processing failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()