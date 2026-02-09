"""
Kafka → Bronze streaming pipeline with Structured Streaming.

Subscribes to Kafka topic and writes customer events to S3 bronze layer.
"""

import logging
import os
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

logger = logging.getLogger(__name__)


def get_spark(config: dict[str, Any] | None = None):
    """Get or create Spark session with streaming support."""
    checkpoint_location = (
        (config or {})
        .get("paths", {})
        .get("checkpoints_root", "s3://my-etl-lake-demo/_checkpoints/")
        if config
        else os.getenv("CHECKPOINT_LOCATION", "s3://my-etl-lake-demo/_checkpoints/")
    )
    return (
        SparkSession.builder.appName("kafka-customer-events")
        .config("spark.sql.streaming.checkpointLocation", checkpoint_location)
        .getOrCreate()
    )


def main(config: dict[str, Any] | None = None):
    """Stream customer events from Kafka to Bronze S3."""
    spark = get_spark(config)

    # Schema for customer events
    schema = StructType(
        [
            StructField("event_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("event_name", StringType()),
            StructField("event_ts", TimestampType()),
            StructField("session_id", StringType()),
            StructField("url_1", StringType()),
            StructField("url_2", StringType()),
            StructField("device_type", StringType()),
            StructField("browser", StringType()),
            StructField("country", StringType()),
            StructField("revenue", DoubleType()),
        ]
    )

    # Configuration
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9092")
    # Use config-based bucket resolution
    bucket = (
        (config or {}).get("buckets", {}).get("lake", "my-etl-lake-demo")
        if config
        else os.getenv("S3_BUCKET", "my-etl-lake-demo")
    )
    topic = os.getenv("KAFKA_TOPIC", "customer_events")

    logger.info(f"Connecting to Kafka: {kafka_brokers}, topic: {topic}")

    # Read stream from Kafka
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")  # Don't fail on data loss
        .load()
    )

    logger.info("Kafka stream connected")

    # Parse JSON and extract fields
    json_df = (
        df.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .filter(col("event_id").isNotNull())  # Filter out invalid records
    )

    # Write stream to S3 Bronze with checkpointing
    query = (
        json_df.writeStream.format("parquet")
        .option("path", f"s3a://{bucket}/bronze/customer_events_stream/")
        .option("checkpointLocation", f"s3a://{bucket}/_checkpoints/customer_events_stream/")
        .outputMode("append")
        .partitionBy("event_ts")  # Partition by timestamp for performance
        .start()
    )

    logger.info("Stream started, writing to bronze/customer_events_stream/")
    logger.info("Press Ctrl+C to stop")

    # Wait for termination (or trigger.stop() in production)
    query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
