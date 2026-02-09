"""
Kafka streaming ingestion for orders events.

Streams from Kafka topic to Bronze layer with offset tracking.
"""

import logging
import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit, to_date, coalesce
from pyspark.sql.types import (
    DoubleType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from project_a.streaming.schema_registry import load_schema, required_fields
logger = logging.getLogger(__name__)


def get_kafka_stream(
    spark: SparkSession, config: dict[str, Any], topic: str = "orders_events"
) -> DataFrame:
    """
    Create Spark Structured Streaming DataFrame from Kafka.

    Args:
        spark: SparkSession
        config: Configuration dictionary
        topic: Kafka topic name

    Returns:
        Streaming DataFrame
    """
    kafka_config = (
        config.get("sources", {}).get("kafka", {})
        or config.get("data_sources", {}).get("kafka", {})
        or config.get("kafka", {})
    )

    # Kafka connection options
    bootstrap_servers = kafka_config.get("bootstrap_servers") or kafka_config.get(
        "local_bootstrap_servers", "localhost:9092"
    )
    kafka_options = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": topic,
        "startingOffsets": kafka_config.get("starting_offsets", "latest"),  # 'earliest' or 'latest'
        "failOnDataLoss": "false",
    }

    # For Confluent Cloud or MSK
    if kafka_config.get("security_protocol") == "SASL_SSL":
        kafka_options.update(
            {
                "kafka.security.protocol": "SASL_SSL",
                "kafka.sasl.mechanism": kafka_config.get("sasl_mechanism", "PLAIN"),
                "kafka.sasl.jaas.config": kafka_config.get("jaas_config"),
            }
        )

    logger.info(f"Connecting to Kafka topic: {topic}")
    logger.info(f"Bootstrap servers: {kafka_options['kafka.bootstrap.servers']}")

    # Read stream
    stream_df = spark.readStream.format("kafka").options(**kafka_options).load()

    return stream_df


def parse_kafka_messages(stream_df: DataFrame, schema: StructType) -> DataFrame:
    """
    Parse Kafka messages (assumed JSON format).

    Args:
        stream_df: Raw Kafka stream DataFrame
        schema: Expected schema for parsed data

    Returns:
        Parsed DataFrame
    """
    # Parse value column (assumed JSON)
    parsed_df = stream_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), schema).alias("data"),
        col("partition"),
        col("offset"),
    ).select("kafka_key", "kafka_timestamp", "partition", "offset", "data.*")

    return parsed_df


def write_kafka_stream_to_bronze(
    stream_df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    trigger_interval: str = "10 seconds",
):
    """
    Write Kafka stream to Bronze layer with checkpointing.

    Args:
        stream_df: Parsed streaming DataFrame
        output_path: Bronze output path (e.g., s3://bucket/bronze/kafka/orders_events)
        checkpoint_path: Checkpoint location for recovery
        trigger_interval: Trigger interval (e.g., "10 seconds", "1 minute")

    Returns:
        StreamingQuery object
    """
    logger.info(f"Writing Kafka stream to Bronze: {output_path}")
    logger.info(f"Checkpoint: {checkpoint_path}")

    # Add metadata
    stream_with_metadata = stream_df.withColumn("_ingestion_ts", current_timestamp()).withColumn(
        "_source", lit("kafka")
    )

    # Write stream
    query = (
        stream_with_metadata.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
        .partitionBy("_proc_date")
        .trigger(processingTime=trigger_interval)
        .start()
    )

    logger.info(f"✅ Started streaming query: {query.id}")

    return query


def stream_orders_from_kafka(
    spark: SparkSession, config: dict[str, Any], topic: str = "orders_events"
) -> None:
    """
    Main function to stream orders from Kafka to Bronze.

    Args:
        spark: SparkSession
        config: Configuration dictionary
        topic: Kafka topic name
    """
    logger.info(f"🚀 Starting Kafka streaming for topic: {topic}")

    # Get stream
    kafka_stream = get_kafka_stream(spark, config, topic)

    # Define schema for orders events (should match Avro schema)
    kafka_cfg = (
        config.get("sources", {}).get("kafka", {})
        or config.get("data_sources", {}).get("kafka", {})
        or {}
    )
    schema_path = kafka_cfg.get("schema_registry_path", "config/schema_registry/kafka/orders_events.json")
    schema_doc = load_schema(schema_path)
    required = required_fields(schema_doc)

    orders_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", TimestampType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
        ]
    )

    # Parse messages
    parsed_stream = parse_kafka_messages(kafka_stream, orders_schema)

    # Enrich with processing date for partitioning
    parsed_stream = parsed_stream.withColumn(
        "_proc_date",
        to_date(coalesce(col("event_ts"), col("kafka_timestamp"))),
    )

    # Split valid vs invalid (DLQ)
    required_cols = [c for c in required if c in parsed_stream.columns]
    valid_expr = None
    for col_name in required_cols:
        expr = col(col_name).isNotNull()
        valid_expr = expr if valid_expr is None else (valid_expr & expr)
    if valid_expr is None:
        valid_expr = col("order_id").isNotNull()

    valid_stream = parsed_stream.filter(valid_expr)
    invalid_stream = parsed_stream.filter(~valid_expr)

    # Write to Bronze
    bronze_path = config.get("data_lake", {}).get("bronze_path", "data/lakehouse_delta/bronze")
    output_path = f"{bronze_path}/kafka/{topic}"

    checkpoint_path = config.get("data_lake", {}).get("checkpoint_path", "data/checkpoints/kafka")
    checkpoint_path = f"{checkpoint_path}/{topic}"

    query = write_kafka_stream_to_bronze(
        valid_stream, output_path, checkpoint_path, trigger_interval="10 seconds"
    )

    # Route invalid rows to DLQ
    dlq_path = kafka_cfg.get("dlq_path", f"{bronze_path}/kafka/dlq/{topic}")
    dlq_checkpoint = f"{checkpoint_path}_dlq"
    _ = (
        invalid_stream.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", dlq_checkpoint)
        .option("path", dlq_path)
        .partitionBy("_proc_date")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Wait for termination (or run in background)
    logger.info("Streaming query started. Waiting for termination...")
    query.awaitTermination()


def save_kafka_offsets(spark: SparkSession, offsets: dict[str, Any], offset_path: str):
    """
    Save Kafka offsets for manual tracking/resume.

    Args:
        spark: SparkSession
        offsets: Offset dictionary
        offset_path: Path to save offsets (S3 or local)
    """
    import json
    from datetime import datetime

    offset_data = {"timestamp": datetime.utcnow().isoformat(), "offsets": offsets}

    # Write to S3 or local
    if offset_path.startswith("s3://"):
        # Use boto3 or Spark to write
        logger.info(f"Saving offsets to S3: {offset_path}")
        # Implementation would use boto3.put_object()
    else:
        # Local file
        os.makedirs(os.path.dirname(offset_path), exist_ok=True)
        with open(offset_path, "w") as f:
            json.dump(offset_data, f, indent=2)
        logger.info(f"Saved offsets to: {offset_path}")


if __name__ == "__main__":
    import argparse
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent.parent.parent))

    from project_a.utils.config import load_conf
    from project_a.utils.spark_session import build_spark

    parser = argparse.ArgumentParser(description="Kafka orders streaming job")
    parser.add_argument("--config", default="local/config/local.yaml")
    parser.add_argument("--env", default="local")
    args = parser.parse_args()

    config = load_conf(args.config, args.env)
    spark = build_spark(app_name="kafka_streaming", config=config)

    try:
        stream_orders_from_kafka(spark, config)
    except KeyboardInterrupt:
        logger.info("Streaming stopped by user")
    finally:
        spark.stop()
