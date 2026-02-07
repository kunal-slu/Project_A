"""
Kafka streaming ingestion for orders events.

Streams from Kafka topic to Bronze layer with offset tracking.
"""

import logging
import os
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

logger = logging.getLogger(__name__)


def get_kafka_stream(
    spark: SparkSession,
    config: Dict[str, Any],
    topic: str = "orders_events"
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
    kafka_config = config.get('data_sources', {}).get('kafka', {})
    
    # Kafka connection options
    kafka_options = {
        "kafka.bootstrap.servers": kafka_config.get('bootstrap_servers', 'localhost:9092'),
        "subscribe": topic,
        "startingOffsets": kafka_config.get('starting_offsets', 'latest'),  # 'earliest' or 'latest'
        "failOnDataLoss": "false"
    }
    
    # For Confluent Cloud or MSK
    if kafka_config.get('security_protocol') == 'SASL_SSL':
        kafka_options.update({
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": kafka_config.get('sasl_mechanism', 'PLAIN'),
            "kafka.sasl.jaas.config": kafka_config.get('jaas_config')
        })
    
    logger.info(f"Connecting to Kafka topic: {topic}")
    logger.info(f"Bootstrap servers: {kafka_options['kafka.bootstrap.servers']}")
    
    # Read stream
    stream_df = spark \
        .readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()
    
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
        col("offset")
    ).select(
        "kafka_key",
        "kafka_timestamp",
        "partition",
        "offset",
        "data.*"
    )
    
    return parsed_df


def write_kafka_stream_to_bronze(
    stream_df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    trigger_interval: str = "10 seconds"
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
    stream_with_metadata = stream_df \
        .withColumn("_ingestion_ts", current_timestamp()) \
        .withColumn("_source", lit("kafka"))
    
    # Write stream
    query = stream_with_metadata \
        .writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("path", output_path) \
        .partitionBy("_proc_date") \
        .trigger(processingTime=trigger_interval) \
        .start()
    
    logger.info(f"✅ Started streaming query: {query.id}")
    
    return query


def stream_orders_from_kafka(
    spark: SparkSession,
    config: Dict[str, Any],
    topic: str = "orders_events"
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
    orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    
    # Parse messages
    parsed_stream = parse_kafka_messages(kafka_stream, orders_schema)
    
    # Write to Bronze
    bronze_path = config.get("data_lake", {}).get("bronze_path", "data/lakehouse_delta/bronze")
    output_path = f"{bronze_path}/kafka/{topic}"
    
    checkpoint_path = config.get("data_lake", {}).get("checkpoint_path", "data/checkpoints/kafka")
    checkpoint_path = f"{checkpoint_path}/{topic}"
    
    query = write_kafka_stream_to_bronze(
        parsed_stream,
        output_path,
        checkpoint_path,
        trigger_interval="10 seconds"
    )
    
    # Wait for termination (or run in background)
    logger.info("Streaming query started. Waiting for termination...")
    query.awaitTermination()


def save_kafka_offsets(
    spark: SparkSession,
    offsets: Dict[str, Any],
    offset_path: str
):
    """
    Save Kafka offsets for manual tracking/resume.
    
    Args:
        spark: SparkSession
        offsets: Offset dictionary
        offset_path: Path to save offsets (S3 or local)
    """
    import json
    from datetime import datetime
    
    offset_data = {
        "timestamp": datetime.utcnow().isoformat(),
        "offsets": offsets
    }
    
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
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    
    from project_a.utils.spark_session import build_spark
    from project_a.utils.config import load_conf
    
    config = load_conf("config/local.yaml")
    spark = build_spark(app_name="kafka_streaming", config=config)
    
    try:
        stream_orders_from_kafka(spark, config)
    except KeyboardInterrupt:
        logger.info("Streaming stopped by user")
    finally:
        spark.stop()
