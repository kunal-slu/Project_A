"""Extract streaming orders from Kafka."""
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp

logger = logging.getLogger(__name__)


def extract_kafka_orders_stream(
    spark: SparkSession,
    config: Dict[str, Any],
    **kwargs
) -> DataFrame:
    """
    Extract streaming orders from Kafka source.
    
    Args:
        spark: SparkSession object
        config: Configuration dictionary
        **kwargs: Additional arguments (start_time, end_time, etc.)
        
    Returns:
        DataFrame with Kafka streaming orders data
    """
    logger.info("Extracting Kafka streaming orders data")
    
    try:
        # For local dev, use sample data
        if config.get('environment') == 'local':
            sample_path = "data/stream_kafka_events_100000.csv"
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)
        else:
            # In AWS, use Kafka streaming source
            kafka_config = config.get('data_sources', {}).get('kafka', {})
            # Read from Kafka topic
            df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_config.get('bootstrap_servers')) \
                .option("subscribe", kafka_config.get('topic')) \
                .option("startingOffsets", "latest") \
                .load()
        
        # Add metadata columns
        df = df.withColumn("record_source", lit("kafka")) \
               .withColumn("record_table", lit("orders_stream")) \
               .withColumn("ingest_timestamp", current_timestamp())
        
        logger.info(f"Successfully extracted Kafka streaming orders")
        return df
        
    except Exception as e:
        logger.error(f"Failed to extract Kafka orders: {e}")
        raise

