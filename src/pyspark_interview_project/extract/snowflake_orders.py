"""Extract Snowflake orders data."""
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp

logger = logging.getLogger(__name__)


def extract_snowflake_orders(
    spark: SparkSession,
    config: Dict[str, Any],
    **kwargs
) -> DataFrame:
    """
    Extract orders from Snowflake source.
    
    Args:
        spark: SparkSession object
        config: Configuration dictionary
        **kwargs: Additional arguments
        
    Returns:
        DataFrame with Snowflake orders data
    """
    logger.info("Extracting Snowflake orders data")
    
    try:
        # For local dev, use sample data
        if config.get('environment') == 'local':
            sample_path = "data/snowflake_orders_100000.csv"
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)
        else:
            # In AWS, use Snowflake JDBC connection
            snowflake_config = config.get('data_sources', {}).get('snowflake', {})
            # Read from Snowflake table
            df = spark.read \
                .format("snowflake") \
                .options(**snowflake_config) \
                .option("dbtable", "ORDERS") \
                .load()
        
        # Add metadata columns
        df = df.withColumn("record_source", lit("snowflake")) \
               .withColumn("record_table", lit("orders")) \
               .withColumn("ingest_timestamp", current_timestamp())
        
        logger.info(f"Successfully extracted {df.count()} Snowflake orders")
        return df
        
    except Exception as e:
        logger.error(f"Failed to extract Snowflake orders: {e}")
        raise

