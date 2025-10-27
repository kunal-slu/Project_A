"""Extract Redshift customer behavior data."""
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp

logger = logging.getLogger(__name__)


def extract_redshift_behavior(
    spark: SparkSession,
    config: Dict[str, Any],
    **kwargs
) -> DataFrame:
    """
    Extract customer behavior from Redshift source.
    
    Args:
        spark: SparkSession object
        config: Configuration dictionary
        **kwargs: Additional arguments
        
    Returns:
        DataFrame with Redshift customer behavior data
    """
    logger.info("Extracting Redshift customer behavior data")
    
    try:
        # For local dev, use sample data
        if config.get('environment') == 'local':
            sample_path = "data/redshift_customer_behavior_50000.csv"
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)
        else:
            # In AWS, use Redshift JDBC connection
            redshift_config = config.get('data_sources', {}).get('redshift', {})
            # Read from Redshift table
            df = spark.read \
                .format("jdbc") \
                .option("url", redshift_config.get('url')) \
                .option("dbtable", redshift_config.get('table')) \
                .option("user", redshift_config.get('user')) \
                .option("password", redshift_config.get('password')) \
                .load()
        
        # Add metadata columns
        df = df.withColumn("record_source", lit("redshift")) \
               .withColumn("record_table", lit("customer_behavior")) \
               .withColumn("ingest_timestamp", current_timestamp())
        
        logger.info(f"Successfully extracted {df.count()} Redshift behavior records")
        return df
        
    except Exception as e:
        logger.error(f"Failed to extract Redshift behavior: {e}")
        raise

