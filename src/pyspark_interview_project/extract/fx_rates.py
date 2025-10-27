"""Extract FX rates data."""
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp

logger = logging.getLogger(__name__)


def extract_fx_rates(
    spark: SparkSession,
    config: Dict[str, Any],
    **kwargs
) -> DataFrame:
    """
    Extract FX rates from vendor source.
    
    Args:
        spark: SparkSession object
        config: Configuration dictionary
        **kwargs: Additional arguments
        
    Returns:
        DataFrame with FX rates data
    """
    logger.info("Extracting FX rates data")
    
    try:
        # For local dev, use sample data
        if config.get('environment') == 'local':
            sample_path = "data/fx_rates_historical_730_days.csv"
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)
        else:
            # In AWS, use FX vendor API or database
            fx_config = config.get('data_sources', {}).get('fx_rates', {})
            # Read from source
            df = spark.read \
                .format(fx_config.get('type')) \
                .options(**fx_config.get('config', {})) \
                .load()
        
        # Add metadata columns
        df = df.withColumn("record_source", lit("fx_vendor")) \
               .withColumn("record_table", lit("fx_rates")) \
               .withColumn("ingest_timestamp", current_timestamp())
        
        logger.info(f"Successfully extracted {df.count()} FX rate records")
        return df
        
    except Exception as e:
        logger.error(f"Failed to extract FX rates: {e}")
        raise

