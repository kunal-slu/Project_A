"""Extract HubSpot contacts data."""
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp
from pyspark_interview_project.utils.io import read_source

logger = logging.getLogger(__name__)


def extract_hubspot_contacts(
    spark: SparkSession,
    config: Dict[str, Any],
    **kwargs
) -> DataFrame:
    """
    Extract contacts from HubSpot source.
    
    Args:
        spark: SparkSession object
        config: Configuration dictionary
        **kwargs: Additional arguments
        
    Returns:
        DataFrame with HubSpot contacts data
    """
    logger.info("Extracting HubSpot contacts data")
    
    try:
        source_config = config.get('data_sources', {}).get('hubspot', {})
        table_name = "contacts"
        
        # For local dev, use sample data if available
        if config.get('environment') == 'local':
            sample_path = "data/hubspot_contacts_25000.csv"
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)
        else:
            # In AWS, use actual HubSpot API or database connection
            df = read_source(spark, source_config.get('type'), source_config.get('config'))
        
        # Add metadata columns
        df = df.withColumn("record_source", lit("hubspot")) \
               .withColumn("record_table", lit(table_name)) \
               .withColumn("ingest_timestamp", current_timestamp())
        
        logger.info(f"Successfully extracted {df.count()} HubSpot contacts")
        return df
        
    except Exception as e:
        logger.error(f"Failed to extract HubSpot contacts: {e}")
        raise

