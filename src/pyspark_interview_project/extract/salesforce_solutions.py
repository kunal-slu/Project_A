"""Extract Salesforce solutions data."""
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp

logger = logging.getLogger(__name__)


def extract_salesforce_solutions(
    spark: SparkSession,
    config: Dict[str, Any],
    table_name: str
) -> DataFrame:
    """
    Extracts Salesforce solutions data.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        table_name: Table name for metadata
        
    Returns:
        DataFrame: Salesforce solutions data
    """
    logger.info(f"Extracting Salesforce solutions for table: {table_name}")

    # For local dev, use sample data if available
    if config.get('environment') == 'local':
        sample_path = "aws/data/salesforce/salesforce_solutions.csv"
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)
    else:
        # In AWS, use actual Salesforce API or database connection
        # For now, create empty DataFrame with expected schema
        from pyspark.sql.types import StructType, StructField, StringType, BooleanType
        schema = StructType([
            StructField("solution_id", StringType(), True),
            StructField("solution_name", StringType(), True),
            StructField("solution_number", StringType(), True),
            StructField("description", StringType(), True),
            StructField("category", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_date", StringType(), True),
            StructField("last_modified_date", StringType(), True),
            StructField("owner_id", StringType(), True),
            StructField("is_published", BooleanType(), True)
        ])
        df = spark.createDataFrame([], schema)

    # Add metadata columns
    df = df.withColumn("record_source", lit("salesforce")) \
           .withColumn("record_table", lit(table_name)) \
           .withColumn("ingest_timestamp", current_timestamp())
    
    logger.info(f"Extracted {df.count()} records for Salesforce solutions.")
    return df
