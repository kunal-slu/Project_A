"""Extract Salesforce accounts data."""
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp

logger = logging.getLogger(__name__)


def extract_salesforce_accounts(
    spark: SparkSession,
    config: Dict[str, Any],
    table_name: str
) -> DataFrame:
    """
    Extracts Salesforce accounts data.
    """
    logger.info(f"Extracting Salesforce accounts for table: {table_name}")

    # For local dev, use sample data if available
    if config.get('environment') == 'local':
        sample_path = "aws/data/salesforce/salesforce_accounts_ready.csv"
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)
    else:
        # In AWS, use actual Salesforce API or database connection
        # For now, create empty DataFrame with expected schema
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
        schema = StructType([
            StructField("account_id", StringType(), True),
            StructField("account_name", StringType(), True),
            StructField("account_type", StringType(), True),
            StructField("industry", StringType(), True),
            StructField("billing_street", StringType(), True),
            StructField("billing_city", StringType(), True),
            StructField("billing_state", StringType(), True),
            StructField("billing_country", StringType(), True),
            StructField("billing_postal_code", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("website", StringType(), True),
            StructField("annual_revenue", DoubleType(), True),
            StructField("number_of_employees", IntegerType(), True),
            StructField("created_date", StringType(), True),
            StructField("last_modified_date", StringType(), True),
            StructField("owner_id", StringType(), True),
            StructField("account_source", StringType(), True),
            StructField("rating", StringType(), True)
        ])
        df = spark.createDataFrame([], schema)

    # Add metadata columns
    df = df.withColumn("record_source", lit("salesforce")) \
           .withColumn("record_table", lit(table_name)) \
           .withColumn("ingest_timestamp", current_timestamp())
    
    logger.info(f"Extracted {df.count()} records for Salesforce accounts.")
    return df
