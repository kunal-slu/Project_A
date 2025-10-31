"""Extract Salesforce contacts data."""
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp

logger = logging.getLogger(__name__)


def extract_salesforce_contacts(
    spark: SparkSession,
    config: Dict[str, Any],
    table_name: str
) -> DataFrame:
    """
    Extracts Salesforce contacts data.
    """
    logger.info(f"Extracting Salesforce contacts for table: {table_name}")

    # For local dev, use sample data if available
    if config.get('environment') == 'local':
        sample_path = "aws/data/salesforce/salesforce_contacts_ready.csv"
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)
    else:
        # In AWS, use actual Salesforce API or database connection
        # For now, create empty DataFrame with expected schema
        from pyspark.sql.types import StructType, StructField, StringType, BooleanType
        schema = StructType([
            StructField("contact_id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("mobile_phone", StringType(), True),
            StructField("title", StringType(), True),
            StructField("department", StringType(), True),
            StructField("lead_source", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_date", StringType(), True),
            StructField("last_modified_date", StringType(), True),
            StructField("owner_id", StringType(), True),
            StructField("email_opt_out", BooleanType(), True),
            StructField("do_not_call", BooleanType(), True),
            StructField("last_activity_date", StringType(), True)
        ])
        df = spark.createDataFrame([], schema)

    # Add metadata columns
    df = df.withColumn("record_source", lit("salesforce")) \
           .withColumn("record_table", lit(table_name)) \
           .withColumn("ingest_timestamp", current_timestamp())
    
    logger.info(f"Extracted {df.count()} records for Salesforce contacts.")
    return df
