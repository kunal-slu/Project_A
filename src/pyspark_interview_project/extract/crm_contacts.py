"""Extract CRM contacts data from local CSV files."""
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp

logger = logging.getLogger(__name__)


def extract_crm_contacts(
    spark: SparkSession,
    config: Dict[str, Any],
    table_name: str
) -> DataFrame:
    """
    Extracts CRM contacts data from local CSV files.
    
    In production, this would connect to Salesforce API.
    For local development, we simulate with CSV files.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        table_name: Table name for metadata
        
    Returns:
        DataFrame: CRM contacts data
    """
    logger.info(f"Extracting CRM contacts for table: {table_name}")

    # For local dev, use CSV files
    if config.get('environment') == 'local':
        sample_path = "aws/data/crm/contacts.csv"
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)
        logger.info(f"Loaded CRM contacts from local CSV: {sample_path}")
    else:
        # In AWS production, this would connect to Salesforce API
        # For now, create empty DataFrame with expected schema
        from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType
        schema = StructType([
            StructField("Id", StringType(), True),
            StructField("LastName", StringType(), True),
            StructField("AccountId", StringType(), True),
            StructField("FirstName", StringType(), True),
            StructField("Email", StringType(), True),
            StructField("Phone", StringType(), True),
            StructField("MobilePhone", StringType(), True),
            StructField("Title", StringType(), True),
            StructField("Department", StringType(), True),
            StructField("LeadSource", StringType(), True),
            StructField("MailingStreet", StringType(), True),
            StructField("MailingCity", StringType(), True),
            StructField("MailingState", StringType(), True),
            StructField("MailingPostalCode", StringType(), True),
            StructField("MailingCountry", StringType(), True),
            StructField("DoNotCall", BooleanType(), True),
            StructField("HasOptedOutOfEmail", BooleanType(), True),
            StructField("Description", StringType(), True),
            StructField("CreatedDate", StringType(), True),
            StructField("LastModifiedDate", StringType(), True),
            StructField("OwnerId", StringType(), True),
            StructField("ContactRole", StringType(), True),
            StructField("ContactLevel", StringType(), True),
            StructField("EngagementScore", IntegerType(), True)
        ])
        df = spark.createDataFrame([], schema)
        logger.info("Created empty DataFrame for production Salesforce API connection")

    # Add metadata columns
    df = df.withColumn("record_source", lit("crm_salesforce")) \
           .withColumn("record_table", lit(table_name)) \
           .withColumn("ingest_timestamp", current_timestamp())
    
    logger.info(f"Extracted {df.count()} records for CRM contacts.")
    return df
