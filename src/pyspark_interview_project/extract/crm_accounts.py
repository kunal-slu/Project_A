"""Extract CRM accounts data from local CSV files."""
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp

logger = logging.getLogger(__name__)


def extract_crm_accounts(
    spark: SparkSession,
    config: Dict[str, Any],
    table_name: str
) -> DataFrame:
    """
    Extracts CRM accounts data from local CSV files.
    
    In production, this would connect to Salesforce API.
    For local development, we simulate with CSV files.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        table_name: Table name for metadata
        
    Returns:
        DataFrame: CRM accounts data
    """
    logger.info(f"Extracting CRM accounts for table: {table_name}")

    # For local dev, use CSV files
    if config.get('environment') == 'local':
        sample_path = "aws/data/crm/accounts.csv"
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)
        logger.info(f"Loaded CRM accounts from local CSV: {sample_path}")
    else:
        # In AWS production, this would connect to Salesforce API
        # For now, create empty DataFrame with expected schema
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
        schema = StructType([
            StructField("Id", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("Phone", StringType(), True),
            StructField("Website", StringType(), True),
            StructField("Industry", StringType(), True),
            StructField("AnnualRevenue", DoubleType(), True),
            StructField("NumberOfEmployees", IntegerType(), True),
            StructField("BillingStreet", StringType(), True),
            StructField("BillingCity", StringType(), True),
            StructField("BillingState", StringType(), True),
            StructField("BillingPostalCode", StringType(), True),
            StructField("BillingCountry", StringType(), True),
            StructField("Rating", StringType(), True),
            StructField("Type", StringType(), True),
            StructField("AccountSource", StringType(), True),
            StructField("AccountNumber", StringType(), True),
            StructField("Site", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("Ownership", StringType(), True),
            StructField("ParentId", StringType(), True),
            StructField("TickerSymbol", StringType(), True),
            StructField("YearStarted", IntegerType(), True),
            StructField("CreatedDate", StringType(), True),
            StructField("LastModifiedDate", StringType(), True),
            StructField("OwnerId", StringType(), True),
            StructField("CustomerSegment", StringType(), True),
            StructField("GeographicRegion", StringType(), True),
            StructField("AccountStatus", StringType(), True),
            StructField("LastActivityDate", StringType(), True)
        ])
        df = spark.createDataFrame([], schema)
        logger.info("Created empty DataFrame for production Salesforce API connection")

    # Add metadata columns
    df = df.withColumn("record_source", lit("crm_salesforce")) \
           .withColumn("record_table", lit(table_name)) \
           .withColumn("ingest_timestamp", current_timestamp())
    
    logger.info(f"Extracted {df.count()} records for CRM accounts.")
    return df
