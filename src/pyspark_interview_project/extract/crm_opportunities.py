"""Extract CRM opportunities data from local CSV files."""
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp

logger = logging.getLogger(__name__)


def extract_crm_opportunities(
    spark: SparkSession,
    config: Dict[str, Any],
    table_name: str
) -> DataFrame:
    """
    Extracts CRM opportunities data from local CSV files.
    
    In production, this would connect to Salesforce API.
    For local development, we simulate with CSV files.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        table_name: Table name for metadata
        
    Returns:
        DataFrame: CRM opportunities data
    """
    logger.info(f"Extracting CRM opportunities for table: {table_name}")

    # For local dev, use CSV files
    if config.get('environment') == 'local':
        sample_path = "aws/data/crm/opportunities.csv"
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)
        logger.info(f"Loaded CRM opportunities from local CSV: {sample_path}")
    else:
        # In AWS production, this would connect to Salesforce API
        # For now, create empty DataFrame with expected schema
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
        schema = StructType([
            StructField("Id", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("AccountId", StringType(), True),
            StructField("StageName", StringType(), True),
            StructField("CloseDate", StringType(), True),
            StructField("Amount", DoubleType(), True),
            StructField("Probability", IntegerType(), True),
            StructField("LeadSource", StringType(), True),
            StructField("Type", StringType(), True),
            StructField("NextStep", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("ForecastCategory", StringType(), True),
            StructField("IsClosed", BooleanType(), True),
            StructField("IsWon", BooleanType(), True),
            StructField("CreatedDate", StringType(), True),
            StructField("LastModifiedDate", StringType(), True),
            StructField("OwnerId", StringType(), True),
            StructField("DealSize", StringType(), True),
            StructField("SalesCycle", IntegerType(), True),
            StructField("ProductInterest", StringType(), True),
            StructField("Budget", StringType(), True),
            StructField("Timeline", StringType(), True)
        ])
        df = spark.createDataFrame([], schema)
        logger.info("Created empty DataFrame for production Salesforce API connection")

    # Add metadata columns
    df = df.withColumn("record_source", lit("crm_salesforce")) \
           .withColumn("record_table", lit(table_name)) \
           .withColumn("ingest_timestamp", current_timestamp())
    
    logger.info(f"Extracted {df.count()} records for CRM opportunities.")
    return df
