#!/usr/bin/env python3
"""
Salesforce incremental data extraction job.
Extracts Lead and Account data from Salesforce and writes to S3 bronze layer.
"""

import os
import sys
import json
import logging
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.logging import setup_json_logging

logger = logging.getLogger(__name__)


def get_salesforce_credentials() -> Dict[str, str]:
    """
    Get Salesforce credentials from environment or AWS Secrets Manager.
    
    Returns:
        Dictionary with Salesforce credentials
    """
    # Try AWS Secrets Manager first
    secret_name = os.getenv("SF_SECRET_NAME")
    if secret_name:
        try:
            import boto3
            secrets_client = boto3.client('secretsmanager')
            response = secrets_client.get_secret_value(SecretId=secret_name)
            secret_data = json.loads(response['SecretString'])
            return {
                "username": secret_data["username"],
                "password": secret_data["password"],
                "security_token": secret_data["security_token"],
                "domain": secret_data.get("domain", "login")
            }
        except Exception as e:
            logger.warning(f"Failed to get credentials from Secrets Manager: {e}")
    
    # Fall back to environment variables
    return {
        "username": os.getenv("SF_USER"),
        "password": os.getenv("SF_PASS"),
        "security_token": os.getenv("SF_TOKEN"),
        "domain": os.getenv("SF_DOMAIN", "login")
    }


def get_salesforce_connection_string(creds: Dict[str, str]) -> str:
    """
    Build Salesforce JDBC connection string.
    
    Args:
        creds: Salesforce credentials dictionary
        
    Returns:
        JDBC connection string
    """
    return (
        f"jdbc:salesforce:user={creds['username']};"
        f"password={creds['password']};"
        f"securityToken={creds['security_token']};"
        f"serverURL=https://{creds['domain']}.salesforce.com"
    )


def get_last_checkpoint(spark: SparkSession, table_name: str, lake_root: str) -> Optional[str]:
    """
    Get the last checkpoint timestamp for incremental loading.
    
    Args:
        spark: Spark session
        table_name: Table name
        lake_root: S3 root path of the data lake
        
    Returns:
        Last checkpoint timestamp or None
    """
    checkpoint_path = f"{lake_root}/_checkpoints/salesforce/{table_name}"
    
    try:
        # Try to read the last checkpoint
        checkpoint_df = spark.read.format("delta").load(checkpoint_path)
        last_checkpoint = checkpoint_df.select("last_checkpoint").collect()[0][0]
        logger.info(f"Found last checkpoint for {table_name}: {last_checkpoint}")
        return last_checkpoint
    except Exception:
        logger.info(f"No checkpoint found for {table_name}, using LAST_N_DAYS:1")
        return None


def save_checkpoint(spark: SparkSession, table_name: str, lake_root: str, checkpoint_time: str) -> None:
    """
    Save the checkpoint timestamp for incremental loading.
    
    Args:
        spark: Spark session
        table_name: Table name
        lake_root: S3 root path of the data lake
        checkpoint_time: Checkpoint timestamp
    """
    checkpoint_path = f"{lake_root}/_checkpoints/salesforce/{table_name}"
    
    # Create checkpoint DataFrame
    checkpoint_df = spark.createDataFrame(
        [(checkpoint_time,)], 
        ["last_checkpoint"]
    )
    
    # Write checkpoint
    checkpoint_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(checkpoint_path)
    
    logger.info(f"Saved checkpoint for {table_name}: {checkpoint_time}")


def extract_salesforce_data(
    spark: SparkSession,
    table_name: str,
    soql_query: str,
    connection_string: str
) -> Any:
    """
    Extract data from Salesforce using JDBC.
    
    Args:
        spark: Spark session
        table_name: Salesforce table name
        soql_query: SOQL query to execute
        connection_string: JDBC connection string
        
    Returns:
        Spark DataFrame with Salesforce data
    """
    logger.info(f"Extracting data from Salesforce {table_name}")
    
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", connection_string) \
            .option("query", soql_query) \
            .option("driver", "com.salesforce.jdbc.SalesforceDriver") \
            .load()
        
        logger.info(f"Successfully extracted {df.count()} records from {table_name}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to extract data from Salesforce {table_name}: {e}")
        raise


def get_lead_soql(last_checkpoint: Optional[str]) -> str:
    """
    Get SOQL query for Lead table with incremental logic.
    
    Args:
        last_checkpoint: Last checkpoint timestamp
        
    Returns:
        SOQL query string
    """
    if last_checkpoint:
        where_clause = f"WHERE SystemModstamp >= {last_checkpoint}"
    else:
        where_clause = "WHERE SystemModstamp >= LAST_N_DAYS:1"
    
    return f"""
    SELECT 
        Id,
        FirstName,
        LastName,
        Email,
        Phone,
        Company,
        Status,
        LeadSource,
        CreatedDate,
        LastModifiedDate,
        SystemModstamp
    FROM Lead 
    {where_clause}
    ORDER BY SystemModstamp
    """


def get_account_soql(last_checkpoint: Optional[str]) -> str:
    """
    Get SOQL query for Account table with incremental logic.
    
    Args:
        last_checkpoint: Last checkpoint timestamp
        
    Returns:
        SOQL query string
    """
    if last_checkpoint:
        where_clause = f"WHERE SystemModstamp >= {last_checkpoint}"
    else:
        where_clause = "WHERE SystemModstamp >= LAST_N_DAYS:1"
    
    return f"""
    SELECT 
        Id,
        Name,
        Type,
        Industry,
        BillingStreet,
        BillingCity,
        BillingState,
        BillingPostalCode,
        BillingCountry,
        Phone,
        Website,
        CreatedDate,
        LastModifiedDate,
        SystemModstamp
    FROM Account 
    {where_clause}
    ORDER BY SystemModstamp
    """


def add_metadata_columns(df: Any, source: str, table_name: str) -> Any:
    """
    Add metadata columns to the DataFrame.
    
    Args:
        df: Input DataFrame
        source: Source system name
        table_name: Table name
        
    Returns:
        DataFrame with metadata columns
    """
    return df.withColumn("_source", lit(source)) \
             .withColumn("_table", lit(table_name)) \
             .withColumn("_extracted_at", current_timestamp()) \
             .withColumn("_proc_date", to_date(current_timestamp()))


def write_to_bronze(df: Any, table_name: str, lake_root: str) -> None:
    """
    Write DataFrame to bronze layer with partitioning by date.
    
    Args:
        df: Input DataFrame
        table_name: Table name
        lake_root: S3 root path of the data lake
    """
    output_path = f"{lake_root}/bronze/salesforce/{table_name}"
    
    logger.info(f"Writing {table_name} to bronze layer: {output_path}")
    
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("_proc_date") \
        .save(output_path)
    
    logger.info(f"Successfully wrote {table_name} to bronze layer")


def process_salesforce_incremental(spark: SparkSession, config: Dict[str, Any]) -> None:
    """
    Process Salesforce incremental data extraction.
    
    Args:
        spark: Spark session
        config: Configuration dictionary
    """
    logger.info("Starting Salesforce incremental data extraction")
    
    # Get credentials
    creds = get_salesforce_credentials()
    connection_string = get_salesforce_connection_string(creds)
    
    # Get data lake configuration
    lake_root = config["lake"]["root"]
    
    # Process Lead table
    try:
        last_checkpoint = get_last_checkpoint(spark, "lead", lake_root)
        lead_soql = get_lead_soql(last_checkpoint)
        lead_df = extract_salesforce_data(spark, "Lead", lead_soql, connection_string)
        lead_df_with_meta = add_metadata_columns(lead_df, "salesforce", "lead")
        write_to_bronze(lead_df_with_meta, "lead", lake_root)
        
        # Save checkpoint
        if lead_df.count() > 0:
            max_timestamp = lead_df.select("SystemModstamp").rdd.max()[0]
            save_checkpoint(spark, "lead", lake_root, max_timestamp)
            
    except Exception as e:
        logger.error(f"Failed to process Lead table: {e}")
        raise
    
    # Process Account table
    try:
        last_checkpoint = get_last_checkpoint(spark, "account", lake_root)
        account_soql = get_account_soql(last_checkpoint)
        account_df = extract_salesforce_data(spark, "Account", account_soql, connection_string)
        account_df_with_meta = add_metadata_columns(account_df, "salesforce", "account")
        write_to_bronze(account_df_with_meta, "account", lake_root)
        
        # Save checkpoint
        if account_df.count() > 0:
            max_timestamp = account_df.select("SystemModstamp").rdd.max()[0]
            save_checkpoint(spark, "account", lake_root, max_timestamp)
            
    except Exception as e:
        logger.error(f"Failed to process Account table: {e}")
        raise
    
    logger.info("Salesforce incremental data extraction completed successfully")


def main():
    """Main function to run the Salesforce incremental job."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract incremental data from Salesforce")
    parser.add_argument("--config", required=True, help="Configuration file path")
    args = parser.parse_args()
    
    # Setup logging
    setup_json_logging()
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logging.getLogger().setLevel(getattr(logging, log_level.upper()))
    
    try:
        # Load configuration
        config = load_conf(args.config)
        
        # Create Spark session with Delta support
        spark = get_spark_session(
            "SalesforceToBronze",
            extra_conf={
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
            }
        )
        
        # Process Salesforce data
        process_salesforce_incremental(spark, config)
        
    except Exception as e:
        logger.error(f"Salesforce extraction failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()