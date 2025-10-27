#!/usr/bin/env python3
"""
Salesforce bronze to silver transformation job.
Transforms raw Salesforce data from bronze to silver layer with proper schema.
"""

import argparse
import logging
import os
import sys
from typing import Dict, Any
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when, to_timestamp, regexp_replace

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.logging import setup_json_logging

logger = logging.getLogger(__name__)


def transform_accounts_to_silver(spark: SparkSession, accounts_bronze: SparkSession) -> SparkSession:
    """
    Transform Salesforce accounts from bronze to silver.
    
    Args:
        spark: Spark session
        accounts_bronze: Bronze accounts DataFrame
        
    Returns:
        Silver accounts DataFrame with schema: id, name, type, phone, billing_city, billing_state, billing_country, created_at, system_modstamp
    """
    logger.info("Transforming Salesforce accounts to silver layer")
    
    silver_accounts = accounts_bronze.select(
        col("Id").alias("id"),
        col("Name").alias("name"),
        col("Type").alias("type"),
        col("Phone").alias("phone"),
        col("BillingCity").alias("billing_city"),
        col("BillingState").alias("billing_state"),
        col("BillingCountry").alias("billing_country"),
        to_timestamp(col("CreatedDate")).alias("created_at"),
        to_timestamp(col("SystemModstamp")).alias("system_modstamp")
    ).filter(
        col("id").isNotNull() & col("name").isNotNull()
    )
    
    logger.info(f"Silver accounts created. Rows: {silver_accounts.count()}")
    return silver_accounts


def transform_leads_to_silver(spark: SparkSession, leads_bronze: SparkSession) -> SparkSession:
    """
    Transform Salesforce leads from bronze to silver.
    
    Args:
        spark: Spark session
        leads_bronze: Bronze leads DataFrame
        
    Returns:
        Silver leads DataFrame with schema: id, first_name, last_name, company, status, email, phone, created_at, system_modstamp
    """
    logger.info("Transforming Salesforce leads to silver layer")
    
    silver_leads = leads_bronze.select(
        col("Id").alias("id"),
        col("FirstName").alias("first_name"),
        col("LastName").alias("last_name"),
        col("Company").alias("company"),
        col("Status").alias("status"),
        col("Email").alias("email"),
        col("Phone").alias("phone"),
        to_timestamp(col("CreatedDate")).alias("created_at"),
        to_timestamp(col("SystemModstamp")).alias("system_modstamp")
    ).filter(
        col("id").isNotNull()
    )
    
    logger.info(f"Silver leads created. Rows: {silver_leads.count()}")
    return silver_leads


def main():
    """Main entry point for Salesforce bronze to silver transformation."""
    parser = argparse.ArgumentParser(description="Salesforce bronze to silver transformation")
    parser.add_argument("--config", required=True, help="Configuration file path")
    parser.add_argument("--lake-root", required=True, help="Data lake root path")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_json_logging()
    logger.info("Starting Salesforce bronze to silver transformation")
    
    # Load configuration
    config = load_conf(args.config)
    lake_root = args.lake_root
    
    # Create Spark session with Delta config
    spark = get_spark_session(
        app_name="salesforce-bronze-to-silver",
        config=get_delta_config()
    )
    
    try:
        # Read Bronze layer data
        logger.info("Reading Bronze layer data")
        
        # Read Salesforce accounts from bronze
        accounts_bronze = spark.read.format("delta").load(f"{lake_root}/bronze/salesforce_accounts")
        
        # Read Salesforce leads from bronze
        leads_bronze = spark.read.format("delta").load(f"{lake_root}/bronze/salesforce_leads")
        
        # Transform to Silver layer
        logger.info("Transforming to Silver layer")
        
        # Transform accounts
        silver_accounts = transform_accounts_to_silver(spark, accounts_bronze)
        
        # Transform leads
        silver_leads = transform_leads_to_silver(spark, leads_bronze)
        
        # Write Silver layer tables
        logger.info("Writing Silver layer tables")
        
        # Write accounts
        silver_accounts.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{lake_root}/silver/salesforce_accounts")
        
        # Write leads
        silver_leads.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{lake_root}/silver/salesforce_leads")
        
        logger.info("Salesforce bronze to silver transformation completed successfully")
        
        # Show sample data
        logger.info("Sample Silver layer data:")
        silver_accounts.show(5, truncate=False)
        silver_leads.show(5, truncate=False)
        
    except Exception as e:
        logger.error(f"Salesforce bronze to silver transformation failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
