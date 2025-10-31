#!/usr/bin/env python3
"""
Salesforce to Bronze Layer Ingestion

This job extracts data from Salesforce CRM and writes it to the Bronze layer
in S3 with proper partitioning and schema validation.
"""

import os
import sys
import logging
import pandas as pd
import boto3
from datetime import date, datetime
from typing import Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col

# Add project root to path
sys.path.append('/opt/airflow/dags/src')

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_config
from pyspark_interview_project.extract.salesforce_accounts import extract_salesforce_accounts
from pyspark_interview_project.extract.salesforce_contacts import extract_salesforce_contacts
from pyspark_interview_project.extract.salesforce_opportunities import extract_salesforce_opportunities

logger = logging.getLogger(__name__)


def upload_to_s3(df, logical_name: str, config: Dict[str, Any], ingest_date: str) -> None:
    """Upload DataFrame to S3 Bronze layer with proper partitioning."""
    
    try:
        # Convert Spark DataFrame to Pandas for S3 upload
        pandas_df = df.toPandas()
        
        # Create local parquet file
        local_path = f"/tmp/{logical_name}_{ingest_date}.parquet"
        pandas_df.to_parquet(local_path, index=False)
        
        # S3 key with proper partitioning
        s3_key = f"bronze/crm/salesforce/{logical_name}/ingest_date={ingest_date}/{logical_name}.parquet"
        
        # Upload to S3
        s3_client = boto3.client('s3', region_name=config['aws']['region'])
        bucket_name = config['aws']['s3']['data_lake_bucket']
        
        s3_client.upload_file(local_path, bucket_name, s3_key)
        
        logger.info(f"Uploaded {logical_name} → s3://{bucket_name}/{s3_key} rows={len(pandas_df)}")
        
        # Clean up local file
        os.remove(local_path)
        
    except Exception as e:
        logger.error(f"Failed to upload {logical_name} to S3: {str(e)}")
        raise


def validate_data_quality(df, table_name: str) -> bool:
    """Validate data quality for Salesforce data."""
    
    logger.info(f"Running data quality checks for {table_name}")
    
    try:
        # Basic row count check
        row_count = df.count()
        if row_count == 0:
            logger.error(f"Data quality check failed: {table_name} has 0 rows")
            return False
        
        # Table-specific quality checks
        if table_name == "contacts":
            # Check for non-null email
            null_email_count = df.filter(col("email").isNull()).count()
            if null_email_count > 0:
                logger.warning(f"Data quality warning: {null_email_count} contacts have null email")
            
            # Check email format
            invalid_email_count = df.filter(~col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")).count()
            if invalid_email_count > 0:
                logger.warning(f"Data quality warning: {invalid_email_count} contacts have invalid email format")
        
        elif table_name == "opportunities":
            # Check for non-null stage_name
            null_stage_count = df.filter(col("stage_name").isNull()).count()
            if null_stage_count > 0:
                logger.error(f"Data quality check failed: {null_stage_count} opportunities have null stage_name")
                return False
            
            # Check for positive amount
            negative_amount_count = df.filter(col("amount") <= 0).count()
            if negative_amount_count > 0:
                logger.warning(f"Data quality warning: {negative_amount_count} opportunities have non-positive amount")
        
        elif table_name == "accounts":
            # Check for non-null account_name
            null_name_count = df.filter(col("account_name").isNull()).count()
            if null_name_count > 0:
                logger.error(f"Data quality check failed: {null_name_count} accounts have null account_name")
                return False
        
        logger.info(f"Data quality checks passed for {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Data quality check failed for {table_name}: {str(e)}")
        return False


def main():
    """Main entry point for Salesforce to Bronze ingestion."""
    
    logger.info("Starting Salesforce to Bronze ingestion")
    
    # Load configuration
    config_path = os.getenv('CONFIG_PATH', 'aws/config/config-prod.yaml')
    config = load_config(config_path)
    
    # Initialize Spark session
    spark = build_spark("SalesforceToBronze", config)
    
    try:
        # Get ingest date
        ingest_date = date.today().isoformat()
        logger.info(f"Ingesting Salesforce data for date: {ingest_date}")
        
        # Extract Salesforce data
        logger.info("Extracting Salesforce accounts...")
        accounts_df = extract_salesforce_accounts(spark, config, "salesforce_accounts")
        
        logger.info("Extracting Salesforce contacts...")
        contacts_df = extract_salesforce_contacts(spark, config, "salesforce_contacts")
        
        logger.info("Extracting Salesforce opportunities...")
        opportunities_df = extract_salesforce_opportunities(spark, config, "salesforce_opportunities")
        
        # Validate data quality
        if not validate_data_quality(accounts_df, "accounts"):
            raise Exception("Data quality check failed for accounts")
        
        if not validate_data_quality(contacts_df, "contacts"):
            raise Exception("Data quality check failed for contacts")
        
        if not validate_data_quality(opportunities_df, "opportunities"):
            raise Exception("Data quality check failed for opportunities")
        
        # Upload to S3 Bronze layer
        logger.info("Uploading accounts to Bronze layer...")
        upload_to_s3(accounts_df, "accounts", config, ingest_date)
        
        logger.info("Uploading contacts to Bronze layer...")
        upload_to_s3(contacts_df, "contacts", config, ingest_date)
        
        logger.info("Uploading opportunities to Bronze layer...")
        upload_to_s3(opportunities_df, "opportunities", config, ingest_date)
        
        logger.info("Salesforce to Bronze ingestion completed successfully")
        
        # Emit lineage and metrics
        try:
            from pyspark_interview_project.jobs.emit_lineage_and_metrics import emit_lineage_and_metrics
            
            run_id = f"salesforce_bronze_{ingest_date}_{datetime.now().strftime('%H%M%S')}"
            
            # Emit lineage for each table
            for table_name, df in [("accounts", accounts_df), ("contacts", contacts_df), ("opportunities", opportunities_df)]:
                emit_lineage_and_metrics(
                    spark=spark,
                    config=config,
                    run_id=run_id,
                    task_name=f"salesforce_{table_name}_ingestion",
                    source_path=f"salesforce_api/{table_name}",
                    target_path=f"s3://{config['aws']['s3']['data_lake_bucket']}/bronze/crm/salesforce/{table_name}/ingest_date={ingest_date}",
                    row_count=df.count(),
                    status="SUCCESS",
                    start_time=datetime.now(),
                    end_time=datetime.now(),
                    additional_metadata={"source_system": "Salesforce CRM", "object_type": table_name}
                )
        except Exception as e:
            logger.warning(f"Failed to emit lineage: {str(e)}")
        
    except Exception as e:
        logger.error(f"Salesforce to Bronze ingestion failed: {str(e)}")
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
