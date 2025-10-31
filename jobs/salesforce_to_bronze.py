#!/usr/bin/env python3
"""
Salesforce to Bronze ETL Job

This job extracts Salesforce data (Accounts, Contacts, Leads, Solutions) and lands it in the Bronze layer.
Supports both local development (CSV files) and AWS production (Salesforce API).
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime, date
from typing import Dict, Any, List
import boto3
from botocore.exceptions import ClientError

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_config
from pyspark_interview_project.extract.salesforce_accounts import extract_salesforce_accounts
from pyspark_interview_project.extract.salesforce_contacts import extract_salesforce_contacts
from pyspark_interview_project.extract.salesforce_leads import extract_salesforce_leads
from pyspark_interview_project.extract.salesforce_solutions import extract_salesforce_solutions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def upload_to_s3(df, bucket: str, key: str, region: str = "us-east-1") -> bool:
    """
    Upload DataFrame to S3 as Parquet.
    
    Args:
        df: Spark DataFrame to upload
        bucket: S3 bucket name
        key: S3 object key
        region: AWS region
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Convert Spark DataFrame to Pandas for S3 upload
        pandas_df = df.toPandas()
        
        # Write to local temp file
        temp_path = f"/tmp/{os.path.basename(key)}.parquet"
        pandas_df.to_parquet(temp_path, index=False)
        
        # Upload to S3
        s3_client = boto3.client('s3', region_name=region)
        s3_client.upload_file(temp_path, bucket, key)
        
        # Clean up temp file
        os.remove(temp_path)
        
        logger.info(f"✅ Uploaded {len(pandas_df)} records to s3://{bucket}/{key}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to upload to S3: {str(e)}")
        return False


def extract_and_upload_salesforce_data(spark, config: Dict[str, Any], ingest_date: str) -> bool:
    """
    Extract Salesforce data and upload to Bronze layer.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        ingest_date: Date in YYYY-MM-DD format
        
    Returns:
        bool: True if all extractions successful, False otherwise
    """
    try:
        # Get S3 configuration
        s3_config = config.get('aws', {}).get('s3', {})
        bucket = s3_config.get('data_lake_bucket', 'company-data-lake-dev')
        region = config.get('aws', {}).get('region', 'us-east-1')
        
        logger.info(f"🚀 Starting Salesforce extraction for {ingest_date}")
        logger.info(f"📦 Target bucket: {bucket}")
        
        # Define Salesforce objects to extract
        salesforce_objects = [
            ('accounts', extract_salesforce_accounts),
            ('contacts', extract_salesforce_contacts),
            ('leads', extract_salesforce_leads),
            ('solutions', extract_salesforce_solutions)
        ]
        
        success_count = 0
        
        for object_name, extract_func in salesforce_objects:
            try:
                logger.info(f"📊 Extracting {object_name}...")
                
                # Extract data
                df = extract_func(spark, config, object_name)
                
                if df.count() == 0:
                    logger.warning(f"⚠️ No data found for {object_name}")
                    continue
                
                # Define S3 key
                s3_key = f"bronze/crm/salesforce/{object_name}/ingest_date={ingest_date}/{object_name}.parquet"
                
                # Upload to S3
                if upload_to_s3(df, bucket, s3_key, region):
                    success_count += 1
                    logger.info(f"✅ {object_name} extraction completed successfully")
                else:
                    logger.error(f"❌ Failed to upload {object_name}")
                    
            except Exception as e:
                logger.error(f"❌ Failed to extract {object_name}: {str(e)}")
                continue
        
        logger.info(f"🎯 Salesforce extraction completed: {success_count}/{len(salesforce_objects)} objects successful")
        return success_count == len(salesforce_objects)
        
    except Exception as e:
        logger.error(f"❌ Salesforce extraction failed: {str(e)}")
        return False


def main():
    """Main execution function."""
    try:
        # Load configuration
        config_path = os.getenv('CONFIG_PATH', 'config/prod.yaml')
        config = load_config(config_path)
        
        # Get ingest date
        ingest_date = os.getenv('INGEST_DATE', date.today().isoformat())
        
        logger.info(f"🚀 Starting Salesforce to Bronze ETL")
        logger.info(f"📅 Ingest date: {ingest_date}")
        logger.info(f"⚙️ Config: {config_path}")
        
        # Build Spark session
        spark = build_spark(
            app_name="SalesforceToBronze",
            config=config
        )
        
        # Extract and upload Salesforce data
        success = extract_and_upload_salesforce_data(spark, config, ingest_date)
        
        if success:
            logger.info("🎉 Salesforce to Bronze ETL completed successfully!")
            sys.exit(0)
        else:
            logger.error("💥 Salesforce to Bronze ETL failed!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"💥 Fatal error: {str(e)}")
        sys.exit(1)
    
    finally:
        # Clean up Spark session
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()
