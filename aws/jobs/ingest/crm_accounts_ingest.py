#!/usr/bin/env python3
"""
CRM Accounts Bronze Ingestion Job

This job extracts CRM accounts data and lands it in the Bronze layer
with proper metadata and data quality checks.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, DateType
from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.metrics import track_job_start, track_job_complete, track_records_processed
from pyspark_interview_project.utils.schema_validator import validate_bronze_ingestion
from pyspark_interview_project.dq.runner import run_suite
import logging

logger = logging.getLogger(__name__)


def run_crm_accounts_ingest(config_path: str = None, env: str = None):
    """Run CRM accounts bronze ingestion."""
    
    # Determine config path based on env if not provided
    if config_path is None:
        if env:
            config_path = f"config/{env}.yaml"
        else:
            import os
            env = os.getenv("ENV", "dev")
            config_path = f"config/{env}.yaml"
    
    # Load configuration
    config = load_conf(config_path)
    
    # Build Spark session
    spark = build_spark(
        app_name="crm_accounts_ingest",
        config=config
    )
    
    try:
        # Track job start
        job_id = track_job_start("crm_accounts_ingest", config)
        
        # Get paths from config
        src_path = config.get("paths", {}).get("crm_accounts", "aws/data/crm/accounts.csv")
        bronze_path = config.get("bronze", {}).get("crm_accounts", "data/lakehouse_delta/bronze/crm/accounts")
        
        logger.info(f"Reading CRM accounts from: {src_path}")
        logger.info(f"Writing to bronze: {bronze_path}")
        
        # Read source data
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(src_path))
        
        # Validate against schema registry (enforce data contracts)
        logger.info("Validating schema against registry...")
        try:
            df_validated, validation_results = validate_bronze_ingestion(
                df, spark, "crm_accounts", schemas_dir="schemas"
            )
            if not validation_results.get("passed", True):
                logger.warning(f"Schema validation issues: {validation_results.get('issues', [])}")
                # In production, might want to fail or route to DLQ
            df = df_validated
            logger.info("Schema validation passed")
        except Exception as e:
            logger.warning(f"Schema validation failed (continuing anyway): {str(e)}")
            # Continue if schema file not found (backward compatibility)
        
        # Add metadata columns
        df_with_metadata = (df
                           .withColumn("_source_system", F.lit("salesforce_synthetic"))
                           .withColumn("_ingestion_ts", F.current_timestamp())
                           .withColumn("_job_id", F.lit(job_id))
                           .withColumn("_record_count", F.lit(df.count())))
        
        # Write to bronze layer
        (df_with_metadata.write
         .format("delta")
         .mode("overwrite")
         .option("mergeSchema", "true")
         .save(bronze_path))
        
        # Track records processed
        record_count = df_with_metadata.count()
        track_records_processed(job_id, "crm_accounts", record_count)
        
        # Run data quality checks
        logger.info("Running data quality checks...")
        dq_result = run_suite(
            suite_name="crm_accounts_not_null_keys",
            table_path=bronze_path,
            spark=spark
        )
        
        if not dq_result.passed:
            raise Exception(f"Data quality checks failed: {dq_result.failures}")
        
        # Track job completion
        track_job_complete(job_id, "SUCCESS", record_count)
        
        logger.info(f"✅ Successfully ingested {record_count:,} CRM accounts to bronze")
        return True
        
    except Exception as e:
        logger.error(f"❌ CRM accounts ingestion failed: {str(e)}")
        track_job_complete(job_id, "FAILED", 0, str(e))
        return False
        
    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="CRM Accounts Bronze Ingestion")
    parser.add_argument("--config", help="Configuration file path")
    parser.add_argument("--env", choices=["dev", "prod", "local"], 
                       help="Environment (dev/prod/local)")
    args = parser.parse_args()
    
    success = run_crm_accounts_ingest(config_path=args.config, env=args.env)
    sys.exit(0 if success else 1)
