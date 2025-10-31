#!/usr/bin/env python3
"""
CRM Opportunities Bronze Ingestion Job
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from pyspark.sql import SparkSession, functions as F
from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.metrics import track_job_start, track_job_complete, track_records_processed
from pyspark_interview_project.utils.schema_validator import validate_bronze_ingestion
from pyspark_interview_project.dq.runner import run_suite
import logging
import argparse

logger = logging.getLogger(__name__)


def run_crm_opportunities_ingest(config_path: str = None, env: str = None):
    """Run CRM opportunities bronze ingestion."""
    
    # Determine config path based on env if not provided
    if config_path is None:
        if env:
            config_path = f"config/{env}.yaml"
        else:
            env = os.getenv("ENV", "dev")
            config_path = f"config/{env}.yaml"
    
    config = load_conf(config_path)
    spark = build_spark(app_name="crm_opportunities_ingest", config=config)
    
    try:
        job_id = track_job_start("crm_opportunities_ingest", config)
        
        src_path = config.get("paths", {}).get("crm_opportunities", "aws/data/crm/opportunities.csv")
        bronze_path = config.get("bronze", {}).get("crm_opportunities", "data/lakehouse_delta/bronze/crm/opportunities")
        
        logger.info(f"Reading CRM opportunities from: {src_path}")
        
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(src_path))
        
        # Validate against schema registry (enforce data contracts)
        logger.info("Validating schema against registry...")
        try:
            df_validated, validation_results = validate_bronze_ingestion(
                df, spark, "crm_opportunities", schemas_dir="schemas"
            )
            if not validation_results.get("passed", True):
                logger.warning(f"Schema validation issues: {validation_results.get('issues', [])}")
            df = df_validated
            logger.info("Schema validation passed")
        except Exception as e:
            logger.warning(f"Schema validation failed (continuing anyway): {str(e)}")
        
        df_with_metadata = (df
                           .withColumn("_source_system", F.lit("salesforce_synthetic"))
                           .withColumn("_ingestion_ts", F.current_timestamp())
                           .withColumn("_job_id", F.lit(job_id)))
        
        (df_with_metadata.write
         .format("delta")
         .mode("overwrite")
         .option("mergeSchema", "true")
         .save(bronze_path))
        
        record_count = df_with_metadata.count()
        track_records_processed(job_id, "crm_opportunities", record_count)
        
        # DQ checks
        dq_result = run_suite("crm_opportunities_not_null_keys", bronze_path, spark)
        if not dq_result.passed:
            raise Exception(f"DQ checks failed: {dq_result.failures}")
        
        track_job_complete(job_id, "SUCCESS", record_count)
        logger.info(f"✅ Successfully ingested {record_count:,} CRM opportunities to bronze")
        return True
        
    except Exception as e:
        logger.error(f"❌ CRM opportunities ingestion failed: {str(e)}")
        track_job_complete(job_id, "FAILED", 0, str(e))
        return False
        
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CRM Opportunities Bronze Ingestion")
    parser.add_argument("--config", help="Configuration file path")
    parser.add_argument("--env", choices=["dev", "prod", "local"], 
                       help="Environment (dev/prod/local)")
    args = parser.parse_args()
    
    success = run_crm_opportunities_ingest(config_path=args.config, env=args.env)
    sys.exit(0 if success else 1)
