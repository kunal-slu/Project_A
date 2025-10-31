#!/usr/bin/env python3
"""
Bronze Layer Data Quality Check

Validates data quality for Bronze tables and fails if expectations not met.
"""
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark_interview_project.utils import build_spark
from pyspark_interview_project.config_loader import load_config_resolved
from pyspark_interview_project.dq.dq_runner import run_dq_suite
from pyspark_interview_project.utils.path_resolver import bronze_path
from pyspark_interview_project.utils.dq import validate_schema_contract

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Check Bronze data quality."""
    config = load_config_resolved('aws/config/config-prod.yaml')
    spark = build_spark(config)
    
    try:
        logger.info("Running Bronze layer DQ checks")
        
        # Check each Bronze table
        bronze_tables = [
            "crm.accounts",
            "crm.contacts",
            "crm.opportunities",
            "snowflake.orders",
            "redshift.customer_behavior",
            "fx.fx_rates",
            "fx.financial_metrics"
        ]
        
        failed_tables = []
        
        for table in bronze_tables:
            logger.info(f"Checking DQ for: {table}")
            
            try:
                # Validate schema contract
                schema_name = table.replace(".", "_") + "_bronze"
                schema_contract_path = f"aws/config/schema_definitions/{schema_name}.json"
                
                df = spark.read.format("delta").load(
                    bronze_path(config['lake']['root'], *table.split("."))
                )
                
                # Run schema validation
                validate_schema_contract(df, schema_contract_path)
                
                # Run DQ suite
                run_dq_suite(spark, df, table, config)
                
                logger.info(f"✅ DQ passed for: {table}")
                
            except Exception as e:
                logger.error(f"❌ DQ failed for {table}: {e}")
                failed_tables.append(table)
        
        # Fail if any table failed DQ
        if failed_tables:
            logger.error(f"DQ checks failed for: {failed_tables}")
            sys.exit(1)
        
        logger.info("✅ All Bronze DQ checks passed")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

