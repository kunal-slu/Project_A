#!/usr/bin/env python3
"""
Silver Layer Data Quality Check

Validates data quality for Silver tables including referential integrity.
"""
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from project_a.utils import build_spark
from project_a.config_loader import load_config_resolved
from project_a.dq.dq_runner import run_dq_suite
from project_a.utils.path_resolver import silver_path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Check Silver data quality."""
    config = load_config_resolved('aws/config/config-prod.yaml')
    spark = build_spark(config)
    
    try:
        logger.info("Running Silver layer DQ checks")
        
        # Check each Silver table
        silver_tables = [
            "customers",
            "orders", 
            "fx_rates"
        ]
        
        failed_tables = []
        
        for table in silver_tables:
            logger.info(f"Checking DQ for: {table}")
            
            try:
                # Read Silver table
                df = spark.read.format("delta").load(
                    silver_path(config['lake']['root'], table)
                )
                
                # Run DQ suite from YAML
                suite_name = f"silver_{table}"
                run_dq_suite(spark, df, suite_name, config)
                
                logger.info(f"✅ DQ passed for: {table}")
                
            except Exception as e:
                logger.error(f"❌ DQ failed for {table}: {e}")
                failed_tables.append(table)
        
        # Fail if any table failed DQ
        if failed_tables:
            logger.error(f"DQ checks failed for: {failed_tables}")
            sys.exit(1)
        
        logger.info("✅ All Silver DQ checks passed")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

