#!/usr/bin/env python3
"""
AWS EMR Silver to Gold Transformation

This is the EMR entry point that uses the shared library code.
All business logic is in src/project_a/transform/
"""
import sys
import os
from pathlib import Path

# On EMR, the wheel is already in PYTHONPATH via --py-files
# No need to add src/ path

# Import shared library
from project_a.utils.spark_session import build_spark
from project_a.config_loader import load_config_resolved
from project_a.utils.logging import setup_json_logging, get_trace_id

import argparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main entry point for EMR Silver→Gold transformation."""
    parser = argparse.ArgumentParser(description="EMR Silver to Gold ETL")
    parser.add_argument("--env", default="dev", help="Environment name")
    parser.add_argument("--config", required=True, help="S3 config file path (e.g., s3://bucket/config/dev.yaml)")
    args = parser.parse_args()
    
    # Load config from S3
    config = load_config_resolved(args.config)
    
    # Set environment
    config["environment"] = "emr"
    config["env"] = args.env
    
    # Setup logging
    trace_id = get_trace_id()
    setup_json_logging(include_trace_id=True)
    logger.info(f"Starting EMR Silver→Gold transformation (trace_id={trace_id})")
    
    # Build Spark session (EMR mode with Delta Lake)
    spark = build_spark(config=config)
    
    try:
        # Import the actual transformation function from the canonical job file
        # This ensures AWS and local use the exact same code
        from jobs.transform.silver_to_gold import silver_to_gold_complete
        from datetime import datetime
        
        run_date = datetime.utcnow().strftime("%Y-%m-%d")
        results = silver_to_gold_complete(spark=spark, config=config, run_date=run_date)
        
        logger.info("✅ EMR Silver→Gold transformation completed successfully")
        logger.info(f"   Results: {list(results.keys())}")
        
    except Exception as e:
        logger.error(f"❌ EMR Silver→Gold transformation failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
