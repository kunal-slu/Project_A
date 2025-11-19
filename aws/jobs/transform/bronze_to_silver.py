#!/usr/bin/env python3
"""
AWS EMR Bronze to Silver Transformation

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
    """Main entry point for EMR Bronze→Silver transformation."""
    parser = argparse.ArgumentParser(description="EMR Bronze to Silver ETL")
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
    logger.info(f"Starting EMR Bronze→Silver transformation (trace_id={trace_id})")
    
    # Build Spark session (EMR mode with Delta Lake)
    spark = build_spark(config=config)
    
    try:
        # Import and run the actual transformation
        # Use the same code as local execution - jobs/transform/bronze_to_silver.py
        # On EMR, we upload jobs/transform/bronze_to_silver.py to S3 and run it directly
        # This wrapper ensures we use the same entry point
        
        # Import the main function from the canonical job file
        # Note: On EMR, jobs/transform/bronze_to_silver.py is uploaded to S3
        # and executed as the entry point, so this wrapper may not be needed
        # But keeping it for consistency
        
        # Import the actual transformation function from the canonical job file
        # This ensures AWS and local use the exact same code
        from jobs.transform.bronze_to_silver import bronze_to_silver_complete
        from datetime import datetime
        
        run_date = datetime.utcnow().strftime("%Y-%m-%d")
        results = bronze_to_silver_complete(spark=spark, config=config, run_date=run_date)
        
        logger.info("✅ EMR Bronze→Silver transformation completed successfully")
        logger.info(f"   Results: {list(results.keys())}")
        
    except Exception as e:
        logger.error(f"❌ EMR Bronze→Silver transformation failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
