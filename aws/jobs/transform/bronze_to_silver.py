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
from project_a.pyspark_interview_project.utils.config_loader import load_config_resolved
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
        
        # Import and run the actual transformation from shared library
        # All business logic is in src/project_a/pyspark_interview_project/transform/
        # This ensures AWS and local use the exact same code
        from project_a.pyspark_interview_project.transform.bronze_loaders import (
            load_crm_bronze_data,
            load_snowflake_bronze_data,
            load_redshift_behavior_bronze_data,
            load_kafka_bronze_data
        )
        from project_a.pyspark_interview_project.transform.silver_builders import (
            build_customers_silver,
            build_orders_silver,
            build_products_silver,
            build_behavior_silver
        )
        from project_a.extract.fx_json_reader import read_fx_rates_from_bronze
        from project_a.pyspark_interview_project.io.delta_writer import write_table
        from project_a.utils.path_resolver import resolve_data_path
        from datetime import datetime
        
        # Run the transformation (same logic as jobs/transform/bronze_to_silver.py)
        run_date = datetime.utcnow().strftime("%Y-%m-%d")
        
        # Load bronze data
        df_accounts, df_contacts, df_opps = load_crm_bronze_data(spark, config)
        df_customers, df_orders, df_products = load_snowflake_bronze_data(spark, config)
        df_behavior = load_redshift_behavior_bronze_data(spark, config)
        df_fx = read_fx_rates_from_bronze(spark, resolve_data_path(config, "bronze"))
        df_kafka = load_kafka_bronze_data(spark, config)
        
        # Build silver tables
        df_customers_silver = build_customers_silver(df_customers, df_accounts, df_contacts, df_opps, df_behavior)
        df_orders_silver = build_orders_silver(df_orders, df_fx, config)
        df_products_silver = build_products_silver(df_products)
        df_behavior_silver = build_behavior_silver(df_behavior)
        
        # Write silver tables
        silver_root = resolve_data_path(config, "silver")
        tables_config = config.get("tables", {}).get("silver", {})
        
        write_table(df_customers_silver, f"{silver_root}/{tables_config.get('customers', 'customers_silver')}", config)
        write_table(df_orders_silver, f"{silver_root}/{tables_config.get('orders', 'orders_silver')}", config)
        write_table(df_products_silver, f"{silver_root}/{tables_config.get('products', 'products_silver')}", config)
        write_table(df_behavior_silver, f"{silver_root}/{tables_config.get('behavior', 'customer_behavior_silver')}", config)
        
        results = {
            "customers": df_customers_silver,
            "orders": df_orders_silver,
            "products": df_products_silver,
            "behavior": df_behavior_silver
        }
        
        logger.info("✅ EMR Bronze→Silver transformation completed successfully")
        logger.info(f"   Results: {list(results.keys())}")
        
    except Exception as e:
        logger.error(f"❌ EMR Bronze→Silver transformation failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
        print("Job completed successfully")  # shows up in logs
        sys.exit(0)  # ✅ tell EMR this step SUCCEEDED
    except Exception as e:
        # this will show a real stack trace in logs
        import traceback
        print("ERROR in job:", e)
        traceback.print_exc()
        sys.exit(1)  # ❌ only fail step if we really hit an exception
