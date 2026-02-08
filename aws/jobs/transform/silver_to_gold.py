#!/usr/bin/env python3
"""
AWS EMR Silver to Gold Transformation

This is the EMR entry point that uses the shared library code.
All business logic is in src/project_a/transform/
"""

import argparse
import logging
import sys

from project_a.pyspark_interview_project.utils.config_loader import load_config_resolved
from project_a.utils.logging import get_trace_id, setup_json_logging

# On EMR, the wheel is already in PYTHONPATH via --py-files
# No need to add src/ path
# Import shared library
from project_a.utils.spark_session import build_spark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main entry point for EMR Silver→Gold transformation."""
    parser = argparse.ArgumentParser(description="EMR Silver to Gold ETL")
    parser.add_argument("--env", default="dev", help="Environment name")
    parser.add_argument(
        "--config", required=True, help="S3 config file path (e.g., s3://bucket/config/dev.yaml)"
    )
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
        # Import and run the actual transformation from shared library
        # All business logic is in src/project_a/pyspark_interview_project/transform/
        # This ensures AWS and local use the exact same code
        from datetime import datetime

        from project_a.pyspark_interview_project.io.delta_writer import optimize_table, write_table
        from project_a.pyspark_interview_project.transform.gold_builders import (
            build_customer_360,
            build_dim_customer,
            build_dim_date,
            build_dim_product,
            build_fact_orders,
            build_product_performance,
        )
        from project_a.utils.path_resolver import resolve_data_path

        # Run the transformation (same logic as jobs/transform/silver_to_gold.py)
        datetime.utcnow().strftime("%Y-%m-%d")

        # Read silver tables
        silver_root = resolve_data_path(config, "silver")
        gold_root = resolve_data_path(config, "gold")
        tables_config = config.get("tables", {})
        silver_tables = tables_config.get("silver", {})
        gold_tables = tables_config.get("gold", {})

        # Read silver data
        df_customers_silver = spark.read.format("delta").load(
            f"{silver_root}/{silver_tables.get('customers', 'customers_silver')}"
        )
        df_orders_silver = spark.read.format("delta").load(
            f"{silver_root}/{silver_tables.get('orders', 'orders_silver')}"
        )
        df_products_silver = spark.read.format("delta").load(
            f"{silver_root}/{silver_tables.get('products', 'products_silver')}"
        )

        # Build gold tables
        df_dim_date = build_dim_date(spark, df_orders_silver)
        df_dim_customer = build_dim_customer(df_customers_silver, spark)
        df_dim_product = build_dim_product(df_products_silver, spark)
        df_fact_orders = build_fact_orders(
            df_orders_silver, df_dim_customer, df_dim_product, df_dim_date, spark
        )
        df_customer_360 = build_customer_360(df_customers_silver, df_orders_silver, spark)
        df_product_performance = build_product_performance(
            df_products_silver, df_orders_silver, spark
        )

        # Write gold tables
        write_table(
            df_dim_date,
            gold_root,
            gold_tables.get("dim_date", "dim_date"),
            config,
            partition_by=None,
        )
        write_table(
            df_dim_customer,
            gold_root,
            gold_tables.get("dim_customer", "dim_customer"),
            config,
            partition_by=["country"],
        )
        write_table(
            df_dim_product,
            gold_root,
            gold_tables.get("dim_product", "dim_product"),
            config,
            partition_by=None,
        )
        write_table(
            df_fact_orders,
            gold_root,
            gold_tables.get("fact_orders", "fact_orders"),
            config,
            partition_by=["order_date"],
        )
        write_table(
            df_customer_360,
            gold_root,
            gold_tables.get("customer_360", "customer_360"),
            config,
            partition_by=["country"],
        )
        write_table(
            df_product_performance,
            gold_root,
            gold_tables.get("product_performance", "product_performance"),
            config,
            partition_by=None,
        )

        # Optimize large fact tables
        optimize_table(
            spark,
            gold_root,
            gold_tables.get("fact_orders", "fact_orders"),
            config,
            z_order_by=["order_date", "customer_sk"],
        )

        results = {
            "dim_date": df_dim_date,
            "dim_customer": df_dim_customer,
            "dim_product": df_dim_product,
            "fact_orders": df_fact_orders,
            "customer_360": df_customer_360,
            "product_performance": df_product_performance,
        }

        logger.info("✅ EMR Silver→Gold transformation completed successfully")
        logger.info(f"   Results: {list(results.keys())}")

    except Exception as e:
        logger.error(f"❌ EMR Silver→Gold transformation failed: {e}", exc_info=True)
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
