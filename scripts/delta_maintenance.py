#!/usr/bin/env python3
"""
Delta Lake Data Retention and Cleanup Script

This script demonstrates how to set up data retention policies and clean up
old data in Delta Lake tables based on configurable retention periods.
"""

import sys
import os
import logging

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from pyspark_interview_project.utils import get_spark_session, load_config
from pyspark_interview_project.delta_retention import (
    set_retention_policy,
    cleanup_old_partitions,
    vacuum_table,
    optimize_table,
    get_table_stats,
    cleanup_by_date_range,
    maintenance_routine,
    RETENTION_CONFIGS
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main function to demonstrate Delta Lake retention and cleanup."""

    # Load configuration
    config_path = "config/config-dev.yaml"
    cfg = load_config(config_path)

    # Create Spark session
    spark = get_spark_session(cfg)

    try:
        # Define table paths
        bronze_tables = [
            "data/lakehouse/bronze/customers_raw",
            "data/lakehouse/bronze/products_raw",
            "data/lakehouse/bronze/orders_raw",
            "data/lakehouse/bronze/returns_raw",
            "data/lakehouse/bronze/fx_rates",
            "data/lakehouse/bronze/inventory_snapshots"
        ]

        silver_tables = [
            "data/lakehouse/silver/dim_customers",
            "data/lakehouse/silver/dim_products",
            "data/lakehouse/silver/orders_cleansed",
            "data/lakehouse/silver/dim_customers_scd2"
        ]

        gold_tables = [
            "data/lakehouse/gold/fact_orders"
        ]

        all_tables = bronze_tables + silver_tables + gold_tables

        logger.info("=== Delta Lake Data Retention and Cleanup Demo ===")

        # 1. Set retention policies for different layers
        logger.info("\n1. Setting retention policies...")

        for table_path in bronze_tables:
            if os.path.exists(table_path):
                set_retention_policy(spark, table_path, RETENTION_CONFIGS["bronze"]["retention_days"])

        for table_path in silver_tables:
            if os.path.exists(table_path):
                set_retention_policy(spark, table_path, RETENTION_CONFIGS["silver"]["retention_days"])

        for table_path in gold_tables:
            if os.path.exists(table_path):
                set_retention_policy(spark, table_path, RETENTION_CONFIGS["gold"]["retention_days"])

        # 2. Get table statistics
        logger.info("\n2. Getting table statistics...")

        for table_path in all_tables:
            if os.path.exists(table_path):
                stats = get_table_stats(spark, table_path)
                if stats:
                    print(f"📊 {table_path}: {stats['row_count']} rows, {stats['size_mb']} MB")

        # 3. Optimize tables for better performance
        logger.info("\n3. Optimizing tables...")

        for table_path in all_tables:
            if os.path.exists(table_path):
                try:
                    # Use different optimization strategies based on table type
                    if "orders" in table_path or "fact" in table_path:
                        # Optimize fact tables with ZORDER on commonly queried columns
                        optimize_table(spark, table_path, ["customer_id", "product_id", "order_date"])
                    else:
                        # Basic optimization for dimension tables
                        optimize_table(spark, table_path)
                except Exception as e:
                    logger.warning(f"Failed to optimize {table_path}: {str(e)}")

        # 4. Clean up old partitions (example for orders table)
        logger.info("\n4. Cleaning up old partitions...")

        orders_table = "data/lakehouse/silver/orders_cleansed"
        if os.path.exists(orders_table):
            try:
                # Clean up orders older than 30 days
                cleanup_old_partitions(
                    spark,
                    orders_table,
                    "order_date",
                    retention_days=30
                )
            except Exception as e:
                logger.warning(f"Failed to cleanup partitions for {orders_table}: {str(e)}")

        # 5. Run VACUUM to remove old files
        logger.info("\n5. Running VACUUM operations...")

        for table_path in all_tables:
            if os.path.exists(table_path):
                try:
                    # Use different retention periods based on table type
                    if table_path in bronze_tables:
                        retain_hours = RETENTION_CONFIGS["bronze"]["vacuum_retain_hours"]
                    elif table_path in silver_tables:
                        retain_hours = RETENTION_CONFIGS["silver"]["vacuum_retain_hours"]
                    else:
                        retain_hours = RETENTION_CONFIGS["gold"]["vacuum_retain_hours"]

                    vacuum_table(spark, table_path, retain_hours)
                except Exception as e:
                    logger.warning(f"Failed to VACUUM {table_path}: {str(e)}")

        # 6. Demonstrate date range cleanup
        logger.info("\n6. Demonstrating date range cleanup...")

        # Example: Clean up data from a specific date range
        example_table = "data/lakehouse/silver/orders_cleansed"
        if os.path.exists(example_table):
            try:
                # Clean up data from 2023 (example)
                cleanup_by_date_range(
                    spark,
                    example_table,
                    "order_date",
                    "2023-01-01",
                    "2023-12-31"
                )
            except Exception as e:
                logger.warning(f"Failed to cleanup date range for {example_table}: {str(e)}")

        # 7. Run complete maintenance routine
        logger.info("\n7. Running complete maintenance routine...")

        maintenance_routine(
            spark,
            all_tables,
            retention_days=30,
            vacuum_retain_hours=168
        )

        logger.info("\n✅ Delta Lake maintenance completed successfully!")

        # 8. Show final statistics
        logger.info("\n8. Final table statistics:")

        total_rows = 0
        total_size_mb = 0

        for table_path in all_tables:
            if os.path.exists(table_path):
                stats = get_table_stats(spark, table_path)
                if stats:
                    total_rows += stats['row_count']
                    total_size_mb += stats['size_mb']
                    print(f"📈 {table_path}: {stats['row_count']:,} rows, {stats['size_mb']:.2f} MB")

        print(f"\n📊 Total: {total_rows:,} rows, {total_size_mb:.2f} MB")

    except Exception as e:
        logger.error(f"Maintenance failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
