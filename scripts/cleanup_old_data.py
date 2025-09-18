#!/usr/bin/env python3
"""
Simple Data Cleanup Script

This script provides simple commands to clean up old data from Delta Lake tables.
"""

import sys
import os
import logging
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from pyspark_interview_project.utils import get_spark_session, load_config

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def cleanup_old_orders(spark, days_to_keep=30):
    """Clean up orders older than specified days."""
    try:
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        cutoff_date_str = cutoff_date.strftime("%Y-%m-%d")

        # Read orders data
        orders_df = spark.read.format("delta").load("data/lakehouse/silver/orders_cleansed")

        # Filter out old orders
        filtered_orders = orders_df.filter(orders_df.order_date >= cutoff_date_str)

        # Write back filtered data
        filtered_orders.write.format("delta").mode("overwrite").save("data/lakehouse/silver/orders_cleansed")

        logger.info(f"Cleaned up orders older than {cutoff_date_str}")
        logger.info(f"Remaining orders: {filtered_orders.count()}")

    except Exception as e:
        logger.error(f"Failed to cleanup orders: {str(e)}")


def cleanup_old_fact_orders(spark, days_to_keep=30):
    """Clean up fact orders older than specified days."""
    try:
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        cutoff_date_str = cutoff_date.strftime("%Y-%m-%d")

        # Read fact orders data
        fact_df = spark.read.format("delta").load("data/lakehouse/gold/fact_orders")

        # Filter out old fact orders
        filtered_fact = fact_df.filter(fact_df.order_date >= cutoff_date_str)

        # Write back filtered data
        filtered_fact.write.format("delta").mode("overwrite").save("data/lakehouse/gold/fact_orders")

        logger.info(f"Cleaned up fact orders older than {cutoff_date_str}")
        logger.info(f"Remaining fact orders: {filtered_fact.count()}")

    except Exception as e:
        logger.error(f"Failed to cleanup fact orders: {str(e)}")


def cleanup_by_date_range(spark, table_path, date_column, start_date, end_date):
    """Clean up data within a specific date range."""
    try:
        # Read data
        df = spark.read.format("delta").load(table_path)

        # Filter out data in the specified range
        filtered_df = df.filter(
            (df[date_column] < start_date) | (df[date_column] > end_date)
        )

        # Write back filtered data
        filtered_df.write.format("delta").mode("overwrite").save(table_path)

        logger.info(f"Cleaned up data from {start_date} to {end_date} in {table_path}")
        logger.info(f"Remaining rows: {filtered_df.count()}")

    except Exception as e:
        logger.error(f"Failed to cleanup date range for {table_path}: {str(e)}")


def get_table_info(spark, table_path):
    """Get information about a table."""
    try:
        df = spark.read.format("delta").load(table_path)
        row_count = df.count()
        column_count = len(df.columns)

        # Get date range if date column exists
        date_info = ""
        if "order_date" in df.columns:
            min_date = df.agg({"order_date": "min"}).collect()[0][0]
            max_date = df.agg({"order_date": "max"}).collect()[0][0]
            date_info = f", Date range: {min_date} to {max_date}"

        logger.info(f"📊 {table_path}: {row_count:,} rows, {column_count} columns{date_info}")
        return row_count

    except Exception as e:
        logger.error(f"Failed to get info for {table_path}: {str(e)}")
        return 0


def main():
    """Main function for data cleanup operations."""

    # Load configuration
    config_path = "config/config-dev.yaml"
    cfg = load_config(config_path)

    # Create Spark session
    spark = get_spark_session(cfg)

    try:
        logger.info("=== Data Cleanup Operations ===")

        # Show current table information
        logger.info("\n📈 Current table statistics:")
        total_rows = 0

        tables = [
            "data/lakehouse/silver/orders_cleansed",
            "data/lakehouse/gold/fact_orders",
            "data/lakehouse/silver/dim_customers",
            "data/lakehouse/silver/dim_products"
        ]

        for table in tables:
            if os.path.exists(table):
                rows = get_table_info(spark, table)
                total_rows += rows

        logger.info(f"\n📊 Total rows across all tables: {total_rows:,}")

        # Example cleanup operations
        logger.info("\n🧹 Running cleanup operations...")

        # 1. Clean up orders older than 30 days
        cleanup_old_orders(spark, days_to_keep=30)

        # 2. Clean up fact orders older than 30 days
        cleanup_old_fact_orders(spark, days_to_keep=30)

        # 3. Clean up specific date range (example: remove 2023 data)
        cleanup_by_date_range(
            spark,
            "data/lakehouse/silver/orders_cleansed",
            "order_date",
            "2023-01-01",
            "2023-12-31"
        )

        # Show final statistics
        logger.info("\n📈 Final table statistics:")
        final_total = 0

        for table in tables:
            if os.path.exists(table):
                rows = get_table_info(spark, table)
                final_total += rows

        logger.info(f"\n📊 Final total rows: {final_total:,}")
        logger.info(f"🗑️  Rows removed: {total_rows - final_total:,}")

        logger.info("\n✅ Data cleanup completed successfully!")

    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
