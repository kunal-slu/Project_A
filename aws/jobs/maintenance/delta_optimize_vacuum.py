#!/usr/bin/env python3
"""
Delta Lake Optimization and Maintenance Job

Runs OPTIMIZE and VACUUM operations on Delta tables to:
- Compact small files into larger files
- Remove old log files and data files
- Apply Z-ORDER optimization for query performance
- Control storage costs
"""

import logging
import os
import sys
from typing import Any

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../..", "src"))

from project_a.utils.config import load_config
from project_a.utils.spark_session import build_spark

logger = logging.getLogger(__name__)


def optimize_table(spark: SparkSession, table_path: str, zorder_columns: list[str] = None) -> None:
    """
    Optimize a Delta table by compacting files and applying Z-ORDER.

    Args:
        spark: Spark session
        table_path: Path to Delta table
        zorder_columns: Columns to use for Z-ORDER optimization
    """
    logger.info(f"Optimizing table: {table_path}")

    try:
        # Read Delta table
        spark.read.format("delta").load(table_path)

        # OPTIMIZE with Z-ORDER if specified
        if zorder_columns:
            zorder_expr = ", ".join(zorder_columns)
            spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({zorder_expr})")
            logger.info(f"Applied Z-ORDER on columns: {zorder_columns}")
        else:
            spark.sql(f"OPTIMIZE delta.`{table_path}`")
            logger.info("Applied file compaction")

        # Get optimization stats
        stats = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
        logger.info(f"Optimization complete - Files: {stats.get('numFiles', 'N/A')}")

    except Exception as e:
        logger.error(f"Failed to optimize table {table_path}: {str(e)}")
        raise


def vacuum_table(spark: SparkSession, table_path: str, retention_hours: int = 168) -> None:
    """
    Vacuum a Delta table to remove old files.

    Args:
        spark: Spark session
        table_path: Path to Delta table
        retention_hours: Retention period in hours (default: 7 days)
    """
    logger.info(f"Vacuuming table: {table_path} (retention: {retention_hours} hours)")

    try:
        spark.sql(f"VACUUM delta.`{table_path}` RETAIN {retention_hours} HOURS")
        logger.info(f"Vacuum complete for {table_path}")

    except Exception as e:
        logger.error(f"Failed to vacuum table {table_path}: {str(e)}")
        raise


def optimize_all_tables(spark: SparkSession, config: dict[str, Any]) -> None:
    """
    Optimize all specified Delta tables.

    Args:
        spark: Spark session
        config: Configuration dictionary
    """
    lake_config = config.get("lake", {})
    silver_base = lake_config.get("silver", {}).get("base", "s3://bucket/silver")
    gold_base = lake_config.get("gold", {}).get("base", "s3://bucket/gold")

    # Tables to optimize with their Z-ORDER columns
    optimization_config = config.get("maintenance", {}).get(
        "optimize",
        {
            "silver": {
                "dim_customer": ["customer_id"],
                "orders_clean": ["order_date", "customer_id"],
            },
            "gold": {
                "fact_sales": ["order_date", "customer_id", "product_id"],
                "dim_customer": ["customer_id"],
            },
        },
    )

    # Optimize Silver tables
    logger.info("Optimizing Silver layer tables...")
    for table, zorder_cols in optimization_config.get("silver", {}).items():
        table_path = f"{silver_base}/{table}"
        try:
            optimize_table(spark, table_path, zorder_cols)
        except Exception as e:
            logger.error(f"Failed to optimize {table_path}: {str(e)}")

    # Optimize Gold tables
    logger.info("Optimizing Gold layer tables...")
    for table, zorder_cols in optimization_config.get("gold", {}).items():
        table_path = f"{gold_base}/{table}"
        try:
            optimize_table(spark, table_path, zorder_cols)
        except Exception as e:
            logger.error(f"Failed to optimize {table_path}: {str(e)}")


def vacuum_all_tables(spark: SparkSession, config: dict[str, Any]) -> None:
    """
    Vacuum all specified Delta tables.

    Args:
        spark: Spark session
        config: Configuration dictionary
    """
    lake_config = config.get("lake", {})
    silver_base = lake_config.get("silver", {}).get("base", "s3://bucket/silver")
    gold_base = lake_config.get("gold", {}).get("base", "s3://bucket/gold")

    retention_hours = config.get("maintenance", {}).get("vacuum_retention_hours", 168)  # 7 days

    tables_to_vacuum = [
        f"{silver_base}/dim_customer",
        f"{silver_base}/orders_clean",
        f"{gold_base}/fact_sales",
        f"{gold_base}/dim_customer",
    ]

    logger.info(f"Vacuuming tables (retention: {retention_hours} hours)...")
    for table_path in tables_to_vacuum:
        try:
            vacuum_table(spark, table_path, retention_hours)
        except Exception as e:
            logger.error(f"Failed to vacuum {table_path}: {str(e)}")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Delta Lake optimization and maintenance")
    parser.add_argument("--config", default="config/prod.yaml", help="Configuration file")
    parser.add_argument("--optimize-only", action="store_true", help="Only run OPTIMIZE")
    parser.add_argument("--vacuum-only", action="store_true", help="Only run VACUUM")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    spark = build_spark("DeltaOptimizeVacuum")
    config = load_config(args.config)

    try:
        if not args.vacuum_only:
            optimize_all_tables(spark, config)

        if not args.optimize_only:
            vacuum_all_tables(spark, config)

        logger.info("Delta Lake maintenance completed successfully")

    except Exception as e:
        logger.error(f"Maintenance job failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
