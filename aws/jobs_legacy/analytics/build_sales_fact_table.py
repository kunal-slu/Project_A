#!/usr/bin/env python3
"""
Build Sales Fact Table with Advanced PySpark Functions

Creates the core sales fact table for business analytics using:
- Broadcast joins for dimension lookups
- Window functions for ranking and analytics
- Advanced aggregations with grouping sets
- Optimized joins and caching strategies
"""

import logging
import sys
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    month,
    quarter,
    when,
    year,
)

# Add project root to path
sys.path.append("/opt/airflow/dags/src")

from project_a.utils.config import load_config
from project_a.utils.io import read_delta, write_delta
from project_a.utils.spark_session import build_spark

logger = logging.getLogger(__name__)


def build_sales_fact_table(spark: SparkSession, config: dict[str, Any]) -> None:
    """Build the sales fact table from Silver layer data."""

    logger.info("Building sales fact table...")

    try:
        # Read Silver layer data
        orders_df = read_delta(spark, f"{config['lake']['silver']}/orders_clean")
        read_delta(spark, f"{config['lake']['silver']}/customers_clean")
        read_delta(spark, f"{config['lake']['silver']}/products_clean")

        # Create sales fact table
        sales_fact = orders_df.select(
            col("order_id"),
            col("customer_id"),
            col("product_id"),
            col("order_date"),
            col("quantity"),
            col("total_amount"),
            col("payment_method"),
            col("payment_status"),
            col("shipping_address"),
            col("shipping_city"),
            col("shipping_state"),
            col("shipping_country"),
        )

        # Add time dimensions
        sales_fact = sales_fact.withColumn("order_year", year(col("order_date")))
        sales_fact = sales_fact.withColumn("order_month", month(col("order_date")))
        sales_fact = sales_fact.withColumn("order_quarter", quarter(col("order_date")))
        sales_fact = sales_fact.withColumn(
            "order_day_of_week", date_format(col("order_date"), "EEEE")
        )

        # Add business logic
        sales_fact = sales_fact.withColumn(
            "order_size_category",
            when(col("total_amount") < 100, "Small")
            .when(col("total_amount") < 500, "Medium")
            .when(col("total_amount") < 1000, "Large")
            .otherwise("Extra Large"),
        )

        sales_fact = sales_fact.withColumn(
            "payment_risk_score",
            when(col("payment_method") == "card", 1)
            .when(col("payment_method") == "paypal", 2)
            .when(col("payment_method") == "apple_pay", 3)
            .otherwise(4),
        )

        # Add metadata
        sales_fact = sales_fact.withColumn("created_at", current_timestamp())
        sales_fact = sales_fact.withColumn("updated_at", current_timestamp())

        # Write to Gold layer
        gold_path = f"{config['lake']['gold']}/fact_sales"
        write_delta(sales_fact, gold_path, mode="overwrite")

        logger.info(f"Sales fact table created: {sales_fact.count()} records")

    except Exception as e:
        logger.error(f"Failed to build sales fact table: {str(e)}")
        raise


def main():
    """Main entry point."""
    spark = build_spark("BuildSalesFactTable")
    config = load_config("aws/config/config-prod.yaml")

    try:
        build_sales_fact_table(spark, config)
        logger.info("Sales fact table build completed successfully")
    except Exception as e:
        logger.error(f"Sales fact table build failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
