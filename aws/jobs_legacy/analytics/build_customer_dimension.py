#!/usr/bin/env python3
"""
Build Customer Dimension Table

Creates a comprehensive customer dimension with segmentation,
demographics, and business-ready attributes.
"""

import logging
import sys
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    current_date,
    current_timestamp,
    datediff,
    lit,
    split,
    when,
)

# Add project root to path
sys.path.append("/opt/airflow/dags/src")

from project_a.utils.config import load_config
from project_a.utils.io import read_delta, write_delta
from project_a.utils.spark_session import build_spark

logger = logging.getLogger(__name__)


def build_customer_dimension(spark: SparkSession, config: dict[str, Any]) -> None:
    """Build the customer dimension table."""

    logger.info("Building customer dimension...")

    try:
        # Read Silver layer data
        customers_df = read_delta(spark, f"{config['lake']['silver']}/customers_clean")

        # Create customer dimension
        customer_dim = customers_df.select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("email"),
            col("address"),
            col("city"),
            col("state"),
            col("country"),
            col("zip"),
            col("phone"),
            col("registration_date"),
            col("gender"),
            col("age"),
        )

        # Add computed fields
        customer_dim = customer_dim.withColumn(
            "customer_name", concat(col("first_name"), lit(" "), col("last_name"))
        )

        customer_dim = customer_dim.withColumn("email_domain", split(col("email"), "@").getItem(1))

        customer_dim = customer_dim.withColumn(
            "age_group",
            when(col("age") < 25, "18-24")
            .when(col("age") < 35, "25-34")
            .when(col("age") < 50, "35-49")
            .when(col("age") < 65, "50-64")
            .otherwise("65+"),
        )

        customer_dim = customer_dim.withColumn(
            "customer_segment",
            when(col("age") < 30, "Young").when(col("age") < 50, "Middle-aged").otherwise("Senior"),
        )

        customer_dim = customer_dim.withColumn(
            "days_since_registration", datediff(current_date(), col("registration_date"))
        )

        customer_dim = customer_dim.withColumn(
            "customer_tier",
            when(col("days_since_registration") > 365, "Established")
            .when(col("days_since_registration") > 180, "Growing")
            .otherwise("New"),
        )

        # Add geographic segments
        customer_dim = customer_dim.withColumn(
            "region",
            when(col("state").isin(["CA", "OR", "WA", "NV", "AZ"]), "West")
            .when(col("state").isin(["NY", "NJ", "CT", "MA", "VT", "NH", "ME"]), "Northeast")
            .when(col("state").isin(["TX", "OK", "AR", "LA", "MS", "AL", "TN", "KY"]), "South")
            .otherwise("Midwest"),
        )

        # Add metadata
        customer_dim = customer_dim.withColumn("created_at", current_timestamp())
        customer_dim = customer_dim.withColumn("updated_at", current_timestamp())

        # Write to Gold layer
        gold_path = f"{config['lake']['gold']}/dim_customers"
        write_delta(customer_dim, gold_path, mode="overwrite")

        logger.info(f"Customer dimension created: {customer_dim.count()} records")

    except Exception as e:
        logger.error(f"Failed to build customer dimension: {str(e)}")
        raise


def main():
    """Main entry point."""
    spark = build_spark("BuildCustomerDimension")
    config = load_config("aws/config/config-prod.yaml")

    try:
        build_customer_dimension(spark, config)
        logger.info("Customer dimension build completed successfully")
    except Exception as e:
        logger.error(f"Customer dimension build failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
