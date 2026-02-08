#!/usr/bin/env python3
"""
Build Marketing Attribution Table

Creates marketing attribution analysis with campaign performance,
customer acquisition channels, and ROI metrics.
"""

import logging
import sys
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, current_timestamp, when
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum

# Add project root to path
sys.path.append("/opt/airflow/dags/src")

from project_a.utils.config import load_config
from project_a.utils.io import read_delta, write_delta
from project_a.utils.spark_session import build_spark

logger = logging.getLogger(__name__)


def build_marketing_attribution(spark: SparkSession, config: dict[str, Any]) -> None:
    """Build marketing attribution analysis."""

    logger.info("Building marketing attribution...")

    try:
        # Read Silver layer data
        orders_df = read_delta(spark, f"{config['lake']['silver']}/orders_clean")
        customers_df = read_delta(spark, f"{config['lake']['silver']}/customers_clean")

        # Simulate marketing data (in real scenario, this would come from marketing platforms)
        marketing_data = spark.createDataFrame(
            [
                ("C00001", "google_ads", "summer_sale_2024", 150.0, "2024-06-01"),
                ("C00002", "facebook_ads", "brand_awareness", 200.0, "2024-05-15"),
                ("C00003", "email_campaign", "newsletter_signup", 50.0, "2024-07-01"),
                ("C00004", "organic_search", "seo_optimization", 0.0, "2024-04-01"),
                ("C00005", "referral", "customer_referral", 100.0, "2024-06-15"),
            ],
            [
                "customer_id",
                "acquisition_channel",
                "campaign_name",
                "acquisition_cost",
                "first_touch_date",
            ],
        )

        # Join with customer data
        attribution_df = customers_df.join(marketing_data, "customer_id", "left")

        # Fill missing marketing data with defaults
        attribution_df = attribution_df.fillna(
            {"acquisition_channel": "unknown", "campaign_name": "organic", "acquisition_cost": 0.0}
        )

        # Join with order data for revenue analysis
        customer_revenue = orders_df.groupBy("customer_id").agg(
            spark_sum("total_amount").alias("total_revenue"),
            count("order_id").alias("total_orders"),
            avg("total_amount").alias("avg_order_value"),
            spark_max("order_date").alias("last_order_date"),
            spark_min("order_date").alias("first_order_date"),
        )

        attribution_df = attribution_df.join(customer_revenue, "customer_id", "left")

        # Calculate marketing metrics
        attribution_df = attribution_df.withColumn(
            "customer_lifetime_value", col("total_revenue") - col("acquisition_cost")
        )

        attribution_df = attribution_df.withColumn(
            "roi",
            when(
                col("acquisition_cost") > 0,
                (col("total_revenue") - col("acquisition_cost")) / col("acquisition_cost"),
            ).otherwise(0.0),
        )

        attribution_df = attribution_df.withColumn(
            "payback_period_days",
            when(
                col("acquisition_cost") > 0, col("acquisition_cost") / (col("total_revenue") / 365)
            ).otherwise(0),
        )

        # Add channel performance tiers
        attribution_df = attribution_df.withColumn(
            "channel_tier",
            when(col("acquisition_channel").isin(["google_ads", "facebook_ads"]), "Paid")
            .when(col("acquisition_channel").isin(["email_campaign", "referral"]), "Owned")
            .when(col("acquisition_channel").isin(["organic_search", "social_media"]), "Earned")
            .otherwise("Unknown"),
        )

        # Add metadata
        attribution_df = attribution_df.withColumn("created_at", current_timestamp())
        attribution_df = attribution_df.withColumn("updated_at", current_timestamp())

        # Write to Gold layer
        gold_path = f"{config['lake']['gold']}/marketing_attribution"
        write_delta(attribution_df, gold_path, mode="overwrite")

        logger.info(f"Marketing attribution created: {attribution_df.count()} records")

    except Exception as e:
        logger.error(f"Failed to build marketing attribution: {str(e)}")
        raise


def main():
    """Main entry point."""
    spark = build_spark("BuildMarketingAttribution")
    config = load_config("aws/config/config-prod.yaml")

    try:
        build_marketing_attribution(spark, config)
        logger.info("Marketing attribution build completed successfully")
    except Exception as e:
        logger.error(f"Marketing attribution build failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
