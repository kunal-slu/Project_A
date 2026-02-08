#!/usr/bin/env python3
"""
Update Customer Dimension SCD2

Implements Slowly Changing Dimension Type 2 for customer data,
maintaining historical truth and tracking changes over time.
"""

import logging
import sys
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    when,
)
from pyspark.sql.functions import max as spark_max

# Add project root to path
sys.path.append("/opt/airflow/dags/src")

from project_a.utils.config import load_config
from project_a.utils.io import read_delta, write_delta
from project_a.utils.spark_session import build_spark

logger = logging.getLogger(__name__)


def update_customer_dimension_scd2(spark: SparkSession, config: dict[str, Any]) -> None:
    """Update customer dimension using SCD2 pattern."""

    logger.info("Updating customer dimension with SCD2...")

    try:
        # Read current customer data from Silver layer
        new_customers_df = read_delta(spark, f"{config['lake']['silver']}/customers_clean")

        # Read existing customer dimension (if exists)
        try:
            existing_customers_df = read_delta(
                spark, f"{config['lake']['gold']}/dim_customers_scd2"
            )
            logger.info(
                f"Found existing customer dimension: {existing_customers_df.count()} records"
            )
        except:
            logger.info("No existing customer dimension found, creating new one")
            existing_customers_df = spark.createDataFrame(
                [],
                """
                customer_id STRING,
                first_name STRING,
                last_name STRING,
                email STRING,
                address STRING,
                city STRING,
                state STRING,
                country STRING,
                zip INTEGER,
                phone STRING,
                registration_date TIMESTAMP,
                gender STRING,
                age INTEGER,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP,
                is_current BOOLEAN,
                change_reason STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            """,
            )

        # Prepare new customer data with SCD2 fields
        new_customers_scd2 = new_customers_df.select(
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
            lit(current_timestamp()).alias("valid_from"),
            lit(None).cast("timestamp").alias("valid_to"),
            lit(True).alias("is_current"),
            lit("initial_load").alias("change_reason"),
            lit(current_timestamp()).alias("created_at"),
            lit(current_timestamp()).alias("updated_at"),
        )

        # If no existing data, just write new data
        if existing_customers_df.count() == 0:
            write_delta(
                new_customers_scd2, f"{config['lake']['gold']}/dim_customers_scd2", mode="overwrite"
            )
            logger.info(f"Initial customer dimension created: {new_customers_scd2.count()} records")
            return

        # Identify changed records
        # Join on customer_id and compare key fields
        key_fields = [
            "first_name",
            "last_name",
            "email",
            "address",
            "city",
            "state",
            "country",
            "phone",
            "age",
        ]

        # Create comparison logic
        comparison_conditions = []
        for field in key_fields:
            comparison_conditions.append(
                (col(f"new.{field}") != col(f"existing.{field}"))
                | (col(f"new.{field}").isNull() & col(f"existing.{field}").isNotNull())
                | (col(f"new.{field}").isNotNull() & col(f"existing.{field}").isNull())
            )

        # Combine all conditions with OR
        has_changes = comparison_conditions[0]
        for condition in comparison_conditions[1:]:
            has_changes = has_changes | condition

        # Find changed records
        changed_records = (
            new_customers_scd2.alias("new")
            .join(
                existing_customers_df.filter(col("is_current")).alias("existing"),
                "customer_id",
                "inner",
            )
            .filter(has_changes)
            .select(col("new.*"))
        )

        logger.info(f"Found {changed_records.count()} changed customer records")

        if changed_records.count() > 0:
            # Close existing records for changed customers
            customers_to_close = changed_records.select("customer_id")

            # Update existing records to close them
            existing_customers_updated = (
                existing_customers_df.join(customers_to_close, "customer_id", "left")
                .withColumn(
                    "is_current",
                    when(col("customer_id").isNotNull(), False).otherwise(col("is_current")),
                )
                .withColumn(
                    "valid_to",
                    when(col("customer_id").isNotNull(), current_timestamp()).otherwise(
                        col("valid_to")
                    ),
                )
                .withColumn(
                    "updated_at",
                    when(col("customer_id").isNotNull(), current_timestamp()).otherwise(
                        col("updated_at")
                    ),
                )
                .drop("customer_id")
            )

            # Prepare new records for changed customers
            new_records_scd2 = changed_records.withColumn("change_reason", lit("data_update"))

            # Combine all records
            all_records = existing_customers_updated.unionByName(new_records_scd2)

            # Add new customers (not in existing dimension)
            new_customers_only = new_customers_scd2.join(
                existing_customers_df.select("customer_id").distinct(), "customer_id", "left_anti"
            )

            if new_customers_only.count() > 0:
                logger.info(f"Adding {new_customers_only.count()} new customers")
                all_records = all_records.unionByName(new_customers_only)

            # Write updated dimension
            write_delta(
                all_records, f"{config['lake']['gold']}/dim_customers_scd2", mode="overwrite"
            )

            logger.info(f"Customer dimension updated: {all_records.count()} total records")

            # Log SCD2 statistics
            current_records = all_records.filter(col("is_current"))
            historical_records = all_records.filter(not col("is_current"))

            logger.info(f"Current records: {current_records.count()}")
            logger.info(f"Historical records: {historical_records.count()}")

        else:
            logger.info("No customer changes detected")

    except Exception as e:
        logger.error(f"Failed to update customer dimension SCD2: {str(e)}")
        raise


def analyze_scd2_changes(spark: SparkSession, config: dict[str, Any]) -> None:
    """Analyze SCD2 changes for reporting."""

    logger.info("Analyzing SCD2 changes...")

    try:
        customers_scd2 = read_delta(spark, f"{config['lake']['gold']}/dim_customers_scd2")

        # Analyze change patterns
        change_analysis = customers_scd2.groupBy("change_reason").agg(
            spark_max("created_at").alias("last_change"),
            spark_max("updated_at").alias("last_update"),
        )

        logger.info("SCD2 Change Analysis:")
        change_analysis.show()

        # Analyze change frequency by customer
        customer_changes = customers_scd2.groupBy("customer_id").agg(
            spark_max("created_at").alias("first_record"),
            spark_max("updated_at").alias("last_update"),
            spark_max("is_current").alias("has_current_record"),
        )

        customers_with_multiple_versions = customer_changes.filter(
            col("first_record") != col("last_update")
        )

        logger.info(f"Customers with multiple versions: {customers_with_multiple_versions.count()}")

    except Exception as e:
        logger.error(f"Failed to analyze SCD2 changes: {str(e)}")


def main():
    """Main entry point."""
    spark = build_spark("UpdateCustomerDimensionSCD2")
    config = load_config("aws/config/config-prod.yaml")

    try:
        update_customer_dimension_scd2(spark, config)
        analyze_scd2_changes(spark, config)
        logger.info("Customer dimension SCD2 update completed successfully")
    except Exception as e:
        logger.error(f"Customer dimension SCD2 update failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
