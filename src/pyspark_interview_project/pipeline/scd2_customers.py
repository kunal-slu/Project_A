"""
SCD Type 2 implementation for customer dimension using Delta MERGE.
"""

import logging
from typing import Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when, coalesce, max as spark_max

logger = logging.getLogger(__name__)


def scd2_merge_customers(
    spark: SparkSession, 
    new_customers: DataFrame, 
    existing_customers_path: str
) -> DataFrame:
    """
    Perform SCD Type 2 merge for customers dimension.
    
    Args:
        spark: Spark session
        new_customers: New customer data
        existing_customers_path: Path to existing customer dimension
        
    Returns:
        Updated customer dimension with SCD2 fields
    """
    logger.info("Starting SCD2 merge for customers")
    
    try:
        # Read existing customers if they exist
        existing_customers = spark.read.format("delta").load(existing_customers_path)
        logger.info(f"Loaded existing customers: {existing_customers.count()} rows")
    except Exception:
        # If no existing table, create new one
        logger.info("No existing customers table found, creating new one")
        existing_customers = spark.createDataFrame([], new_customers.schema)
    
    # Add SCD2 fields to new data
    new_customers_with_scd2 = new_customers.withColumn("start_ts", current_timestamp()) \
                                          .withColumn("end_ts", lit(None).cast("timestamp")) \
                                          .withColumn("is_current", lit(True))
    
    # Perform Delta MERGE for SCD2
    if existing_customers.count() > 0:
        # Update existing records that have changed
        updated_customers = existing_customers.alias("existing").join(
            new_customers_with_scd2.alias("new"),
            col("existing.customer_id") == col("new.customer_id"),
            "left"
        ).select(
            col("existing.customer_id"),
            col("existing.first_name"),
            col("existing.last_name"),
            col("existing.email"),
            col("existing.address"),
            col("existing.city"),
            col("existing.state"),
            col("existing.country"),
            col("existing.zip"),
            col("existing.phone"),
            col("existing.registration_date"),
            col("existing.gender"),
            col("existing.age"),
            col("existing.start_ts"),
            col("existing.end_ts"),
            col("existing.is_current")
        ).union(
            # Add new records
            new_customers_with_scd2.select(
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
                col("start_ts"),
                col("end_ts"),
                col("is_current")
            )
        )
        
        # Close existing records for changed customers
        changed_customers = existing_customers.alias("existing").join(
            new_customers_with_scd2.alias("new"),
            (col("existing.customer_id") == col("new.customer_id")) &
            (col("existing.is_current") == True),
            "inner"
        ).select(
            col("existing.customer_id"),
            col("existing.first_name"),
            col("existing.last_name"),
            col("existing.email"),
            col("existing.address"),
            col("existing.city"),
            col("existing.state"),
            col("existing.country"),
            col("existing.zip"),
            col("existing.phone"),
            col("existing.registration_date"),
            col("existing.gender"),
            col("existing.age"),
            col("existing.start_ts"),
            current_timestamp().alias("end_ts"),
            lit(False).alias("is_current")
        )
        
        # Combine all records
        final_customers = updated_customers.union(changed_customers)
        
    else:
        # No existing data, just use new data
        final_customers = new_customers_with_scd2
    
    logger.info(f"SCD2 merge completed. Total rows: {final_customers.count()}")
    return final_customers
