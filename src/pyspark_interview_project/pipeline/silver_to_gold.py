"""
Silver to Gold transformation creating curated dimensions and facts.
"""

import argparse
import logging
import os
import sys
from typing import Dict, Any
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when, coalesce, to_date

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark_interview_project.utils.spark import get_spark_session, get_delta_config
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.logging import setup_json_logging

logger = logging.getLogger(__name__)


def create_dim_customers(spark: SparkSession, salesforce_accounts: DataFrame) -> DataFrame:
    """
    Create customer dimension table from Salesforce accounts.
    
    Args:
        spark: Spark session
        salesforce_accounts: Silver Salesforce accounts DataFrame
        
    Returns:
        Customer dimension DataFrame with columns: id, name, type, region, created_at, is_active
    """
    logger.info("Creating customer dimension table from Salesforce accounts")
    
    dim_customers = salesforce_accounts.select(
        col("Id").alias("id"),
        col("Name").alias("name"),
        col("Type").alias("type"),
        col("BillingState").alias("region"),
        col("CreatedDate").alias("created_at"),
        when(col("IsActive") == "true", True).otherwise(False).alias("is_active")
    ).filter(
        col("Id").isNotNull() & col("Name").isNotNull()
    )
    
    logger.info(f"Customer dimension created. Rows: {dim_customers.count()}")
    return dim_customers


def create_dim_fx(spark: SparkSession, fx_rates: DataFrame) -> DataFrame:
    """
    Create FX dimension table from silver FX rates.
    
    Args:
        spark: Spark session
        fx_rates: Silver FX rates DataFrame
        
    Returns:
        FX dimension DataFrame with columns: as_of_date, ccy, rate_to_usd
    """
    logger.info("Creating FX dimension table from silver FX rates")
    
    dim_fx = fx_rates.select(
        col("as_of_date"),
        col("ccy"),
        col("rate_to_base").alias("rate_to_usd")
    ).filter(
        col("as_of_date").isNotNull() & 
        col("ccy").isNotNull() & 
        col("rate_to_base").isNotNull()
    )
    
    logger.info(f"FX dimension created. Rows: {dim_fx.count()}")
    return dim_fx


def create_fact_orders(
    spark: SparkSession, 
    orders_silver: DataFrame, 
    dim_customers: DataFrame,
    dim_fx: DataFrame
) -> DataFrame:
    """
    Create orders fact table with enriched data.
    
    Args:
        spark: Spark session
        orders_silver: Silver orders DataFrame
        dim_customers: Customer dimension DataFrame
        dim_fx: FX dimension DataFrame
        
    Returns:
        Orders fact DataFrame with columns: order_id, customer_id, order_date, currency, amount_native, amount_usd, status
    """
    logger.info("Creating orders fact table")
    
    # Join with customer dimension
    orders_with_customers = orders_silver.join(
        dim_customers.select("id", "name"),
        orders_silver.customer_id == dim_customers.id,
        "left"
    )
    
    # Join with FX rates for USD conversion
    orders_with_fx = orders_with_customers.join(
        dim_fx,
        (orders_with_customers.order_date == dim_fx.as_of_date) & 
        (orders_with_customers.currency == dim_fx.ccy),
        "left"
    )
    
    # Calculate USD amounts
    fact_orders = orders_with_fx.select(
        col("order_id"),
        col("customer_id"),
        to_date(col("order_date")).alias("order_date"),
        col("currency"),
        col("amount").alias("amount_native"),
        when(col("currency") == "USD", col("amount"))
        .otherwise(col("amount") / coalesce(col("rate_to_usd"), lit(1.0)))
        .alias("amount_usd"),
        col("status")
    ).filter(
        col("order_id").isNotNull() & 
        col("customer_id").isNotNull() &
        col("order_date").isNotNull()
    )
    
    logger.info(f"Orders fact table created. Rows: {fact_orders.count()}")
    return fact_orders


def main():
    """Main entry point for Silver to Gold transformation."""
    parser = argparse.ArgumentParser(description="Silver to Gold transformation")
    parser.add_argument("--config", required=True, help="Configuration file path")
    parser.add_argument("--lake-root", required=True, help="Data lake root path")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_json_logging()
    logger.info("Starting Silver to Gold transformation")
    
    # Load configuration
    config = load_conf(args.config)
    lake_root = args.lake_root
    
    # Create Spark session with Delta config
    spark = get_spark_session(
        app_name="silver-to-gold",
        config=get_delta_config()
    )
    
    try:
        # Read Silver layer data
        logger.info("Reading Silver layer data")
        
        # Read Salesforce accounts (customers)
        salesforce_accounts = spark.read.format("delta").load(f"{lake_root}/silver/salesforce_accounts")
        
        # Read FX rates
        fx_rates = spark.read.format("delta").load(f"{lake_root}/silver/fx_rates")
        
        # Read orders
        orders_silver = spark.read.format("delta").load(f"{lake_root}/silver/orders")
        
        # Create Gold layer dimensions and facts
        logger.info("Creating Gold layer tables")
        
        # Create dimensions
        dim_customers = create_dim_customers(spark, salesforce_accounts)
        dim_fx = create_dim_fx(spark, fx_rates)
        
        # Create fact table
        fact_orders = create_fact_orders(spark, orders_silver, dim_customers, dim_fx)
        
        # Write Gold layer tables
        logger.info("Writing Gold layer tables")
        
        # Write dimensions
        dim_customers.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{lake_root}/gold/dim_customers")
        
        dim_fx.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{lake_root}/gold/dim_fx")
        
        # Write fact table
        fact_orders.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{lake_root}/gold/fact_orders")
        
        logger.info("Silver to Gold transformation completed successfully")
        
        # Show sample data
        logger.info("Sample Gold layer data:")
        dim_customers.show(5, truncate=False)
        dim_fx.show(5, truncate=False)
        fact_orders.show(5, truncate=False)
        
    except Exception as e:
        logger.error(f"Silver to Gold transformation failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()