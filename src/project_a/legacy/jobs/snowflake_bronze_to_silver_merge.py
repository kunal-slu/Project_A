#!/usr/bin/env python3
"""
Snowflake bronze to silver merge job.
Merges Snowflake data into silver layer using Delta MERGE operations.
"""

import os
import sys
import logging
from typing import Dict, Any
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, coalesce
from delta.tables import DeltaTable

from project_a.utils.spark_session import build_spark
from project_a.utils.config import load_conf
from project_a.utils.logging import setup_json_logging

logger = logging.getLogger(__name__)


def merge_orders_data(spark: SparkSession, lake_root: str) -> None:
    """
    Merge Snowflake orders data into silver layer.
    
    Args:
        spark: Spark session
        lake_root: S3 root path of the data lake
    """
    logger.info("Merging Snowflake orders data into silver layer")
    
    # Read bronze data
    bronze_orders_df = spark.read \
        .format("delta") \
        .load(f"{lake_root}/bronze/snowflake/orders")
    
    # Read existing silver data (if exists)
    silver_orders_path = f"{lake_root}/silver/orders"
    
    try:
        # Check if silver table exists
        silver_orders_df = spark.read.format("delta").load(silver_orders_path)
        silver_table_exists = True
    except Exception:
        logger.info("Silver orders table does not exist, creating new one")
        silver_table_exists = False
    
    if not silver_table_exists:
        # Create new silver table
        bronze_orders_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("path", silver_orders_path) \
            .saveAsTable("silver.orders")
        logger.info("Created new silver orders table")
    else:
        # Merge data using Delta MERGE
        silver_table = DeltaTable.forPath(spark, silver_orders_path)
        
        # Prepare bronze data for merge
        bronze_orders_for_merge = bronze_orders_df.select(
            col("order_id"),
            col("customer_id"),
            col("status"),
            col("total_price"),
            col("order_date"),
            col("priority"),
            col("clerk"),
            col("ship_priority"),
            col("comment"),
            col("_source"),
            col("_extracted_at"),
            col("_proc_date"),
            current_timestamp().alias("_updated_at")
        )
        
        # Perform MERGE operation
        silver_table.alias("silver") \
            .merge(
                bronze_orders_for_merge.alias("bronze"),
                "silver.order_id = bronze.order_id AND silver._source = bronze._source"
            ) \
            .whenMatchedUpdate(set={
                "customer_id": "bronze.customer_id",
                "status": "bronze.status",
                "total_price": "bronze.total_price",
                "order_date": "bronze.order_date",
                "priority": "bronze.priority",
                "clerk": "bronze.clerk",
                "ship_priority": "bronze.ship_priority",
                "comment": "bronze.comment",
                "_extracted_at": "bronze._extracted_at",
                "_proc_date": "bronze._proc_date",
                "_updated_at": "bronze._updated_at"
            }) \
            .whenNotMatchedInsert(values={
                "order_id": "bronze.order_id",
                "customer_id": "bronze.customer_id",
                "status": "bronze.status",
                "total_price": "bronze.total_price",
                "order_date": "bronze.order_date",
                "priority": "bronze.priority",
                "clerk": "bronze.clerk",
                "ship_priority": "bronze.ship_priority",
                "comment": "bronze.comment",
                "_source": "bronze._source",
                "_extracted_at": "bronze._extracted_at",
                "_proc_date": "bronze._proc_date",
                "_updated_at": "bronze._updated_at"
            }) \
            .execute()
        
        logger.info("Successfully merged orders data into silver layer")


def merge_customers_data(spark: SparkSession, lake_root: str) -> None:
    """
    Merge Snowflake customers data into silver layer.
    
    Args:
        spark: Spark session
        lake_root: S3 root path of the data lake
    """
    logger.info("Merging Snowflake customers data into silver layer")
    
    # Read bronze data
    bronze_customers_df = spark.read \
        .format("delta") \
        .load(f"{lake_root}/bronze/snowflake/customers")
    
    # Read existing silver data (if exists)
    silver_customers_path = f"{lake_root}/silver/customers"
    
    try:
        # Check if silver table exists
        silver_customers_df = spark.read.format("delta").load(silver_customers_path)
        silver_table_exists = True
    except Exception:
        logger.info("Silver customers table does not exist, creating new one")
        silver_table_exists = False
    
    if not silver_table_exists:
        # Create new silver table
        bronze_customers_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("path", silver_customers_path) \
            .saveAsTable("silver.customers")
        logger.info("Created new silver customers table")
    else:
        # Merge data using Delta MERGE
        silver_table = DeltaTable.forPath(spark, silver_customers_path)
        
        # Prepare bronze data for merge
        bronze_customers_for_merge = bronze_customers_df.select(
            col("customer_id"),
            col("name"),
            col("address"),
            col("nation_key"),
            col("phone"),
            col("account_balance"),
            col("market_segment"),
            col("comment"),
            col("_source"),
            col("_extracted_at"),
            col("_proc_date"),
            current_timestamp().alias("_updated_at")
        )
        
        # Perform MERGE operation
        silver_table.alias("silver") \
            .merge(
                bronze_customers_for_merge.alias("bronze"),
                "silver.customer_id = bronze.customer_id AND silver._source = bronze._source"
            ) \
            .whenMatchedUpdate(set={
                "name": "bronze.name",
                "address": "bronze.address",
                "nation_key": "bronze.nation_key",
                "phone": "bronze.phone",
                "account_balance": "bronze.account_balance",
                "market_segment": "bronze.market_segment",
                "comment": "bronze.comment",
                "_extracted_at": "bronze._extracted_at",
                "_proc_date": "bronze._proc_date",
                "_updated_at": "bronze._updated_at"
            }) \
            .whenNotMatchedInsert(values={
                "customer_id": "bronze.customer_id",
                "name": "bronze.name",
                "address": "bronze.address",
                "nation_key": "bronze.nation_key",
                "phone": "bronze.phone",
                "account_balance": "bronze.account_balance",
                "market_segment": "bronze.market_segment",
                "comment": "bronze.comment",
                "_source": "bronze._source",
                "_extracted_at": "bronze._extracted_at",
                "_proc_date": "bronze._proc_date",
                "_updated_at": "bronze._updated_at"
            }) \
            .execute()
        
        logger.info("Successfully merged customers data into silver layer")


def merge_lineitems_data(spark: SparkSession, lake_root: str) -> None:
    """
    Merge Snowflake lineitems data into silver layer.
    
    Args:
        spark: Spark session
        lake_root: S3 root path of the data lake
    """
    logger.info("Merging Snowflake lineitems data into silver layer")
    
    # Read bronze data
    bronze_lineitems_df = spark.read \
        .format("delta") \
        .load(f"{lake_root}/bronze/snowflake/lineitems")
    
    # Read existing silver data (if exists)
    silver_lineitems_path = f"{lake_root}/silver/lineitems"
    
    try:
        # Check if silver table exists
        silver_lineitems_df = spark.read.format("delta").load(silver_lineitems_path)
        silver_table_exists = True
    except Exception:
        logger.info("Silver lineitems table does not exist, creating new one")
        silver_table_exists = False
    
    if not silver_table_exists:
        # Create new silver table
        bronze_lineitems_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("path", silver_lineitems_path) \
            .saveAsTable("silver.lineitems")
        logger.info("Created new silver lineitems table")
    else:
        # Merge data using Delta MERGE
        silver_table = DeltaTable.forPath(spark, silver_lineitems_path)
        
        # Prepare bronze data for merge
        bronze_lineitems_for_merge = bronze_lineitems_df.select(
            col("order_id"),
            col("part_id"),
            col("supplier_id"),
            col("line_number"),
            col("quantity"),
            col("extended_price"),
            col("discount"),
            col("tax"),
            col("return_flag"),
            col("line_status"),
            col("ship_date"),
            col("commit_date"),
            col("receipt_date"),
            col("ship_instruction"),
            col("ship_mode"),
            col("comment"),
            col("_source"),
            col("_extracted_at"),
            col("_proc_date"),
            current_timestamp().alias("_updated_at")
        )
        
        # Perform MERGE operation
        silver_table.alias("silver") \
            .merge(
                bronze_lineitems_for_merge.alias("bronze"),
                "silver.order_id = bronze.order_id AND silver.part_id = bronze.part_id AND silver.line_number = bronze.line_number AND silver._source = bronze._source"
            ) \
            .whenMatchedUpdate(set={
                "supplier_id": "bronze.supplier_id",
                "quantity": "bronze.quantity",
                "extended_price": "bronze.extended_price",
                "discount": "bronze.discount",
                "tax": "bronze.tax",
                "return_flag": "bronze.return_flag",
                "line_status": "bronze.line_status",
                "ship_date": "bronze.ship_date",
                "commit_date": "bronze.commit_date",
                "receipt_date": "bronze.receipt_date",
                "ship_instruction": "bronze.ship_instruction",
                "ship_mode": "bronze.ship_mode",
                "comment": "bronze.comment",
                "_extracted_at": "bronze._extracted_at",
                "_proc_date": "bronze._proc_date",
                "_updated_at": "bronze._updated_at"
            }) \
            .whenNotMatchedInsert(values={
                "order_id": "bronze.order_id",
                "part_id": "bronze.part_id",
                "supplier_id": "bronze.supplier_id",
                "line_number": "bronze.line_number",
                "quantity": "bronze.quantity",
                "extended_price": "bronze.extended_price",
                "discount": "bronze.discount",
                "tax": "bronze.tax",
                "return_flag": "bronze.return_flag",
                "line_status": "bronze.line_status",
                "ship_date": "bronze.ship_date",
                "commit_date": "bronze.commit_date",
                "receipt_date": "bronze.receipt_date",
                "ship_instruction": "bronze.ship_instruction",
                "ship_mode": "bronze.ship_mode",
                "comment": "bronze.comment",
                "_source": "bronze._source",
                "_extracted_at": "bronze._extracted_at",
                "_proc_date": "bronze._proc_date",
                "_updated_at": "bronze._updated_at"
            }) \
            .execute()
        
        logger.info("Successfully merged lineitems data into silver layer")


def process_snowflake_merge(spark: SparkSession, config: Dict[str, Any]) -> None:
    """
    Process Snowflake bronze to silver merge.
    
    Args:
        spark: Spark session
        config: Configuration dictionary
    """
    logger.info("Starting Snowflake bronze to silver merge")
    
    # Get data lake configuration
    lake_root = config["lake"]["root"]
    
    # Merge each table
    merge_orders_data(spark, lake_root)
    merge_customers_data(spark, lake_root)
    merge_lineitems_data(spark, lake_root)
    
    logger.info("Snowflake bronze to silver merge completed successfully")


def main():
    """Main function to run the Snowflake merge job."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Merge Snowflake data from bronze to silver")
    parser.add_argument("--config", required=True, help="Configuration file path")
    args = parser.parse_args()
    
    # Setup logging
    setup_json_logging()
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logging.getLogger().setLevel(getattr(logging, log_level.upper()))
    
    try:
        # Load configuration
        config = load_conf(args.config)
        
        # Create Spark session with Delta support
        spark = get_spark_session(
            "SnowflakeBronzeToSilverMerge",
            extra_conf={
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
            }
        )
        
        # Process Snowflake merge
        process_snowflake_merge(spark, config)
        
    except Exception as e:
        logger.error(f"Snowflake merge failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()