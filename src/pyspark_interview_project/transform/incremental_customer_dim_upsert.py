"""
Incremental Customer Dimension Upsert with Delta Lake MERGE

Implements CDC-style incremental upsert for customer dimension table using:
- Watermark control for incremental loading
- Delta Lake MERGE INTO for upsert operations
- Deduplication by customer_id + last_modified_date
"""

import sys
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, max as spark_max, current_timestamp, to_timestamp,
    coalesce, lit, when, row_number
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Add project root to path
sys.path.insert(0, str(__file__).rsplit('/', 3)[0])

from pyspark_interview_project.utils.watermark import load_watermark, save_watermark
from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_config

logger = logging.getLogger(__name__)


def deduplicate_by_latest(df: DataFrame, key_col: str, ts_col: str) -> DataFrame:
    """
    Deduplicate DataFrame keeping only the latest record per key based on timestamp.
    
    Args:
        df: Input DataFrame
        key_col: Column to use as business key
        ts_col: Timestamp column for determining latest
        
    Returns:
        Deduplicated DataFrame
    """
    window_spec = Window.partitionBy(key_col).orderBy(col(ts_col).desc())
    
    return df.withColumn("_row_num", row_number().over(window_spec)) \
             .filter(col("_row_num") == 1) \
             .drop("_row_num")


def incremental_customer_dim_upsert(
    spark: SparkSession,
    config: Dict[str, Any],
    watermark_checkpoint_path: str = "data/checkpoints"
) -> None:
    """
    Perform incremental upsert into customer dimension table using Delta Lake MERGE.
    
    Args:
        spark: Spark session
        config: Configuration dictionary
        watermark_checkpoint_path: Path for watermark storage
    """
    logger.info("Starting incremental customer dimension upsert")
    
    # Get paths from config
    bronze_base = config.get("lake", {}).get("bronze", {}).get("base", "s3://bucket/bronze")
    silver_base = config.get("lake", {}).get("silver", {}).get("base", "s3://bucket/silver")
    
    bronze_crm_path = f"{bronze_base}/crm/contacts"
    silver_customer_path = f"{silver_base}/dim_customer"
    
    # Load watermark
    watermark_ts = load_watermark(watermark_checkpoint_path, "customer_dim")
    logger.info(f"Loaded watermark: {watermark_ts}")
    
    # Read new batch from bronze
    logger.info(f"Reading bronze data from: {bronze_crm_path}")
    bronze_df = spark.read.format("delta").load(bronze_crm_path)
    
    # Filter by watermark if exists
    if watermark_ts:
        bronze_df = bronze_df.filter(col("_ingestion_ts") > to_timestamp(lit(watermark_ts)))
        logger.info(f"Filtered to records after watermark: {watermark_ts}")
    else:
        logger.info("No watermark found, processing all records")
    
    # Deduplicate by customer_id + last_modified_date (assuming these columns exist)
    # Map CRM contact fields to customer dimension structure
    customer_updates = bronze_df.select(
        col("Id").alias("customer_id"),
        coalesce(col("FirstName"), lit("")).alias("first_name"),
        coalesce(col("LastName"), lit("")).alias("last_name"),
        col("Email").alias("email"),
        col("Phone").alias("phone"),
        col("AccountId").alias("account_id"),
        col("Title").alias("title"),
        col("LeadSource").alias("lead_source"),
        col("CreatedDate").alias("created_date"),
        coalesce(col("LastModifiedDate"), col("_ingestion_ts")).alias("last_modified_date"),
        col("_ingestion_ts"),
        col("_source_system")
    )
    
    # Deduplicate within this batch
    customer_updates = deduplicate_by_latest(
        customer_updates,
        key_col="customer_id",
        ts_col="last_modified_date"
    )
    
    # Cache the updates since we'll use it multiple times
    customer_updates.cache()
    record_count = customer_updates.count()
    logger.info(f"Prepared {record_count} customer records for merge")
    
    # Check if silver table exists
    try:
        silver_table = DeltaTable.forPath(spark, silver_customer_path)
        table_exists = True
        logger.info("Silver customer table exists, performing MERGE")
    except Exception:
        table_exists = False
        logger.info("Silver customer table does not exist, creating new table")
    
    if table_exists:
        # Perform Delta Lake MERGE
        merge_condition = "target.customer_id = source.customer_id"
        
        silver_table.alias("target").merge(
            customer_updates.alias("source"),
            merge_condition
        ).whenMatchedUpdate(
            condition="source.last_modified_date > target.last_modified_date",
            set={
                "first_name": "source.first_name",
                "last_name": "source.last_name",
                "email": "source.email",
                "phone": "source.phone",
                "account_id": "source.account_id",
                "title": "source.title",
                "lead_source": "source.lead_source",
                "last_modified_date": "source.last_modified_date",
                "updated_at": current_timestamp(),
                "_source_system": "source._source_system"
            }
        ).whenNotMatchedInsert(
            values={
                "customer_id": "source.customer_id",
                "first_name": "source.first_name",
                "last_name": "source.last_name",
                "email": "source.email",
                "phone": "source.phone",
                "account_id": "source.account_id",
                "title": "source.title",
                "lead_source": "source.lead_source",
                "created_date": "source.created_date",
                "last_modified_date": "source.last_modified_date",
                "created_at": current_timestamp(),
                "updated_at": current_timestamp(),
                "_source_system": "source._source_system"
            }
        ).execute()
        
        logger.info("MERGE operation completed successfully")
    else:
        # Create new table with first batch
        customer_updates.withColumn("created_at", current_timestamp()) \
                       .withColumn("updated_at", current_timestamp()) \
                       .write \
                       .format("delta") \
                       .mode("overwrite") \
                       .option("mergeSchema", "true") \
                       .partitionBy("_source_system") \
                       .save(silver_customer_path)
        
        logger.info("Created new silver customer dimension table")
    
    # Update watermark to latest ingestion timestamp
    if record_count > 0:
        latest_ts = customer_updates.select(spark_max(col("_ingestion_ts"))).collect()[0][0]
        if latest_ts:
            watermark_iso = latest_ts.isoformat() if hasattr(latest_ts, 'isoformat') else str(latest_ts)
            save_watermark(watermark_checkpoint_path, "customer_dim", watermark_iso)
            logger.info(f"Updated watermark to: {watermark_iso}")
    
    # Unpersist cached DataFrame
    customer_updates.unpersist()
    
    logger.info("Incremental customer dimension upsert completed successfully")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Incremental customer dimension upsert")
    parser.add_argument("--config", default="config/dev.yaml", help="Configuration file")
    parser.add_argument("--checkpoint-path", default="data/checkpoints", 
                       help="Watermark checkpoint path")
    args = parser.parse_args()
    
    spark = build_spark("IncrementalCustomerDimUpsert")
    config = load_config(args.config)
    
    try:
        incremental_customer_dim_upsert(spark, config, args.checkpoint_path)
    except Exception as e:
        logger.error(f"Failed to perform incremental upsert: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

