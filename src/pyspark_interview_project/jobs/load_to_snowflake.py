"""
Load Gold layer tables to Snowflake for analytics consumption.

Supports MERGE for idempotent upserts.
"""

import os
import sys
import logging
import argparse
from typing import Dict, Any, List
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.secrets import get_snowflake_credentials

logger = logging.getLogger(__name__)


def load_delta_to_snowflake(
    spark: SparkSession,
    delta_path: str,
    snowflake_table: str,
    config: Dict[str, Any],
    merge_keys: List[str] = None
) -> bool:
    """
    Load Delta table to Snowflake with MERGE support.
    
    Args:
        spark: SparkSession
        delta_path: Path to Delta table (S3 or local)
        snowflake_table: Target Snowflake table (e.g., 'ANALYTICS.CUSTOMER_360')
        config: Configuration dictionary
        merge_keys: List of columns to use for MERGE (primary key)
        
    Returns:
        True if successful
    """
    logger.info(f"Loading {delta_path} to Snowflake table {snowflake_table}")
    
    try:
        # Get Snowflake credentials
        sf_creds = get_snowflake_credentials(config)
        
        # Read Delta table
        logger.info(f"Reading Delta table from {delta_path}")
        df = spark.read.format("delta").load(delta_path)
        
        record_count = df.count()
        logger.info(f"Read {record_count:,} records from Delta table")
        
        # Add load metadata
        df = df.withColumn("_load_timestamp", current_timestamp())
        
        # Write to Snowflake
        sf_options = {
            "sfURL": f"{sf_creds.get('account')}.snowflakecomputing.com",
            "sfUser": sf_creds.get('user'),
            "sfPassword": sf_creds.get('password'),
            "sfDatabase": sf_creds.get('database', 'ANALYTICS'),
            "sfSchema": sf_creds.get('schema', 'PUBLIC'),
            "sfWarehouse": sf_creds.get('warehouse'),
            "dbtable": snowflake_table.split('.')[-1]  # Table name only
        }
        
        # If merge_keys provided, use MERGE mode
        if merge_keys:
            logger.info(f"Using MERGE mode with keys: {merge_keys}")
            
            # Create temporary staging table
            staging_table = f"{snowflake_table}_staging"
            
            # Write to staging
            df.write \
                .format("snowflake") \
                .options(**sf_options) \
                .option("dbtable", staging_table) \
                .mode("overwrite") \
                .save()
            
            # Execute MERGE statement
            merge_sql = _build_merge_sql(
                target_table=snowflake_table,
                staging_table=staging_table,
                merge_keys=merge_keys,
                columns=df.columns
            )
            
            logger.info(f"Executing MERGE SQL: {merge_sql}")
            
            # Execute via JDBC
            sf_url = f"jdbc:snowflake://{sf_options['sfURL']}"
            spark.read.format("jdbc") \
                .option("url", sf_url) \
                .option("user", sf_options['sfUser']) \
                .option("password", sf_options['sfPassword']) \
                .option("query", merge_sql) \
                .load().collect()  # Execute query
            
            logger.info(f"✅ MERGE completed for {snowflake_table}")
            
        else:
            # Simple overwrite
            logger.info("Using OVERWRITE mode")
            df.write \
                .format("snowflake") \
                .options(**sf_options) \
                .option("dbtable", snowflake_table.split('.')[-1]) \
                .mode("overwrite") \
                .save()
            
            logger.info(f"✅ Overwrite completed for {snowflake_table}")
        
        logger.info(f"✅ Successfully loaded {record_count:,} records to {snowflake_table}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to load to Snowflake: {e}")
        raise


def _build_merge_sql(target_table: str, staging_table: str, merge_keys: List[str], columns: List[str]) -> str:
    """Build Snowflake MERGE SQL statement."""
    target_alias = "target"
    source_alias = "source"
    
    # Build ON clause
    on_clause = " AND ".join([
        f"{target_alias}.{key} = {source_alias}.{key}" for key in merge_keys
    ])
    
    # Build UPDATE clause
    update_cols = [col for col in columns if col not in merge_keys]
    update_clause = ", ".join([
        f"{col} = {source_alias}.{col}" for col in update_cols
    ])
    
    # Build INSERT clause
    insert_cols = ", ".join(columns)
    insert_values = ", ".join([f"{source_alias}.{col}" for col in columns])
    
    merge_sql = f"""
        MERGE INTO {target_table} AS {target_alias}
        USING {staging_table} AS {source_alias}
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_values})
    """
    
    return merge_sql.strip()


def load_gold_tables_to_snowflake(config: Dict[str, Any], tables: List[str] = None) -> bool:
    """
    Load all Gold tables to Snowflake.
    
    Args:
        config: Configuration dictionary
        tables: List of table names to load (default: all gold tables)
        
    Returns:
        True if successful
    """
    if tables is None:
        tables = [
            "customer_360",
            "orders_metrics",
            "sales_fact"
        ]
    
    spark = build_spark(app_name="load_to_snowflake", config=config)
    
    gold_base = config.get("data_lake", {}).get("gold_path", "data/lakehouse_delta/gold")
    
    # Table configurations
    table_configs = {
        "customer_360": {
            "delta_path": f"{gold_base}/customer_360",
            "snowflake_table": "ANALYTICS.CUSTOMER_360",
            "merge_keys": ["customer_id"]
        },
        "orders_metrics": {
            "delta_path": f"{gold_base}/fact_sales",
            "snowflake_table": "ANALYTICS.ORDERS_METRICS",
            "merge_keys": ["order_id"]
        },
        "sales_fact": {
            "delta_path": f"{gold_base}/fact_sales",
            "snowflake_table": "ANALYTICS.FACT_SALES",
            "merge_keys": ["order_id", "order_date"]
        }
    }
    
    try:
        for table_name in tables:
            if table_name not in table_configs:
                logger.warning(f"Table {table_name} not configured, skipping")
                continue
            
            table_config = table_configs[table_name]
            
            success = load_delta_to_snowflake(
                spark=spark,
                delta_path=table_config["delta_path"],
                snowflake_table=table_config["snowflake_table"],
                config=config,
                merge_keys=table_config.get("merge_keys")
            )
            
            if not success:
                logger.error(f"Failed to load {table_name}")
                return False
        
        logger.info("✅ All Gold tables loaded to Snowflake successfully")
        return True
        
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Gold tables to Snowflake")
    parser.add_argument("--config", default="config/prod.yaml", help="Config file path")
    parser.add_argument("--env", choices=["dev", "prod", "local"], default="prod", help="Environment")
    parser.add_argument("--tables", nargs="+", help="Specific tables to load (default: all)")
    
    args = parser.parse_args()
    
    config = load_conf(args.config)
    
    success = load_gold_tables_to_snowflake(config, tables=args.tables)
    sys.exit(0 if success else 1)

