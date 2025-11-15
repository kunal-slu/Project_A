#!/usr/bin/env python3
"""
Gold to Snowflake Publish Job

Publishes curated Gold layer tables to Snowflake for BI consumption.
Implements dual sink pattern: S3 Delta (canonical) + Snowflake (serving layer).

This job reads Gold Delta tables from S3 and writes them to Snowflake using
the Snowflake Spark Connector.
"""
import sys
import argparse
import logging
from pathlib import Path
from typing import Dict, Any, Optional
import os

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.config_loader import load_config_resolved
from pyspark_interview_project.utils.path_resolver import resolve_path
from pyspark_interview_project.utils.logging import setup_json_logging, get_trace_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_snowflake_options(config: Dict[str, Any]) -> Dict[str, str]:
    """
    Build Snowflake connection options from config.
    
    Supports password from Secrets Manager or plain config.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Dictionary of Snowflake connection options
    """
    sf_cfg = config.get("sinks", {}).get("snowflake", {})
    
    if not sf_cfg.get("enabled", False):
        raise ValueError("Snowflake sink is disabled in config")
    
    # Get password from Secrets Manager or config
    password = sf_cfg.get("password")
    
    if not password and "password_secret_name" in sf_cfg:
        try:
            import boto3
            sm = boto3.client("secretsmanager", region_name=config.get("aws", {}).get("region", "us-east-1"))
            secret_name = sf_cfg["password_secret_name"]
            secret_response = sm.get_secret_value(SecretId=secret_name)
            password = secret_response["SecretString"]
            logger.info(f"✅ Retrieved password from Secrets Manager: {secret_name}")
        except Exception as e:
            logger.error(f"❌ Failed to retrieve password from Secrets Manager: {e}")
            raise
    
    if not password:
        raise ValueError("Snowflake password not found in config or Secrets Manager")
    
    # Build Snowflake URL
    account = sf_cfg.get("account", "")
    if not account:
        raise ValueError("Snowflake account not configured")
    
    # Handle account format (with or without .snowflakecomputing.com)
    if ".snowflakecomputing.com" in account:
        sf_url = account
    else:
        sf_url = f"{account}.snowflakecomputing.com"
    
    options = {
        "sfURL": sf_url,
        "sfUser": sf_cfg.get("user", ""),
        "sfPassword": password,
        "sfRole": sf_cfg.get("role", ""),
        "sfWarehouse": sf_cfg.get("warehouse", ""),
        "sfDatabase": sf_cfg.get("database", ""),
        "sfSchema": sf_cfg.get("schema", ""),
    }
    
    # Validate required fields
    required = ["sfUser", "sfPassword", "sfRole", "sfWarehouse", "sfDatabase", "sfSchema"]
    for field in required:
        if not options.get(field):
            raise ValueError(f"Snowflake {field} not configured")
    
    logger.info(f"✅ Snowflake options configured: {sf_cfg.get('database')}.{sf_cfg.get('schema')}")
    return options


def load_gold_table(
    spark: SparkSession,
    config: Dict[str, Any],
    table_name: str
) -> DataFrame:
    """
    Load Gold table from S3 Delta.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        table_name: Name of the gold table (e.g., "fact_orders", "dim_customer")
        
    Returns:
        DataFrame with Gold table data
    """
    gold_root = resolve_path(config["paths"]["gold_root"], config=config)
    tables_cfg = config.get("tables", {}).get("gold", {})
    
    # Map table name to actual table path
    table_mapping = {
        "orders": tables_cfg.get("fact_orders", "fact_orders"),
        "fact_orders": tables_cfg.get("fact_orders", "fact_orders"),
        "customer": tables_cfg.get("dim_customer", "dim_customer"),
        "dim_customer": tables_cfg.get("dim_customer", "dim_customer"),
        "product": tables_cfg.get("dim_product", "dim_product"),
        "dim_product": tables_cfg.get("dim_product", "dim_product"),
        "customer_360": tables_cfg.get("fact_customer_24m", "fact_customer_24m"),
        "fact_customer_24m": tables_cfg.get("fact_customer_24m", "fact_customer_24m"),
    }
    
    actual_table = table_mapping.get(table_name, table_name)
    gold_path = f"{gold_root}/{actual_table}"
    
    logger.info(f"📥 Loading Gold table from: {gold_path}")
    
    try:
        df = spark.read.format("delta").load(gold_path)
        record_count = df.count()
        logger.info(f"✅ Loaded {record_count:,} records from {actual_table}")
        return df
    except Exception as e:
        logger.error(f"❌ Failed to load Gold table {gold_path}: {e}")
        raise


def prepare_dataframe_for_snowflake(df: DataFrame) -> DataFrame:
    """
    Prepare DataFrame for Snowflake:
    - Rename columns to uppercase (Snowflake convention)
    - Handle special characters in column names
    - Ensure compatible data types
    
    Args:
        df: Input DataFrame
        
    Returns:
        Prepared DataFrame
    """
    logger.info("🔧 Preparing DataFrame for Snowflake...")
    
    # Rename columns to uppercase
    for col_name in df.columns:
        if col_name != col_name.upper():
            df = df.withColumnRenamed(col_name, col_name.upper())
    
    # Replace any remaining special characters that Snowflake doesn't like
    # (Snowflake allows alphanumeric and underscores)
    for col_name in df.columns:
        clean_name = col_name.replace("-", "_").replace(".", "_").replace(" ", "_")
        if clean_name != col_name:
            df = df.withColumnRenamed(col_name, clean_name)
    
    logger.info(f"✅ Prepared DataFrame with {len(df.columns)} columns")
    return df


def merge_into_snowflake(
    sf_options: Dict[str, str],
    target_table: str,
    staging_table: str,
    merge_keys: list = None
) -> None:
    """
    Execute MERGE statement in Snowflake to upsert data.
    
    Uses staging table pattern for idempotent upserts.
    
    Args:
        sf_options: Snowflake connection options
        target_table: Target table name
        staging_table: Staging table name
        merge_keys: List of columns to use for matching (defaults to primary key columns)
    """
    logger.info(f"🔄 Executing MERGE: {staging_table} → {target_table}")
    
    try:
        import snowflake.connector
        from textwrap import dedent
        
        # Parse account from URL
        sf_url = sf_options["sfURL"]
        account = sf_url.replace(".snowflakecomputing.com", "")
        
        conn = snowflake.connector.connect(
            user=sf_options["sfUser"],
            password=sf_options["sfPassword"],
            account=account,
            warehouse=sf_options["sfWarehouse"],
            database=sf_options["sfDatabase"],
            schema=sf_options["sfSchema"],
            role=sf_options["sfRole"],
        )
        
        cur = conn.cursor()
        
        # Default merge keys (can be overridden)
        if merge_keys is None:
            # Common patterns based on table name
            if "FACT" in target_table.upper():
                merge_keys = ["ORDER_ID", "ORDER_DATE"] if "ORDER" in target_table.upper() else ["ID", "DATE"]
            elif "DIM" in target_table.upper():
                merge_keys = ["CUSTOMER_ID"] if "CUSTOMER" in target_table.upper() else ["PRODUCT_ID"]
            else:
                merge_keys = ["ID"]
        
        # Build MERGE statement
        merge_conditions = " AND ".join([f"tgt.{key} = src.{key}" for key in merge_keys])
        
        # Get all columns from staging (assume staging has same schema as target)
        # In production, you'd query INFORMATION_SCHEMA, but for now we'll use a generic pattern
        update_columns = [col for col in merge_keys if col not in ["ID", "ORDER_ID", "CUSTOMER_ID", "PRODUCT_ID"]]
        insert_columns = "*"  # Insert all columns
        
        sql = dedent(f"""
            MERGE INTO {target_table} AS tgt
            USING {staging_table} AS src
            ON {merge_conditions}
            WHEN MATCHED THEN UPDATE SET
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT ({insert_columns})
            VALUES (src.*);
        """)
        
        logger.info(f"Executing MERGE SQL:\n{sql}")
        cur.execute(sql)
        conn.commit()
        
        # Get merge statistics
        cur.execute(f"SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))")
        result = cur.fetchone()
        
        cur.close()
        conn.close()
        
        logger.info(f"✅ MERGE completed successfully")
        
    except ImportError:
        logger.warning("⚠️  snowflake.connector not available, falling back to Spark-only write")
        raise
    except Exception as e:
        logger.error(f"❌ MERGE failed: {e}", exc_info=True)
        raise


def publish_to_snowflake(
    df: DataFrame,
    sf_options: Dict[str, str],
    target_table: str,
    mode: str = "merge"
) -> None:
    """
    Publish DataFrame to Snowflake table using MERGE pattern.
    
    Args:
        df: DataFrame to publish
        sf_options: Snowflake connection options
        target_table: Target table name in Snowflake
        mode: Write mode ("merge", "overwrite", or "append")
    """
    logger.info(f"📤 Publishing to Snowflake table: {target_table} (mode: {mode})")
    
    try:
        if mode == "merge":
            # Use staging table + MERGE pattern for idempotent upserts
            staging_table = f"{target_table}_STAGE"
            
            logger.info(f"📝 Writing to staging table: {staging_table}")
            
            # Write to staging table
            df.write.format("net.snowflake.spark.snowflake").options(**sf_options) \
                .option("dbtable", staging_table) \
                .mode("overwrite") \
                .save()
            
            record_count = df.count()
            logger.info(f"✅ Wrote {record_count:,} records to staging table")
            
            # Execute MERGE
            merge_into_snowflake(sf_options, target_table, staging_table)
            
            logger.info(f"✅ Successfully published {record_count:,} records to {target_table} via MERGE")
            
        else:
            # Fallback to direct write for overwrite/append
            writer = df.write.format("net.snowflake.spark.snowflake").options(**sf_options)
            writer = writer.option("dbtable", target_table).mode(mode)
            
            if mode == "overwrite":
                writer = writer.option("truncate_table", "true")
            
            writer.save()
            
            record_count = df.count()
            logger.info(f"✅ Successfully published {record_count:,} records to {target_table}")
        
    except Exception as e:
        logger.error(f"❌ Failed to publish to Snowflake: {e}", exc_info=True)
        raise


def main():
    """Main entry point for EMR job."""
    parser = argparse.ArgumentParser(description="Publish Gold tables to Snowflake")
    parser.add_argument("--env", default="dev", help="Environment (dev/prod)")
    parser.add_argument("--config", help="Config file path (local or S3)")
    parser.add_argument("--table", default="orders", help="Gold table to publish (orders, customer, product, customer_360)")
    parser.add_argument("--mode", default="merge", choices=["merge", "overwrite", "append"], help="Write mode (merge=staging+MERGE, overwrite=direct overwrite, append=direct append)")
    args = parser.parse_args()
    
    # Setup structured logging
    trace_id = get_trace_id()
    setup_json_logging(level="INFO", include_trace_id=True)
    logger.info(f"🚀 Starting Gold to Snowflake publish job (trace_id={trace_id}, table={args.table})")
    
    # Load configuration
    config = load_config_resolved(args.config, args.env)
    
    # Check if Snowflake sink is enabled
    sf_cfg = config.get("sinks", {}).get("snowflake", {})
    if not sf_cfg.get("enabled", False):
        logger.warning("⚠️  Snowflake sink disabled in config, exiting gracefully")
        return 0
    
    # Build Spark session
    spark = build_spark(config)
    
    try:
        # Get Snowflake options
        sf_options = get_snowflake_options(config)
        
        # Get target table name from config
        tables_cfg = sf_cfg.get("tables", {})
        table_mapping = {
            "orders": tables_cfg.get("fact_orders", "FACT_ORDERS_DAILY"),
            "fact_orders": tables_cfg.get("fact_orders", "FACT_ORDERS_DAILY"),
            "customer": tables_cfg.get("dim_customer", "DIM_CUSTOMER"),
            "dim_customer": tables_cfg.get("dim_customer", "DIM_CUSTOMER"),
            "product": tables_cfg.get("dim_product", "DIM_PRODUCT"),
            "dim_product": tables_cfg.get("dim_product", "DIM_PRODUCT"),
            "customer_360": tables_cfg.get("customer_360", "CUSTOMER_360"),
            "fact_customer_24m": tables_cfg.get("customer_360", "CUSTOMER_360"),
        }
        target_table = table_mapping.get(args.table, args.table.upper())
        
        # Load Gold table
        gold_df = load_gold_table(spark, config, args.table)
        
        # Prepare for Snowflake
        gold_df_prepared = prepare_dataframe_for_snowflake(gold_df)
        
        # Publish to Snowflake
        publish_to_snowflake(gold_df_prepared, sf_options, target_table, mode=args.mode)
        
        logger.info("🎉 Gold to Snowflake publish job completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Job failed: {e}", exc_info=True)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

