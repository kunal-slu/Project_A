"""Publish Gold tables to Snowflake using MERGE pattern.

Enterprise-grade pattern: write to staging table, then MERGE into target.
This provides idempotent upserts without blind overwrites.

Reads Delta from S3 and upserts into Snowflake fact/dimension tables.
Supports fact_orders (ORDER_ID + ORDER_DATE) and customer_360 (customer_id).
"""

import argparse
import logging
import sys
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional
from textwrap import dedent

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.config_loader import load_config_resolved
from pyspark_interview_project.utils.run_audit import write_run_audit
import time
import uuid

logger = logging.getLogger(__name__)


def merge_into_snowflake(
    sf_options: Dict[str, str],
    target_table: str,
    staging_table: str,
    merge_keys: list,
    spark: SparkSession
) -> None:
    """
    Execute MERGE statement in Snowflake.
    
    Args:
        sf_options: Snowflake connection options
        target_table: Target table name
        staging_table: Staging table name
        merge_keys: List of columns to match on (e.g., ["ORDER_ID", "ORDER_DATE"])
        spark: SparkSession for JDBC execution
    """
    schema = sf_options["sfSchema"]
    full_target = f"{schema}.{target_table}"
    full_staging = f"{schema}.{staging_table}"
    
    # Build ON clause
    on_clause = " AND ".join([f"t.{key} = s.{key}" for key in merge_keys])
    
    # Get columns from staging table (exclude merge keys from UPDATE SET)
    # For now, we'll use a generic approach
    # In production, you'd query INFORMATION_SCHEMA to get actual columns
    update_set = "AMOUNT = s.AMOUNT, UPDATED_AT = s.UPDATED_AT"
    insert_cols = "ORDER_ID, ORDER_DATE, CUSTOMER_ID, AMOUNT, CREATED_AT, UPDATED_AT"
    insert_vals = "s.ORDER_ID, s.ORDER_DATE, s.CUSTOMER_ID, s.AMOUNT, s.CREATED_AT, s.UPDATED_AT"
    
    merge_sql = dedent(f"""
        MERGE INTO {full_target} AS tgt
        USING {full_staging} AS src
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET
            {update_set}
        WHEN NOT MATCHED THEN INSERT (
            {insert_cols}
        ) VALUES (
            {insert_vals}
        )
    """).strip()
    
    logger.info(f"Executing MERGE SQL:\n{merge_sql}")
    
    # Build JDBC URL
    jdbc_url = (
        f"jdbc:snowflake://{sf_options['sfURL']}/?"
        f"db={sf_options['sfDatabase']}&"
        f"schema={sf_options['sfSchema']}&"
        f"warehouse={sf_options['sfWarehouse']}"
    )
    
    # Execute MERGE using JDBC
    spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("user", sf_options["sfUser"]) \
        .option("password", sf_options["sfPassword"]) \
        .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
        .option("query", merge_sql) \
        .load() \
        .collect()
    
    logger.info(f"✅ MERGE completed for {full_target}")


def publish_gold_to_snowflake(
    spark: SparkSession,
    config: Dict[str, Any],
    table_name: str = "fact_orders",
    mode: str = "merge"
) -> bool:
    """
    Publish Gold table to Snowflake using staging + MERGE pattern.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        table_name: Gold table name (fact_orders, customer_360, etc.)
        mode: "merge" (default) or "overwrite"
    """
    run_id = str(uuid.uuid4())
    start_time = time.time()
    
    # Get paths from config
    gold_root = config.get("paths", {}).get("gold_root", "")
    if not gold_root:
        gold_root = config.get("data_lake", {}).get("gold_path", "s3a://my-etl-lake-demo/gold")
    
    gold_input = f"{gold_root}/{table_name}"
    
    # Get Snowflake config
    sf_cfg = config.get("snowflake", {})
    sf_options = {
        "sfURL": sf_cfg.get("url", "").replace(".snowflakecomputing.com", "") + ".snowflakecomputing.com",
        "sfUser": sf_cfg.get("user", ""),
        "sfPassword": sf_cfg.get("password", ""),
        "sfDatabase": sf_cfg.get("database", "ANALYTICS"),
        "sfSchema": sf_cfg.get("schema", "PUBLIC"),
        "sfWarehouse": sf_cfg.get("warehouse", ""),
        "sfRole": sf_cfg.get("role", ""),
    }
    
    logger.info(f"📥 Reading Gold table: {gold_input}")
    gold_df = spark.read.format("delta").load(gold_input)
    rows_in = gold_df.count()
    
    # Add metadata columns
    gold_df = gold_df.withColumn("UPDATED_AT", current_timestamp())
    if "CREATED_AT" not in gold_df.columns:
        gold_df = gold_df.withColumn("CREATED_AT", current_timestamp())
    
    # Determine merge keys based on table
    if table_name == "fact_orders":
        merge_keys = ["ORDER_ID", "ORDER_DATE"]
        target_table = "FACT_ORDERS"
    elif table_name == "customer_360":
        merge_keys = ["customer_id"]
        target_table = "CUSTOMER_360"
    else:
        # Default: use first column as key
        merge_keys = [gold_df.columns[0]]
        target_table = table_name.upper()
    
    staging_table = f"{target_table}_STAGE"
    schema = sf_options["sfSchema"]
    
    logger.info(f"💾 Writing {rows_in:,} rows to staging table: {schema}.{staging_table}")
    
    # Write to staging table
    gold_df.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", staging_table) \
        .mode("overwrite") \
        .save()
    
    logger.info("✅ Staging table written successfully")
    
    if mode == "merge":
        # Execute MERGE
        try:
            merge_into_snowflake(
                sf_options=sf_options,
                target_table=target_table,
                staging_table=staging_table,
                merge_keys=merge_keys,
                spark=spark
            )
            
            # Clean up staging table
            jdbc_url = (
                f"jdbc:snowflake://{sf_options['sfURL']}/?"
                f"db={sf_options['sfDatabase']}&"
                f"schema={sf_options['sfSchema']}&"
                f"warehouse={sf_options['sfWarehouse']}"
            )
            drop_sql = f"DROP TABLE IF EXISTS {schema}.{staging_table}"
            spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("user", sf_options["sfUser"]) \
                .option("password", sf_options["sfPassword"]) \
                .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
                .option("query", drop_sql) \
                .load() \
                .collect()
            logger.info(f"✅ Dropped staging table: {staging_table}")
            
        except Exception as e:
            logger.error(f"❌ MERGE failed: {e}")
            logger.warning(f"⚠️  Staging table {staging_table} may need manual cleanup")
            raise
    else:
        # Overwrite mode (not recommended for production)
        logger.warning("⚠️  Using overwrite mode (not recommended for production)")
        gold_df.write \
            .format("snowflake") \
            .options(**sf_options) \
            .option("dbtable", target_table) \
            .mode("overwrite") \
            .save()
    
    # Write run audit
    duration_ms = (time.time() - start_time) * 1000
    lake_bucket = config.get("buckets", {}).get("lake", "")
    if lake_bucket:
        try:
            write_run_audit(
                bucket=lake_bucket,
                job_name="publish_gold_to_snowflake",
                env=config.get("environment", "dev"),
                source=gold_input,
                target=f"snowflake://{sf_options['sfDatabase']}/{sf_options['sfSchema']}/{target_table}",
                rows_in=rows_in,
                rows_out=rows_in,
                status="SUCCESS",
                run_id=run_id,
                duration_ms=duration_ms,
                config=config
            )
        except Exception as e:
            logger.warning(f"⚠️  Failed to write run audit: {e}")
    
    logger.info(f"🎉 Published {rows_in:,} rows to Snowflake {target_table} in {duration_ms:.0f}ms")
    return True


def main():
    """Main entry point for EMR job."""
    parser = argparse.ArgumentParser(description="Publish Gold to Snowflake")
    parser.add_argument("--env", default="dev", help="Environment (dev/prod)")
    parser.add_argument("--config", help="Config file path (local or S3)")
    parser.add_argument("--table", default="fact_orders", help="Gold table name (fact_orders, customer_360)")
    parser.add_argument("--mode", default="merge", choices=["merge", "overwrite"], help="Publish mode")
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    logger.info(f"Job started (env={args.env}, table={args.table}, mode={args.mode})")
    
    # Load config
    if args.config:
        config_path = args.config
        if config_path.startswith("s3://"):
            from pyspark.sql import SparkSession
            spark_temp = SparkSession.builder \
                .appName("config_loader") \
                .config("spark.sql.adaptive.enabled", "false") \
                .getOrCreate()
            try:
                config_lines = spark_temp.sparkContext.textFile(config_path).collect()
                config_content = "\n".join(config_lines)
                tmp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8")
                tmp_file.write(config_content)
                tmp_file.close()
                config_path = tmp_file.name
            finally:
                spark_temp.stop()
        else:
            config_path = str(Path(config_path))
    else:
        config_path = Path(f"config/{args.env}.yaml")
        if not config_path.exists():
            config_path = Path("config/prod.yaml")
        if not config_path.exists():
            config_path = Path("config/local.yaml")
        config_path = str(config_path)
    
    config = load_config_resolved(config_path)
    
    if not config.get("environment"):
        config["environment"] = "emr"
    
    spark = build_spark(config)
    
    try:
        publish_gold_to_snowflake(spark, config, table_name=args.table, mode=args.mode)
        return 0
    except Exception as e:
        logger.error(f"❌ Job failed: {e}", exc_info=True)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    main()


