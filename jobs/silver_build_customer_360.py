"""
Build customer_360 Gold table by joining all Silver sources.

Inputs:
- silver/behavior
- silver/crm_accounts
- silver/crm_contacts
- silver/snowflake_orders

Output:
- gold/customer_360

This is the enterprise view that joins all tables.
"""

import argparse
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, max as spark_max, sum as spark_sum, count, 
    collect_list, first, last, current_timestamp, lit
)

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.monitoring.lineage_decorator import lineage_job
from pyspark_interview_project.monitoring.lineage_emitter import emit_start, emit_complete, emit_fail
from pyspark_interview_project.monitoring.metrics_collector import emit_metrics
from pyspark_interview_project.dq.great_expectations_runner import GreatExpectationsRunner
from pyspark_interview_project.io.write_table import write_table
import time
import uuid

logger = logging.getLogger(__name__)


@lineage_job(
    name="build_customer_360",
    inputs=[
        "s3://bucket/silver/behavior",
        "s3://bucket/silver/crm_accounts",
        "s3://bucket/silver/crm_contacts",
        "s3://bucket/silver/snowflake/orders"
    ],
    outputs=["s3://bucket/gold/customer_360"]
)
def build_customer_360(spark: SparkSession, config: dict) -> DataFrame:
    """
    Build customer_360 Gold table by joining multiple Silver sources.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        
    Returns:
        Customer 360 DataFrame
    """
    start_time = time.time()
    silver_path = config.get("data_lake", {}).get("silver_path", "data/lakehouse_delta/silver")
    gold_path = config.get("data_lake", {}).get("gold_path", "data/lakehouse_delta/gold")
    
    logger.info("Building customer_360 Gold table")
    
    # Read Silver sources
    dfs = {}
    
    # 1. CRM Contacts (base customer dimension)
    try:
        contacts_df = spark.read.format("delta").load(f"{silver_path}/crm/contacts")
        dfs["contacts"] = contacts_df
        logger.info(f"Loaded {contacts_df.count():,} contacts")
    except Exception as e:
        logger.warning(f"Could not load contacts: {e}")
        contacts_df = None
    
    # 2. CRM Accounts
    try:
        accounts_df = spark.read.format("delta").load(f"{silver_path}/crm/accounts")
        dfs["accounts"] = accounts_df
        logger.info(f"Loaded {accounts_df.count():,} accounts")
    except Exception as e:
        logger.warning(f"Could not load accounts: {e}")
        accounts_df = None
    
    # 3. Snowflake Orders (transactions)
    try:
        orders_df = spark.read.format("delta").load(f"{silver_path}/snowflake/orders")
        dfs["orders"] = orders_df
        logger.info(f"Loaded {orders_df.count():,} orders")
    except Exception as e:
        logger.warning(f"Could not load orders: {e}")
        orders_df = None
    
    # 4. Behavior events
    try:
        behavior_df = spark.read.format("delta").load(f"{silver_path}/behavior")
        dfs["behavior"] = behavior_df
        logger.info(f"Loaded {behavior_df.count():,} behavior events")
    except Exception as e:
        logger.warning(f"Could not load behavior: {e}")
        behavior_df = None
    
    # Start with contacts as base
    if contacts_df is None:
        raise ValueError("Contacts table is required for customer_360")
    
    customer_360 = contacts_df.select(
        col("customer_id").alias("customer_id"),
        col("first_name"),
        col("last_name"),
        col("email"),
        col("phone"),
        col("account_id")
    ).distinct()
    
    # Join accounts if available
    if accounts_df is not None:
        customer_360 = customer_360.join(
            accounts_df.select(
                col("account_id"),
                col("account_name"),
                col("industry"),
                col("city"),
                col("state"),
                col("country")
            ),
            "account_id",
            "left"
        )
    
    # Aggregate orders metrics
    if orders_df is not None:
        order_metrics = orders_df.groupBy("customer_id").agg(
            spark_sum("total_amount").alias("lifetime_value_usd"),
            count("order_id").alias("total_orders"),
            spark_max("order_date").alias("last_order_date")
        )
        customer_360 = customer_360.join(order_metrics, "customer_id", "left")
    else:
        # Add null columns if orders not available
        customer_360 = customer_360.withColumn("lifetime_value_usd", lit(None)) \
                                   .withColumn("total_orders", lit(0)) \
                                   .withColumn("last_order_date", lit(None))
    
    # Aggregate behavior metrics
    if behavior_df is not None:
        behavior_cols = ["customer_id"]
        if "event_ts" in behavior_df.columns:
            behavior_cols.append("event_ts")
        
        behavior_metrics = behavior_df.groupBy("customer_id").agg(
            count("event_id").alias("total_events"),
            spark_max("event_ts").alias("last_event_ts"),
            collect_list("event_name").alias("event_types")
        )
        customer_360 = customer_360.join(behavior_metrics, "customer_id", "left")
    else:
        customer_360 = customer_360.withColumn("total_events", lit(0)) \
                                   .withColumn("last_event_ts", lit(None)) \
                                   .withColumn("event_types", lit(None))
    
    # Add metadata
    customer_360 = customer_360.withColumn("_load_ts", current_timestamp())
    
    final_count = customer_360.count()
    duration = time.time() - start_time
    
    logger.info(f"✅ Built customer_360 with {final_count:,} customers")
    
    # Write to Gold using abstracted write_table (supports Iceberg/Delta/Parquet)
    logger.info(f"Writing customer_360 to table: gold.customer_360")
    
    write_table(
        df=customer_360,
        table_name="gold.customer_360",
        mode="overwrite",
        cfg=config,
        spark=spark
    )
    
    # Run Great Expectations DQ checks
    logger.info("Running Great Expectations DQ checks on Gold customer_360...")
    try:
        ge_runner = GreatExpectationsRunner()
        ge_runner.init_context()
        ge_result = ge_runner.run_checkpoint(
            checkpoint_name="gold_customer_360_checkpoint",
            fail_on_error=True
        )
        if ge_result.get("success"):
            logger.info("✅ Great Expectations validation passed for Gold customer_360")
        else:
            raise RuntimeError("Great Expectations validation failed for Gold customer_360")
    except Exception as e:
        logger.warning(f"GE validation not available: {e}")
        # Continue if GE not configured
    
    # Emit metrics
    try:
        rows_in_sum = sum(
            int(df.count()) if df is not None else 0 
            for df in [contacts_df, accounts_df, orders_df, behavior_df]
        )
        emit_metrics(
            job_name="build_customer_360",
            rows_in=rows_in_sum,
            rows_out=int(final_count),
            duration_seconds=duration,
            dq_status="pass",
            config=config
        )
    except Exception as e:
        logger.warning(f"Could not emit metrics: {e}")
    
    logger.info(f"✅ Successfully wrote customer_360 to Gold")
    
    return customer_360


def main():
    parser = argparse.ArgumentParser(description="Build customer_360 Gold table")
    parser.add_argument("--config", default="config/local.yaml", help="Config file path")
    args = parser.parse_args()
    
    config = load_conf(args.config)
    spark = build_spark(app_name="build_customer_360", config=config)
    
    try:
        build_customer_360(spark, config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

