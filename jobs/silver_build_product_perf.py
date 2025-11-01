"""
Build product_perf_daily Gold table.

Inputs:
- silver/snowflake/orders
- silver/fx_rates

Output:
- gold/product_perf_daily

Daily product performance metrics with FX conversion.
"""

import argparse
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    current_timestamp, to_date, lit, coalesce, countDistinct
)

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.monitoring.lineage_decorator import lineage_job
from pyspark_interview_project.monitoring.metrics_collector import emit_metrics
import time

logger = logging.getLogger(__name__)


@lineage_job(
    name="build_product_perf_daily",
    inputs=[
        "s3://bucket/silver/snowflake/orders",
        "s3://bucket/silver/fx_rates"
    ],
    outputs=["s3://bucket/gold/product_perf_daily"]
)
def build_product_perf_daily(spark: SparkSession, config: dict) -> DataFrame:
    """
    Build daily product performance metrics.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        
    Returns:
        Product performance DataFrame
    """
    start_time = time.time()
    silver_path = config.get("data_lake", {}).get("silver_path", "data/lakehouse_delta/silver")
    gold_path = config.get("data_lake", {}).get("gold_path", "data/lakehouse_delta/gold")
    
    logger.info("Building product_perf_daily Gold table")
    
    # Read orders
    orders_df = spark.read.format("delta").load(f"{silver_path}/snowflake/orders")
    logger.info(f"Loaded {orders_df.count():,} orders")
    
    # Read FX rates if available
    fx_df = None
    try:
        fx_df = spark.read.format("delta").load(f"{silver_path}/fx/rates")
        logger.info(f"Loaded {fx_df.count():,} FX rates")
    except Exception as e:
        logger.warning(f"FX rates not available: {e}")
    
    # Extract date from order_date
    if "order_date" in orders_df.columns:
        orders_df = orders_df.withColumn("order_date_only", to_date(col("order_date")))
    elif "_proc_date" in orders_df.columns:
        orders_df = orders_df.withColumn("order_date_only", col("_proc_date"))
    else:
        raise ValueError("No date column found in orders")
    
    # Join FX rates if available (convert to USD)
    if fx_df is not None and "currency" in orders_df.columns:
        # Join on date and currency
        orders_df = orders_df.join(
            fx_df.select(col("date").alias("order_date_only"), col("currency"), col("rate_usd")),
            ["order_date_only", "currency"],
            "left"
        )
        # Convert amount to USD
        orders_df = orders_df.withColumn(
            "amount_usd",
            col("total_amount") * coalesce(col("rate_usd"), lit(1.0))
        )
    else:
        # Assume USD if no FX
        orders_df = orders_df.withColumn("amount_usd", col("total_amount"))
    
    # Aggregate daily product metrics
    product_perf = orders_df.groupBy("order_date_only", "product_id").agg(
        count("order_id").alias("order_count"),
        spark_sum("quantity").alias("total_quantity"),
        spark_sum("amount_usd").alias("revenue_usd"),
        avg("amount_usd").alias("avg_order_value_usd"),
        spark_max("amount_usd").alias("max_order_value_usd"),
        spark_min("amount_usd").alias("min_order_value_usd"),
        countDistinct("customer_id").alias("unique_customers")
    ).withColumnRenamed("order_date_only", "date")
    
    # Add metadata
    product_perf = product_perf.withColumn("_load_ts", current_timestamp())
    
    final_count = product_perf.count()
    duration = time.time() - start_time
    
    logger.info(f"✅ Built product_perf_daily with {final_count:,} records")
    
    # Write to Gold
    gold_output = f"{gold_path}/product_perf_daily"
    logger.info(f"Writing product_perf_daily to: {gold_output}")
    
    product_perf.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy("date") \
        .save(gold_output)
    
    # Emit metrics
    emit_metrics(
        job_name="build_product_perf_daily",
        rows_in=orders_df.count(),
        rows_out=final_count,
        duration_seconds=duration,
        dq_status="pass",
        config=config
    )
    
    logger.info(f"✅ Successfully wrote product_perf_daily to Gold")
    
    return product_perf


def main():
    parser = argparse.ArgumentParser(description="Build product_perf_daily Gold table")
    parser.add_argument("--config", default="config/local.yaml", help="Config file path")
    args = parser.parse_args()
    
    config = load_conf(args.config)
    spark = build_spark(app_name="build_product_perf_daily", config=config)
    
    try:
        build_product_perf_daily(spark, config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

