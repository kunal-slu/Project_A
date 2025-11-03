#!/usr/bin/env python3
"""
Gold layer star schema builder (P1-8).

Creates proper star schema with:
- dim_customer (SCD2)
- dim_product
- dim_date
- fact_sales (with surrogate keys)
- fact_behavior (optional)
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.path_resolver import resolve_path
from pyspark_interview_project.utils.io import read_delta, write_delta
from pyspark_interview_project.config_loader import load_config_resolved
from pyspark_interview_project.monitoring.lineage_decorator import lineage_job
from pyspark_interview_project.monitoring.metrics_collector import emit_rowcount
from pyspark_interview_project.utils.logging import setup_json_logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_dim_date(spark: SparkSession, start_date: str = "2020-01-01", end_date: str = "2030-12-31") -> DataFrame:
    """
    Build dim_date dimension table.
    
    Args:
        spark: SparkSession
        start_date: Start date YYYY-MM-DD
        end_date: End date YYYY-MM-DD
        
    Returns:
        dim_date DataFrame with surrogate key date_sk
    """
    from datetime import datetime, timedelta
    
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    dates = []
    current = start
    sk = 1
    
    while current <= end:
        dates.append({
            "date_sk": sk,
            "date": current.date(),
            "year": current.year,
            "month": current.month,
            "day": current.day,
            "quarter": (current.month - 1) // 3 + 1,
            "day_of_week": current.weekday() + 1,
            "day_name": current.strftime("%A"),
            "month_name": current.strftime("%B"),
            "is_weekend": (current.weekday() >= 5),
            "is_month_end": (current + timedelta(days=1)).month != current.month
        })
        current += timedelta(days=1)
        sk += 1
    
    # Create DataFrame
    dim_date = spark.createDataFrame(dates)
    
    logger.info(f"✅ dim_date: {len(dates):,} dates created")
    
    return dim_date


def build_dim_product(spark: SparkSession, config: Dict[str, Any]) -> DataFrame:
    """
    Build dim_product from silver products.
    
    Args:
        spark: SparkSession
        config: Configuration dict
        
    Returns:
        dim_product DataFrame with surrogate key product_sk
    """
    # Read silver products (or create from orders if products table doesn't exist)
    silver_path = resolve_path("lake://silver", "products", config=config)
    
    try:
        products = read_delta(spark, silver_path)
        
        if products.isEmpty():
            # If no products table, extract unique products from orders
            orders_path = resolve_path("lake://silver", "orders", config=config)
            orders = read_delta(spark, orders_path)
            
            if "product_id" in orders.columns:
                products = orders.select("product_id", "product_name", "product_category") \
                    .distinct()
            else:
                logger.warning("⚠️  No product data found, creating empty dim_product")
                return spark.createDataFrame([], schema=spark.sql("""
                    SELECT 
                        0 as product_sk,
                        '' as product_id,
                        '' as product_name,
                        '' as product_category,
                        '' as brand,
                        0.0 as price
                """).schema)
    except:
        logger.warning("⚠️  Products table not found, extracting from orders")
        orders_path = resolve_path("lake://silver", "orders", config=config)
        orders = read_delta(spark, orders_path)
        
        if "product_id" in orders.columns:
            products = orders.select("product_id", "product_name", "product_category").distinct()
        else:
            return spark.createDataFrame([], schema=spark.sql("SELECT 0 as product_sk, '' as product_id").schema)
    
    # Add surrogate key
    dim_product = products \
        .withColumn("product_sk", F.monotonically_increasing_id() + F.lit(1000000)) \
        .withColumn("product_name", F.coalesce(F.col("product_name"), F.lit("UNKNOWN"))) \
        .withColumn("product_category", F.coalesce(F.col("product_category"), F.lit("UNKNOWN")))
    
    logger.info(f"✅ dim_product: {dim_product.count():,} products")
    
    return dim_product


@lineage_job(
    name="gold_star_schema",
    inputs=[
        "s3://bucket/silver/orders",
        "s3://bucket/silver/customers",
        "s3://bucket/silver/customer_activity",
        "s3://bucket/gold/dim_customer"
    ],
    outputs=[
        "s3://bucket/gold/dim_date",
        "s3://bucket/gold/dim_product",
        "s3://bucket/gold/fact_sales",
        "s3://bucket/gold/fact_behavior"
    ]
)
def build_gold_star_schema(
    spark: SparkSession,
    config: Dict[str, Any],
    run_date: str = None
) -> Dict[str, DataFrame]:
    """
    Build complete star schema in Gold layer.
    
    Args:
        spark: SparkSession
        config: Configuration dict
        run_date: Processing date
        
    Returns:
        Dictionary with all gold tables
    """
    if run_date is None:
        run_date = datetime.now().strftime("%Y-%m-%d")
    
    logger.info(f"🚀 Building gold star schema (run_date={run_date})")
    
    gold_root = resolve_path("lake://gold", config=config)
    silver_root = resolve_path("lake://silver", config=config)
    
    # 1. Build dim_date
    logger.info("📊 Building dim_date...")
    dim_date = build_dim_date(spark)
    write_delta(dim_date, f"{gold_root}/dim_date", mode="overwrite")
    
    # 2. Build dim_product
    logger.info("📊 Building dim_product...")
    dim_product = build_dim_product(spark, config)
    write_delta(dim_product, f"{gold_root}/dim_product", mode="overwrite")
    
    # 3. Read dim_customer (SCD2, already created)
    logger.info("📊 Reading dim_customer (SCD2)...")
    dim_customer = read_delta(spark, f"{gold_root}/dim_customer")
    
    # Get current records only
    dim_customer_current = dim_customer.filter(F.col("is_current") == True)
    
    # 4. Read silver fact data
    logger.info("📥 Reading silver fact data...")
    silver_orders = read_delta(spark, f"{silver_root}/orders")
    silver_activity = read_delta(spark, f"{silver_root}/customer_activity")
    
    # 5. Build fact_sales with surrogate keys
    logger.info("📊 Building fact_sales...")
    
    # Join orders with dimensions
    fact_sales = silver_orders \
        .join(
            dim_customer_current.select("customer_sk", "customer_id"),
            on="customer_id",
            how="left"
        ) \
        .join(
            dim_product.select("product_sk", "product_id"),
            on="product_id",
            how="left"
        ) \
        .join(
            dim_date.select("date_sk", "date"),
            F.to_date(F.col("order_date")) == F.col("date"),
            how="left"
        ) \
        .select(
            # Surrogate keys
            F.coalesce(F.col("customer_sk"), F.lit(0)).alias("customer_sk"),
            F.coalesce(F.col("product_sk"), F.lit(0)).alias("product_sk"),
            F.coalesce(F.col("date_sk"), F.lit(0)).alias("date_sk"),
            # Natural keys (keep for reference)
            "order_id",
            "customer_id",
            "product_id",
            F.to_date("order_date").alias("order_date"),
            # Measures
            F.coalesce(F.col("total_amount"), F.col("amount_usd"), F.lit(0.0)).alias("amount_usd"),
            F.coalesce(F.col("quantity"), F.lit(1)).alias("quantity"),
            # Metadata
            F.col("_ingest_ts").alias("ingest_timestamp"),
            F.col("_run_date")
        )
    
    write_delta(
        fact_sales,
        f"{gold_root}/fact_sales",
        mode="append",
        partitionBy=["order_date"]
    )
    
    logger.info(f"✅ fact_sales: {fact_sales.count():,} records")
    
    # 6. Build fact_behavior (optional)
    logger.info("📊 Building fact_behavior...")
    
    fact_behavior = silver_activity \
        .join(
            dim_customer_current.select("customer_sk", "customer_id"),
            on="customer_id",
            how="left"
        ) \
        .join(
            dim_date.select("date_sk", "date"),
            F.to_date(F.col("event_date")) == F.col("date"),
            how="left"
        ) \
        .select(
            F.coalesce(F.col("customer_sk"), F.lit(0)).alias("customer_sk"),
            F.coalesce(F.col("date_sk"), F.lit(0)).alias("date_sk"),
            "event_id",
            "customer_id",
            "session_id",
            "event_name",
            "event_date",
            F.col("_ingest_ts").alias("ingest_timestamp")
        )
    
    write_delta(
        fact_behavior,
        f"{gold_root}/fact_behavior",
        mode="append",
        partitionBy=["event_date"]
    )
    
    logger.info(f"✅ fact_behavior: {fact_behavior.count():,} records")
    
    # Metrics
    emit_rowcount("gold_fact_sales_total", fact_sales.count(), {"layer": "gold"}, config)
    emit_rowcount("gold_fact_behavior_total", fact_behavior.count(), {"layer": "gold"}, config)
    
    logger.info("🎉 Gold star schema build complete")
    
    return {
        "dim_date": dim_date,
        "dim_product": dim_product,
        "dim_customer": dim_customer_current,
        "fact_sales": fact_sales,
        "fact_behavior": fact_behavior
    }


def main():
    """Main entry point."""
    setup_json_logging(level="INFO")
    
    config_path = Path("config/prod.yaml")
    if not config_path.exists():
        config_path = Path("config/local.yaml")
    
    config = load_config_resolved(str(config_path))
    spark = build_spark(config)
    
    try:
        build_gold_star_schema(spark, config)
        logger.info("🎉 Star schema job completed")
        return 0
    except Exception as e:
        logger.error(f"❌ Job failed: {e}", exc_info=True)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

