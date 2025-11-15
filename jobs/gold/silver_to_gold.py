#!/usr/bin/env python3
"""
Silver to Gold Transformation - Analytics-Ready Star Schema

Features:
- Config-driven paths
- Star schema (fact_orders, dim_customer, dim_product, dim_date)
- Customer 360 view
- Product performance metrics
- FX-normalized revenue
- Structured logging
- Comprehensive error handling
"""
import sys
import argparse
import tempfile
import uuid
import logging
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.config_loader import load_config_resolved
from pyspark_interview_project.utils.logging import setup_json_logging, get_trace_id
from pyspark_interview_project.utils.run_audit import write_run_audit
from pyspark_interview_project.monitoring.lineage_decorator import lineage_job
from pyspark_interview_project.monitoring.metrics_collector import emit_rowcount, emit_duration
import time
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_dim_date(spark: SparkSession, orders_df: DataFrame) -> DataFrame:
    """Build date dimension table."""
    logger.info("🔧 Building dim_date...")
    
    # Extract date column
    date_col = None
    for col in ["order_date", "event_ts", "date", "created_at"]:
        if col in orders_df.columns:
            date_col = col
            break
    
    if date_col:
        dim_date = orders_df.select(F.col(date_col).alias("date")).distinct()
    else:
        # Fallback: create date range
        from datetime import date, timedelta
        dates = [(date.today() - timedelta(days=x),) for x in range(730, 0, -1)]
        dim_date = spark.createDataFrame(dates, ["date"])
    
    dim_date = dim_date.filter(F.col("date").isNotNull()) \
                           .withColumn("date_sk", F.date_format("date", "yyyyMMdd").cast("int")) \
                           .withColumn("year", F.year("date")) \
                           .withColumn("quarter", F.quarter("date")) \
                           .withColumn("month", F.month("date")) \
                           .withColumn("day_of_week", F.dayofweek("date")) \
                           .withColumn("is_weekend", F.when(F.dayofweek("date").isin([1, 7]), True).otherwise(False)) \
                           .select("date_sk", "date", "year", "quarter", "month", "day_of_week", "is_weekend")
    
    logger.info(f"✅ dim_date: {dim_date.count():,} rows")
    return dim_date


def build_dim_customer(customers_silver: DataFrame) -> DataFrame:
    """Build customer dimension with SCD-lite."""
    logger.info("🔧 Building dim_customer...")
    
    dim_customer = customers_silver.select(
        F.col("customer_id"),
        F.col("customer_name"),
        F.col("primary_email").alias("email"),
        F.col("primary_phone").alias("phone"),
        F.col("country"),
        F.col("segment"),
        F.col("customer_since"),
        F.col("behavior_events_24m"),
        F.col("total_logins_24m"),
        F.col("total_revenue_24m")
    ).withColumn("customer_sk", F.monotonically_increasing_id()) \
     .withColumn("is_current", F.lit(True)) \
     .withColumn("effective_from", F.coalesce(F.col("customer_since"), F.current_date())) \
     .withColumn("effective_to", F.lit(None).cast("date"))
    
    logger.info(f"✅ dim_customer: {dim_customer.count():,} rows")
    return dim_customer


def build_dim_product(products_silver: DataFrame) -> DataFrame:
    """Build product dimension."""
    logger.info("🔧 Building dim_product...")
    
    dim_product = products_silver.select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("price"),
        F.col("currency")
    ).withColumn("product_sk", F.monotonically_increasing_id())
    
    logger.info(f"✅ dim_product: {dim_product.count():,} rows")
    return dim_product


def add_revenue_usd(orders_df: DataFrame, fx_df: DataFrame, base_currency: str = "USD") -> DataFrame:
    """
    Add revenue_usd column by joining with FX rates.
    
    Args:
        orders_df: Orders DataFrame with order_date, currency, amount columns
        fx_df: FX rates DataFrame with trade_date, base_ccy, quote_ccy, rate
        base_currency: Target currency (default: USD)
        
    Returns:
        Orders DataFrame with revenue_usd column
    """
    logger.info("💱 Adding revenue_usd using FX rates...")
    
    # Prepare FX rates for USD conversion
    # If base_ccy is the order currency and quote_ccy is USD, use rate directly
    # If quote_ccy is the order currency and base_ccy is USD, use 1/rate
    fx_usd = fx_df.filter(
        ((F.col("base_ccy") == base_currency) & (F.col("quote_ccy") != base_currency)) |
        ((F.col("quote_ccy") == base_currency) & (F.col("base_ccy") != base_currency))
    ).select(
        F.col("trade_date"),
        F.when(F.col("base_ccy") == base_currency, F.col("quote_ccy"))
         .otherwise(F.col("base_ccy")).alias("currency"),
        F.when(F.col("base_ccy") == base_currency, F.col("rate"))
         .otherwise(F.lit(1.0) / F.col("rate")).alias("usd_rate")
    )
    
    # Join orders with FX rates
    joined = orders_df.alias("o").join(
        fx_usd.alias("fx"),
        (F.to_date(F.col("o.order_date")) == F.col("fx.trade_date")) &
        (F.col("o.currency") == F.col("fx.currency")),
        "left"
    )
    
    # Calculate revenue_usd
    orders_with_usd = joined.withColumn(
        "revenue_usd",
        F.when(F.col("o.currency") == base_currency, F.col("o.amount_usd"))
         .when(F.col("fx.usd_rate").isNotNull(), F.col("o.amount_usd") * F.col("fx.usd_rate"))
         .otherwise(F.col("o.amount_usd"))  # Fallback if FX rate not found
    ).select(
        "o.*",
        "revenue_usd"
    )
    
    logger.info("✅ Revenue USD conversion complete")
    return orders_with_usd


def build_fact_orders(
    orders_silver: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
    dim_date: DataFrame,
    fx_silver: DataFrame = None
) -> DataFrame:
    """Build fact_orders with surrogate keys and FX-normalized revenue."""
    logger.info("🔧 Building fact_orders...")
    
    # If FX rates available, ensure revenue_usd is calculated
    if fx_silver is not None and "amount_usd" in orders_silver.columns:
        # Use existing amount_usd (already calculated in silver layer)
        orders_with_revenue = orders_silver
    else:
        # Fallback: use amount_usd if exists, otherwise use amount_orig
        orders_with_revenue = orders_silver.withColumn(
            "amount_usd",
            F.coalesce(F.col("amount_usd"), F.col("amount_orig"), F.lit(0.0))
        )
    
    # Join with dimensions to get surrogate keys
    fact = orders_with_revenue \
        .join(dim_customer.select("customer_id", "customer_sk"), "customer_id", "left") \
        .join(dim_product.select("product_id", "product_sk"), "product_id", "left")
    
    # Join with dim_date
    fact = fact.join(
        dim_date.select("date", "date_sk"),
        F.to_date(F.col("order_date")) == F.col("date"),
        "left"
    )
    
    # Select fact columns
    fact_orders = fact.select(
        F.col("order_id"),
        F.coalesce(F.col("customer_sk"), F.lit(-1)).alias("customer_sk"),
        F.coalesce(F.col("product_sk"), F.lit(-1)).alias("product_sk"),
        F.coalesce(F.col("date_sk"), F.lit(-1)).alias("date_sk"),
        F.col("order_date"),
        F.coalesce(F.col("amount_usd"), F.col("sales_amount"), F.lit(0.0)).alias("sales_amount"),
        F.col("quantity"),
        F.coalesce(F.col("currency"), F.lit("USD")).alias("currency")
    ).filter(F.col("order_id").isNotNull())
    
    logger.info(f"✅ fact_orders: {fact_orders.count():,} rows")
    return fact_orders


def build_customer_360(
    customers_silver: DataFrame,
    orders_silver: DataFrame,
    behavior_silver: DataFrame
) -> DataFrame:
    """Build customer 360 view with aggregated metrics."""
    logger.info("🔧 Building customer_360...")
    
    # Aggregate orders per customer
    orders_agg = orders_silver.groupBy("customer_id").agg(
        F.count("*").alias("total_orders"),
        F.sum("amount_usd").alias("total_revenue_usd"),
        F.avg("amount_usd").alias("avg_order_value"),
        F.max("order_date").alias("last_order_date"),
        F.min("order_date").alias("first_order_date")
    )
    
    # Aggregate behavior per customer
    behavior_agg = behavior_silver.groupBy("customer_id").agg(
        F.sum("login_count").alias("total_logins"),
        F.sum("page_views").alias("total_page_views"),
        F.sum("purchases").alias("total_purchases"),
        F.avg("session_duration_minutes").alias("avg_session_duration")
    )
    
    # Join all sources
    customer_360 = customers_silver \
        .join(orders_agg, "customer_id", "left") \
        .join(behavior_agg, "customer_id", "left")
    
    # Fill nulls
    customer_360 = customer_360.fillna({
        "total_orders": 0,
        "total_revenue_usd": 0.0,
        "avg_order_value": 0.0,
        "total_logins": 0,
        "total_page_views": 0,
        "total_purchases": 0,
        "avg_session_duration": 0.0
    })
    
    # Calculate LTV (simplified)
    customer_360 = customer_360.withColumn(
        "estimated_ltv",
        F.col("total_revenue_usd") * 1.5  # Simple multiplier
    )
    
    logger.info(f"✅ customer_360: {customer_360.count():,} rows")
    return customer_360


def build_product_performance(
    products_silver: DataFrame,
    orders_silver: DataFrame
) -> DataFrame:
    """Build product performance metrics."""
    logger.info("🔧 Building product_performance...")
    
    product_perf = orders_silver.groupBy("product_id", "product_name", "category").agg(
        F.count("*").alias("total_orders"),
        F.sum("amount_usd").alias("total_revenue_usd"),
        F.sum("quantity").alias("total_quantity_sold"),
        F.avg("amount_usd").alias("avg_order_value"),
        F.max("order_date").alias("last_sale_date")
    )
    
    logger.info(f"✅ product_performance: {product_perf.count():,} rows")
    return product_perf


@lineage_job(
    name="silver_to_gold_complete",
    inputs=[
        "s3://bucket/silver/customers_silver",
        "s3://bucket/silver/orders_silver",
        "s3://bucket/silver/products_silver",
        "s3://bucket/silver/customer_behavior_silver"
    ],
    outputs=[
        "s3://bucket/gold/fact_orders",
        "s3://bucket/gold/dim_customer",
        "s3://bucket/gold/dim_product",
        "s3://bucket/gold/dim_date",
        "s3://bucket/gold/customer_360",
        "s3://bucket/gold/product_performance"
    ]
)
def silver_to_gold_complete(
    spark: SparkSession,
    config: Dict[str, Any],
    run_date: str = None
) -> Dict[str, DataFrame]:
    """
    Complete silver to gold transformation with star schema.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        run_date: Processing date YYYY-MM-DD
        
    Returns:
        Dictionary with all gold DataFrames
    """
    if run_date is None:
        run_date = datetime.utcnow().strftime("%Y-%m-%d")
    
    logger.info(f"🚀 Starting silver to gold transformation (run_date={run_date})")
    start_time = time.time()
    
    # Get paths from config
    silver_root = config["paths"]["silver_root"]
    gold_root = config["paths"]["gold_root"]
    tables = config["tables"]
    
    try:
        # 1. Read silver tables
        logger.info("📥 Reading silver tables...")
        customers_silver = spark.read.format("delta").load(f"{silver_root}/{tables['silver']['customers']}")
        orders_silver = spark.read.format("delta").load(f"{silver_root}/{tables['silver']['orders']}")
        products_silver = spark.read.format("delta").load(f"{silver_root}/{tables['silver']['products']}")
        behavior_silver = spark.read.format("delta").load(f"{silver_root}/{tables['silver']['behavior']}")
        
        # Read FX silver if available
        fx_silver = None
        try:
            fx_silver = spark.read.format("delta").load(f"{silver_root}/{tables['silver']['fx_rates']}")
            logger.info(f"  FX Rates: {fx_silver.count():,} rows")
        except Exception as e:
            logger.warning(f"FX silver table not found, skipping FX normalization: {e}")
        
        logger.info(f"  Customers: {customers_silver.count():,} rows")
        logger.info(f"  Orders: {orders_silver.count():,} rows")
        logger.info(f"  Products: {products_silver.count():,} rows")
        logger.info(f"  Behavior: {behavior_silver.count():,} rows")
        
        # 2. Build dimensions
        dim_date = build_dim_date(spark, orders_silver)
        dim_customer = build_dim_customer(customers_silver)
        dim_product = build_dim_product(products_silver)
        
        # 3. Build facts (with FX normalization if available)
        fact_orders = build_fact_orders(orders_silver, dim_customer, dim_product, dim_date, fx_silver)
        
        # 4. Build analytics tables
        customer_360 = build_customer_360(customers_silver, orders_silver, behavior_silver)
        product_perf = build_product_performance(products_silver, orders_silver)
        
        # 5. Write gold tables
        logger.info("💾 Writing gold tables...")
        
        fact_orders.write.format("delta").mode("overwrite") \
            .partitionBy("order_date") \
            .save(f"{gold_root}/{tables['gold']['fact_orders']}")
        
        dim_customer.write.format("delta").mode("overwrite") \
            .save(f"{gold_root}/{tables['gold']['dim_customer']}")
        
        dim_product.write.format("delta").mode("overwrite") \
            .save(f"{gold_root}/{tables['gold']['dim_product']}")
        
        dim_date.write.format("delta").mode("overwrite") \
            .save(f"{gold_root}/dim_date")
        
        customer_360.write.format("delta").mode("overwrite") \
            .partitionBy("country") \
            .save(f"{gold_root}/customer_360")
        
        product_perf.write.format("delta").mode("overwrite") \
            .save(f"{gold_root}/product_performance")
        
        # 6. Emit metrics
        duration_ms = (time.time() - start_time) * 1000
        emit_rowcount("gold_fact_orders_total", fact_orders.count(), {"layer": "gold"}, config)
        emit_rowcount("gold_dim_customer_total", dim_customer.count(), {"layer": "gold"}, config)
        emit_duration("gold_transformation_duration", duration_ms, {"stage": "silver_to_gold"}, config)
        
        logger.info(f"✅ Gold transformation complete in {duration_ms:.0f}ms")
        
        return {
            "fact_orders": fact_orders,
            "dim_customer": dim_customer,
            "dim_product": dim_product,
            "dim_date": dim_date,
            "customer_360": customer_360,
            "product_performance": product_perf
        }
        
    except Exception as e:
        logger.error(f"❌ Gold transformation failed: {e}", exc_info=True)
        raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Silver to Gold transformation job")
    parser.add_argument("--env", default="dev", help="Environment (dev/prod)")
    parser.add_argument("--config", help="Config file path (local or S3)")
    args = parser.parse_args()
    
    # Setup structured logging
    trace_id = get_trace_id()
    setup_json_logging(level="INFO", include_trace_id=True)
    logger.info(f"Job started (trace_id={trace_id}, env={args.env})")
    
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
    
    run_id = str(uuid.uuid4())
    start_time = time.time()
    spark = build_spark(config)
    
    try:
        run_date = datetime.utcnow().strftime("%Y-%m-%d")
        
        # Count input rows from silver
        silver_root = config["paths"]["silver_root"]
        rows_in_approx = 0
        try:
            tables = config["tables"]["silver"]
            for table_name in ["customers", "orders", "products", "behavior"]:
                if table_name in tables:
                    try:
                        df_temp = spark.read.format("delta").load(f"{silver_root}/{tables[table_name]}")
                        rows_in_approx += df_temp.count()
                    except:
                        pass
        except:
            rows_in_approx = 0
        
        results = silver_to_gold_complete(spark, config, run_date=run_date)
        
        # Count output rows
        rows_out = sum([
            results['fact_orders'].count(),
            results['dim_customer'].count(),
            results['dim_product'].count(),
            results['dim_date'].count(),
            results['customer_360'].count(),
            results['product_performance'].count()
        ])
        
        duration_ms = (time.time() - start_time) * 1000
        
        logger.info("✅ Silver to Gold transformation completed")
        logger.info(f"  - fact_orders: {results['fact_orders'].count():,} rows")
        logger.info(f"  - dim_customer: {results['dim_customer'].count():,} rows")
        logger.info(f"  - dim_product: {results['dim_product'].count():,} rows")
        logger.info(f"  - dim_date: {results['dim_date'].count():,} rows")
        logger.info(f"  - customer_360: {results['customer_360'].count():,} rows")
        logger.info(f"  - product_performance: {results['product_performance'].count():,} rows")
        
        # Write run audit
        lake_bucket = config.get("buckets", {}).get("lake", "")
        if lake_bucket:
            try:
                write_run_audit(
                    bucket=lake_bucket,
                    job_name="silver_to_gold",
                    env=config.get("environment", "dev"),
                    source=silver_root,
                    target=config["paths"]["gold_root"],
                    rows_in=rows_in_approx,
                    rows_out=rows_out,
                    status="SUCCESS",
                    run_id=run_id,
                    duration_ms=duration_ms,
                    config=config
                )
            except Exception as e:
                logger.warning(f"⚠️  Failed to write run audit: {e}")
        
    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}", exc_info=True, extra={"trace_id": trace_id})
        
        # Write failure audit
        lake_bucket = config.get("buckets", {}).get("lake", "")
        if lake_bucket:
            try:
                write_run_audit(
                    bucket=lake_bucket,
                    job_name="silver_to_gold",
                    env=config.get("environment", "dev"),
                    source=config.get("paths", {}).get("silver_root", ""),
                    target=config.get("paths", {}).get("gold_root", ""),
                    rows_in=0,
                    rows_out=0,
                    status="FAILED",
                    run_id=run_id,
                    error_message=str(e),
                    config=config
                )
            except:
                pass
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

