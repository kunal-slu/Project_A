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

# Add local src/ path for local execution (when not using wheel)
# This allows the script to work both locally and on EMR
if not any("deps" in p for p in sys.path):
    # Local execution - add src to path
    project_root = Path(__file__).parent.parent.parent
    src_path = project_root / "src"
    if src_path.exists() and str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from project_a.utils.spark_session import build_spark
from project_a.config_loader import load_config_resolved
from project_a.utils.logging import setup_json_logging, get_trace_id
from project_a.utils.run_audit import write_run_audit
from project_a.pyspark_interview_project.monitoring.lineage_decorator import lineage_job
from project_a.pyspark_interview_project.monitoring.metrics_collector import emit_rowcount, emit_duration
import time

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


def build_dim_customer(customers_silver: DataFrame, spark: SparkSession = None) -> DataFrame:
    """Build customer dimension with SCD-lite."""
    logger.info("🔧 Building dim_customer...")
    
    # Get SparkSession if not provided
    if spark is None:
        try:
            spark = customers_silver.sql_ctx.sparkSession
        except Exception:
            try:
                spark = customers_silver.sparkSession
            except Exception:
                from pyspark.sql import SparkSession
                spark = SparkSession.getActiveSession()
    
    # Handle empty DataFrame
    try:
        if not customers_silver.columns or customers_silver.count() == 0:
            logger.warning("customers_silver is empty, creating empty dim_customer")
            from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType, BooleanType
            schema = StructType([
                StructField("customer_id", StringType(), True),
                StructField("customer_name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("country", StringType(), True),
                StructField("segment", StringType(), True),
                StructField("customer_since", DateType(), True),
                StructField("customer_sk", LongType(), True),
                StructField("is_current", BooleanType(), True),
                StructField("effective_from", DateType(), True),
                StructField("effective_to", DateType(), True),
            ])
            return spark.createDataFrame([], schema)
    except Exception:
        pass  # Continue if count fails
    
    # Select columns that exist, with fallbacks
    available_cols = customers_silver.columns
    select_exprs = []
    
    # Required: customer_id
    if "customer_id" in available_cols:
        select_exprs.append(F.col("customer_id"))
    else:
        # If no customer_id, we can't build dim_customer properly
        logger.warning("customers_silver has no customer_id column, creating empty dim_customer")
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("country", StringType(), True),
            StructField("segment", StringType(), True),
            StructField("customer_since", DateType(), True),
            StructField("customer_sk", LongType(), True),
            StructField("is_current", BooleanType(), True),
            StructField("effective_from", DateType(), True),
            StructField("effective_to", DateType(), True),
        ])
        return spark.createDataFrame([], schema)
    
    # Optional columns with fallbacks
    if "customer_name" in available_cols:
        select_exprs.append(F.col("customer_name"))
    else:
        select_exprs.append(F.lit(None).cast("string").alias("customer_name"))
    
    if "primary_email" in available_cols:
        select_exprs.append(F.col("primary_email").alias("email"))
    elif "email" in available_cols:
        select_exprs.append(F.col("email"))
    else:
        select_exprs.append(F.lit(None).cast("string").alias("email"))
    
    if "primary_phone" in available_cols:
        select_exprs.append(F.col("primary_phone").alias("phone"))
    elif "phone" in available_cols:
        select_exprs.append(F.col("phone"))
    else:
        select_exprs.append(F.lit(None).cast("string").alias("phone"))
    
    if "country" in available_cols:
        select_exprs.append(F.col("country"))
    else:
        select_exprs.append(F.lit(None).cast("string").alias("country"))
    
    if "segment" in available_cols:
        select_exprs.append(F.col("segment"))
    else:
        select_exprs.append(F.lit(None).cast("string").alias("segment"))
    
    if "customer_since" in available_cols:
        select_exprs.append(F.col("customer_since"))
    else:
        select_exprs.append(F.lit(None).cast("date").alias("customer_since"))
    
    dim_customer = customers_silver.select(*select_exprs) \
        .withColumn("customer_sk", F.monotonically_increasing_id()) \
        .withColumn("is_current", F.lit(True)) \
        .withColumn("effective_from", F.coalesce(F.col("customer_since"), F.current_date())) \
        .withColumn("effective_to", F.lit(None).cast("date"))
    
    try:
        count = dim_customer.count()
        logger.info(f"✅ dim_customer: {count:,} rows")
    except Exception as e:
        logger.warning(f"Could not count dim_customer: {e}")
    return dim_customer


def build_dim_product(products_silver: DataFrame, spark: SparkSession = None) -> DataFrame:
    """Build product dimension."""
    logger.info("🔧 Building dim_product...")
    
    # Get SparkSession - try multiple methods
    if spark is None:
        try:
            spark = products_silver.sql_ctx.sparkSession
        except Exception:
            try:
                spark = products_silver.sparkSession
            except Exception:
                # Last resort: get from SparkContext
                spark = SparkSession.getActiveSession()
                if spark is None:
                    raise RuntimeError("Cannot get SparkSession from DataFrame")
    
    # Handle empty DataFrame or missing columns
    try:
        # Check if DataFrame has any columns
        if not products_silver.columns:
            raise Exception("DataFrame has no columns")
        
        # Try to get count - if this fails, DataFrame might be empty or invalid
        row_count = products_silver.count()
        if row_count == 0:
            logger.warning("products_silver is empty, creating empty dim_product")
            from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
            schema = StructType([
                StructField("product_id", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("currency", StringType(), True),
                StructField("product_sk", LongType(), True),
            ])
            return spark.createDataFrame([], schema)
    except Exception as e:
        logger.warning(f"Could not process products_silver, creating empty dim_product: {e}")
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("product_sk", LongType(), True),
        ])
        return spark.createDataFrame([], schema)
    
    # Select columns that exist
    available_cols = products_silver.columns
    select_cols = []
    for col_name in ["product_id", "product_name", "category", "price", "currency"]:
        if col_name in available_cols:
            select_cols.append(F.col(col_name))
        else:
            # Create null column with appropriate type
            if col_name in ["product_id", "product_name", "category", "currency"]:
                select_cols.append(F.lit(None).cast("string").alias(col_name))
            else:  # price
                select_cols.append(F.lit(None).cast("double").alias(col_name))
    
    dim_product = products_silver.select(*select_cols).withColumn("product_sk", F.monotonically_increasing_id())
    
    try:
        count = dim_product.count()
        logger.info(f"✅ dim_product: {count:,} rows")
    except Exception as e:
        logger.warning(f"Could not count dim_product: {e}")
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
    fx_silver: DataFrame = None,
    spark: SparkSession = None
) -> DataFrame:
    """Build fact_orders with surrogate keys and FX-normalized revenue."""
    logger.info("🔧 Building fact_orders...")
    
    # Get SparkSession if not provided
    if spark is None:
        try:
            spark = orders_silver.sql_ctx.sparkSession
        except Exception:
            try:
                spark = orders_silver.sparkSession
            except Exception:
                spark = SparkSession.getActiveSession()
    
    # Handle empty orders_silver
    try:
        if not orders_silver.columns or orders_silver.count() == 0:
            logger.warning("orders_silver is empty, creating empty fact_orders")
            from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType, DoubleType, IntegerType
            schema = StructType([
                StructField("order_id", StringType(), True),
                StructField("customer_sk", LongType(), True),
                StructField("product_sk", LongType(), True),
                StructField("date_sk", LongType(), True),
                StructField("order_date", DateType(), True),
                StructField("sales_amount", DoubleType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("currency", StringType(), True),
            ])
            return spark.createDataFrame([], schema)
    except Exception:
        pass  # Continue if count fails
    
    # If FX rates available, ensure revenue_usd is calculated
    try:
        if fx_silver is not None and "amount_usd" in orders_silver.columns:
            # Use existing amount_usd (already calculated in silver layer)
            orders_with_revenue = orders_silver
        else:
            # Fallback: use amount_usd if exists, otherwise use amount_orig
            orders_cols = orders_silver.columns
            if "amount_usd" not in orders_cols:
                if "amount_orig" in orders_cols:
                    orders_with_revenue = orders_silver.withColumn("amount_usd", F.col("amount_orig"))
                elif "sales_amount" in orders_cols:
                    orders_with_revenue = orders_silver.withColumn("amount_usd", F.col("sales_amount"))
                else:
                    orders_with_revenue = orders_silver.withColumn("amount_usd", F.lit(0.0))
            else:
                orders_with_revenue = orders_silver
    except Exception as e:
        logger.warning(f"Failed to prepare revenue column: {e}, using orders_silver as-is")
        orders_with_revenue = orders_silver
        if "amount_usd" not in orders_with_revenue.columns:
            orders_with_revenue = orders_with_revenue.withColumn("amount_usd", F.lit(0.0))
    
    # Join with dimensions to get surrogate keys (handle missing columns/empty DataFrames)
    fact = orders_with_revenue
    
    # Join dim_customer if both have customer_id
    if "customer_id" in fact.columns:
        try:
            if "customer_id" in dim_customer.columns and dim_customer.count() > 0:
                fact = fact.join(
                    dim_customer.select("customer_id", "customer_sk"),
                    "customer_id",
                    "left"
                )
            else:
                fact = fact.withColumn("customer_sk", F.lit(-1))
        except Exception as e:
            logger.warning(f"Failed to join dim_customer: {e}, using default -1")
            fact = fact.withColumn("customer_sk", F.lit(-1))
    else:
        fact = fact.withColumn("customer_sk", F.lit(-1))
    
    # Join dim_product if both have product_id
    if "product_id" in fact.columns:
        try:
            if "product_id" in dim_product.columns and dim_product.count() > 0:
                fact = fact.join(
                    dim_product.select("product_id", "product_sk"),
                    "product_id",
                    "left"
                )
            else:
                fact = fact.withColumn("product_sk", F.lit(-1))
        except Exception as e:
            logger.warning(f"Failed to join dim_product: {e}, using default -1")
            fact = fact.withColumn("product_sk", F.lit(-1))
    else:
        fact = fact.withColumn("product_sk", F.lit(-1))
    
    # Join with dim_date if order_date exists
    if "order_date" in fact.columns:
        try:
            if "date" in dim_date.columns and dim_date.count() > 0:
                fact = fact.join(
                    dim_date.select("date", "date_sk"),
                    F.to_date(F.col("order_date")) == F.col("date"),
                    "left"
                )
            else:
                fact = fact.withColumn("date_sk", F.lit(-1))
        except Exception as e:
            logger.warning(f"Failed to join dim_date: {e}, using default -1")
            fact = fact.withColumn("date_sk", F.lit(-1))
    else:
        fact = fact.withColumn("date_sk", F.lit(-1))
    
    # Select fact columns (handle missing columns)
    select_exprs = []
    
    if "order_id" in fact.columns:
        select_exprs.append(F.col("order_id"))
    else:
        select_exprs.append(F.lit(None).cast("string").alias("order_id"))
    
    select_exprs.append(F.coalesce(F.col("customer_sk"), F.lit(-1)).alias("customer_sk"))
    select_exprs.append(F.coalesce(F.col("product_sk"), F.lit(-1)).alias("product_sk"))
    select_exprs.append(F.coalesce(F.col("date_sk"), F.lit(-1)).alias("date_sk"))
    
    if "order_date" in fact.columns:
        select_exprs.append(F.col("order_date"))
    else:
        select_exprs.append(F.lit(None).cast("date").alias("order_date"))
    
    # Sales amount
    if "amount_usd" in fact.columns:
        select_exprs.append(F.coalesce(F.col("amount_usd"), F.lit(0.0)).alias("sales_amount"))
    elif "sales_amount" in fact.columns:
        select_exprs.append(F.coalesce(F.col("sales_amount"), F.lit(0.0)).alias("sales_amount"))
    else:
        select_exprs.append(F.lit(0.0).alias("sales_amount"))
    
    # Quantity
    if "quantity" in fact.columns:
        select_exprs.append(F.coalesce(F.col("quantity"), F.lit(1)).alias("quantity"))
    else:
        select_exprs.append(F.lit(1).alias("quantity"))
    
    # Currency
    if "currency" in fact.columns:
        select_exprs.append(F.coalesce(F.col("currency"), F.lit("USD")).alias("currency"))
    else:
        select_exprs.append(F.lit("USD").alias("currency"))
    
    fact_orders = fact.select(*select_exprs)
    
    # Filter only if order_id exists and is not null
    if "order_id" in fact_orders.columns:
        fact_orders = fact_orders.filter(F.col("order_id").isNotNull())
    
    try:
        count = fact_orders.count()
        logger.info(f"✅ fact_orders: {count:,} rows")
    except Exception as e:
        logger.warning(f"Could not count fact_orders: {e}")
    return fact_orders


def build_customer_360(
    customers_silver: DataFrame,
    orders_silver: DataFrame,
    behavior_silver: DataFrame,
    spark: SparkSession = None
) -> DataFrame:
    """Build customer 360 view with aggregated metrics."""
    logger.info("🔧 Building customer_360...")
    
    # Get SparkSession if not provided
    if spark is None:
        try:
            spark = customers_silver.sql_ctx.sparkSession
        except Exception:
            try:
                spark = customers_silver.sparkSession
            except Exception:
                spark = SparkSession.getActiveSession()
    
    # Handle empty DataFrames
    try:
        if not customers_silver.columns or customers_silver.count() == 0:
            logger.warning("customers_silver is empty, creating empty customer_360")
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
            schema = StructType([
                StructField("customer_id", StringType(), True),
                StructField("total_orders", IntegerType(), True),
                StructField("total_revenue_usd", DoubleType(), True),
                StructField("avg_order_value", DoubleType(), True),
                StructField("last_order_date", DateType(), True),
                StructField("first_order_date", DateType(), True),
                StructField("total_logins", IntegerType(), True),
                StructField("total_page_views", IntegerType(), True),
                StructField("total_purchases", IntegerType(), True),
                StructField("avg_session_duration", DoubleType(), True),
                StructField("estimated_ltv", DoubleType(), True),
            ])
            return spark.createDataFrame([], schema)
    except Exception:
        pass  # Continue if count fails
    
    # Aggregate orders per customer (handle missing columns)
    orders_cols = orders_silver.columns
    orders_agg_exprs = []
    if "customer_id" in orders_cols:
        if "amount_usd" in orders_cols:
            orders_agg_exprs = [
                F.count("*").alias("total_orders"),
                F.sum("amount_usd").alias("total_revenue_usd"),
                F.avg("amount_usd").alias("avg_order_value"),
            ]
        else:
            orders_agg_exprs = [F.count("*").alias("total_orders")]
        
        if "order_date" in orders_cols:
            orders_agg_exprs.extend([
                F.max("order_date").alias("last_order_date"),
                F.min("order_date").alias("first_order_date")
            ])
        
        if orders_agg_exprs:
            orders_agg = orders_silver.groupBy("customer_id").agg(*orders_agg_exprs)
        else:
            orders_agg = orders_silver.select("customer_id").distinct().withColumn("total_orders", F.lit(0))
    else:
        # Create empty orders_agg
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("total_orders", IntegerType(), True),
            StructField("total_revenue_usd", DoubleType(), True),
            StructField("avg_order_value", DoubleType(), True),
            StructField("last_order_date", DateType(), True),
            StructField("first_order_date", DateType(), True),
        ])
        orders_agg = spark.createDataFrame([], schema)
    
    # Aggregate behavior per customer (handle missing columns)
    behavior_cols = behavior_silver.columns
    behavior_agg_exprs = []
    if "customer_id" in behavior_cols:
        if "login_count" in behavior_cols:
            behavior_agg_exprs.append(F.sum("login_count").alias("total_logins"))
        if "page_views" in behavior_cols:
            behavior_agg_exprs.append(F.sum("page_views").alias("total_page_views"))
        if "purchases" in behavior_cols:
            behavior_agg_exprs.append(F.sum("purchases").alias("total_purchases"))
        if "session_duration_minutes" in behavior_cols:
            behavior_agg_exprs.append(F.avg("session_duration_minutes").alias("avg_session_duration"))
        
        if behavior_agg_exprs:
            behavior_agg = behavior_silver.groupBy("customer_id").agg(*behavior_agg_exprs)
        else:
            behavior_agg = behavior_silver.select("customer_id").distinct()
    else:
        # Create empty behavior_agg
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("total_logins", IntegerType(), True),
            StructField("total_page_views", IntegerType(), True),
            StructField("total_purchases", IntegerType(), True),
            StructField("avg_session_duration", DoubleType(), True),
        ])
        behavior_agg = spark.createDataFrame([], schema)
    
    # Join all sources (handle missing customer_id)
    if "customer_id" in customers_silver.columns:
        customer_360 = customers_silver \
            .join(orders_agg, "customer_id", "left") \
            .join(behavior_agg, "customer_id", "left")
    else:
        # If customers_silver has no customer_id, create minimal schema
        customer_360 = customers_silver
    
    # Fill nulls (only for columns that exist)
    fill_dict = {}
    for col_name in ["total_orders", "total_revenue_usd", "avg_order_value", "total_logins", "total_page_views", "total_purchases", "avg_session_duration"]:
        if col_name in customer_360.columns:
            fill_dict[col_name] = 0 if col_name in ["total_orders", "total_logins", "total_page_views", "total_purchases"] else 0.0
    
    if fill_dict:
        customer_360 = customer_360.fillna(fill_dict)
    
    # Calculate LTV (if total_revenue_usd exists)
    if "total_revenue_usd" in customer_360.columns:
        customer_360 = customer_360.withColumn(
            "estimated_ltv",
            F.col("total_revenue_usd") * 1.5
        )
    else:
        customer_360 = customer_360.withColumn("estimated_ltv", F.lit(0.0))
    
    try:
        count = customer_360.count()
        logger.info(f"✅ customer_360: {count:,} rows")
    except Exception as e:
        logger.warning(f"Could not count customer_360: {e}")
    return customer_360


def build_product_performance(
    products_silver: DataFrame,
    orders_silver: DataFrame,
    spark: SparkSession = None
) -> DataFrame:
    """Build product performance metrics."""
    logger.info("🔧 Building product_performance...")
    
    # Get SparkSession if not provided
    if spark is None:
        try:
            spark = orders_silver.sql_ctx.sparkSession
        except Exception:
            try:
                spark = orders_silver.sparkSession
            except Exception:
                spark = SparkSession.getActiveSession()
    
    # Handle empty DataFrames
    try:
        if not orders_silver.columns or orders_silver.count() == 0:
            logger.warning("orders_silver is empty, creating empty product_performance")
            schema = StructType([
                StructField("product_id", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("total_orders", IntegerType(), True),
                StructField("total_revenue_usd", DoubleType(), True),
                StructField("total_quantity_sold", IntegerType(), True),
                StructField("avg_order_value", DoubleType(), True),
                StructField("last_sale_date", DateType(), True),
            ])
            return spark.createDataFrame([], schema)
    except Exception:
        pass  # Continue if count fails
    
    # Build aggregation expressions based on available columns
    orders_cols = orders_silver.columns
    group_by_cols = []
    agg_exprs = []
    
    # Group by columns
    for col_name in ["product_id", "product_name", "category"]:
        if col_name in orders_cols:
            group_by_cols.append(col_name)
    
    if not group_by_cols:
        # If no product columns, create empty DataFrame
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("total_orders", IntegerType(), True),
            StructField("total_revenue_usd", DoubleType(), True),
            StructField("total_quantity_sold", IntegerType(), True),
            StructField("avg_order_value", DoubleType(), True),
            StructField("last_sale_date", DateType(), True),
        ])
        return spark.createDataFrame([], schema)
    
    # Aggregation expressions
    agg_exprs.append(F.count("*").alias("total_orders"))
    
    if "amount_usd" in orders_cols:
        agg_exprs.append(F.sum("amount_usd").alias("total_revenue_usd"))
        agg_exprs.append(F.avg("amount_usd").alias("avg_order_value"))
    else:
        agg_exprs.append(F.lit(0.0).alias("total_revenue_usd"))
        agg_exprs.append(F.lit(0.0).alias("avg_order_value"))
    
    if "quantity" in orders_cols:
        agg_exprs.append(F.sum("quantity").alias("total_quantity_sold"))
    else:
        agg_exprs.append(F.lit(0).alias("total_quantity_sold"))
    
    if "order_date" in orders_cols:
        agg_exprs.append(F.max("order_date").alias("last_sale_date"))
    
    product_perf = orders_silver.groupBy(*group_by_cols).agg(*agg_exprs)
    
    try:
        count = product_perf.count()
        logger.info(f"✅ product_performance: {count:,} rows")
    except Exception as e:
        logger.warning(f"Could not count product_performance: {e}")
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
    
    # Get paths from config with defaults
    paths = config.get("paths", {})
    silver_root = paths.get("silver_root", config.get("data_lake", {}).get("silver_path", "s3://my-etl-lake-demo-424570854632/silver"))
    gold_root = paths.get("gold_root", config.get("data_lake", {}).get("gold_path", "s3://my-etl-lake-demo-424570854632/gold"))
    
    # Get table names from config with defaults
    tables_config = config.get("tables", {})
    silver_tables = tables_config.get("silver", {})
    gold_tables = tables_config.get("gold", {})
    
    # Default table names (matching bronze_to_silver.py defaults)
    silver_table_names = {
        "customers": silver_tables.get("customers", "customers_silver"),
        "orders": silver_tables.get("orders", "orders_silver"),
        "products": silver_tables.get("products", "products_silver"),
        "behavior": silver_tables.get("behavior", "customer_behavior_silver"),
        "fx_rates": silver_tables.get("fx_rates", "fx_rates_silver"),
    }
    
    gold_table_names = {
        "fact_orders": gold_tables.get("fact_orders", "fact_orders"),
        "dim_customer": gold_tables.get("dim_customer", "dim_customer"),
        "dim_product": gold_tables.get("dim_product", "dim_product"),
    }
    
    try:
        # 1. Read silver tables (with graceful fallback for missing tables)
        logger.info("📥 Reading silver tables...")
        
        def read_silver_table_or_empty(spark, path, table_name, default_schema=None):
            """Read Parquet (local) or Delta (AWS) table, or create empty DataFrame with schema."""
            # Try Parquet first (for local), then Delta (for AWS)
            try:
                df = spark.read.format("parquet").load(path)
                schema = df.schema
                if not schema:
                    raise Exception("Empty schema")
                _ = df.columns
                logger.info(f"  {table_name}: {df.count():,} rows (Parquet)")
                return df
            except Exception as e:
                logger.debug(f"{table_name} not found at {path} (Parquet), trying Delta: {e}")
                try:
                    df = spark.read.format("delta").load(path)
                    schema = df.schema
                    if not schema:
                        raise Exception("Empty schema")
                    _ = df.columns
                    logger.info(f"  {table_name}: {df.count():,} rows (Delta)")
                    return df
                except Exception as e2:
                    logger.warning(f"{table_name} not found at {path} (Parquet/Delta), creating empty DataFrame: {e2}")
                    if default_schema:
                        return spark.createDataFrame([], default_schema)
                    from pyspark.sql.types import StructType, StructField, StringType
                    return spark.createDataFrame([], StructType([StructField("id", StringType(), True)]))
        
        customers_silver = read_silver_table_or_empty(
            spark, f"{silver_root}/{silver_table_names['customers']}", "customers_silver"
        )
        orders_silver = read_silver_table_or_empty(
            spark, f"{silver_root}/{silver_table_names['orders']}", "orders_silver"
        )
        products_silver = read_silver_table_or_empty(
            spark, f"{silver_root}/{silver_table_names['products']}", "products_silver"
        )
        behavior_silver = read_silver_table_or_empty(
            spark, f"{silver_root}/{silver_table_names['behavior']}", "behavior_silver"
        )
        
        # Read FX silver if available
        fx_silver = None
        try:
            fx_silver = spark.read.format("delta").load(f"{silver_root}/{silver_table_names['fx_rates']}")
            logger.info(f"  FX Rates: {fx_silver.count():,} rows")
        except Exception as e:
            logger.warning(f"FX silver table not found, skipping FX normalization: {e}")
        
        # Safely get counts (handle empty DataFrames)
        try:
            logger.info(f"  Customers: {customers_silver.count():,} rows")
            logger.info(f"  Orders: {orders_silver.count():,} rows")
            logger.info(f"  Products: {products_silver.count():,} rows")
            logger.info(f"  Behavior: {behavior_silver.count():,} rows")
        except Exception:
            logger.info("  Row counts unavailable (may be empty DataFrames)")
        
        # 2. Build dimensions
        dim_date = build_dim_date(spark, orders_silver)
        dim_customer = build_dim_customer(customers_silver, spark)
        dim_product = build_dim_product(products_silver, spark)
        
        # 3. Build facts (with FX normalization if available)
        fact_orders = build_fact_orders(orders_silver, dim_customer, dim_product, dim_date, fx_silver, spark)
        
        # 4. Build analytics tables
        customer_360 = build_customer_360(customers_silver, orders_silver, behavior_silver, spark)
        product_perf = build_product_performance(products_silver, orders_silver, spark)
        
        # 5. Write gold tables
        logger.info("💾 Writing gold tables...")
        
        # Determine format: Parquet for local, Delta for AWS
        use_delta = True
        try:
            spark_env = spark.conf.get("spark.sql.extensions", "")
            if "delta" not in spark_env.lower():
                use_delta = False
        except Exception:
            use_delta = False
        
        # Check environment from config
        try:
            import os
            config_env = os.getenv("SPARK_ENV") or config.get("environment", "local")
            if config_env == "local":
                use_delta = False
        except Exception:
            pass
        
        write_format = "delta" if use_delta else "parquet"
        logger.info(f"Using {write_format} format for gold tables")
        
        # Helper to delete existing local directory before writing
        def delete_if_exists(path: str):
            if not use_delta and not path.startswith("s3://"):
                try:
                    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
                    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                        spark.sparkContext._jvm.java.net.URI(path),
                        hadoop_conf
                    )
                    fs_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
                    if fs.exists(fs_path):
                        fs.delete(fs_path, True)
                        logger.debug(f"Deleted existing directory: {path}")
                except Exception as e:
                    logger.debug(f"Could not delete {path}: {e}")
        
        try:
            target = f"{gold_root}/{gold_table_names['fact_orders']}"
            delete_if_exists(target)
            writer = fact_orders.write.format(write_format).mode("overwrite")
            if "order_date" in fact_orders.columns:
                writer = writer.partitionBy("order_date")
            writer.save(target)
            logger.info(f"✅ Wrote fact_orders to {target}")
        except Exception as e:
            logger.error(f"Failed to write fact_orders: {e}", exc_info=True)
            raise
        
        try:
            target = f"{gold_root}/{gold_table_names['dim_customer']}"
            delete_if_exists(target)
            dim_customer.write.format(write_format).mode("overwrite") \
                .save(target)
            logger.info(f"✅ Wrote dim_customer to {target}")
        except Exception as e:
            logger.error(f"Failed to write dim_customer: {e}", exc_info=True)
            raise
        
        try:
            target = f"{gold_root}/{gold_table_names['dim_product']}"
            delete_if_exists(target)
            dim_product.write.format(write_format).mode("overwrite") \
                .save(target)
            logger.info(f"✅ Wrote dim_product to {target}")
        except Exception as e:
            logger.error(f"Failed to write dim_product: {e}", exc_info=True)
            raise
        
        try:
            target = f"{gold_root}/dim_date"
            delete_if_exists(target)
            dim_date.write.format(write_format).mode("overwrite") \
                .save(target)
            logger.info(f"✅ Wrote dim_date to {target}")
        except Exception as e:
            logger.error(f"Failed to write dim_date: {e}", exc_info=True)
            raise
        
        try:
            target = f"{gold_root}/customer_360"
            delete_if_exists(target)
            writer = customer_360.write.format(write_format).mode("overwrite")
            if "country" in customer_360.columns:
                writer = writer.partitionBy("country")
            writer.save(target)
            logger.info(f"✅ Wrote customer_360 to {target}")
        except Exception as e:
            logger.error(f"Failed to write customer_360: {e}", exc_info=True)
            raise
        
        try:
            target = f"{gold_root}/product_performance"
            delete_if_exists(target)
            product_perf.write.format(write_format).mode("overwrite") \
                .save(target)
            logger.info(f"✅ Wrote product_performance to {target}")
        except Exception as e:
            logger.error(f"Failed to write product_performance: {e}", exc_info=True)
            raise
        
        # 6. Emit metrics
        duration_ms = (time.time() - start_time) * 1000
        try:
            fact_count = fact_orders.count()
            emit_rowcount("gold_fact_orders_total", fact_count, {"layer": "gold"}, config)
        except Exception as e:
            logger.warning(f"Could not emit fact_orders metric: {e}")
        
        try:
            customer_count = dim_customer.count()
            emit_rowcount("gold_dim_customer_total", customer_count, {"layer": "gold"}, config)
        except Exception as e:
            logger.warning(f"Could not emit dim_customer metric: {e}")
        
        try:
            emit_duration("gold_transformation_duration", duration_ms, {"stage": "silver_to_gold"}, config)
        except Exception as e:
            logger.warning(f"Could not emit duration metric: {e}")
        
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
            # Use boto3 to read from S3 (no SparkSession needed)
            import boto3
            s3_client = boto3.client('s3')
            bucket = config_path.split('/')[2]
            key = '/'.join(config_path.split('/')[3:])
            try:
                response = s3_client.get_object(Bucket=bucket, Key=key)
                config_content = response['Body'].read().decode('utf-8')
                tmp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8")
                tmp_file.write(config_content)
                tmp_file.close()
                config_path = tmp_file.name
            except Exception as e:
                logger.error(f"Failed to load config from S3: {e}")
                raise
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
        
        # Count input rows from silver (with defensive config access)
        paths = config.get("paths", {})
        silver_root = paths.get("silver_root", config.get("data_lake", {}).get("silver_path", "s3://my-etl-lake-demo-424570854632/silver"))
        rows_in_approx = 0
        try:
            tables_config = config.get("tables", {})
            tables = tables_config.get("silver", {})
            for table_name in ["customers", "orders", "products", "behavior"]:
                if table_name in tables:
                    try:
                        df_temp = spark.read.format("delta").load(f"{silver_root}/{tables[table_name]}")
                        rows_in_approx += df_temp.count()
                    except Exception:
                        pass
        except Exception:
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
                    target=paths.get("gold_root", config.get("data_lake", {}).get("gold_path", "s3://my-etl-lake-demo-424570854632/gold")),
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
            except Exception:
                pass
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

