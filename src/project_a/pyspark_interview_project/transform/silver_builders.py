"""
Silver Layer Transform Builders

Reusable functions for building Silver layer tables from Bronze data.
Handles joins, aggregations, and data quality transformations.
"""

import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def build_customers_silver(
    df_accounts: DataFrame,
    df_contacts: DataFrame,
    df_opps: DataFrame,
    df_behavior: DataFrame
) -> DataFrame:
    """
    Build silver customers table with joins and aggregations.
    
    Maps CRM account data to customer dimension with:
    - Account information
    - Latest contact information
    - Aggregated behavior metrics
    """
    logger.info("🔧 Building silver.customers...")
    
    # Clean accounts - map actual CSV columns to expected schema
    accounts = df_accounts.select(
        F.col("Id").alias("customer_id"),
        F.trim(F.col("Name")).alias("customer_name"),
        F.coalesce(F.col("CustomerSegment"), F.lit("UNKNOWN")).alias("segment"),
        F.to_date("CreatedDate").alias("customer_since"),
        F.coalesce(F.col("BillingCountry"), F.lit("UNKNOWN")).alias("country"),
        F.col("Industry").alias("industry"),
        F.coalesce(F.col("AnnualRevenue"), F.lit(0.0)).alias("annual_revenue")
    ).filter(F.col("customer_id").isNotNull())
    
    # Get latest contact per customer
    last_contact = df_contacts.withColumn(
        "contact_ts",
        F.coalesce(
            F.to_timestamp("LastModifiedDate"),
            F.to_timestamp("CreatedDate"),
            F.current_timestamp()
        )
    ).withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("AccountId").orderBy(F.col("contact_ts").desc())
        )
    ).filter(F.col("rn") == 1).select(
        F.col("AccountId").alias("customer_id"),
        F.col("Email").alias("primary_email"),
        F.col("Phone").alias("primary_phone")
    )
    
    # Aggregate behavior metrics - derive from event_name and conversion_value
    behavior_agg = df_behavior.groupBy("customer_id").agg(
        F.count("*").alias("behavior_events_24m"),
        F.sum(F.when(F.col("event_name") == "login", 1).otherwise(0)).alias("total_logins_24m"),
        F.sum(F.when(F.col("event_name") == "page_view", 1).otherwise(0)).alias("total_page_views_24m"),
        F.sum(F.when(F.col("event_name") == "purchase", 1).otherwise(0)).alias("total_purchases_24m"),
        F.sum(F.coalesce(F.col("conversion_value"), F.lit(0.0))).alias("total_revenue_24m")
    )
    
    # Join all sources
    customers = accounts.join(last_contact, "customer_id", "left") \
                       .join(behavior_agg, "customer_id", "left")
    
    # Null handling
    customers = customers.fillna({
        "segment": "UNKNOWN",
        "country": "UNKNOWN",
        "behavior_events_24m": 0,
        "total_logins_24m": 0,
        "total_page_views_24m": 0,
        "total_purchases_24m": 0,
        "total_revenue_24m": 0.0
    })
    
    try:
        logger.info(f"✅ silver.customers: {customers.count():,} rows")
    except Exception as e:
        logger.warning(f"Could not count customers: {e}")
    
    return customers


def build_orders_silver(
    df_orders: DataFrame,
    df_customers: DataFrame,
    df_products: DataFrame,
    df_fx: DataFrame
) -> DataFrame:
    """
    Build silver orders table with FX normalization.
    
    Joins orders with customers, products, and FX rates to:
    - Enrich with customer/product information
    - Normalize amounts to USD using FX rates
    """
    logger.info("🔧 Building silver.orders...")
    
    # Handle empty DataFrame case
    try:
        if df_orders.count() == 0:
            logger.warning("⚠️  df_orders is empty, creating empty DataFrame with expected schema")
            from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType
            empty_schema = StructType([
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("order_date", DateType(), True),
                StructField("amount_orig", DoubleType(), True),
                StructField("currency", StringType(), True),
                StructField("amount_usd", DoubleType(), True),
                StructField("quantity", IntegerType(), True)
            ])
            return df_orders.sparkSession.createDataFrame([], empty_schema)
    except Exception:
        pass  # Continue if count fails
    
    # Map order columns - use total_amount (not amount) as that's what the CSV has
    orders = df_orders.select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("product_id"),
        F.to_date("order_date").alias("order_date"),
        F.coalesce(F.col("total_amount"), F.lit(0.0)).alias("amount_orig"),
        F.coalesce(F.col("currency"), F.lit("USD")).alias("currency"),
        F.coalesce(F.col("quantity"), F.lit(1)).alias("quantity")
    )
    
    # Join with products to get product_name and category
    products = df_products.select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("price_usd").alias("price")
    )
    
    orders = orders.join(products, "product_id", "left")
    
    # Join with FX rates for currency conversion (if FX data is available)
    # FX DataFrame has trade_date or date, not rate_date
    if df_fx.count() > 0:
        fx_rates = df_fx.select(
            F.col("base_currency"),
            F.col("target_currency"),
            F.col("exchange_rate"),
            F.coalesce(F.to_date("trade_date"), F.to_date("date")).alias("fx_date")
        )
        
        # Join orders with FX rates (match on currency and date)
        # Use direct column references since DataFrame is not aliased
        orders = orders.join(
            fx_rates,
            (F.col("currency") == F.col("target_currency")) &
            (F.col("order_date") == F.col("fx_date")),
            "left"
        ).withColumn(
            "amount_usd",
            F.when(
                F.col("currency") == "USD",
                F.col("amount_orig")
            ).otherwise(
                F.coalesce(
                    F.col("amount_orig") * F.col("exchange_rate"),
                    F.col("amount_orig")  # Fallback if FX rate not found
                )
            )
        ).select(
            "order_id",
            "customer_id",
            "product_id",
            "product_name",
            "category",
            "order_date",
            "amount_orig",
            "currency",
            "amount_usd",
            "quantity"
        )
    else:
        # No FX data - just use amount_orig as amount_usd
        orders = orders.withColumn(
            "amount_usd",
            F.col("amount_orig")
        ).select(
            "order_id",
            "customer_id",
            "product_id",
            "product_name",
            "category",
            "order_date",
            "amount_orig",
            "currency",
            "amount_usd",
            "quantity"
        )
    
    try:
        logger.info(f"✅ silver.orders: {orders.count():,} rows")
    except Exception as e:
        logger.warning(f"Could not count orders: {e}")
    
    return orders


def build_products_silver(df_products: DataFrame) -> DataFrame:
    """Build silver products table."""
    logger.info("🔧 Building silver.products...")
    
    products = df_products.select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("category"),
        F.coalesce(F.col("price_usd"), F.lit(0.0)).alias("price_usd"),
        F.lit("USD").alias("currency")  # Hardcode currency as it's not in source
    )
    
    try:
        logger.info(f"✅ silver.products: {products.count():,} rows")
    except Exception as e:
        logger.warning(f"Could not count products: {e}")
    
    return products


def build_behavior_silver(df_behavior: DataFrame) -> DataFrame:
    """Build silver behavior table with aggregated metrics."""
    logger.info("🔧 Building silver.customer_behavior...")
    
    # Derive metrics from event_name and conversion_value
    behavior = df_behavior.withColumn(
        "event_date",
        F.to_date(F.col("event_timestamp"))
    ).withColumn(
        "login_count",
        F.when(F.col("event_name") == "login", 1).otherwise(0)
    ).withColumn(
        "page_views",
        F.when(F.col("event_name") == "page_view", 1).otherwise(0)
    ).withColumn(
        "purchases",
        F.when(F.col("event_name") == "purchase", 1).otherwise(0)
    ).withColumn(
        "revenue",
        F.coalesce(F.col("conversion_value"), F.lit(0.0))
    ).groupBy("customer_id", "event_date").agg(
        F.sum("login_count").alias("login_count"),
        F.sum("page_views").alias("page_views"),
        F.sum("purchases").alias("purchases"),
        F.sum("revenue").alias("revenue"),
        F.avg("duration_seconds").alias("session_duration_minutes")
    ).withColumn(
        "session_duration_minutes",
        F.col("session_duration_minutes") / 60.0
    )
    
    try:
        logger.info(f"✅ silver.customer_behavior: {behavior.count():,} rows")
    except Exception as e:
        logger.warning(f"Could not count behavior: {e}")
    
    return behavior

