#!/usr/bin/env python3
"""
Bronze to Silver Transformation - Enterprise-Grade Multi-Source ETL

Features:
- Config-driven paths (no hardcoded)
- All 5 sources: CRM, Redshift, Snowflake, FX, Kafka
- Proper joins with null handling
- FX normalization and currency conversion
- Data quality checks
- Structured logging
- Comprehensive error handling
"""
import sys
import os
import argparse
import tempfile
import uuid
import logging
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

# Add EMR Serverless dependencies directory to Python path
# When --archives s3://.../emr_dependencies.zip#deps is used, it extracts to ./deps/
# Try multiple possible locations where deps might be extracted
possible_deps_paths = [
    os.path.abspath(os.path.join(os.getcwd(), "deps")),
    os.path.abspath("deps"),
    "/tmp/deps",
    os.path.join(os.path.dirname(__file__), "deps") if "__file__" in globals() else None
]

deps_path = None
for path in possible_deps_paths:
    if path and os.path.exists(path) and os.path.isdir(path):
        deps_path = path
        break

if deps_path:
    # Add deps directory to Python path (packages are at root of deps/)
    if deps_path not in sys.path:
        sys.path.insert(0, deps_path)
    # Set PYTHONPATH for Spark executors and subprocesses  
    current_pythonpath = os.environ.get("PYTHONPATH", "")
    if deps_path not in current_pythonpath:
        os.environ["PYTHONPATH"] = deps_path + (os.pathsep + current_pythonpath if current_pythonpath else "")

# Add local src/ path for local execution (when not using wheel)
# This allows the script to work both locally and on EMR
if not any("deps" in p for p in sys.path):
    # Local execution - add src to path
    project_root = Path(__file__).parent.parent.parent
    src_path = project_root / "src"
    if src_path.exists() and str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from project_a.utils.spark_session import build_spark
from project_a.config_loader import load_config_resolved
from project_a.utils.logging import setup_json_logging, get_trace_id
from project_a.utils.run_audit import write_run_audit
from project_a.utils.path_resolver import resolve_data_path, resolve_source_file_path
from project_a.pyspark_interview_project.monitoring.lineage_decorator import lineage_job
from project_a.pyspark_interview_project.monitoring.metrics_collector import emit_rowcount, emit_duration
from project_a.extract.fx_json_reader import read_fx_rates_from_bronze
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_crm_bronze(spark: SparkSession, bronze_root: str, crm_cfg: Dict[str, Any], config: Dict[str, Any] = None) -> tuple:
    """Load CRM bronze data (accounts, contacts, opportunities)."""
    logger.info("📥 Loading CRM bronze data...")
    
    # Use path resolver for proper local/AWS path handling
    from project_a.utils.path_resolver import resolve_source_file_path
    
    if config:
        try:
            accounts_path = resolve_source_file_path(config, "crm", "accounts")
            contacts_path = resolve_source_file_path(config, "crm", "contacts")
            opps_path = resolve_source_file_path(config, "crm", "opportunities")
        except (ValueError, KeyError):
            # Fallback to old method
            base_path = crm_cfg.get("base_path", f"{bronze_root}/crm")
            files = crm_cfg.get("files", {})
            accounts_path = f"{base_path}/{files.get('accounts', 'accounts.csv')}"
            contacts_path = f"{base_path}/{files.get('contacts', 'contacts.csv')}"
            opps_path = f"{base_path}/{files.get('opportunities', 'opportunities.csv')}"
    else:
        # Fallback to old method
        base_path = crm_cfg.get("base_path", f"{bronze_root}/crm")
        files = crm_cfg.get("files", {})
        accounts_path = f"{base_path}/{files.get('accounts', 'accounts.csv')}"
        contacts_path = f"{base_path}/{files.get('contacts', 'contacts.csv')}"
        opps_path = f"{base_path}/{files.get('opportunities', 'opportunities.csv')}"
    
    # Try to read Delta first, fallback to CSV, or create empty DataFrame if neither exists
    from project_a.schemas.bronze_schemas import CRM_ACCOUNTS_SCHEMA, CRM_CONTACTS_SCHEMA, CRM_OPPORTUNITIES_SCHEMA
    from pyspark.sql.types import StructType
    
    try:
        delta_accounts_path = accounts_path.replace(".csv", "")
        # Try Delta first, fallback to Parquet, then CSV
        try:
            df_accounts = spark.read.format("delta").load(delta_accounts_path)
        except Exception:
            try:
                df_accounts = spark.read.format("parquet").load(delta_accounts_path)
            except Exception:
                raise Exception("Neither Delta nor Parquet found")
        # Check if empty by trying to get first row
        try:
            _ = df_accounts.first()
        except Exception:
            raise Exception("Delta table is empty or inaccessible")
    except Exception as e:
        # Fallback to CSV
        try:
            logger.info(f"Reading CRM accounts from CSV: {accounts_path} (Delta failed: {e})")
            df_accounts = spark.read.schema(CRM_ACCOUNTS_SCHEMA).option("header", "true").csv(accounts_path)
        except Exception as e2:
            # If CSV also fails, create empty DataFrame with schema
            logger.warning(f"CRM accounts file not found at {accounts_path}, creating empty DataFrame")
            df_accounts = spark.createDataFrame([], CRM_ACCOUNTS_SCHEMA)
    
    try:
        delta_contacts_path = contacts_path.replace(".csv", "")
        # Try Delta first, fallback to Parquet, then CSV
        try:
            df_contacts = spark.read.format("delta").load(delta_contacts_path)
        except Exception:
            try:
                df_contacts = spark.read.format("parquet").load(delta_contacts_path)
            except Exception:
                raise Exception("Neither Delta nor Parquet found")
        try:
            _ = df_contacts.first()
        except Exception:
            raise Exception("Delta table is empty or inaccessible")
    except Exception as e:
        try:
            logger.info(f"Reading CRM contacts from CSV: {contacts_path} (Delta failed: {e})")
            df_contacts = spark.read.schema(CRM_CONTACTS_SCHEMA).option("header", "true").csv(contacts_path)
        except Exception as e2:
            logger.warning(f"CRM contacts file not found at {contacts_path}, creating empty DataFrame")
            df_contacts = spark.createDataFrame([], CRM_CONTACTS_SCHEMA)
    
    try:
        delta_opps_path = opps_path.replace(".csv", "")
        # Try Delta first, fallback to Parquet, then CSV
        try:
            df_opps = spark.read.format("delta").load(delta_opps_path)
        except Exception:
            try:
                df_opps = spark.read.format("parquet").load(delta_opps_path)
            except Exception:
                raise Exception("Neither Delta nor Parquet found")
        try:
            _ = df_opps.first()
        except Exception:
            raise Exception("Delta table is empty or inaccessible")
    except Exception as e:
        try:
            logger.info(f"Reading CRM opportunities from CSV: {opps_path} (Delta failed: {e})")
            df_opps = spark.read.schema(CRM_OPPORTUNITIES_SCHEMA).option("header", "true").csv(opps_path)
        except Exception as e2:
            logger.warning(f"CRM opportunities file not found at {opps_path}, creating empty DataFrame")
            df_opps = spark.createDataFrame([], CRM_OPPORTUNITIES_SCHEMA)
    
    # Count rows safely
    try:
        accounts_count = int(df_accounts.count())
        contacts_count = int(df_contacts.count())
        opps_count = int(df_opps.count())
        logger.info(f"  Accounts: {accounts_count:,} rows")
        logger.info(f"  Contacts: {contacts_count:,} rows")
        logger.info(f"  Opportunities: {opps_count:,} rows")
    except Exception as e:
        logger.warning(f"Could not count rows: {e}")
        logger.info("  Loaded CRM data (count unavailable)")
    
    return df_accounts, df_contacts, df_opps


def load_redshift_behavior_bronze(spark: SparkSession, bronze_root: str, redshift_cfg: Dict[str, Any], config: Dict[str, Any] = None) -> DataFrame:
    """Load Redshift behavior bronze data."""
    logger.info("📥 Loading Redshift behavior bronze data...")
    
    # Use path resolver for proper local/AWS path handling
    
    if config:
        try:
            behavior_path = resolve_source_file_path(config, "redshift", "behavior")
        except (ValueError, KeyError):
            # Fallback to old method
            base_path = redshift_cfg.get("base_path", f"{bronze_root}/redshift")
            files = redshift_cfg.get("files", {})
            behavior_path = f"{base_path}/{files.get('behavior', 'redshift_customer_behavior_50000.csv')}"
    else:
        # Fallback to old method
        base_path = redshift_cfg.get("base_path", f"{bronze_root}/redshift")
        files = redshift_cfg.get("files", {})
        behavior_path = f"{base_path}/{files.get('behavior', 'redshift_customer_behavior_50000.csv')}"
    
    from project_a.schemas.bronze_schemas import REDSHIFT_BEHAVIOR_SCHEMA
    
    try:
        # Try Delta first, fallback to Parquet, then CSV
        try:
            df = spark.read.format("delta").load(behavior_path.replace(".csv", ""))
        except Exception:
            try:
                df = spark.read.format("parquet").load(behavior_path.replace(".csv", ""))
            except Exception:
                raise Exception("Neither Delta nor Parquet found")
    except Exception:
        try:
            df = spark.read.schema(REDSHIFT_BEHAVIOR_SCHEMA).option("header", "true").csv(behavior_path)
        except Exception as e:
            logger.warning(f"Redshift behavior file not found at {behavior_path}, creating empty DataFrame: {e}")
            df = spark.createDataFrame([], REDSHIFT_BEHAVIOR_SCHEMA)
    
    try:
        count = int(df.count())
        logger.info(f"  Behavior: {count:,} rows")
    except Exception:
        logger.info("  Behavior: loaded (count unavailable)")
    return df


def load_snowflake_bronze(spark: SparkSession, bronze_root: str, snowflake_cfg: Dict[str, Any], config: Dict[str, Any] = None) -> tuple:
    """Load Snowflake bronze data (customers, orders, products)."""
    logger.info("📥 Loading Snowflake bronze data...")
    
    # Use path resolver for proper local/AWS path handling
    
    if config:
        try:
            customers_path = resolve_source_file_path(config, "snowflake", "customers")
            orders_path = resolve_source_file_path(config, "snowflake", "orders")
            products_path = resolve_source_file_path(config, "snowflake", "products")
        except (ValueError, KeyError):
            # Fallback to old method
            base_path = snowflake_cfg.get("base_path", f"{bronze_root}/snowflakes")
            files = snowflake_cfg.get("files", {})
            customers_path = f"{base_path}/{files.get('customers', 'snowflake_customers_50000.csv')}"
            orders_path = f"{base_path}/{files.get('orders', 'snowflake_orders_100000.csv')}"
            products_path = f"{base_path}/{files.get('products', 'snowflake_products_10000.csv')}"
    else:
        # Fallback to old method
        base_path = snowflake_cfg.get("base_path", f"{bronze_root}/snowflakes")
        files = snowflake_cfg.get("files", {})
        customers_path = f"{base_path}/{files.get('customers', 'snowflake_customers_50000.csv')}"
        orders_path = f"{base_path}/{files.get('orders', 'snowflake_orders_100000.csv')}"
        products_path = f"{base_path}/{files.get('products', 'snowflake_products_10000.csv')}"
    
    try:
        # Try Delta first, fallback to Parquet, then CSV
        try:
            df_customers = spark.read.format("delta").load(customers_path.replace(".csv", ""))
            df_orders = spark.read.format("delta").load(orders_path.replace(".csv", ""))
            df_products = spark.read.format("delta").load(products_path.replace(".csv", ""))
        except Exception:
            try:
                df_customers = spark.read.format("parquet").load(customers_path.replace(".csv", ""))
                df_orders = spark.read.format("parquet").load(orders_path.replace(".csv", ""))
                df_products = spark.read.format("parquet").load(products_path.replace(".csv", ""))
            except Exception:
                raise Exception("Neither Delta nor Parquet found")
    except Exception:
        from project_a.schemas.bronze_schemas import (
            SNOWFLAKE_CUSTOMERS_SCHEMA, SNOWFLAKE_ORDERS_SCHEMA, SNOWFLAKE_PRODUCTS_SCHEMA
        )
        df_customers = spark.read.schema(SNOWFLAKE_CUSTOMERS_SCHEMA).option("header", "true").csv(customers_path)
        df_orders = spark.read.schema(SNOWFLAKE_ORDERS_SCHEMA).option("header", "true").csv(orders_path)
        df_products = spark.read.schema(SNOWFLAKE_PRODUCTS_SCHEMA).option("header", "true").csv(products_path)
    
    logger.info(f"  Customers: {df_customers.count():,} rows")
    logger.info(f"  Orders: {df_orders.count():,} rows")
    logger.info(f"  Products: {df_products.count():,} rows")
    
    return df_customers, df_orders, df_products


def load_fx_bronze(spark: SparkSession, bronze_root: str, fx_cfg: Dict[str, Any]) -> DataFrame:
    """Load FX rates bronze data (from normalized Delta table)."""
    logger.info("📥 Loading FX rates bronze data...")
    base_path = fx_cfg.get("base_path", f"{bronze_root}/fx")
    
    # Read from normalized Delta table (created by fx_json_to_bronze.py)
    fx_path = f"{base_path}/normalized/"
    
    try:
        # Try Delta first, fallback to Parquet, then CSV
        try:
            df = spark.read.format("delta").load(fx_path)
        except Exception:
            try:
                df = spark.read.format("parquet").load(fx_path)
            except Exception:
                raise Exception("Neither Delta nor Parquet found")
        logger.info(f"  FX Rates: {df.count():,} rows")
    except Exception as e:
        logger.warning(f"FX normalized table not found, trying CSV: {e}")
        # Fallback to CSV
        files = fx_cfg.get("files", {})
        csv_path = f"{base_path}/{files.get('daily_rates', 'fx_rates_historical_730_days.csv')}"
        from project_a.schemas.bronze_schemas import FX_RATES_SCHEMA
        df = spark.read.schema(FX_RATES_SCHEMA).option("header", "true").csv(csv_path)
        df = df.withColumn("date", F.coalesce(F.to_date("date"), F.to_date("trade_date")))
        logger.info(f"  FX Rates (CSV): {df.count():,} rows")
    
    return df


def load_kafka_bronze(spark: SparkSession, bronze_root: str, kafka_cfg: Dict[str, Any], config: Dict[str, Any] = None) -> DataFrame:
    """Load Kafka events bronze data."""
    logger.info("📥 Loading Kafka events bronze data...")
    
    # Use path resolver for proper local/AWS path handling
    
    if config:
        try:
            events_path = resolve_source_file_path(config, "kafka_sim", "orders_seed")
        except (ValueError, KeyError):
            # Fallback to old method
            base_path = kafka_cfg.get("base_path", f"{bronze_root}/kafka")
            files = kafka_cfg.get("files", {})
            events_path = f"{base_path}/{files.get('orders_seed', 'stream_kafka_events_100000.csv')}"
    else:
        # Fallback to old method
        base_path = kafka_cfg.get("base_path", f"{bronze_root}/kafka")
        files = kafka_cfg.get("files", {})
        events_path = f"{base_path}/{files.get('orders_seed', 'stream_kafka_events_100000.csv')}"
    
    from project_a.schemas.bronze_schemas import KAFKA_EVENTS_SCHEMA
    
    try:
        # Try Delta first, fallback to Parquet, then CSV
        try:
            df = spark.read.format("delta").load(events_path.replace(".csv", ""))
        except Exception:
            try:
                df = spark.read.format("parquet").load(events_path.replace(".csv", ""))
            except Exception:
                raise Exception("Neither Delta nor Parquet found")
    except Exception:
        try:
            df = spark.read.schema(KAFKA_EVENTS_SCHEMA).option("header", "true").csv(events_path)
        except Exception as e:
            logger.warning(f"Kafka events file not found at {events_path}, creating empty DataFrame: {e}")
            df = spark.createDataFrame([], KAFKA_EVENTS_SCHEMA)
    
    try:
        count = int(df.count())
        logger.info(f"  Kafka Events: {count:,} rows")
    except Exception:
        logger.info("  Kafka Events: loaded (count unavailable)")
    return df


def build_customers_silver(
    df_accounts: DataFrame,
    df_contacts: DataFrame,
    df_opps: DataFrame,
    df_behavior: DataFrame
) -> DataFrame:
    """Build silver customers table with joins and aggregations."""
    logger.info("🔧 Building silver.customers...")
    
    # Clean accounts - map actual CSV columns to expected schema
    # CSV has: Id, Name, Industry, AnnualRevenue, CreatedDate, BillingCountry, CustomerSegment
    accounts = df_accounts.select(
        F.col("Id").alias("customer_id"),  # Id -> customer_id
        F.trim(F.col("Name")).alias("customer_name"),  # Name -> customer_name
        F.coalesce(F.col("CustomerSegment"), F.lit("UNKNOWN")).alias("segment"),  # CustomerSegment -> segment
        F.to_date("CreatedDate").alias("customer_since"),  # CreatedDate -> customer_since
        F.coalesce(F.col("BillingCountry"), F.lit("UNKNOWN")).alias("country"),  # BillingCountry -> country
        F.col("Industry").alias("industry"),  # Industry -> industry
        F.coalesce(F.col("AnnualRevenue"), F.lit(0.0)).alias("annual_revenue")  # AnnualRevenue -> annual_revenue
    ).filter(F.col("customer_id").isNotNull())
    
    # Get latest contact per customer - map actual CSV columns
    # CSV has: AccountId, Email, Phone, LastModifiedDate (not LastActivityDate)
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
        F.col("AccountId").alias("customer_id"),  # AccountId -> customer_id
        F.col("Email").alias("primary_email"),  # Email -> primary_email
        F.col("Phone").alias("primary_phone")  # Phone -> primary_phone
    )
    
    # Aggregate behavior metrics - derive from event_name and conversion_value
    # CSV has: event_name, conversion_value, not login_count/page_views/purchases/revenue
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
    
    logger.info(f"✅ silver.customers: {customers.count():,} rows")
    return customers


def build_orders_silver(
    df_orders: DataFrame,
    df_customers: DataFrame,
    df_products: DataFrame,
    df_fx: DataFrame
) -> DataFrame:
    """Build silver orders table with FX normalization."""
    logger.info("🔧 Building silver.orders...")
    
    # Handle empty DataFrame case - return empty DataFrame with expected schema
    try:
        row_count = df_orders.count()
        if row_count == 0:
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
    except Exception as e:
        logger.warning(f"Could not check df_orders count: {e}, proceeding anyway")
    
    # Clean orders - handle both 'amount' and 'total_amount' column names
    order_cols = df_orders.columns
    amount_col = None
    if "total_amount" in order_cols:
        amount_col = F.col("total_amount")
    elif "amount" in order_cols:
        amount_col = F.col("amount")
    else:
        amount_col = F.lit(0.0)
    
    orders = df_orders.select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("product_id"),
        F.to_date("order_date").alias("order_date"),
        F.coalesce(amount_col, F.lit(0.0)).alias("amount_orig"),
        F.coalesce(F.col("currency"), F.lit("USD")).alias("currency"),
        F.coalesce(F.col("quantity"), F.lit(1)).alias("quantity")
    ).filter(F.col("order_id").isNotNull())
    
    # Join with products for product details - map actual CSV columns
    # CSV has: price_usd, not price
    orders = orders.join(
        df_products.select("product_id", "product_name", "category", F.col("price_usd").alias("price")).alias("p"),
        "product_id",
        "left"
    )
    
    # Join with FX rates for currency conversion
    # FX data is normalized to base_currency, target_currency, exchange_rate by read_fx_rates_from_bronze
    # Match FX rate by date and currency (base_currency = currency, target_currency = USD)
    fx_usd = df_fx.filter(
        (F.col("base_currency") == "USD") | (F.col("target_currency") == "USD")
    ).select(
        F.col("date").alias("fx_date"),
        F.when(F.col("base_currency") == "USD", F.lit(1.0) / F.col("exchange_rate"))
         .otherwise(F.col("exchange_rate")).alias("usd_rate"),
        F.col("base_currency").alias("fx_currency")
    )
    
    orders = orders.join(
        fx_usd,
        (F.to_date(F.col("order_date")) == F.col("fx_date")) & 
        (F.col("currency") == F.col("fx_currency")),
        "left"
    )
    
    # Calculate USD amount
    orders = orders.withColumn(
        "amount_usd",
        F.when(F.col("currency") == "USD", F.col("amount_orig"))
         .when(F.col("usd_rate").isNotNull(), F.col("amount_orig") * F.col("usd_rate"))
         .otherwise(F.col("amount_orig"))  # Fallback if FX rate not found
    )
    
    # Select final columns - handle missing columns from joins gracefully
    available_cols = orders.columns
    select_exprs = []
    
    # Required columns
    for col_name in ["order_id", "customer_id", "product_id", "order_date", "amount_orig", "currency", "amount_usd", "quantity"]:
        if col_name in available_cols:
            select_exprs.append(F.col(col_name))
        else:
            logger.warning(f"Column {col_name} not found, using null/literal")
            if col_name == "amount_usd":
                select_exprs.append(F.coalesce(F.col("amount_orig"), F.lit(0.0)).alias("amount_usd"))
            else:
                select_exprs.append(F.lit(None).alias(col_name))
    
    # Optional columns from product join
    for col_name in ["product_name", "category"]:
        if col_name in available_cols:
            select_exprs.append(F.col(col_name))
        else:
            select_exprs.append(F.lit(None).alias(col_name))
    
    orders_silver = orders.select(*select_exprs)
    
    logger.info(f"✅ silver.orders: {orders_silver.count():,} rows")
    return orders_silver


def build_products_silver(df_products: DataFrame) -> DataFrame:
    """Build silver products table."""
    logger.info("🔧 Building silver.products...")
    
    # Map actual CSV columns - CSV has price_usd, not price, and no currency column
    products = df_products.select(
        F.col("product_id"),
        F.trim(F.col("product_name")).alias("product_name"),
        F.col("category"),
        F.coalesce(F.col("price_usd"), F.lit(0.0)).alias("price"),  # price_usd -> price
        F.lit("USD").alias("currency")  # Products are always USD
    ).filter(F.col("product_id").isNotNull()) \
     .dropDuplicates(["product_id"])
    
    logger.info(f"✅ silver.products: {products.count():,} rows")
    return products


def build_behavior_silver(df_behavior: DataFrame) -> DataFrame:
    """Build silver behavior table."""
    logger.info("🔧 Building silver.customer_behavior...")
    
    # CSV has event_timestamp, not event_date
    # Metrics need to be derived from event_name and conversion_value
    behavior = df_behavior.select(
        F.col("customer_id"),
        F.to_date(F.col("event_timestamp")).alias("event_date"),  # event_timestamp -> event_date
        # Derive metrics from event_name (like we do in build_customers_silver)
        F.when(F.col("event_name") == "login", F.lit(1)).otherwise(F.lit(0)).alias("login_count"),
        F.when(F.col("event_name") == "page_view", F.lit(1)).otherwise(F.lit(0)).alias("page_views"),
        F.coalesce(F.col("duration_seconds") / 60.0, F.lit(0.0)).alias("session_duration_minutes"),  # duration_seconds -> minutes
        F.when(F.col("event_name") == "purchase", F.lit(1)).otherwise(F.lit(0)).alias("purchases"),
        F.coalesce(F.col("conversion_value"), F.lit(0.0)).alias("revenue")  # conversion_value -> revenue
    ).filter(F.col("customer_id").isNotNull()) \
     .groupBy("customer_id", "event_date") \
     .agg(
         F.sum("login_count").alias("login_count"),
         F.sum("page_views").alias("page_views"),
         F.sum("session_duration_minutes").alias("session_duration_minutes"),
         F.sum("purchases").alias("purchases"),
         F.sum("revenue").alias("revenue")
     )
    
    logger.info(f"✅ silver.customer_behavior: {behavior.count():,} rows")
    return behavior


def load_fx_bronze(spark: SparkSession, config: Dict[str, Any]) -> DataFrame:
    """
    Load FX rates from the Bronze layer using S3-backed files only.

    This delegates to ``read_fx_rates_from_bronze`` so that the Bronze → Silver
    transform has **no dependency on HTTP clients or external APIs** at runtime.
    Upstream ingestion is responsible for landing FX data to:

        s3://<lake-bucket>/bronze/fx/
    """
    from project_a.utils.path_resolver import resolve_data_path
    bronze_root = resolve_data_path(config, "bronze")
    return read_fx_rates_from_bronze(spark, bronze_root)


def build_fx_silver(df_fx: DataFrame) -> DataFrame:
    """Build silver FX rates table with deduplication and averaging."""
    logger.info("🔧 Building silver.fx_rates...")
    
    # Normalize column names if needed
    if "trade_date" not in df_fx.columns and "date" in df_fx.columns:
        df_fx = df_fx.withColumn("trade_date", F.to_date("date"))
    
    # Deduplicate and average rates per (date, base_ccy, quote_ccy)
    fx = df_fx.groupBy("trade_date", "base_ccy", "quote_ccy").agg(
        F.avg("rate").alias("rate"),
        F.min(F.coalesce(F.col("bid_rate"), F.col("rate"))).alias("bid_rate"),
        F.max(F.coalesce(F.col("ask_rate"), F.col("rate"))).alias("ask_rate"),
        F.avg(F.coalesce(F.col("mid_rate"), F.col("rate"))).alias("mid_rate"),
        F.first(F.coalesce(F.col("source"), F.lit("fx-json-demo"))).alias("source")
    ).withColumn("rate", F.round("rate", 6)) \
     .withColumn("bid_rate", F.round("bid_rate", 6)) \
     .withColumn("ask_rate", F.round("ask_rate", 6)) \
     .withColumn("mid_rate", F.round("mid_rate", 6)) \
     .filter(
        F.col("trade_date").isNotNull() &
        F.col("base_ccy").isNotNull() &
        F.col("quote_ccy").isNotNull() &
        F.col("rate").isNotNull() &
        (F.col("rate") > 0)
    )
    
    logger.info(f"✅ silver.fx_rates: {fx.count():,} rows")
    return fx


def build_order_events_silver(df_kafka: DataFrame, df_orders: DataFrame) -> DataFrame:
    """Build silver order_events table from Kafka events."""
    logger.info("🔧 Building silver.order_events...")
    
    # Kafka CSV has Kafka metadata columns (event_id, topic, partition, offset, timestamp, key, value, headers)
    # The actual event data is in the 'value' column as JSON, or the CSV may have direct columns
    # Check if we have direct columns or need to parse JSON from 'value'
    kafka_cols = df_kafka.columns
    
    if "order_id" in kafka_cols:
        # Direct columns (CSV seed file)
        events = df_kafka.select(
            F.col("event_id"),
            F.col("order_id"),
            F.col("event_type"),
            F.to_timestamp("event_ts").alias("event_ts"),
            F.col("status"),
            F.col("metadata")
        ).filter(F.col("order_id").isNotNull())
    elif "value" in kafka_cols:
        # Parse JSON from 'value' column
        from pyspark.sql.types import StructType, StructField, StringType
        from pyspark.sql.functions import from_json
        
        event_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", StringType(), True),
            StructField("status", StringType(), True),
            StructField("metadata", StringType(), True)
        ])
        
        events = df_kafka.select(
            F.col("event_id").alias("kafka_event_id"),
            from_json(F.col("value").cast("string"), event_schema).alias("event_data")
        ).select(
            F.col("kafka_event_id").alias("event_id"),
            F.col("event_data.order_id"),
            F.col("event_data.event_type"),
            F.to_timestamp(F.col("event_data.event_ts")).alias("event_ts"),
            F.col("event_data.status"),
            F.col("event_data.metadata")
        ).filter(F.col("order_id").isNotNull())
    else:
        # Fallback: create empty DataFrame with expected schema
        logger.warning("Kafka DataFrame has unexpected columns, creating empty order_events")
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        empty_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("metadata", StringType(), True)
        ])
        spark = df_kafka.sql_ctx.sparkSession
        events = spark.createDataFrame([], empty_schema)
    
    # Join with orders for order context - handle missing amount_usd column
    order_cols = df_orders.columns
    if "amount_usd" in order_cols:
        order_select_expr = [F.col("order_id"), F.col("customer_id"), F.col("order_date"), F.col("amount_usd")]
    elif "amount_orig" in order_cols:
        order_select_expr = [F.col("order_id"), F.col("customer_id"), F.col("order_date"), F.col("amount_orig").alias("amount_usd")]
    elif "amount" in order_cols:
        order_select_expr = [F.col("order_id"), F.col("customer_id"), F.col("order_date"), F.col("amount").alias("amount_usd")]
    else:
        # No amount column found, add a literal 0.0
        order_select_expr = [F.col("order_id"), F.col("customer_id"), F.col("order_date"), F.lit(0.0).alias("amount_usd")]
    
    events = events.join(
        df_orders.select(*order_select_expr),
        "order_id",
        "left"
    )
    
    try:
        count_val = int(events.count())
        logger.info(f"✅ silver.order_events: {count_val:,} rows")
    except Exception as e:
        logger.warning(f"Could not count order_events rows: {e}")
        logger.info("✅ silver.order_events created")
    
    return events


def write_silver(df: DataFrame, silver_root: str, table_name: str, partition_by: list = None) -> None:
    """Write silver table to Delta or Parquet format (Parquet for local, Delta for EMR)."""
    target = f"{silver_root}/{table_name}"
    logger.info(f"💾 Writing {table_name} to {target}")
    
    # Check if Delta Lake is available (try to use it, fallback to Parquet)
    use_delta = True
    try:
        # Try to check if Delta format is available
        spark = df.sql_ctx.sparkSession
        spark_env = spark.conf.get("spark.sql.extensions", "")
        if "delta" not in spark_env.lower():
            use_delta = False
    except Exception:
        use_delta = False
    
    # Use environment from config if available
    try:
        config_env = os.getenv("SPARK_ENV") or "local"
        if config_env == "local":
            use_delta = False
    except Exception:
        pass
    
    # For local Parquet writes, delete existing directory first using Spark's filesystem API
    if not use_delta and not target.startswith("s3://"):
        try:
            spark = df.sql_ctx.sparkSession
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark.sparkContext._jvm.java.net.URI(target),
                hadoop_conf
            )
            path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(target)
            if fs.exists(path):
                fs.delete(path, True)  # True = recursive delete
                logger.debug(f"Deleted existing directory: {target}")
        except Exception as e:
            logger.warning(f"Could not delete existing directory {target} using Spark FS: {e}")
            # Fallback: try Python shutil
            try:
                import shutil
                target_path = Path(target)
                if target_path.exists():
                    shutil.rmtree(target_path)
                    logger.debug(f"Deleted existing directory using shutil: {target_path}")
            except Exception as e2:
                logger.warning(f"Could not delete existing directory {target} using shutil: {e2}")
    
    if use_delta:
        writer = df.write.format("delta").mode("overwrite")
    else:
        writer = df.write.format("parquet").mode("overwrite")
        logger.info(f"  Using Parquet format (Delta not available)")
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    
    writer.save(target)
    
    try:
        count_val = int(df.count())
        logger.info(f"✅ Wrote {table_name}: {count_val:,} rows")
    except Exception as e:
        logger.warning(f"Could not count {table_name} rows: {e}")
        logger.info(f"✅ Wrote {table_name}")


@lineage_job(
    name="bronze_to_silver_complete",
    inputs=[
        "s3://bucket/bronze/crm/",
        "s3://bucket/bronze/redshift/",
        "s3://bucket/bronze/snowflakes/",
        "s3://bucket/bronze/fx/",
        "s3://bucket/bronze/kafka/"
    ],
    outputs=[
        "s3://bucket/silver/customers_silver",
        "s3://bucket/silver/orders_silver",
        "s3://bucket/silver/products_silver",
        "s3://bucket/silver/customer_behavior_silver",
        "s3://bucket/silver/fx_rates_silver",
        "s3://bucket/silver/order_events_silver"
    ]
)
def bronze_to_silver_complete(
    spark: SparkSession,
    config: Dict[str, Any],
    run_date: str = None
) -> Dict[str, DataFrame]:
    """
    Complete bronze to silver transformation with all 5 sources.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        run_date: Processing date YYYY-MM-DD
        
    Returns:
        Dictionary with all silver DataFrames
    """
    if run_date is None:
        run_date = datetime.utcnow().strftime("%Y-%m-%d")
    
    logger.info(f"🚀 Starting complete bronze to silver transformation (run_date={run_date})")
    start_time = time.time()
    
    # Get paths from config using path resolver
    bronze_root = resolve_data_path(config, "bronze")
    silver_root = resolve_data_path(config, "silver")
    sources = config.get("sources", {})
    tables_config = config.get("tables", {}).get("silver", {})
    # Provide default table names if not in config
    tables = {
        "customers": tables_config.get("customers", "customers_silver"),
        "orders": tables_config.get("orders", "orders_silver"),
        "products": tables_config.get("products", "products_silver"),
        "behavior": tables_config.get("behavior", "customer_behavior_silver"),
        "fx_rates": tables_config.get("fx_rates", "fx_rates_silver"),
        "order_events": tables_config.get("order_events", "order_events_silver")
    }
    
    try:
        # 1. Load all bronze sources
        df_accounts, df_contacts, df_opps = load_crm_bronze(spark, bronze_root, sources.get("crm", {}), config)
        df_behavior = load_redshift_behavior_bronze(spark, bronze_root, sources.get("redshift", {}), config)
        df_cust, df_orders, df_products = load_snowflake_bronze(spark, bronze_root, sources.get("snowflake", {}), config)
        df_fx = load_fx_bronze(spark, config)
        df_kafka = load_kafka_bronze(spark, bronze_root, sources.get("kafka_sim", {}), config)
        
        # 2. Build silver tables
        customers_silver = build_customers_silver(df_accounts, df_contacts, df_opps, df_behavior)
        orders_silver = build_orders_silver(df_orders, df_cust, df_products, df_fx)
        products_silver = build_products_silver(df_products)
        behavior_silver = build_behavior_silver(df_behavior)
        fx_silver = build_fx_silver(df_fx)
        # Pass orders_silver (not df_orders) so it has amount_usd column
        order_events_silver = build_order_events_silver(df_kafka, orders_silver)
        
        # 3. Write silver tables
        write_silver(customers_silver, silver_root, tables["customers"], ["country"])
        write_silver(orders_silver, silver_root, tables["orders"], ["order_date"])
        write_silver(products_silver, silver_root, tables["products"])
        write_silver(behavior_silver, silver_root, tables["behavior"], ["event_date"])
        write_silver(fx_silver, silver_root, tables["fx_rates"], ["trade_date"])
        write_silver(order_events_silver, silver_root, tables["order_events"], ["event_ts"])
        
        # 4. Emit metrics
        duration_ms = (time.time() - start_time) * 1000
        emit_rowcount("silver_customers_total", customers_silver.count(), {"layer": "silver"}, config)
        emit_rowcount("silver_orders_total", orders_silver.count(), {"layer": "silver"}, config)
        emit_duration("silver_transformation_duration", duration_ms, {"stage": "bronze_to_silver"}, config)
        
        logger.info(f"✅ Complete silver transformation finished in {duration_ms:.0f}ms")
        
        return {
            "customers": customers_silver,
            "orders": orders_silver,
            "products": products_silver,
            "behavior": behavior_silver,
            "fx_rates": fx_silver,
            "order_events": order_events_silver
        }
        
    except Exception as e:
        logger.error(f"❌ Silver transformation failed: {e}", exc_info=True)
        raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Bronze to Silver transformation job")
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
            # Use boto3 to read from S3 instead of Spark
            import boto3
            s3_client = boto3.client("s3")
            bucket_key = config_path.replace("s3://", "").split("/", 1)
            bucket = bucket_key[0]
            key = bucket_key[1] if len(bucket_key) > 1 else ""
            try:
                response = s3_client.get_object(Bucket=bucket, Key=key)
                config_content = response["Body"].read().decode("utf-8")
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
        
        # Count input rows (approximate from bronze)
        bronze_root = resolve_data_path(config, "bronze")
        sources = config.get("sources", {})
        rows_in_approx = 0
        try:
            # Try to count from bronze sources
            for source_name in ["crm", "redshift", "snowflake", "fx", "kafka_sim"]:
                if source_name in sources:
                    source_path = f"{bronze_root}/{source_name}"
                    try:
                        df_temp = spark.read.format("delta").load(source_path)
                        rows_in_approx += df_temp.count()
                    except Exception:
                        pass
        except Exception:
            rows_in_approx = 0
        
        results = bronze_to_silver_complete(spark, config, run_date=run_date)
        
        # Count output rows
        rows_out = sum([
            results['customers'].count(),
            results['orders'].count(),
            results['products'].count(),
            results['behavior'].count(),
            results['fx_rates'].count(),
            results['order_events'].count()
        ])
        
        duration_ms = (time.time() - start_time) * 1000
        
        logger.info("✅ Bronze to Silver transformation completed")
        logger.info(f"  - Customers: {results['customers'].count():,} rows")
        logger.info(f"  - Orders: {results['orders'].count():,} rows")
        logger.info(f"  - Products: {results['products'].count():,} rows")
        logger.info(f"  - Behavior: {results['behavior'].count():,} rows")
        logger.info(f"  - FX Rates: {results['fx_rates'].count():,} rows")
        logger.info(f"  - Order Events: {results['order_events'].count():,} rows")
        
        # Write run audit
        lake_bucket = config.get("buckets", {}).get("lake", "")
        if lake_bucket:
            try:
                write_run_audit(
                    bucket=lake_bucket,
                    job_name="bronze_to_silver",
                    env=config.get("environment", "dev"),
                    source=bronze_root,
                    target=resolve_data_path(config, "silver"),
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
                    job_name="bronze_to_silver",
                    env=config.get("environment", "dev"),
                    source=config.get("paths", {}).get("bronze_root", ""),
                    target=config.get("paths", {}).get("silver_root", ""),
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
