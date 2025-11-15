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
import argparse
import tempfile
import uuid
import logging
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

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


def load_crm_bronze(spark: SparkSession, bronze_root: str, crm_cfg: Dict[str, Any]) -> tuple:
    """Load CRM bronze data (accounts, contacts, opportunities)."""
    logger.info("📥 Loading CRM bronze data...")
    base_path = crm_cfg.get("base_path", f"{bronze_root}/crm")
    files = crm_cfg.get("files", {})
    
    accounts_path = f"{base_path}/{files.get('accounts', 'accounts.csv')}"
    contacts_path = f"{base_path}/{files.get('contacts', 'contacts.csv')}"
    opps_path = f"{base_path}/{files.get('opportunities', 'opportunities.csv')}"
    
    # Read CSV files (will be converted to Parquet/Delta in real pipeline)
    df_accounts = spark.read.format("delta").load(accounts_path.replace(".csv", "")) if spark._jsparkSession else None
    df_contacts = spark.read.format("delta").load(contacts_path.replace(".csv", "")) if spark._jsparkSession else None
    df_opps = spark.read.format("delta").load(opps_path.replace(".csv", "")) if spark._jsparkSession else None
    
    # Fallback: read CSV if Delta doesn't exist
    try:
        if df_accounts is None or df_accounts.rdd.isEmpty():
            from pyspark_interview_project.schemas.bronze_schemas import CRM_ACCOUNTS_SCHEMA, CRM_CONTACTS_SCHEMA, CRM_OPPORTUNITIES_SCHEMA
            df_accounts = spark.read.schema(CRM_ACCOUNTS_SCHEMA).option("header", "true").csv(accounts_path)
            df_contacts = spark.read.schema(CRM_CONTACTS_SCHEMA).option("header", "true").csv(contacts_path)
            df_opps = spark.read.schema(CRM_OPPORTUNITIES_SCHEMA).option("header", "true").csv(opps_path)
    except Exception as e:
        logger.warning(f"Could not read CRM as Delta, trying CSV: {e}")
        from pyspark_interview_project.schemas.bronze_schemas import CRM_ACCOUNTS_SCHEMA, CRM_CONTACTS_SCHEMA, CRM_OPPORTUNITIES_SCHEMA
        df_accounts = spark.read.schema(CRM_ACCOUNTS_SCHEMA).option("header", "true").csv(accounts_path)
        df_contacts = spark.read.schema(CRM_CONTACTS_SCHEMA).option("header", "true").csv(contacts_path)
        df_opps = spark.read.schema(CRM_OPPORTUNITIES_SCHEMA).option("header", "true").csv(opps_path)
    
    logger.info(f"  Accounts: {df_accounts.count():,} rows")
    logger.info(f"  Contacts: {df_contacts.count():,} rows")
    logger.info(f"  Opportunities: {df_opps.count():,} rows")
    
    return df_accounts, df_contacts, df_opps


def load_redshift_behavior_bronze(spark: SparkSession, bronze_root: str, redshift_cfg: Dict[str, Any]) -> DataFrame:
    """Load Redshift behavior bronze data."""
    logger.info("📥 Loading Redshift behavior bronze data...")
    base_path = redshift_cfg.get("base_path", f"{bronze_root}/redshift")
    files = redshift_cfg.get("files", {})
    
    behavior_path = f"{base_path}/{files.get('behavior', 'redshift_customer_behavior_50000.csv')}"
    
    try:
        df = spark.read.format("delta").load(behavior_path.replace(".csv", ""))
    except Exception:
        from pyspark_interview_project.schemas.bronze_schemas import REDSHIFT_BEHAVIOR_SCHEMA
        df = spark.read.schema(REDSHIFT_BEHAVIOR_SCHEMA).option("header", "true").csv(behavior_path)
    
    logger.info(f"  Behavior: {df.count():,} rows")
    return df


def load_snowflake_bronze(spark: SparkSession, bronze_root: str, snowflake_cfg: Dict[str, Any]) -> tuple:
    """Load Snowflake bronze data (customers, orders, products)."""
    logger.info("📥 Loading Snowflake bronze data...")
    base_path = snowflake_cfg.get("base_path", f"{bronze_root}/snowflakes")
    files = snowflake_cfg.get("files", {})
    
    customers_path = f"{base_path}/{files.get('customers', 'snowflake_customers_50000.csv')}"
    orders_path = f"{base_path}/{files.get('orders', 'snowflake_orders_100000.csv')}"
    products_path = f"{base_path}/{files.get('products', 'snowflake_products_10000.csv')}"
    
    try:
        df_customers = spark.read.format("delta").load(customers_path.replace(".csv", ""))
        df_orders = spark.read.format("delta").load(orders_path.replace(".csv", ""))
        df_products = spark.read.format("delta").load(products_path.replace(".csv", ""))
    except Exception:
        from pyspark_interview_project.schemas.bronze_schemas import (
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
        df = spark.read.format("delta").load(fx_path)
        logger.info(f"  FX Rates: {df.count():,} rows")
    except Exception as e:
        logger.warning(f"FX normalized table not found, trying CSV: {e}")
        # Fallback to CSV
        files = fx_cfg.get("files", {})
        csv_path = f"{base_path}/{files.get('daily_rates', 'fx_rates_historical_730_days.csv')}"
        from pyspark_interview_project.schemas.bronze_schemas import FX_RATES_SCHEMA
        df = spark.read.schema(FX_RATES_SCHEMA).option("header", "true").csv(csv_path)
        df = df.withColumn("date", F.coalesce(F.to_date("date"), F.to_date("trade_date")))
        logger.info(f"  FX Rates (CSV): {df.count():,} rows")
    
    return df


def load_kafka_bronze(spark: SparkSession, bronze_root: str, kafka_cfg: Dict[str, Any]) -> DataFrame:
    """Load Kafka events bronze data."""
    logger.info("📥 Loading Kafka events bronze data...")
    base_path = kafka_cfg.get("base_path", f"{bronze_root}/kafka")
    files = kafka_cfg.get("files", {})
    
    events_path = f"{base_path}/{files.get('orders_seed', 'stream_kafka_events_100000.csv')}"
    
    try:
        df = spark.read.format("delta").load(events_path.replace(".csv", ""))
    except Exception:
        from pyspark_interview_project.schemas.bronze_schemas import KAFKA_EVENTS_SCHEMA
        df = spark.read.schema(KAFKA_EVENTS_SCHEMA).option("header", "true").csv(events_path)
    
    logger.info(f"  Kafka Events: {df.count():,} rows")
    return df


def build_customers_silver(
    df_accounts: DataFrame,
    df_contacts: DataFrame,
    df_opps: DataFrame,
    df_behavior: DataFrame
) -> DataFrame:
    """Build silver customers table with joins and aggregations."""
    logger.info("🔧 Building silver.customers...")
    
    # Clean accounts
    accounts = df_accounts.select(
        F.col("account_id").alias("customer_id"),
        F.trim(F.col("account_name")).alias("customer_name"),
        F.col("segment"),
        F.to_date("created_date").alias("customer_since"),
        F.col("country"),
        F.col("industry"),
        F.coalesce(F.col("annual_revenue"), F.lit(0.0)).alias("annual_revenue")
    ).filter(F.col("customer_id").isNotNull())
    
    # Get latest contact per customer
    last_contact = df_contacts.withColumn(
        "contact_ts",
        F.to_timestamp(F.col("last_contact_ts"))
    ).withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("account_id").orderBy(F.col("contact_ts").desc())
        )
    ).filter(F.col("rn") == 1).select(
        F.col("account_id").alias("customer_id"),
        F.col("contact_email").alias("primary_email"),
        F.col("contact_phone").alias("primary_phone")
    )
    
    # Aggregate behavior metrics
    behavior_agg = df_behavior.groupBy("customer_id").agg(
        F.count("*").alias("behavior_events_24m"),
        F.sum("login_count").alias("total_logins_24m"),
        F.sum("page_views").alias("total_page_views_24m"),
        F.sum("purchases").alias("total_purchases_24m"),
        F.sum("revenue").alias("total_revenue_24m")
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
    
    # Clean orders
    orders = df_orders.select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("product_id"),
        F.to_date("order_date").alias("order_date"),
        F.coalesce(F.col("amount"), F.col("total_amount"), F.lit(0.0)).alias("amount_orig"),
        F.coalesce(F.col("currency"), F.lit("USD")).alias("currency"),
        F.coalesce(F.col("quantity"), F.lit(1)).alias("quantity")
    ).filter(F.col("order_id").isNotNull())
    
    # Join with products for product details
    orders = orders.join(
        df_products.select("product_id", "product_name", "category", "price").alias("p"),
        "product_id",
        "left"
    )
    
    # Join with FX rates for currency conversion
    # Match FX rate by date and currency (base_ccy = currency, quote_ccy = USD)
    fx_usd = df_fx.filter(
        (F.col("base_ccy") == "USD") | (F.col("quote_ccy") == "USD")
    ).select(
        F.col("date").alias("fx_date"),
        F.when(F.col("base_ccy") == "USD", F.lit(1.0) / F.col("rate"))
         .otherwise(F.col("rate")).alias("usd_rate"),
        F.col("base_ccy").alias("fx_currency")
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
    
    # Select final columns
    orders_silver = orders.select(
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
    
    logger.info(f"✅ silver.orders: {orders_silver.count():,} rows")
    return orders_silver


def build_products_silver(df_products: DataFrame) -> DataFrame:
    """Build silver products table."""
    logger.info("🔧 Building silver.products...")
    
    products = df_products.select(
        F.col("product_id"),
        F.trim(F.col("product_name")).alias("product_name"),
        F.col("category"),
        F.coalesce(F.col("price"), F.lit(0.0)).alias("price"),
        F.coalesce(F.col("currency"), F.lit("USD")).alias("currency")
    ).filter(F.col("product_id").isNotNull()) \
     .dropDuplicates(["product_id"])
    
    logger.info(f"✅ silver.products: {products.count():,} rows")
    return products


def build_behavior_silver(df_behavior: DataFrame) -> DataFrame:
    """Build silver behavior table."""
    logger.info("🔧 Building silver.customer_behavior...")
    
    behavior = df_behavior.select(
        F.col("customer_id"),
        F.to_date("event_date").alias("event_date"),
        F.coalesce(F.col("login_count"), F.lit(0)).alias("login_count"),
        F.coalesce(F.col("page_views"), F.lit(0)).alias("page_views"),
        F.coalesce(F.col("session_duration_minutes"), F.lit(0.0)).alias("session_duration_minutes"),
        F.coalesce(F.col("purchases"), F.lit(0)).alias("purchases"),
        F.coalesce(F.col("revenue"), F.lit(0.0)).alias("revenue")
    ).filter(F.col("customer_id").isNotNull()) \
     .dropDuplicates(["customer_id", "event_date"])
    
    logger.info(f"✅ silver.customer_behavior: {behavior.count():,} rows")
    return behavior


def load_fx_bronze(spark: SparkSession, config: Dict[str, Any]) -> DataFrame:
    """Load FX rates from Bronze Delta table."""
    logger.info("Loading FX Bronze data...")
    fx_cfg = config["sources"]["fx"]
    bronze_path = fx_cfg.get("bronze_path", "")
    
    if not bronze_path:
        # Fallback: try to read from JSON or CSV
        base_path = resolve_path(fx_cfg["base_path"], config=config)
        json_path = f"{base_path}/json/{fx_cfg['files'].get('daily_rates_json')}"
        csv_path = f"{base_path}/{fx_cfg['files'].get('daily_rates')}"
        
        try:
            df = spark.read.schema(FX_RATES_SCHEMA).json(json_path)
            logger.info(f"Loaded FX rates from JSON: {json_path}")
        except Exception as e:
            logger.warning(f"Could not load FX from JSON ({e}), trying CSV: {csv_path}")
            df = spark.read.schema(FX_RATES_SCHEMA).csv(csv_path, header=True)
            logger.info(f"Loaded FX rates from CSV: {csv_path}")
    else:
        # Read from Bronze Delta
        bronze_path_resolved = resolve_path(bronze_path, config=config)
        try:
            df = spark.read.format("delta").load(bronze_path_resolved)
            logger.info(f"Loaded FX rates from Bronze Delta: {bronze_path_resolved}")
        except Exception as e:
            logger.warning(f"Could not load FX from Delta ({e}), falling back to raw JSON")
            base_path = resolve_path(fx_cfg["base_path"], config=config)
            json_path = f"{base_path}/json/{fx_cfg['files'].get('daily_rates_json')}"
            df = spark.read.schema(FX_RATES_SCHEMA).json(json_path)
    
    return df


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
    
    # Parse Kafka events
    events = df_kafka.select(
        F.col("event_id"),
        F.col("order_id"),
        F.col("event_type"),
        F.to_timestamp("event_ts").alias("event_ts"),
        F.col("status"),
        F.col("metadata")
    ).filter(F.col("order_id").isNotNull())
    
    # Join with orders for order context
    events = events.join(
        df_orders.select("order_id", "customer_id", "order_date", "amount_usd"),
        "order_id",
        "left"
    )
    
    logger.info(f"✅ silver.order_events: {events.count():,} rows")
    return events


def write_silver(df: DataFrame, silver_root: str, table_name: str, partition_by: list = None) -> None:
    """Write silver table to Delta format."""
    target = f"{silver_root}/{table_name}"
    logger.info(f"💾 Writing {table_name} to {target}")
    
    writer = df.write.format("delta").mode("overwrite")
    
    if partition_by:
        writer = writer.partitionBy(partition_by)
    
    writer.save(target)
    logger.info(f"✅ Wrote {table_name}: {df.count():,} rows")


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
    
    # Get paths from config
    bronze_root = config["paths"]["bronze_root"]
    silver_root = config["paths"]["silver_root"]
    sources = config["sources"]
    tables = config["tables"]["silver"]
    
    try:
        # 1. Load all bronze sources
        df_accounts, df_contacts, df_opps = load_crm_bronze(spark, bronze_root, sources["crm"])
        df_behavior = load_redshift_behavior_bronze(spark, bronze_root, sources["redshift"])
        df_cust, df_orders, df_products = load_snowflake_bronze(spark, bronze_root, sources["snowflake"])
        df_fx = load_fx_bronze(spark, config)
        df_kafka = load_kafka_bronze(spark, bronze_root, sources["kafka_sim"])
        
        # 2. Build silver tables
        customers_silver = build_customers_silver(df_accounts, df_contacts, df_opps, df_behavior)
        orders_silver = build_orders_silver(df_orders, df_cust, df_products, df_fx)
        products_silver = build_products_silver(df_products)
        behavior_silver = build_behavior_silver(df_behavior)
        fx_silver = build_fx_silver(df_fx)
        order_events_silver = build_order_events_silver(df_kafka, df_orders)
        
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
        
        # Count input rows (approximate from bronze)
        bronze_root = config["paths"]["bronze_root"]
        sources = config["sources"]
        rows_in_approx = 0
        try:
            # Try to count from bronze sources
            for source_name in ["crm", "redshift", "snowflake", "fx", "kafka_sim"]:
                if source_name in sources:
                    source_path = f"{bronze_root}/{source_name}"
                    try:
                        df_temp = spark.read.format("delta").load(source_path)
                        rows_in_approx += df_temp.count()
                    except:
                        pass
        except:
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
                    target=config["paths"]["silver_root"],
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
            except:
                pass
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
