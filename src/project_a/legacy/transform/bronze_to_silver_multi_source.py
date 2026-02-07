"""
Multi-source Bronze to Silver transformation (P1-6).

Canonicalizes, deduplicates, and joins data from multiple sources:
- Snowflake (orders, customers)
- Redshift (behavior)
- Joins on customer_id for unified customer view
"""

import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from project_a.utils.path_resolver import resolve_path
from project_a.utils.io import read_delta, write_delta
from project_a.monitoring.lineage_decorator import lineage_job
from project_a.monitoring.metrics_collector import emit_rowcount, emit_duration
import time

logger = logging.getLogger(__name__)


@lineage_job(
    name="bronze_to_silver_multi_source",
    inputs=[
        "s3://bucket/bronze/snowflake/orders",
        "s3://bucket/bronze/snowflake/customers",
        "s3://bucket/bronze/redshift/behavior"
    ],
    outputs=[
        "s3://bucket/silver/customers",
        "s3://bucket/silver/orders",
        "s3://bucket/silver/customer_activity"
    ]
)
def bronze_to_silver_multi_source(
    spark: SparkSession,
    config: Dict[str, Any],
    run_date: str = None
) -> Dict[str, DataFrame]:
    """
    Transform bronze data from multiple sources to silver with canonicalization and joins.
    
    Args:
        spark: SparkSession
        config: Configuration dict
        run_date: Processing date YYYY-MM-DD
        
    Returns:
        Dictionary with silver DataFrames: {'customers', 'orders', 'customer_activity'}
    """
    if run_date is None:
        from datetime import datetime
        run_date = datetime.now().strftime("%Y-%m-%d")
    
    logger.info(f"🚀 Starting multi-source bronze to silver transformation (run_date={run_date})")
    start_time = time.time()
    
    # Get bronze paths
    bronze_root = resolve_path("lake://bronze", config=config)
    silver_root = resolve_path("lake://silver", config=config)
    
    # 1. READ BRONZE DATA
    logger.info("📥 Reading bronze data from multiple sources...")
    
    orders_bronze = read_delta(spark, f"{bronze_root}/snowflake/orders")
    customers_bronze = read_delta(spark, f"{bronze_root}/snowflake/customers")
    behavior_bronze = read_delta(spark, f"{bronze_root}/redshift/behavior")
    
    logger.info(f"  Orders: {orders_bronze.count():,} rows")
    logger.info(f"  Customers: {customers_bronze.count():,} rows")
    logger.info(f"  Behavior: {behavior_bronze.count():,} rows")
    
    # 2. SILVER CUSTOMERS: Canonicalize + dedupe
    logger.info("🔧 Creating silver.customers...")
    
    silver_customers = customers_bronze \
        .withColumn("effective_ts", F.current_timestamp()) \
        .withColumn("country", F.coalesce(F.col("country"), F.lit("UNKNOWN")))
    
    # Deduplicate by customer_id (keep most recent)
    if "_ingest_ts" in silver_customers.columns:
        window = Window.partitionBy("customer_id").orderBy(F.col("_ingest_ts").desc())
        silver_customers = silver_customers \
            .withColumn("rn", F.row_number().over(window)) \
            .filter(F.col("rn") == 1) \
            .drop("rn")
    else:
        silver_customers = silver_customers.dropDuplicates(["customer_id"])
    
    logger.info(f"✅ silver.customers: {silver_customers.count():,} unique customers")
    
    # 3. SILVER ORDERS: Canonicalize + dedupe
    logger.info("🔧 Creating silver.orders...")
    
    silver_orders = orders_bronze \
        .withColumn("order_date", F.to_date(F.coalesce(F.col("order_date"), F.col("event_ts")))) \
        .withColumn("amount_usd", F.coalesce(F.col("total_amount"), F.col("amount"), F.lit(0.0)))
    
    # Deduplicate by order_id
    if "_ingest_ts" in silver_orders.columns:
        window = Window.partitionBy("order_id").orderBy(F.col("_ingest_ts").desc())
        silver_orders = silver_orders \
            .withColumn("rn", F.row_number().over(window)) \
            .filter(F.col("rn") == 1) \
            .drop("rn")
    else:
        silver_orders = silver_orders.dropDuplicates(["order_id"])
    
    logger.info(f"✅ silver.orders: {silver_orders.count():,} unique orders")
    
    # 4. SILVER CUSTOMER ACTIVITY: Canonicalize + dedupe
    logger.info("🔧 Creating silver.customer_activity...")
    
    silver_customer_activity = behavior_bronze \
        .withColumn("event_date", F.to_date(F.coalesce(F.col("event_ts"), F.col("timestamp")))) \
        .withColumn("event_name", F.coalesce(F.col("event_name"), F.col("event_type"))) \
        .withColumn("session_id", F.coalesce(F.col("session_id"), F.col("session_id")))
    
    # Deduplicate by event_id or composite key
    if "event_id" in silver_customer_activity.columns:
        if "_ingest_ts" in silver_customer_activity.columns:
            window = Window.partitionBy("event_id").orderBy(F.col("_ingest_ts").desc())
            silver_customer_activity = silver_customer_activity \
                .withColumn("rn", F.row_number().over(window)) \
                .filter(F.col("rn") == 1) \
                .drop("rn")
        else:
            silver_customer_activity = silver_customer_activity.dropDuplicates(["event_id"])
    else:
        # Composite key
        key_cols = ["customer_id", "session_id", "event_date", "event_name"]
        existing_key_cols = [c for c in key_cols if c in silver_customer_activity.columns]
        if existing_key_cols:
            silver_customer_activity = silver_customer_activity.dropDuplicates(existing_key_cols)
    
    logger.info(f"✅ silver.customer_activity: {silver_customer_activity.count():,} unique events")
    
    # 5. WRITE SILVER TABLES (partitioned by date)
    logger.info("💾 Writing silver tables...")
    
    write_delta(
        silver_customers,
        f"{silver_root}/customers",
        mode="overwrite",
        partitionBy=["country"]
    )
    
    write_delta(
        silver_orders,
        f"{silver_root}/orders",
        mode="overwrite",
        partitionBy=["order_date"]
    )
    
    write_delta(
        silver_customer_activity,
        f"{silver_root}/customer_activity",
        mode="overwrite",
        partitionBy=["event_date"]
    )
    
    # 6. METRICS
    duration_ms = (time.time() - start_time) * 1000
    
    emit_rowcount("silver_customers_total", silver_customers.count(), {
        "layer": "silver",
        "table": "customers"
    }, config)
    
    emit_rowcount("silver_orders_total", silver_orders.count(), {
        "layer": "silver",
        "table": "orders"
    }, config)
    
    emit_rowcount("silver_activity_total", silver_customer_activity.count(), {
        "layer": "silver",
        "table": "customer_activity"
    }, config)
    
    emit_duration("silver_transformation_duration", duration_ms, {
        "stage": "bronze_to_silver"
    }, config)
    
    logger.info(f"✅ Multi-source silver transformation complete in {duration_ms:.0f}ms")
    
    return {
        "customers": silver_customers,
        "orders": silver_orders,
        "customer_activity": silver_customer_activity
    }


def join_silver_sources_on_customer_id(
    spark: SparkSession,
    silver_customers: DataFrame,
    silver_orders: DataFrame,
    silver_behavior: DataFrame
) -> DataFrame:
    """
    Join silver sources on customer_id to create unified customer view.
    
    Args:
        spark: SparkSession
        silver_customers: Customers silver table
        silver_orders: Orders silver table
        silver_behavior: Behavior/activity silver table
        
    Returns:
        Joined DataFrame with all customer-related data
    """
    logger.info("🔗 Joining silver sources on customer_id...")
    
    # Join orders with customers
    orders_enriched = silver_orders \
        .join(
            silver_customers.select("customer_id", "country", "email", "name"),
            on="customer_id",
            how="left"
        )
    
    # Join behavior with customers (for customer attributes)
    behavior_enriched = silver_behavior \
        .join(
            silver_customers.select("customer_id", "country", "email"),
            on="customer_id",
            how="left"
        )
    
    # Optional: Create fact combining orders + behavior by date
    # This would be done in silver_to_gold typically
    
    logger.info(f"✅ Joined data: {orders_enriched.count():,} enriched orders")
    
    return orders_enriched

