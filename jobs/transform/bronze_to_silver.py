#!/usr/bin/env python3
"""
Bronze to Silver Transformation - Refactored

Clean, production-grade ETL job using shared utilities.
"""

import sys
import argparse
import logging
import uuid
import time
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

# Add src to path for local execution
project_root = Path(__file__).parent.parent.parent
src_path = project_root / "src"
if src_path.exists() and str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from pyspark.sql import SparkSession

# Core utilities
from project_a.utils.spark_session import build_spark
from project_a.pyspark_interview_project.utils.config_loader import load_config_resolved
from project_a.utils.logging import setup_json_logging, get_trace_id
from project_a.utils.run_audit import write_run_audit
from project_a.utils.path_resolver import resolve_data_path

# Monitoring
from project_a.pyspark_interview_project.monitoring.lineage_decorator import lineage_job
from project_a.pyspark_interview_project.monitoring.metrics_collector import emit_rowcount, emit_duration

# Bronze loaders
from project_a.pyspark_interview_project.transform.bronze_loaders import (
    load_crm_bronze_data,
    load_snowflake_bronze_data,
    load_redshift_behavior_bronze_data,
    load_kafka_bronze_data
)

# Silver builders
from project_a.pyspark_interview_project.transform.silver_builders import (
    build_customers_silver,
    build_orders_silver,
    build_products_silver,
    build_behavior_silver
)

# FX loader
from project_a.extract.fx_json_reader import read_fx_rates_from_bronze

# Writer
from project_a.pyspark_interview_project.io.delta_writer import write_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_fx_silver(df_fx):
    """Build FX silver table."""
    from pyspark.sql import functions as F
    
    logger.info("🔧 Building silver.fx_rates...")
    
    # FX DataFrame has trade_date or date, not rate_date
    fx_silver = df_fx.select(
        F.col("base_currency"),
        F.col("target_currency"),
        F.coalesce(F.to_date("trade_date"), F.to_date("date")).alias("trade_date"),
        F.col("exchange_rate"),
        F.col("bid_rate"),
        F.col("ask_rate"),
        F.col("source")
    )
    
    try:
        logger.info(f"✅ silver.fx_rates: {fx_silver.count():,} rows")
    except Exception:
        pass
    
    return fx_silver


def build_order_events_silver(df_kafka, df_orders):
    """Build order events silver from Kafka data."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
    
    logger.info("🔧 Building silver.order_events...")
    
    # Check if Kafka data has direct columns or JSON in value column
    if "order_id" in df_kafka.columns:
        # Direct columns (from CSV seed)
        order_events = df_kafka.select(
            F.col("event_id"),
            F.col("order_id"),
            F.col("event_type"),
            F.to_timestamp("event_ts").alias("event_timestamp"),
            F.col("amount"),
            F.col("currency"),
            F.col("channel")
        )
    elif "value" in df_kafka.columns:
        # JSON in value column (from actual Kafka stream)
        event_schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", TimestampType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("channel", StringType(), True),
        ])
        
        order_events = df_kafka.withColumn(
            "parsed_value",
            F.from_json(F.col("value"), event_schema)
        ).select(
            F.col("event_id"),
            F.col("parsed_value.order_id").alias("order_id"),
            F.col("parsed_value.event_type").alias("event_type"),
            F.col("parsed_value.event_ts").alias("event_timestamp"),
            F.col("parsed_value.amount").alias("amount"),
            F.col("parsed_value.currency").alias("currency"),
            F.col("parsed_value.channel").alias("channel")
        )
    else:
        # Create empty DataFrame
        logger.warning("Kafka data has no recognizable structure, creating empty DataFrame")
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("channel", StringType(), True),
        ])
        return df_kafka.sparkSession.createDataFrame([], schema)
    
    try:
        logger.info(f"✅ silver.order_events: {order_events.count():,} rows")
    except Exception:
        pass
    
    return order_events


@lineage_job(
    name="bronze_to_silver",
    inputs=["bronze"],
    outputs=["silver"]
)
def bronze_to_silver_complete(
    spark: SparkSession,
    config: Dict[str, Any],
    run_date: str = None
) -> Dict[str, Any]:
    """
    Complete bronze to silver transformation.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        run_date: Processing date YYYY-MM-DD
        
    Returns:
        Dictionary with silver DataFrames and metadata
    """
    if run_date is None:
        run_date = datetime.utcnow().strftime("%Y-%m-%d")
    
    logger.info(f"🚀 Starting bronze to silver transformation (run_date={run_date})")
    start_time = time.time()
    
    # Get paths
    silver_root = resolve_data_path(config, "silver")
    tables_config = config.get("tables", {}).get("silver", {})
    tables = {
        "customers": tables_config.get("customers", "customers_silver"),
        "orders": tables_config.get("orders", "orders_silver"),
        "products": tables_config.get("products", "products_silver"),
        "behavior": tables_config.get("behavior", "customer_behavior_silver"),
        "fx_rates": tables_config.get("fx_rates", "fx_rates_silver"),
        "order_events": tables_config.get("order_events", "order_events_silver")
    }
    
    try:
        # 1. Load bronze data
        df_accounts, df_contacts, df_opps = load_crm_bronze_data(spark, config)
        df_customers, df_orders, df_products = load_snowflake_bronze_data(spark, config)
        df_behavior = load_redshift_behavior_bronze_data(spark, config)
        df_fx = read_fx_rates_from_bronze(spark, resolve_data_path(config, "bronze"))
        df_kafka = load_kafka_bronze_data(spark, config)
        
        # 2. Build silver tables
        customers_silver = build_customers_silver(df_accounts, df_contacts, df_opps, df_behavior)
        orders_silver = build_orders_silver(df_orders, df_customers, df_products, df_fx)
        products_silver = build_products_silver(df_products)
        behavior_silver = build_behavior_silver(df_behavior)
        fx_silver = build_fx_silver(df_fx)
        order_events_silver = build_order_events_silver(df_kafka, orders_silver)
        
        # 3. Write silver tables
        write_table(customers_silver, silver_root, tables["customers"], config, partition_by=["country"])
        write_table(orders_silver, silver_root, tables["orders"], config, partition_by=["order_date"])
        write_table(products_silver, silver_root, tables["products"], config)
        write_table(behavior_silver, silver_root, tables["behavior"], config, partition_by=["event_date"])
        write_table(fx_silver, silver_root, tables["fx_rates"], config, partition_by=["trade_date"])
        write_table(order_events_silver, silver_root, tables["order_events"], config, partition_by=["event_timestamp"])
        
        # 4. Emit metrics
        duration_ms = (time.time() - start_time) * 1000
        try:
            emit_rowcount("silver_customers_total", customers_silver.count(), {"layer": "silver"}, config)
            emit_rowcount("silver_orders_total", orders_silver.count(), {"layer": "silver"}, config)
            emit_duration("silver_transformation_duration", duration_ms, {"stage": "bronze_to_silver"}, config)
        except Exception as e:
            logger.warning(f"Could not emit metrics: {e}")
        
        logger.info(f"✅ Bronze to Silver completed in {duration_ms:.0f}ms")
        
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
    parser = argparse.ArgumentParser(description="Bronze to Silver transformation")
    parser.add_argument("--env", default="dev", help="Environment (dev/local/prod)")
    parser.add_argument("--config", help="Config file path (local or s3://...)")
    args = parser.parse_args()
    
    # Setup logging
    trace_id = get_trace_id()
    setup_json_logging(level="INFO", include_trace_id=True)
    logger.info(f"Job started (trace_id={trace_id}, env={args.env})")
    
    # Load config
    if args.config:
        config_path = args.config
    else:
        # Default config resolution
        env = args.env
        if env == "local":
            config_path = "local/config/local.yaml"
        else:
            config_path = f"config/{env}.yaml"
    
    try:
        config = load_config_resolved(config_path, env=args.env)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        raise
    
    # Set environment if not set
    if not config.get("environment"):
        config["environment"] = args.env
    
    run_id = str(uuid.uuid4())
    start_time = time.time()
    spark = build_spark(app_name="bronze_to_silver", config=config)
    
    try:
        run_date = datetime.utcnow().strftime("%Y-%m-%d")
        results = bronze_to_silver_complete(spark, config, run_date=run_date)
        
        duration_ms = (time.time() - start_time) * 1000
        
        logger.info("✅ Bronze to Silver transformation completed")
        try:
            logger.info(f"  - Customers: {results['customers'].count():,} rows")
            logger.info(f"  - Orders: {results['orders'].count():,} rows")
            logger.info(f"  - Products: {results['products'].count():,} rows")
            logger.info(f"  - Behavior: {results['behavior'].count():,} rows")
            logger.info(f"  - FX Rates: {results['fx_rates'].count():,} rows")
            logger.info(f"  - Order Events: {results['order_events'].count():,} rows")
        except Exception as e:
            logger.warning(f"Could not log row counts: {e}")
        
        # Write run audit
        lake_bucket = config.get("buckets", {}).get("lake", "")
        if lake_bucket:
            try:
                write_run_audit(
                    bucket=lake_bucket,
                    job_name="bronze_to_silver",
                    env=config.get("environment", "dev"),
                    source=resolve_data_path(config, "bronze"),
                    target=resolve_data_path(config, "silver"),
                    rows_in=0,  # Could be calculated if needed
                    rows_out=sum([r.count() for r in results.values()]) if results else 0,
                    status="SUCCESS",
                    run_id=run_id,
                    duration_ms=duration_ms,
                    config=config
                )
            except Exception as e:
                logger.warning(f"⚠️  Failed to write run audit: {e}")
        
    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}", exc_info=True)
        
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

