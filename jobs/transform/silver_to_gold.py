#!/usr/bin/env python3
"""
Silver to Gold Transformation - Refactored

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

from pyspark.sql import SparkSession, DataFrame

# Core utilities
from project_a.utils.spark_session import build_spark
from project_a.pyspark_interview_project.utils.config_loader import load_config_resolved
from project_a.utils.logging import setup_json_logging, get_trace_id
from project_a.utils.run_audit import write_run_audit
from project_a.utils.path_resolver import resolve_data_path

# Monitoring
from project_a.pyspark_interview_project.monitoring.lineage_decorator import lineage_job
from project_a.pyspark_interview_project.monitoring.metrics_collector import emit_rowcount, emit_duration

# Gold builders
from project_a.pyspark_interview_project.transform.gold_builders import (
    build_dim_date,
    build_dim_customer,
    build_dim_product,
    build_fact_orders,
    build_customer_360,
    build_product_performance
)

# Writer
from project_a.pyspark_interview_project.io.delta_writer import write_table, optimize_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_silver_table(
    spark: SparkSession,
    path: str,
    table_name: str,
    config: Dict[str, Any]
) -> DataFrame:
    """
    Read Silver table with fallback: Parquet (local) → Delta (AWS).
    
    Args:
        spark: SparkSession
        path: Table path
        table_name: Table name for logging
        config: Configuration dictionary
        
    Returns:
        DataFrame or empty DataFrame if not found
    """
    write_format = "parquet" if (config.get("environment") or config.get("env", "local")) in ("local", "dev_local") else "delta"
    
    try:
        df = spark.read.format(write_format).load(path)
        try:
            count = df.count()
            logger.info(f"  {table_name}: {count:,} rows ({write_format})")
            return df
        except Exception:
            logger.warning(f"{table_name} appears empty at {path}")
            return df
    except Exception as e:
        logger.warning(f"{table_name} not found at {path}, trying alternative format: {e}")
        # Try alternative format
        alt_format = "delta" if write_format == "parquet" else "parquet"
        try:
            df = spark.read.format(alt_format).load(path)
            count = df.count()
            logger.info(f"  {table_name}: {count:,} rows ({alt_format})")
            return df
        except Exception as e2:
            logger.warning(f"{table_name} not found in either format, creating empty DataFrame: {e2}")
            # Return empty DataFrame with minimal schema
            from pyspark.sql.types import StructType, StructField, StringType
            return spark.createDataFrame([], StructType([StructField("id", StringType(), True)]))


@lineage_job(
    name="silver_to_gold",
    inputs=["silver"],
    outputs=["gold"]
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
        Dictionary with gold DataFrames
    """
    if run_date is None:
        run_date = datetime.utcnow().strftime("%Y-%m-%d")
    
    logger.info(f"🚀 Starting silver to gold transformation (run_date={run_date})")
    start_time = time.time()
    
    # Get paths
    silver_root = resolve_data_path(config, "silver")
    gold_root = resolve_data_path(config, "gold")
    
    # Get table names from config
    tables_config = config.get("tables", {})
    silver_tables = tables_config.get("silver", {})
    gold_tables = tables_config.get("gold", {})
    
    silver_table_names = {
        "customers": silver_tables.get("customers", "customers_silver"),
        "orders": silver_tables.get("orders", "orders_silver"),
        "products": silver_tables.get("products", "products_silver"),
        "behavior": silver_tables.get("behavior", "customer_behavior_silver"),
    }
    
    gold_table_names = {
        "fact_orders": gold_tables.get("fact_orders", "fact_orders"),
        "dim_customer": gold_tables.get("dim_customer", "dim_customer"),
        "dim_product": gold_tables.get("dim_product", "dim_product"),
        "dim_date": gold_tables.get("dim_date", "dim_date"),
        "customer_360": gold_tables.get("customer_360", "customer_360"),
        "product_performance": gold_tables.get("product_performance", "product_performance"),
    }
    
    try:
        # 1. Read silver tables
        logger.info("📥 Reading silver tables...")
        customers_silver = read_silver_table(
            spark, f"{silver_root}/{silver_table_names['customers']}", "customers_silver", config
        )
        orders_silver = read_silver_table(
            spark, f"{silver_root}/{silver_table_names['orders']}", "orders_silver", config
        )
        products_silver = read_silver_table(
            spark, f"{silver_root}/{silver_table_names['products']}", "products_silver", config
        )
        behavior_silver = read_silver_table(
            spark, f"{silver_root}/{silver_table_names['behavior']}", "behavior_silver", config
        )
        
        # 2. Build dimensions
        logger.info("🔧 Building dimensions...")
        dim_date = build_dim_date(spark, orders_silver)
        dim_customer = build_dim_customer(customers_silver, spark)
        dim_product = build_dim_product(products_silver, spark)
        
        # 3. Build fact tables
        logger.info("🔧 Building fact tables...")
        fact_orders = build_fact_orders(orders_silver, dim_customer, dim_product, dim_date, spark)
        
        # 4. Build analytics tables
        logger.info("🔧 Building analytics tables...")
        customer_360 = build_customer_360(dim_customer, orders_silver, behavior_silver, spark)
        product_performance = build_product_performance(dim_product, orders_silver, spark)
        
        # 5. Write gold tables
        logger.info("💾 Writing gold tables...")
        write_table(dim_date, gold_root, gold_table_names["dim_date"], config)
        write_table(dim_customer, gold_root, gold_table_names["dim_customer"], config, partition_by=["country"])
        write_table(dim_product, gold_root, gold_table_names["dim_product"], config)
        write_table(fact_orders, gold_root, gold_table_names["fact_orders"], config, partition_by=["order_date"])
        write_table(customer_360, gold_root, gold_table_names["customer_360"], config, partition_by=["country"])
        write_table(product_performance, gold_root, gold_table_names["product_performance"], config)
        
        # 6. Optimize large fact tables
        logger.info("⚡ Optimizing gold tables...")
        optimize_table(spark, gold_root, gold_table_names["fact_orders"], config, z_order_by=["order_date", "customer_sk"])
        
        # 7. Emit metrics
        duration_ms = (time.time() - start_time) * 1000
        try:
            emit_rowcount("gold_fact_orders_total", fact_orders.count(), {"layer": "gold"}, config)
            emit_rowcount("gold_dim_customer_total", dim_customer.count(), {"layer": "gold"}, config)
            emit_duration("gold_transformation_duration", duration_ms, {"stage": "silver_to_gold"}, config)
        except Exception as e:
            logger.warning(f"Could not emit metrics: {e}")
        
        logger.info(f"✅ Silver to Gold completed in {duration_ms:.0f}ms")
        
        return {
            "fact_orders": fact_orders,
            "dim_customer": dim_customer,
            "dim_product": dim_product,
            "dim_date": dim_date,
            "customer_360": customer_360,
            "product_performance": product_performance
        }
        
    except Exception as e:
        logger.error(f"❌ Gold transformation failed: {e}", exc_info=True)
        raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Silver to Gold transformation")
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
    spark = build_spark(app_name="silver_to_gold", config=config)
    
    try:
        run_date = datetime.utcnow().strftime("%Y-%m-%d")
        results = silver_to_gold_complete(spark, config, run_date=run_date)
        
        duration_ms = (time.time() - start_time) * 1000
        
        logger.info("✅ Silver to Gold transformation completed")
        try:
            logger.info(f"  - fact_orders: {results['fact_orders'].count():,} rows")
            logger.info(f"  - dim_customer: {results['dim_customer'].count():,} rows")
            logger.info(f"  - dim_product: {results['dim_product'].count():,} rows")
            logger.info(f"  - dim_date: {results['dim_date'].count():,} rows")
            logger.info(f"  - customer_360: {results['customer_360'].count():,} rows")
            logger.info(f"  - product_performance: {results['product_performance'].count():,} rows")
        except Exception as e:
            logger.warning(f"Could not log row counts: {e}")
        
        # Write run audit
        lake_bucket = config.get("buckets", {}).get("lake", "")
        if lake_bucket:
            try:
                write_run_audit(
                    bucket=lake_bucket,
                    job_name="silver_to_gold",
                    env=config.get("environment", "dev"),
                    source=resolve_data_path(config, "silver"),
                    target=resolve_data_path(config, "gold"),
                    rows_in=0,
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

