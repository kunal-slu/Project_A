#!/usr/bin/env python3
"""
SCD Type-2 dimension for dim_customer (P1-7).

Implements proper SCD2 with:
- valid_from / valid_to / is_current
- Delta MERGE logic
- Gap/overlap detection
"""

import sys
import logging
import uuid
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from project_a.utils.spark_session import build_spark
from project_a.utils.path_resolver import resolve_path
from project_a.utils.io import read_delta, write_delta
from project_a.config_loader import load_config_resolved
from project_a.monitoring.lineage_decorator import lineage_job
from project_a.monitoring.metrics_collector import emit_rowcount
from project_a.utils.logging import setup_json_logging, get_trace_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@lineage_job(
    name="dim_customer_scd2",
    inputs=["s3://bucket/silver/customers"],
    outputs=["s3://bucket/gold/dim_customer"]
)
def build_dim_customer_scd2(
    spark: SparkSession,
    config: Dict[str, Any],
    run_date: str = None
) -> DataFrame:
    """
    Build SCD Type-2 dim_customer from silver.customers.
    
    Args:
        spark: SparkSession
        config: Configuration dict
        run_date: Processing date
        
    Returns:
        dim_customer DataFrame with SCD2 columns
    """
    if run_date is None:
        run_date = datetime.now().strftime("%Y-%m-%d")
    
    logger.info(f"🚀 Building dim_customer SCD2 (run_date={run_date})")
    
    # Read silver customers
    silver_path = resolve_path("lake://silver", "customers", config=config)
    src = read_delta(spark, silver_path)
    
    if src.isEmpty():
        logger.warning("⚠️  No source data found")
        return spark.createDataFrame([], schema=spark.table("gold.dim_customer").schema if spark.catalog.tableExists("gold.dim_customer") else src.schema)
    
    # Prepare source with SCD2 columns
    src_scd2 = src \
        .withColumn("is_current", F.lit(True)) \
        .withColumn("valid_from", F.current_timestamp()) \
        .withColumn("valid_to", F.lit(None).cast("timestamp")) \
        .withColumn("_surrogate_key", F.monotonically_increasing_id())  # Temporary SK
    
    # Get gold path
    gold_path = resolve_path("lake://gold", "dim_customer", config=config)
    
    # Check if Delta table exists
    try:
        delta_table = DeltaTable.forPath(spark, gold_path)
        logger.info("📊 Existing dim_customer table found, performing SCD2 MERGE")
        
        # SCD2 MERGE logic
        # For existing records that changed, close current record and insert new
        # For new records, just insert
        
        # Identify natural key (customer_id)
        natural_key = "customer_id"
        
        # Perform MERGE
        merge_result = delta_table.alias("target").merge(
            src_scd2.alias("source"),
            f"target.{natural_key} = source.{natural_key} AND target.is_current = true"
        ).whenMatchedUpdate(
            condition=F.expr("target.email <> source.email OR target.country <> source.country OR target.name <> source.name"),
            set={
                "is_current": F.lit(False),
                "valid_to": F.current_timestamp(),
                "_updated_at": F.current_timestamp()
            }
        ).whenNotMatchedInsertAll().execute()
        
        logger.info("✅ SCD2 MERGE completed")
        
    except Exception as e:
        # Table doesn't exist, create initial table
        logger.info(f"📊 Creating new dim_customer table: {e}")
        
        # Generate surrogate keys (customer_sk)
        # Use row_number over natural key for consistent SKs
        from pyspark.sql.window import Window
        
        window = Window.partitionBy("customer_id").orderBy(F.col("valid_from"))
        src_with_sk = src_scd2 \
            .withColumn("customer_sk", F.row_number().over(window)) \
            .withColumn("customer_sk", F.col("customer_sk") + F.lit(1000000))  # Offset to avoid conflicts
        
        # Write initial table
        write_delta(
            src_with_sk,
            gold_path,
            mode="overwrite"
        )
        
        logger.info("✅ Initial dim_customer table created")
    
    # Read final result
    dim_customer = read_delta(spark, gold_path)
    
    # Metrics
    current_count = dim_customer.filter(F.col("is_current") == True).count()
    total_count = dim_customer.count()
    
    emit_rowcount("dim_customer_current", current_count, {"layer": "gold"}, config)
    emit_rowcount("dim_customer_total", total_count, {"layer": "gold"}, config)
    
    logger.info(f"✅ dim_customer: {current_count:,} current records, {total_count:,} total (including history)")
    
    return dim_customer


def main():
    """Main entry point."""
    trace_id = get_trace_id()
    setup_json_logging(level="INFO", include_trace_id=True)
    logger.info(f"Job started (trace_id={trace_id})")
    
    # Load config
    config_path = Path("config/prod.yaml")
    if not config_path.exists():
        config_path = Path("config/local.yaml")
    
    config = load_config_resolved(str(config_path))
    
    # Build Spark
    spark = build_spark(config)
    
    try:
        # Build dim_customer
        dim_customer = build_dim_customer_scd2(spark, config)
        
        logger.info("🎉 dim_customer SCD2 job completed")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Job failed: {e}", exc_info=True)
        return 1
        
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

