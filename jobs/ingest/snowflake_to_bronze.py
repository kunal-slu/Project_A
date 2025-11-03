#!/usr/bin/env python3
"""
Production-ready Snowflake to Bronze ingestion job.

P0 Features:
- Schema contract validation
- Incremental ingestion with watermarks
- Run metadata columns (_ingest_ts, _batch_id, _source_system, _run_date)
- Error lanes for bad rows
- OpenLineage + metrics emission
"""

import sys
import logging
import uuid
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, types as T

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.path_resolver import resolve_path
from pyspark_interview_project.utils.contracts import (
    load_schema_contract,
    contract_to_struct_type,
    align_to_schema,
    validate_and_quarantine,
    add_metadata_columns
)
from pyspark_interview_project.utils.error_lanes import ErrorLaneHandler, add_row_id
from pyspark_interview_project.utils.state_store import get_state_store
from pyspark_interview_project.utils.secrets import get_snowflake_credentials
from pyspark_interview_project.monitoring.lineage_decorator import lineage_job
from pyspark_interview_project.monitoring.metrics_collector import emit_rowcount, emit_duration
from pyspark_interview_project.utils.logging import setup_json_logging, get_trace_id
from pyspark_interview_project.config_loader import load_config_resolved

import yaml
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@lineage_job(
    name="bronze_snowflake_orders",
    inputs=["snowflake://ETL_PROJECT_DB.RAW.ORDERS"],
    outputs=["s3://bucket/bronze/snowflake/orders"]
)
def extract_snowflake_orders_production(
    spark: SparkSession,
    config: Dict[str, Any],
    run_id: str = None
) -> DataFrame:
    """
    Production-grade Snowflake orders extraction with all P0 features.
    
    Args:
        spark: SparkSession
        config: Configuration dict
        run_id: Optional run identifier (UUID)
        
    Returns:
        DataFrame with validated, enriched orders data
    """
    if run_id is None:
        run_id = str(uuid.uuid4())
    
    logger.info(f"🚀 Starting Snowflake orders extraction (run_id={run_id})")
    start_time = time.time()
    
    # Get source configuration
    source_cfg = config.get("sources", {}).get("snowflake_orders", {})
    load_type = source_cfg.get("load_type", "incremental")
    watermark_col = source_cfg.get("watermark_column", "updated_at")
    watermark_key = source_cfg.get("watermark_state_key", "snowflake_orders_max_ts")
    
    try:
        # 1. INCREMENTAL INGESTION: Read watermark
        last_ts = None
        if load_type == "incremental":
            state_store = get_state_store(config)
            watermark_str = state_store.get_watermark(watermark_key)
            if watermark_str:
                last_ts = datetime.fromisoformat(watermark_str.replace('Z', '+00:00'))
                logger.info(f"📌 Using watermark: {last_ts.isoformat()}")
            else:
                logger.info("📌 No watermark found, performing full load")
        
        # 2. EXTRACT: Read from Snowflake or local sample
        if config.get('environment') == 'local':
            sample_path = config.get('paths', {}).get('snowflake_orders', 
                "data/samples/snowflake/snowflake_orders_100000.csv")
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)
            if last_ts and "order_date" in df.columns:
                df = df.filter(F.col("order_date") >= F.lit(last_ts))
        else:
            # Production: Snowflake JDBC
            snowflake_config = get_snowflake_credentials(config)
            account = snowflake_config.get('account', '').replace('.snowflakecomputing.com', '')
            snowflake_options = {
                "sfURL": f"{account}.snowflakecomputing.com",
                "sfUser": snowflake_config.get("user"),
                "sfPassword": snowflake_config.get("password"),
                "sfDatabase": snowflake_config.get("database", "ETL_PROJECT_DB"),
                "sfSchema": snowflake_config.get("schema", "RAW"),
                "sfWarehouse": snowflake_config.get("warehouse"),
            }
            
            # Build query with watermark
            cond = f"{watermark_col} > '{last_ts.isoformat()}'" if last_ts else "1=1"
            query = f"SELECT * FROM ORDERS WHERE {cond} ORDER BY {watermark_col}"
            
            logger.info(f"📥 Executing Snowflake query: WHERE {cond}")
            
            df = spark.read \
                .format("snowflake") \
                .options(**snowflake_options) \
                .option("query", query) \
                .load()
        
        if df.isEmpty():
            logger.warning("⚠️  No new records found")
            return df
        
        # Add row_id for error tracking
        df = add_row_id(df)
        
        # 3. SCHEMA CONTRACT: Load and validate
        contract_path = Path("config/schema_definitions/snowflake_orders_bronze.json")
        if contract_path.exists():
            logger.info("✅ Loading schema contract")
            contract = load_schema_contract(str(contract_path))
            struct = contract_to_struct_type(contract)
            required_cols = contract.get("required_columns", [])
            
            # Align to schema
            df, dq = align_to_schema(df, struct, required_cols)
            
            # Get lake root for error lanes
            lake_root = config.get("paths", {}).get("bronze", "").replace("lake://", "")
            if not lake_root.startswith("s3"):
                lake_root = resolve_path("lake://bronze", config=config)
            
            error_handler = ErrorLaneHandler(lake_root)
            
            # Validate and quarantine bad rows
            clean_df, quarantined_df, validation_results = validate_and_quarantine(
                spark,
                df,
                contract,
                error_lane_path=error_handler.get_error_lane_path("bronze", "snowflake_orders", run_id)
            )
            
            logger.info(f"✅ Validation: {validation_results['clean_rows']}/{validation_results['total_rows']} rows passed")
            
            if validation_results['quarantined_rows'] > 0:
                logger.warning(f"⚠️  {validation_results['quarantined_rows']} rows quarantined")
            
            df = clean_df
        
        # 4. METADATA COLUMNS: Add run metadata
        run_date = datetime.now().strftime("%Y-%m-%d")
        df = add_metadata_columns(
            df,
            batch_id=run_id,
            source_system="snowflake",
            run_date=run_date
        )
        
        # 5. UPDATE WATERMARK: If incremental and records loaded
        if load_type == "incremental" and not df.isEmpty():
            max_ts = df.agg(F.max(watermark_col)).first()[0]
            if max_ts:
                state_store.set_watermark(watermark_key, max_ts.isoformat())
                logger.info(f"✅ Updated watermark: {max_ts.isoformat()}")
        
        # 6. METRICS: Emit metrics
        record_count = df.count()
        duration_ms = (time.time() - start_time) * 1000
        
        emit_rowcount("records_processed_total", record_count, {
            "job": "bronze_snowflake_orders",
            "source": "snowflake",
            "table": "orders"
        }, config)
        
        emit_rowcount("dq_quarantined_total", validation_results.get('quarantined_rows', 0), {
            "job": "bronze_snowflake_orders",
            "layer": "bronze"
        }, config)
        
        emit_duration("latency_seconds", duration_ms / 1000.0, {
            "job": "bronze_snowflake_orders",
            "stage": "extraction"
        }, config)
        
        logger.info(f"✅ Extraction complete: {record_count:,} records in {duration_ms:.0f}ms")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Extraction failed: {e}", exc_info=True)
        raise


def main():
    """Main entry point for EMR job."""
    # Setup structured logging
    trace_id = get_trace_id()
    setup_json_logging(level="INFO", include_trace_id=True)
    logger.info(f"Job started (trace_id={trace_id})")
    
    # Load configuration
    config_path = Path("config/prod.yaml")
    if not config_path.exists():
        config_path = Path("config/local.yaml")
    
    config = load_config_resolved(str(config_path))
    
    # Generate run ID
    run_id = str(uuid.uuid4())
    
    # Build Spark session
    spark = build_spark(config)
    
    try:
        # Run extraction
        df = extract_snowflake_orders_production(spark, config, run_id=run_id)
        
        if df.isEmpty():
            logger.warning("⚠️  No data to write")
            return 0
        
        # Write to bronze
        bronze_path = resolve_path("lake://bronze", "snowflake", "orders", config=config)
        logger.info(f"💾 Writing to: {bronze_path}")
        
        df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .partitionBy("_run_date") \
            .save(bronze_path)
        
        logger.info("🎉 Snowflake to Bronze job completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Job failed: {e}", exc_info=True)
        return 1
        
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

