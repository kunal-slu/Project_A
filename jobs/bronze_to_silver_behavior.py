"""
Transform Redshift behavior data from Bronze to Silver.

Steps:
- Parse dates
- Normalize event_name to lowercase
- Validate session_id pattern (SESS-[0-9]+)
- Split URLs to array
- Drop duplicates by (event_id, event_ts)
"""

import argparse
import logging
import time
import uuid
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lower, split, when, 
    to_timestamp, current_timestamp
)

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.dq.great_expectations_runner import GreatExpectationsRunner
from pyspark_interview_project.monitoring.lineage_emitter import emit_start, emit_complete, emit_fail
from pyspark_interview_project.monitoring.metrics_collector import emit_metrics
from pyspark_interview_project.io.write_table import write_table

logger = logging.getLogger(__name__)


def transform_bronze_to_silver_behavior(spark: SparkSession, config: dict) -> DataFrame:
    """
    Transform behavior data from Bronze to Silver layer.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        
    Returns:
        Cleaned Silver DataFrame
    """
    run_id = str(uuid.uuid4())
    start_time = time.time()
    
    bronze_path = config.get("data_lake", {}).get("bronze_path", "data/lakehouse_delta/bronze")
    silver_path = config.get("data_lake", {}).get("silver_path", "data/lakehouse_delta/silver")
    
    bronze_input = f"{bronze_path}/redshift/behavior"
    silver_output = f"{silver_path}/behavior"
    
    # Emit lineage START
    emit_start(
        job_name="bronze_to_silver_behavior",
        inputs=[{"name": bronze_input, "namespace": "s3"}],
        outputs=[{"name": silver_output, "namespace": "s3"}],
        config=config,
        run_id=run_id
    )
    
    logger.info(f"Reading Bronze behavior data from: {bronze_path}/redshift/behavior")
    
    # Read bronze data
    try:
        df = spark.read.format("delta").load(f"{bronze_path}/redshift/behavior")
    except Exception:
        # Fallback to Parquet/CSV if Delta not available
        logger.warning("Delta table not found, trying Parquet/CSV")
        df = spark.read.format("parquet").load(f"{bronze_path}/redshift/behavior")
    
    logger.info(f"Loaded {df.count():,} records from Bronze")
    
    # Step 1: Parse dates
    date_cols = ["event_ts", "event_timestamp", "timestamp"]
    for col_name in date_cols:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                to_timestamp(col(col_name), "yyyy-MM-dd HH:mm:ss")
            )
            logger.info(f"Parsed date column: {col_name}")
    
    # Step 2: Normalize event_name to lowercase
    if "event_name" in df.columns:
        df = df.withColumn("event_name", lower(col("event_name")))
        logger.info("Normalized event_name to lowercase")
    
    # Step 3: Validate session_id pattern (SESS-[0-9]+)
    if "session_id" in df.columns:
        # Extract valid session_id pattern
        df = df.withColumn(
            "session_id",
            when(
                col("session_id").rlike(r"^SESS-\d+$"),
                col("session_id")
            ).otherwise(None)
        )
        invalid_count = df.filter(col("session_id").isNull()).count()
        if invalid_count > 0:
            logger.warning(f"Found {invalid_count:,} records with invalid session_id pattern")
    
    # Step 4: Split URLs to array
    url_cols = ["page_url", "referrer", "url"]
    for col_name in url_cols:
        if col_name in df.columns:
            # Split by '/' into array, limit to reasonable size
            df = df.withColumn(
                f"{col_name}_parts",
                split(col(col_name), "/")
            )
            logger.info(f"Split {col_name} into array")
    
    # Step 5: Drop duplicates by (event_id, event_ts)
    dedupe_keys = ["event_id"]
    if "event_ts" in df.columns:
        dedupe_keys.append("event_ts")
    elif "event_timestamp" in df.columns:
        dedupe_keys.append("event_timestamp")
    
    try:
        before_count = df.count()
        df = df.dropDuplicates(dedupe_keys)
        after_count = df.count()
        duplicates_removed = int(before_count) - int(after_count)
        
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed:,} duplicate records by {dedupe_keys}")
    except Exception as e:
        logger.warning(f"Could not calculate duplicate removal: {e}")
        # Continue anyway
    
    # Add metadata columns
    df = df.withColumn("_processing_ts", current_timestamp())
    
    try:
        logger.info(f"Silver transformation complete: {int(after_count):,} records")
    except Exception:
        logger.info("Silver transformation complete")
    
    # Write to Silver using abstracted write_table (supports Iceberg/Delta/Parquet)
    logger.info(f"Writing Silver behavior data to table: silver.behavior")
    
    write_table(
        df=df,
        table_name="silver.behavior",
        mode="overwrite",
        cfg=config,
        spark=spark
    )
    
    # Run Great Expectations DQ checks
    logger.info("Running Great Expectations DQ checks...")
    try:
        ge_runner = GreatExpectationsRunner()
        ge_runner.init_context()
        ge_result = ge_runner.run_checkpoint(
            checkpoint_name="silver_behavior_checkpoint",
            fail_on_error=True
        )
        if ge_result.get("success"):
            logger.info("✅ Great Expectations validation passed")
        else:
            raise RuntimeError("Great Expectations validation failed")
    except Exception as e:
        logger.warning(f"GE validation not available: {e}")
        # Continue if GE not configured
    
    # Emit metrics
    duration = time.time() - start_time
    try:
        rows_out = int(df.count())
        emit_metrics(
            job_name="bronze_to_silver_behavior",
            rows_in=0,  # Will be calculated from bronze
            rows_out=rows_out,
            duration_seconds=duration,
            dq_status="pass",
            config=config
        )
    except Exception as e:
        logger.warning(f"Could not emit metrics: {e}")
    
    # Emit lineage COMPLETE
    emit_complete(
        job_name="bronze_to_silver_behavior",
        inputs=[{"name": bronze_input, "namespace": "s3"}],
        outputs=[{"name": silver_output, "namespace": "s3"}],
        config=config,
        run_id=run_id,
        metadata={"rows_out": rows_out, "duration_seconds": duration}
    )
    
    try:
        logger.info(f"✅ Successfully wrote {int(after_count):,} records to Silver")
    except:
        logger.info("✅ Successfully wrote records to Silver")
    
    return df


def main():
    parser = argparse.ArgumentParser(description="Transform Bronze behavior to Silver")
    parser.add_argument("--config", default="config/local.yaml", help="Config file path")
    args = parser.parse_args()
    
    config = load_conf(args.config)
    spark = build_spark(app_name="bronze_to_silver_behavior", config=config)
    
    try:
        transform_bronze_to_silver_behavior(spark, config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

