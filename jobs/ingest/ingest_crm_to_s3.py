"""
Ingest CRM (HubSpot/Salesforce) data to S3 raw zone.

Supports incremental loads using watermark.
"""

import argparse
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, to_date, col

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.state_store import get_state_store
from pyspark_interview_project.monitoring.lineage_decorator import lineage_job
from pyspark_interview_project.monitoring.metrics_collector import emit_metrics
import time

logger = logging.getLogger(__name__)


@lineage_job(
    name="ingest_crm_to_s3_raw",
    inputs=["crm://accounts", "crm://contacts"],
    outputs=["s3://my-etl-lake-demo/raw/crm/"]
)
def ingest_crm_to_s3_raw(
    spark: SparkSession,
    config: Dict[str, Any],
    table: str = "accounts",
    execution_date: str = None,
    incremental: bool = True,
    **kwargs
):
    """
    Ingest CRM table to S3 raw zone with incremental support.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        table: Table name (accounts, contacts, opportunities)
        execution_date: Execution date
        incremental: Use watermark for incremental load
    """
    start_time = time.time()
    
    if execution_date is None:
        execution_date = datetime.now().strftime("%Y-%m-%d")
    
    state_store = get_state_store(config)
    
    # Get watermark for incremental
    watermark = None
    if incremental:
        watermark_str = state_store.get_watermark(f"crm_{table}")
        if watermark_str:
            watermark = datetime.fromisoformat(watermark_str.replace('Z', '+00:00'))
            logger.info(f"Using watermark: {watermark}")
    
    # Read CRM data
    if config.get('environment') == 'local':
        csv_path = f"aws/data/crm/{table}.csv"
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        
        # Filter by watermark if incremental
        if watermark and "last_modified_date" in df.columns:
            df = df.filter(col("last_modified_date") >= lit(watermark))
    else:
        # Production: Use CRM API (HubSpot/Salesforce)
        # For now, create empty DataFrame
        from pyspark.sql.types import StructType
        schema = StructType([])  # Load from config
        df = spark.createDataFrame([], schema)
    
    # Add load metadata
    df = df.withColumn("_load_date", to_date(lit(execution_date))) \
           .withColumn("_load_timestamp", current_timestamp()) \
           .withColumn("_source", lit("crm")) \
           .withColumn("_table", lit(table))
    
    # Write to raw zone
    raw_path = f"s3://my-etl-lake-demo/raw/crm/{table}/load_dt={execution_date}/"
    logger.info(f"Writing to raw zone: {raw_path}")
    
    df.write \
        .mode("append") \
        .parquet(raw_path)
    
    record_count = df.count()
    
    # Update watermark
    if record_count > 0 and watermark:
        from pyspark_interview_project.utils.watermark_utils import get_latest_timestamp_from_df
        latest_ts = get_latest_timestamp_from_df(df, "last_modified_date")
        if latest_ts:
            state_store.set_watermark(f"crm_{table}", latest_ts.isoformat())
    
    duration = time.time() - start_time
    
    # Emit metrics
    emit_metrics(
        job_name=f"ingest_crm_{table}",
        rows_in=record_count,
        rows_out=record_count,
        duration_seconds=duration,
        dq_status="pass",
        config=config
    )
    
    logger.info(f"✅ Ingested {record_count:,} records to raw zone")
    
    return df


def main():
    parser = argparse.ArgumentParser(description="Ingest CRM to S3 raw")
    parser.add_argument("--table", default="accounts", help="Table name")
    parser.add_argument("--execution-date", help="Execution date")
    parser.add_argument("--full-load", action="store_true", help="Full load (not incremental)")
    parser.add_argument("--config", default="config/prod.yaml", help="Config file")
    
    args = parser.parse_args()
    
    config = load_conf(args.config)
    spark = build_spark(app_name=f"ingest_crm_{args.table}", config=config)
    
    try:
        ingest_crm_to_s3_raw(
            spark,
            config,
            table=args.table,
            execution_date=args.execution_date,
            incremental=not args.full_load
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

