"""
CDC / Incremental Framework - Watermark Management

Provides watermark utilities for incremental data processing.
Stores watermarks in S3 (Delta/Parquet) or local CSV fallback.
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Optional
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import max as spark_max, lit, current_timestamp

logger = logging.getLogger(__name__)


def get_watermark(source_name: str, config: dict = None, spark: SparkSession = None) -> Optional[datetime]:
    """
    Get the latest watermark for a data source.
    
    Args:
        source_name: Name of the data source (e.g., 'redshift_behavior', 'snowflake_orders')
        config: Configuration dict with S3 paths
        spark: SparkSession (optional, for S3 reads)
        
    Returns:
        Latest watermark datetime or None if not found
    """
    # Try S3 Delta table first
    if config and spark:
        watermark_path = config.get("data_lake", {}).get("watermark_prefix", "s3://bucket/meta/watermarks")
        if watermark_path.startswith("s3://") or watermark_path.startswith("s3a://"):
            try:
                watermark_df = spark.read.format("delta").load(f"{watermark_path}/{source_name}")
                if not watermark_df.isEmpty():
                    watermark_value = watermark_df.select(spark_max("watermark_value")).collect()[0][0]
                    if watermark_value:
                        return watermark_value
            except Exception as e:
                logger.warning(f"Could not read watermark from S3: {e}, falling back to local")
    
    # Fallback to local CSV/JSON
    local_path = Path("data/checkpoints/watermarks")
    local_path.mkdir(parents=True, exist_ok=True)
    
    watermark_file = local_path / f"{source_name}_watermark.json"
    if watermark_file.exists():
        try:
            with open(watermark_file) as f:
                data = json.load(f)
            watermark_str = data.get("watermark_value")
            if watermark_str:
                return datetime.fromisoformat(watermark_str).replace(tzinfo=timezone.utc)
        except Exception as e:
            logger.warning(f"Could not read local watermark: {e}")
    
    return None


def upsert_watermark(source_name: str, value: datetime, config: dict = None, spark: SparkSession = None) -> None:
    """
    Update watermark for a data source.
    
    Args:
        source_name: Name of the data source
        value: New watermark datetime value
        config: Configuration dict
        spark: SparkSession
    """
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    
    schema = StructType([
        StructField("source_name", StringType(), False),
        StructField("watermark_value", TimestampType(), False),
        StructField("updated_at", TimestampType(), False)
    ])
    
    watermark_df = spark.createDataFrame(
        [(source_name, value, datetime.now(timezone.utc))],
        schema=schema
    )
    
    watermark_path = None
    if config:
        watermark_path = config.get("data_lake", {}).get("watermark_prefix", "s3://bucket/meta/watermarks")
    
    if watermark_path and (watermark_path.startswith("s3://") or watermark_path.startswith("s3a://")) and spark:
        # Write to S3 Delta table
        try:
            full_path = f"{watermark_path}/{source_name}"
            
            # Check if Delta table exists
            from delta.tables import DeltaTable
            if DeltaTable.isDeltaTable(spark, full_path):
                # MERGE watermark
                delta_table = DeltaTable.forPath(spark, full_path)
                delta_table.alias("target").merge(
                    watermark_df.alias("source"),
                    "target.source_name = source.source_name"
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            else:
                # Create new table
                watermark_df.write.format("delta").mode("overwrite").save(full_path)
            
            logger.info(f"✅ Updated watermark for {source_name} to {value.isoformat()} in S3")
            return
        except Exception as e:
            logger.warning(f"Could not write watermark to S3: {e}, falling back to local")
    
    # Fallback to local JSON
    local_path = Path("data/checkpoints/watermarks")
    local_path.mkdir(parents=True, exist_ok=True)
    
    watermark_file = local_path / f"{source_name}_watermark.json"
    with open(watermark_file, "w") as f:
        json.dump({
            "source_name": source_name,
            "watermark_value": value.isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        }, f, indent=2)
    
    logger.info(f"✅ Updated watermark for {source_name} to {value.isoformat()} (local)")


def get_latest_timestamp_from_df(df: DataFrame, timestamp_col: str = "_ingestion_ts") -> Optional[datetime]:
    """
    Extract the latest timestamp from a DataFrame.
    
    Args:
        df: DataFrame with timestamp column
        timestamp_col: Name of timestamp column
        
    Returns:
        Latest timestamp or None
    """
    try:
        if df.isEmpty():
            return None
        
        max_ts = df.agg(spark_max(col(timestamp_col))).collect()[0][0]
        return max_ts
    except Exception as e:
        logger.warning(f"Could not extract latest timestamp: {e}")
        return None

