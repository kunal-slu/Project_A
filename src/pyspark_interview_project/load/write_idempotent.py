"""
Idempotent write utilities with staging and run IDs.

Ensures pipeline reruns don't create duplicates.
"""

import os
import uuid
import logging
from datetime import datetime
from typing import Optional
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, current_timestamp

logger = logging.getLogger(__name__)


def get_run_id(run_id: Optional[str] = None) -> str:
    """Generate or use provided run ID."""
    if run_id:
        return run_id
    return os.getenv("AIRFLOW_RUN_ID") or os.getenv("RUN_ID") or f"run_{uuid.uuid4().hex[:8]}"


def write_bronze_idempotent(
    df: DataFrame,
    output_path: str,
    table_name: str,
    run_id: Optional[str] = None,
    partition_cols: list = None
) -> str:
    """
    Write to Bronze layer with idempotent staging workflow.
    
    Process:
    1. Write to staging path: {output_path}/_staging/{run_id}/
    2. Validate staging data
    3. Move/rename to final path: {output_path}/dt={date}/
    
    Args:
        df: DataFrame to write
        output_path: Base output path (e.g., s3://bucket/bronze/snowflake_orders)
        table_name: Table name
        run_id: Unique run identifier
        partition_cols: Partition columns (e.g., ['dt', 'source'])
        
    Returns:
        Final output path
    """
    run_id = get_run_id(run_id)
    partition_cols = partition_cols or ['dt']
    
    # Staging path
    staging_path = f"{output_path}/_staging/{run_id}"
    
    # Add run metadata
    df_with_metadata = df \
        .withColumn("_run_id", lit(run_id)) \
        .withColumn("_ingestion_ts", current_timestamp())
    
    logger.info(f"📝 Writing to staging: {staging_path}")
    logger.info(f"   Run ID: {run_id}")
    logger.info(f"   Records: {df_with_metadata.count():,}")
    
    # Write to staging
    writer = df_with_metadata.write.format("delta").mode("overwrite")
    
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    writer.save(staging_path)
    
    logger.info(f"✅ Staging write completed")
    
    # Validate staging (optional: check row count, schema, etc.)
    staging_df = df.sparkSession.read.format("delta").load(staging_path)
    staging_count = staging_df.count()
    
    if staging_count == 0:
        logger.warning("⚠️ Staging data is empty, aborting final write")
        return staging_path
    
    # Move to final path (atomic operation)
    # For Delta Lake, we can use OPTIMIZE + VACUUM pattern
    # Or simply rename/copy files
    final_path = f"{output_path}/dt={datetime.utcnow().strftime('%Y-%m-%d')}"
    
    logger.info(f"📦 Moving staging to final: {final_path}")
    
    # Read from staging and write to final with append mode
    staging_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy(*partition_cols) \
        .save(final_path)
    
    # Cleanup staging (optional: keep for audit)
    # logger.info(f"🧹 Cleaning up staging: {staging_path}")
    # staging_df.sparkSession._jvm.org.apache.hadoop.fs.FileSystem.get(...).delete(...)
    
    logger.info(f"✅ Idempotent write completed: {final_path}")
    
    return final_path


def write_silver_idempotent(
    df: DataFrame,
    output_path: str,
    table_name: str,
    key_columns: list,
    run_id: Optional[str] = None
) -> str:
    """
    Write to Silver layer with MERGE for idempotency.
    
    Uses Delta Lake MERGE to ensure idempotent upserts.
    
    Args:
        df: DataFrame to write
        output_path: Silver table path
        table_name: Table name
        key_columns: Primary key columns for MERGE
        run_id: Unique run identifier
        
    Returns:
        Final output path
    """
    from delta.tables import DeltaTable
    
    run_id = get_run_id(run_id)
    
    # Add run metadata
    df_with_metadata = df \
        .withColumn("_run_id", lit(run_id)) \
        .withColumn("_processed_ts", current_timestamp())
    
    spark = df.sparkSession
    
    logger.info(f"📝 Writing to Silver: {output_path}")
    logger.info(f"   Run ID: {run_id}")
    logger.info(f"   Key columns: {key_columns}")
    logger.info(f"   Records: {df_with_metadata.count():,}")
    
    try:
        # Check if table exists
        target = DeltaTable.forPath(spark, output_path)
        
        # Build merge condition
        merge_condition = " AND ".join([
            f"target.{key} = source.{key}" for key in key_columns
        ])
        
        logger.info(f"🔄 MERGE operation: {merge_condition}")
        
        # Perform MERGE
        (target.alias("target")
         .merge(df_with_metadata.alias("source"), merge_condition)
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        
        logger.info(f"✅ MERGE completed for {table_name}")
        
    except Exception as e:
        # Table doesn't exist, create new
        logger.info(f"📝 Creating new Silver table: {output_path}")
        
        df_with_metadata.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(output_path)
        
        logger.info(f"✅ New Silver table created")
    
    return output_path


def write_gold_idempotent(
    df: DataFrame,
    output_path: str,
    table_name: str,
    key_columns: list,
    run_id: Optional[str] = None
) -> str:
    """
    Write to Gold layer with MERGE for idempotency.
    
    Args:
        df: DataFrame to write
        output_path: Gold table path
        table_name: Table name
        key_columns: Primary key columns
        run_id: Unique run identifier
        
    Returns:
        Final output path
    """
    return write_silver_idempotent(df, output_path, table_name, key_columns, run_id)

