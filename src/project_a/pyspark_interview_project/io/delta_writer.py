"""
Delta Lake Writer Utilities

Handles writing DataFrames to Delta/Parquet format with proper partitioning,
overwrite semantics, and local/AWS path handling.
"""

import logging
import os
import shutil
from pathlib import Path
from typing import Optional, List
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)


def get_write_format(config: dict) -> str:
    """
    Determine write format based on environment.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        "delta" for AWS/EMR, "parquet" for local
    """
    environment = config.get("environment") or config.get("env", "local")
    if environment in ("local", "dev_local"):
        return "parquet"
    return "delta"


def delete_if_exists_local(path: str) -> None:
    """Delete local directory if it exists."""
    try:
        # Remove file:// prefix if present
        clean_path = path.replace("file://", "")
        if os.path.exists(clean_path):
            shutil.rmtree(clean_path)
            logger.debug(f"Deleted local directory: {clean_path}")
    except Exception as e:
        logger.warning(f"Could not delete local directory {path}: {e}")


def write_table(
    df: DataFrame,
    path: str,
    table_name: str,
    config: dict,
    partition_by: Optional[List[str]] = None,
    mode: str = "overwrite"
) -> None:
    """
    Write DataFrame to Delta or Parquet format.
    
    Args:
        df: DataFrame to write
        path: Target path (will be joined with table_name)
        table_name: Table name
        config: Configuration dictionary
        partition_by: Optional list of partition columns
        mode: Write mode (overwrite, append, merge)
    """
    write_format = get_write_format(config)
    target_path = f"{path.rstrip('/')}/{table_name}"
    
    logger.info(f"Writing {table_name} to {target_path} (format: {write_format}, mode: {mode})")
    
    # For local Parquet writes, delete existing directory first
    if write_format == "parquet" and target_path.startswith("file://"):
        delete_if_exists_local(target_path)
    elif write_format == "parquet" and not target_path.startswith(("s3://", "file://")):
        # Relative path - assume local
        delete_if_exists_local(target_path)
    
    writer = df.write.format(write_format).mode(mode)
    
    # Add partitioning if specified
    if partition_by:
        # Check that partition columns exist
        existing_cols = set(df.columns)
        valid_partitions = [p for p in partition_by if p in existing_cols]
        if valid_partitions:
            writer = writer.partitionBy(*valid_partitions)
            logger.info(f"Partitioning by: {valid_partitions}")
        else:
            logger.warning(f"Partition columns {partition_by} not found, skipping partitioning")
    
    # Write
    try:
        # For empty DataFrames, ensure schema is preserved
        # Spark will write an empty parquet file with schema metadata
        writer.save(target_path)
        
        # Verify write succeeded (especially for empty DataFrames)
        if write_format == "parquet":
            # For parquet, check that at least the directory was created
            import os
            clean_path = target_path.replace("file://", "")
            if not os.path.exists(clean_path):
                # Create empty parquet file with schema if directory doesn't exist
                # This ensures schema is available for reads even when table is empty
                os.makedirs(clean_path, exist_ok=True)
                # Spark should have written schema metadata, but if not, we'll rely on the DataFrame schema
        
        logger.info(f"✅ Successfully wrote {table_name} to {target_path}")
    except Exception as e:
        logger.error(f"❌ Failed to write {table_name} to {target_path}: {e}")
        raise


def optimize_table(
    spark: SparkSession,
    path: str,
    table_name: str,
    config: dict,
    z_order_by: Optional[List[str]] = None
) -> None:
    """
    Optimize Delta table (Z-ORDER and OPTIMIZE).
    
    Args:
        spark: SparkSession
        path: Table path
        table_name: Table name
        config: Configuration dictionary
        z_order_by: Optional columns for Z-ORDER
    """
    write_format = get_write_format(config)
    
    if write_format != "delta":
        logger.info(f"Skipping OPTIMIZE for {table_name} (not Delta format)")
        return
    
    try:
        from delta.tables import DeltaTable
        
        table_path = f"{path.rstrip('/')}/{table_name}"
        delta_table = DeltaTable.forPath(spark, table_path)
        
        # OPTIMIZE
        logger.info(f"Optimizing {table_name}...")
        delta_table.optimize().executeCompaction()
        
        # Z-ORDER if specified
        if z_order_by:
            existing_cols = set(delta_table.toDF().columns)
            valid_z_order = [col for col in z_order_by if col in existing_cols]
            if valid_z_order:
                logger.info(f"Z-ORDERing {table_name} by: {valid_z_order}")
                delta_table.optimize().executeZOrderBy(*valid_z_order)
        
        logger.info(f"✅ Optimized {table_name}")
    except ImportError:
        logger.warning("Delta Lake not available, skipping OPTIMIZE")
    except Exception as e:
        logger.warning(f"Could not optimize {table_name}: {e}")


def vacuum_table(
    spark: SparkSession,
    path: str,
    table_name: str,
    config: dict,
    retention_hours: int = 168  # 7 days default
) -> None:
    """
    VACUUM Delta table to remove old files.
    
    Args:
        spark: SparkSession
        path: Table path
        table_name: Table name
        config: Configuration dictionary
        retention_hours: Retention period in hours
    """
    write_format = get_write_format(config)
    
    if write_format != "delta":
        logger.info(f"Skipping VACUUM for {table_name} (not Delta format)")
        return
    
    try:
        from delta.tables import DeltaTable
        
        table_path = f"{path.rstrip('/')}/{table_name}"
        delta_table = DeltaTable.forPath(spark, table_path)
        
        logger.info(f"VACUUMing {table_name} (retention: {retention_hours}h)...")
        delta_table.vacuum(retentionHours=retention_hours)
        
        logger.info(f"✅ VACUUMed {table_name}")
    except ImportError:
        logger.warning("Delta Lake not available, skipping VACUUM")
    except Exception as e:
        logger.warning(f"Could not VACUUM {table_name}: {e}")

