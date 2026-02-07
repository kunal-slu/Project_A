"""
Unified data writers for Silver and Gold layers.

Handles writing to:
- Delta tables (preferred)
- Parquet (fallback for local)
"""

import logging
from typing import Optional, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from project_a.utils.contracts import validate_dataframe_schema

logger = logging.getLogger(__name__)


def validate_schema_before_write(
    df: DataFrame,
    expected_schema: StructType,
    table_name: str,
    fail_on_mismatch: bool = True,
) -> None:
    """
    Validate DataFrame schema before writing.
    
    Args:
        df: DataFrame to validate
        expected_schema: Expected schema
        table_name: Table name for error messages
        fail_on_mismatch: If True, raise ValueError on mismatch
    """
    validate_dataframe_schema(df, expected_schema, table_name, fail_on_mismatch)


def write_silver_table(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    table_name: str,
    partition_cols: Optional[List[str]] = None,
    expected_schema: Optional[StructType] = None,
    mode: str = "overwrite",
) -> None:
    """
    Write DataFrame to Silver layer (Delta or Parquet).
    
    Args:
        spark: SparkSession
        df: DataFrame to write
        path: Output path
        table_name: Table name for logging
        partition_cols: Optional partition columns
        expected_schema: Optional schema to validate before write
        mode: Write mode (overwrite, append, errorifexists)
    """
    # Validate schema if provided
    if expected_schema:
        validate_schema_before_write(df, expected_schema, table_name, fail_on_mismatch=True)
    
    # Check if Delta Lake is available
    try:
        spark.conf.get("spark.sql.extensions")
        has_delta = "io.delta.sql.DeltaSparkSessionExtension" in spark.conf.get("spark.sql.extensions", "")
    except Exception:
        has_delta = False
    
    writer = df.write.mode(mode)
    
    if partition_cols:
        # Check that partition columns exist
        missing_cols = [c for c in partition_cols if c not in df.columns]
        if missing_cols:
            logger.warning(f"⚠️ Partition columns missing: {missing_cols}, skipping partitionBy")
        else:
            writer = writer.partitionBy(*partition_cols)
    
    if has_delta:
        writer.format("delta").save(path)
        logger.info(f"✅ Wrote {table_name} to Delta: {path}")
    else:
        writer.format("parquet").save(path)
        logger.info(f"✅ Wrote {table_name} to Parquet: {path}")


def write_gold_table(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    table_name: str,
    partition_cols: Optional[List[str]] = None,
    expected_schema: Optional[StructType] = None,
    mode: str = "overwrite",
) -> None:
    """
    Write DataFrame to Gold layer (Delta or Parquet).
    
    Same as write_silver_table but with Gold-specific logging.
    """
    write_silver_table(spark, df, path, table_name, partition_cols, expected_schema, mode)

