"""
Unified data readers for Bronze layer.

Handles reading from:
- CSV files
- JSON files
- Delta tables
- Parquet files
- Kafka streams
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


def read_bronze_table(
    spark: SparkSession,
    path: str,
    schema: StructType,
    format: str = "auto",
) -> DataFrame:
    """
    Read a bronze table with automatic format detection.

    Tries: Delta -> Parquet -> CSV -> JSON

    Args:
        spark: SparkSession
        path: Path to data (file:// or s3://)
        schema: Expected schema
        format: Force format (delta, parquet, csv, json) or "auto" for detection

    Returns:
        DataFrame with schema applied
    """
    if format == "auto":
        # Try Delta first
        try:
            df = spark.read.format("delta").schema(schema).load(path)
            # Check if table has data
            try:
                _ = df.first()
                logger.info(f"✅ Read from Delta: {path}")
                return df
            except Exception:
                logger.debug(f"Delta table empty or inaccessible: {path}")
        except Exception as e:
            logger.debug(f"Delta read failed: {e}")

        # Try Parquet
        try:
            df = spark.read.format("parquet").schema(schema).load(path)
            try:
                _ = df.first()
                logger.info(f"✅ Read from Parquet: {path}")
                return df
            except Exception:
                logger.debug(f"Parquet table empty: {path}")
        except Exception as e:
            logger.debug(f"Parquet read failed: {e}")

        # Try CSV
        try:
            df = read_csv_with_schema(spark, path, schema)
            logger.info(f"✅ Read from CSV: {path}")
            return df
        except Exception as e:
            logger.debug(f"CSV read failed: {e}")

        # Try JSON
        try:
            df = read_json_with_schema(spark, path, schema)
            logger.info(f"✅ Read from JSON: {path}")
            return df
        except Exception as e:
            logger.debug(f"JSON read failed: {e}")

        # All formats failed - return empty DataFrame with schema
        logger.warning(f"⚠️ All read attempts failed for {path}, returning empty DataFrame")
        return spark.createDataFrame([], schema)

    # Force format
    if format == "delta":
        return read_delta_table(spark, path, schema)
    elif format == "parquet":
        return spark.read.format("parquet").schema(schema).load(path)
    elif format == "csv":
        return read_csv_with_schema(spark, path, schema)
    elif format == "json":
        return read_json_with_schema(spark, path, schema)
    else:
        raise ValueError(f"Unsupported format: {format}")


def read_csv_with_schema(
    spark: SparkSession,
    path: str,
    schema: StructType,
    header: bool = True,
    infer_schema: bool = False,
) -> DataFrame:
    """Read CSV with explicit schema."""
    df = (
        spark.read.option("header", str(header).lower())
        .option("inferSchema", str(infer_schema).lower())
        .schema(schema)
        .csv(path)
    )
    return df


def read_json_with_schema(
    spark: SparkSession,
    path: str,
    schema: StructType,
    multi_line: bool = False,
) -> DataFrame:
    """Read JSON with explicit schema."""
    df = spark.read.option("multiLine", str(multi_line).lower()).schema(schema).json(path)
    return df


def read_delta_table(
    spark: SparkSession,
    path: str,
    schema: StructType | None = None,
) -> DataFrame:
    """Read Delta table."""
    if schema:
        df = spark.read.format("delta").schema(schema).load(path)
    else:
        df = spark.read.format("delta").load(path)
    return df
