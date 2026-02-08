"""
IO utilities for Delta Lake operations with retry logic and idempotency.
"""

import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """
    Read Delta table with error handling and retry logic.

    Args:
        spark: Spark session
        path: Path to Delta table

    Returns:
        DataFrame from Delta table

    Raises:
        Exception: If read fails after retries
    """
    try:
        logger.info(f"Reading Delta table from: {path}")
        df = spark.read.format("delta").load(path)
        logger.info(f"Successfully read Delta table: {path}")
        return df
    except Exception as e:
        logger.warning(f"Delta read failed, trying Parquet: {e}")
        try:
            df = spark.read.format("parquet").load(path)
            logger.info(f"Successfully read Parquet table: {path}")
            return df
        except Exception as e2:
            logger.error(f"Failed to read table {path}: {e2}")
            raise


def write_delta(
    df: DataFrame, path: str, mode: str = "overwrite", partitionBy: list[str] | None = None
) -> None:
    """
    Write DataFrame to Delta table with idempotency and retry logic.

    Args:
        df: DataFrame to write
        path: Output path
        mode: Write mode (overwrite, append, etc.)
        partitionBy: Columns to partition by

    Raises:
        Exception: If write fails after retries
    """
    try:
        # Normalize path for local filesystem
        normalized_path = _normalize_path(path)

        # Ensure parent directories exist for local paths
        if normalized_path.startswith("file://"):
            parent_dir = Path(normalized_path[7:]).parent
            parent_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Writing DataFrame to Delta table: {normalized_path} (mode: {mode})")

        writer = df.write.format("delta").mode(mode)

        if partitionBy:
            writer = writer.partitionBy(*partitionBy)
            logger.info(f"Partitioning by: {partitionBy}")

        writer.save(normalized_path)
        logger.info(f"Successfully wrote Delta table: {normalized_path}")

    except Exception as e:
        logger.warning(f"Delta write failed, trying Parquet: {e}")
        try:
            # Fallback to Parquet
            writer = df.write.format("parquet").mode(mode)

            if partitionBy:
                writer = writer.partitionBy(*partitionBy)

            writer.save(normalized_path)
            logger.info(f"Successfully wrote Parquet table: {normalized_path}")

        except Exception as e2:
            logger.error(f"Failed to write table {normalized_path}: {e2}")
            raise


def _normalize_path(path: str) -> str:
    """
    Normalize file path for Spark.

    Args:
        path: Input path

    Returns:
        Normalized path
    """
    if path.startswith("file://./"):
        # Convert relative path to absolute
        abs_path = os.path.abspath(path[7:])  # Remove "file://" prefix
        return f"file://{abs_path}"
    elif path.startswith("file://"):
        return path
    else:
        # Assume local path
        abs_path = os.path.abspath(path)
        return f"file://{abs_path}"


def ensure_directory_exists(path: str) -> None:
    """
    Ensure directory exists for local file paths.

    Args:
        path: File path
    """
    if path.startswith("file://"):
        dir_path = Path(path[7:])
        dir_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Ensured directory exists: {dir_path}")
