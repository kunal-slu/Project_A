"""
Validation helpers for post-write dataset checks.

This module intentionally lives in the canonical package path and avoids
runtime imports from compatibility/legacy modules.
"""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class ValidateOutput:
    """Simple output validators used by tests and local runs."""

    @staticmethod
    def _require_directory(path: str) -> Path:
        target = Path(path)
        if not target.exists() or not target.is_dir():
            raise FileNotFoundError(f"Output directory not found: {path}")
        return target

    def validate_parquet(self, spark: SparkSession, path: str) -> None:
        """Validate that parquet output exists and has at least one row."""
        target = self._require_directory(path)
        has_parquet = any(target.glob("*.parquet")) or any(target.glob("**/*.parquet"))
        if not has_parquet:
            raise FileNotFoundError(f"No parquet files found in: {path}")

        row_count = spark.read.parquet(path).count()
        if row_count == 0:
            raise ValueError(f"Parquet output has zero rows: {path}")
        logger.info("Parquet validation passed at %s (%s rows)", path, row_count)

    def validate_avro(self, spark: SparkSession, path: str) -> None:
        """Validate that avro output exists and has at least one row."""
        self._require_directory(path)
        row_count = spark.read.format("avro").load(path).count()
        if row_count == 0:
            raise ValueError(f"Avro output has zero rows: {path}")
        logger.info("Avro validation passed at %s (%s rows)", path, row_count)

    def validate_json(self, spark: SparkSession, path: str) -> None:
        """Validate that json output exists and has at least one row."""
        self._require_directory(path)
        row_count = spark.read.json(path).count()
        if row_count == 0:
            raise ValueError(f"JSON output has zero rows: {path}")
        logger.info("JSON validation passed at %s (%s rows)", path, row_count)


__all__ = ["ValidateOutput"]
