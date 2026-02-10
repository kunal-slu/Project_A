"""Lightweight data-quality checks used by tests and local validation."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DQChecks:
    """Minimal DQ checks for not-null and uniqueness constraints."""

    def assert_non_null(self, df: DataFrame, column: str) -> None:
        """Raise ValueError if any nulls exist in the given column."""
        if column not in df.columns:
            raise ValueError(f"Column not found: {column}")
        null_count = df.filter(F.col(column).isNull()).count()
        if null_count > 0:
            raise ValueError(f"Null values detected in {column}: {null_count}")

    def assert_unique(self, df: DataFrame, column: str) -> None:
        """Raise ValueError if duplicates exist in the given column."""
        if column not in df.columns:
            raise ValueError(f"Column not found: {column}")
        dup_exists = df.groupBy(column).count().filter(F.col("count") > F.lit(1)).limit(1).count()
        if dup_exists > 0:
            raise ValueError(f"Duplicate values detected in {column}")
