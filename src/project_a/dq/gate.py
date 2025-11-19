"""
Data Quality Gate

Reusable DQ gate to enforce key checks (not null, uniqueness, ranges) and write results.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Optional, Dict, List


@dataclass
class DQCheckResult:
    """Result of a DQ check."""

    table_name: str
    total_rows: int
    null_violations: Dict[str, int]
    passed: bool


def run_not_null_checks(df: DataFrame, table_name: str, cols: List[str]) -> DQCheckResult:
    """
    Run not-null checks on specified columns.

    Args:
        df: DataFrame to check
        table_name: Name of the table
        cols: List of column names to check

    Returns:
        DQCheckResult with violation counts
    """
    total_rows = df.count()
    null_violations: Dict[str, int] = {}

    for c in cols:
        if c in df.columns:
            cnt = df.filter(F.col(c).isNull()).count()
            null_violations[c] = cnt

    passed = all(v == 0 for v in null_violations.values())

    return DQCheckResult(
        table_name=table_name,
        total_rows=total_rows,
        null_violations=null_violations,
        passed=passed,
    )


def run_uniqueness_check(df: DataFrame, table_name: str, pk_cols: List[str]) -> Dict[str, int]:
    """
    Check uniqueness of primary key columns.

    Args:
        df: DataFrame to check
        table_name: Name of the table
        pk_cols: List of primary key column names

    Returns:
        Dictionary with duplicate counts
    """
    if not pk_cols:
        return {}

    # Count total rows
    total_rows = df.count()

    # Count distinct rows by primary key
    distinct_rows = df.select(pk_cols).distinct().count()

    duplicates = total_rows - distinct_rows

    return {"total_rows": total_rows, "distinct_rows": distinct_rows, "duplicates": duplicates}


def run_range_check(
    df: DataFrame, col_name: str, min_val: Optional[float] = None, max_val: Optional[float] = None
) -> int:
    """
    Check if numeric column values are within range.

    Args:
        df: DataFrame to check
        col_name: Column name to check
        min_val: Minimum allowed value (inclusive)
        max_val: Maximum allowed value (inclusive)

    Returns:
        Number of violations
    """
    if col_name not in df.columns:
        return 0

    condition = None
    if min_val is not None:
        condition = F.col(col_name) < min_val
    if max_val is not None:
        max_condition = F.col(col_name) > max_val
        condition = max_condition if condition is None else condition | max_condition

    if condition is None:
        return 0

    violations = df.filter(condition).count()
    return violations


def write_dq_result(spark: SparkSession, result: DQCheckResult, output_path: str) -> None:
    """
    Write DQ result to JSON output path.

    Args:
        spark: SparkSession
        result: DQCheckResult to write
        output_path: Output path (S3 or local)
    """
    data = [
        {
            "table_name": result.table_name,
            "total_rows": result.total_rows,
            "null_violations": result.null_violations,
            "passed": result.passed,
            "check_timestamp": datetime.utcnow().isoformat() + "Z",
        }
    ]
    df = spark.createDataFrame(data)
    df.write.mode("overwrite").json(output_path)


def run_dq_gate(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    primary_key: List[str],
    required_cols: List[str],
    output_path: Optional[str] = None,
    check_uniqueness: bool = True,
    range_checks: Optional[Dict[str, Dict[str, float]]] = None,
) -> DQCheckResult:
    """
    Run complete DQ gate with all checks.

    Args:
        spark: SparkSession
        df: DataFrame to check
        table_name: Name of the table
        primary_key: List of primary key column names
        required_cols: List of required (not-null) column names
        output_path: Optional path to write DQ results
        check_uniqueness: Whether to check primary key uniqueness
        range_checks: Optional dict of {col_name: {"min": val, "max": val}}

    Returns:
        DQCheckResult

    Raises:
        ValueError: If DQ checks fail
    """
    # Run not-null checks
    result = run_not_null_checks(df, table_name, required_cols)

    # Check uniqueness if requested
    if check_uniqueness and primary_key:
        uniqueness = run_uniqueness_check(df, table_name, primary_key)
        if uniqueness.get("duplicates", 0) > 0:
            result.passed = False
            result.null_violations["_duplicates"] = uniqueness["duplicates"]

    # Run range checks if provided
    if range_checks:
        for col_name, ranges in range_checks.items():
            violations = run_range_check(df, col_name, ranges.get("min"), ranges.get("max"))
            if violations > 0:
                result.passed = False
                result.null_violations[f"{col_name}_range_violations"] = violations

    # Write results if path provided
    if output_path:
        write_dq_result(spark, result, output_path)

    # Raise exception if failed
    if not result.passed:
        error_msg = f"DQ Gate FAILED for {table_name}: {result.null_violations}"
        raise ValueError(error_msg)

    return result
