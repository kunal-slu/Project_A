"""
Data Quality (DQ) utilities for fail-fast validation.

This module provides standardized data quality checks that can be used
across the pipeline to ensure data integrity and fail early on issues.
"""

import json
import logging
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


def require_not_null(
    df: DataFrame, columns: list[str], fail_on_violation: bool = True
) -> dict[str, Any]:
    """
    Check that specified columns contain no null values.

    Args:
        df: DataFrame to check
        columns: List of column names to validate
        fail_on_violation: Whether to raise exception on violation

    Returns:
        Dictionary with check results
    """
    results = {}

    for column in columns:
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")

        null_count = df.filter(F.col(column).isNull()).count()
        total_count = df.count()

        result = {
            "check": "not_null",
            "column": column,
            "null_count": null_count,
            "total_count": total_count,
            "null_percentage": (null_count / total_count * 100) if total_count > 0 else 0,
            "passed": null_count == 0,
        }

        results[column] = result

        if fail_on_violation and null_count > 0:
            raise ValueError(
                f"NOT NULL check failed for column '{column}': "
                f"{null_count} null values found ({result['null_percentage']:.2f}%)"
            )

    return results


def require_unique_keys(
    df: DataFrame, key_columns: list[str], fail_on_violation: bool = True
) -> dict[str, Any]:
    """
    Check that specified key columns form unique combinations.

    Args:
        df: DataFrame to check
        key_columns: List of columns that should form unique keys
        fail_on_violation: Whether to raise exception on violation

    Returns:
        Dictionary with check results
    """
    # Validate columns exist
    for column in key_columns:
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")

    # Count total rows
    total_count = df.count()

    # Count distinct combinations
    distinct_count = df.select(*key_columns).distinct().count()

    # Find duplicates
    duplicates = (
        df.groupBy(*key_columns).count().filter("count > 1").orderBy("count", ascending=False)
    )

    duplicate_count = duplicates.count()
    duplicate_rows = duplicates.limit(10).collect()  # Limit to avoid memory issues

    result = {
        "check": "unique_keys",
        "key_columns": key_columns,
        "total_count": total_count,
        "distinct_count": distinct_count,
        "duplicate_count": duplicate_count,
        "duplicate_combinations": duplicate_rows,
        "passed": duplicate_count == 0,
    }

    if fail_on_violation and duplicate_count > 0:
        raise ValueError(
            f"UNIQUE KEYS check failed for columns {key_columns}: "
            f"{duplicate_count} duplicate combinations found"
        )

    return result


def control_total(
    df: DataFrame,
    value_column: str,
    expected_total: float | None = None,
    tolerance_percentage: float = 0.01,  # 1% tolerance
    fail_on_violation: bool = True,
) -> dict[str, Any]:
    """
    Check control total for a numeric column.

    Args:
        df: DataFrame to check
        value_column: Column to sum
        expected_total: Expected total value (if None, just report actual total)
        tolerance_percentage: Acceptable deviation percentage
        fail_on_violation: Whether to raise exception on violation

    Returns:
        Dictionary with check results
    """
    if value_column not in df.columns:
        raise ValueError(f"Column '{value_column}' not found in DataFrame")

    # Calculate actual total
    actual_total = df.agg(F.sum(F.col(value_column))).limit(1).first()[0]
    actual_total = actual_total if actual_total is not None else 0.0

    result = {
        "check": "control_total",
        "column": value_column,
        "actual_total": actual_total,
        "expected_total": expected_total,
        "passed": True,
    }

    if expected_total is not None:
        # Calculate deviation
        deviation = abs(actual_total - expected_total)
        deviation_percentage = (deviation / expected_total * 100) if expected_total != 0 else 0

        result.update(
            {
                "deviation": deviation,
                "deviation_percentage": deviation_percentage,
                "tolerance_percentage": tolerance_percentage,
                "passed": deviation_percentage <= tolerance_percentage,
            }
        )

        if fail_on_violation and deviation_percentage > tolerance_percentage:
            raise ValueError(
                f"CONTROL TOTAL check failed for column '{value_column}': "
                f"Actual total {actual_total} differs from expected {expected_total} "
                f"by {deviation_percentage:.2f}% (tolerance: {tolerance_percentage}%)"
            )

    return result


def validate_schema(
    df: DataFrame, expected_schema: StructType, strict: bool = True, fail_on_violation: bool = True
) -> dict[str, Any]:
    """
    Validate DataFrame schema against expected schema.

    Args:
        df: DataFrame to validate
        expected_schema: Expected schema
        strict: Whether to require exact match (True) or subset (False)
        fail_on_violation: Whether to raise exception on violation

    Returns:
        Dictionary with validation results
    """
    actual_schema = df.schema
    actual_fields = {field.name for field in actual_schema.fields}
    expected_fields = {field.name for field in expected_schema.fields}

    missing_fields = expected_fields - actual_fields
    extra_fields = actual_fields - expected_fields

    result = {
        "check": "schema_validation",
        "missing_fields": list(missing_fields),
        "extra_fields": list(extra_fields),
        "passed": len(missing_fields) == 0 and (not strict or len(extra_fields) == 0),
    }

    if fail_on_violation and not result["passed"]:
        error_msg = "Schema validation failed:"
        if missing_fields:
            error_msg += f" Missing fields: {missing_fields}"
        if strict and extra_fields:
            error_msg += f" Extra fields: {extra_fields}"
        raise ValueError(error_msg)

    return result


def run_dq_checks(
    df: DataFrame, checks: list[dict[str, Any]], fail_fast: bool = True
) -> dict[str, Any]:
    """
    Run multiple data quality checks on a DataFrame.

    Args:
        df: DataFrame to check
        checks: List of check configurations
        fail_fast: Whether to stop on first failure

    Returns:
        Dictionary with all check results
    """
    results = {
        "timestamp": datetime.now().isoformat(),
        "checks": {},
        "overall_passed": True,
        "summary": {"total_checks": len(checks), "passed_checks": 0, "failed_checks": 0},
    }

    for i, check_config in enumerate(checks):
        check_type = check_config.get("type")
        check_name = check_config.get("name", f"check_{i}")

        try:
            if check_type == "not_null":
                check_result = require_not_null(
                    df, check_config["columns"], fail_on_violation=fail_fast
                )
            elif check_type == "unique_keys":
                check_result = require_unique_keys(
                    df, check_config["key_columns"], fail_on_violation=fail_fast
                )
            elif check_type == "control_total":
                check_result = control_total(
                    df,
                    check_config["value_column"],
                    expected_total=check_config.get("expected_total"),
                    tolerance_percentage=check_config.get("tolerance_percentage", 0.01),
                    fail_on_violation=fail_fast,
                )
            elif check_type == "schema":
                check_result = validate_schema(
                    df,
                    check_config["expected_schema"],
                    strict=check_config.get("strict", True),
                    fail_on_violation=fail_fast,
                )
            else:
                raise ValueError(f"Unknown check type: {check_type}")

            results["checks"][check_name] = check_result

            # Update summary
            if isinstance(check_result, dict) and check_result.get("passed", False):
                results["summary"]["passed_checks"] += 1
            else:
                results["summary"]["failed_checks"] += 1
                results["overall_passed"] = False

        except Exception as e:
            results["checks"][check_name] = {"error": str(e), "passed": False}
            results["summary"]["failed_checks"] += 1
            results["overall_passed"] = False

            if fail_fast:
                break

    return results


def generate_dq_report(results: dict[str, Any], output_path: str | None = None) -> str:
    """
    Generate a compact JSON DQ report.

    Args:
        results: DQ check results from run_dq_checks
        output_path: Optional path to save report

    Returns:
        JSON string of the report
    """
    report = {
        "dq_report": {
            "timestamp": results["timestamp"],
            "overall_passed": results["overall_passed"],
            "summary": results["summary"],
            "checks": {},
        }
    }

    # Add check details
    for check_name, check_result in results["checks"].items():
        if isinstance(check_result, dict):
            report["dq_report"]["checks"][check_name] = {
                "passed": check_result.get("passed", False),
                "check_type": check_result.get("check", "unknown"),
            }

            # Add relevant details based on check type
            if check_result.get("check") == "not_null":
                report["dq_report"]["checks"][check_name]["null_count"] = check_result.get(
                    "null_count", 0
                )
            elif check_result.get("check") == "unique_keys":
                report["dq_report"]["checks"][check_name]["duplicate_count"] = check_result.get(
                    "duplicate_count", 0
                )
            elif check_result.get("check") == "control_total":
                report["dq_report"]["checks"][check_name]["actual_total"] = check_result.get(
                    "actual_total", 0
                )
        else:
            report["dq_report"]["checks"][check_name] = {
                "passed": False,
                "error": str(check_result),
            }

    report_json = json.dumps(report, indent=2)

    if output_path:
        with open(output_path, "w") as f:
            f.write(report_json)
        logger.info(f"DQ report saved to {output_path}")

    return report_json
