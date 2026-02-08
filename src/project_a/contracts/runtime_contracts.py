"""Runtime data-contract loading and fail-fast validation helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def load_table_contracts(path: str) -> dict[str, dict[str, Any]]:
    """Load table contracts from YAML."""
    contract_path = Path(path)
    if not contract_path.exists():
        raise FileNotFoundError(f"Contract file not found: {path}")

    with open(contract_path, encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    tables = payload.get("tables", {})
    if not isinstance(tables, dict):
        raise ValueError(f"Contract file malformed: expected 'tables' mapping in {path}")
    return tables


def _validate_required_columns(df: DataFrame, table_name: str, required_columns: list[str]) -> None:
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(f"{table_name}: Missing required columns: {missing}")


def _validate_required_non_null(df: DataFrame, table_name: str, required_columns: list[str]) -> None:
    violations: dict[str, int] = {}
    for column in required_columns:
        count = df.filter(F.col(column).isNull()).count()
        if count > 0:
            violations[column] = count
    if violations:
        raise ValueError(f"{table_name}: Null violations in required columns: {violations}")


def _validate_primary_key(df: DataFrame, table_name: str, primary_key: list[str]) -> None:
    if not primary_key:
        return
    null_expr = None
    for column in primary_key:
        expr = F.col(column).isNull()
        null_expr = expr if null_expr is None else (null_expr | expr)

    null_pk_count = df.filter(null_expr).count()
    if null_pk_count > 0:
        raise ValueError(f"{table_name}: Found {null_pk_count} null primary keys for {primary_key}")

    duplicate_count = (
        df.groupBy(*primary_key).count().filter(F.col("count") > F.lit(1)).count()
    )
    if duplicate_count > 0:
        raise ValueError(
            f"{table_name}: Found {duplicate_count} duplicate primary keys for {primary_key}"
        )


def _validate_ranges(df: DataFrame, table_name: str, ranges: list[dict[str, Any]]) -> None:
    for rule in ranges:
        column = rule.get("column")
        if not column or column not in df.columns:
            continue
        min_value = rule.get("min")
        max_value = rule.get("max")

        condition = None
        if min_value is not None:
            condition = F.col(column) < F.lit(min_value)
        if max_value is not None:
            max_condition = F.col(column) > F.lit(max_value)
            condition = max_condition if condition is None else (condition | max_condition)

        if condition is None:
            continue

        violations = df.filter(condition).count()
        if violations > 0:
            raise ValueError(
                f"{table_name}: Range check failed for '{column}', violations={violations}"
            )


def validate_contract(
    df: DataFrame,
    table_name: str,
    contract: dict[str, Any],
    parent_frames: dict[str, DataFrame] | None = None,
) -> None:
    """Fail-fast validation for a table contract."""
    required_columns = contract.get("required_columns", [])
    primary_key = contract.get("primary_key", [])
    ranges = contract.get("ranges", [])
    relationships = contract.get("relationships", [])

    _validate_required_columns(df, table_name, required_columns)
    _validate_required_non_null(df, table_name, required_columns)
    _validate_primary_key(df, table_name, primary_key)
    _validate_ranges(df, table_name, ranges)

    parent_frames = parent_frames or {}
    for relationship in relationships:
        column = relationship.get("column")
        parent_table = relationship.get("parent_table")
        parent_column = relationship.get("parent_column")
        if not column or not parent_table or not parent_column:
            continue
        if column not in df.columns:
            raise ValueError(f"{table_name}: Relationship column missing: {column}")
        if parent_table not in parent_frames:
            raise ValueError(
                f"{table_name}: Parent DataFrame for relationship check not provided: {parent_table}"
            )
        parent_df = parent_frames[parent_table]
        orphan_count = df.join(
            parent_df.select(parent_column).distinct(),
            df[column] == parent_df[parent_column],
            "left_anti",
        ).count()
        if orphan_count > 0:
            raise ValueError(
                f"{table_name}: Referential integrity failed for {column} -> "
                f"{parent_table}.{parent_column}, orphan_rows={orphan_count}"
            )
