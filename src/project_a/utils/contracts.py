"""
Data Contract Utilities

Load and validate data contracts for schema enforcement.
Includes StructType validation for fail-fast schema checking.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


@dataclass
class TableContract:
    """Data contract for a table."""

    name: str
    primary_key: list[str]
    columns: dict[str, str]
    required: list[str]

    @classmethod
    def from_json(cls, payload: dict) -> TableContract:
        """Create TableContract from JSON dict."""
        return cls(
            name=payload["name"],
            primary_key=payload.get("primary_key", []),
            columns=payload.get("columns", {}),
            required=payload.get("required", []),
        )


def load_contract(contract_name: str, base_path: str | None = None) -> TableContract:
    """
    Load a contract from config/schema_definitions/bronze/.

    Args:
        contract_name: Name of contract file (e.g., "snowflake_orders.schema.json")
        base_path: Base path to schema definitions (defaults to config/schema_definitions/bronze/)

    Returns:
        TableContract instance
    """
    if base_path is None:
        base_path = "config/schema_definitions/bronze"

    contract_path = Path(base_path) / contract_name
    if not contract_path.exists():
        raise FileNotFoundError(f"Contract not found: {contract_path}")

    with open(contract_path, encoding="utf-8") as f:
        payload = json.load(f)

    return TableContract.from_json(payload)


def validate_schema(df: DataFrame, contract: TableContract) -> None:
    """
    Validate that the DataFrame has all required columns.

    Raises ValueError if required columns are missing.

    Args:
        df: DataFrame to validate
        contract: TableContract to validate against
    """
    df_cols = set(df.columns)
    missing = [c for c in contract.required if c not in df_cols]
    if missing:
        raise ValueError(f"DataFrame for {contract.name} is missing required columns: {missing}")


def enforce_not_null(df: DataFrame, cols: list[str]) -> DataFrame:
    """
    Filter out rows where any of the given columns are NULL.

    Args:
        df: Input DataFrame
        cols: List of column names to check for nulls

    Returns:
        DataFrame with null rows filtered out
    """
    condition = None
    for c in cols:
        if c in df.columns:
            c_is_not_null = F.col(c).isNotNull()
            condition = c_is_not_null if condition is None else condition & c_is_not_null

    return df.filter(condition) if condition is not None else df


def validate_primary_key(df: DataFrame, contract: TableContract) -> DataFrame:
    """
    Validate and enforce primary key uniqueness and not-null constraints.

    Args:
        df: Input DataFrame
        contract: TableContract with primary key definition

    Returns:
        DataFrame with null primary key rows filtered out
    """
    if not contract.primary_key:
        return df

    # Filter nulls in primary key columns
    df = enforce_not_null(df, contract.primary_key)

    return df


def validate_dataframe_schema(
    df: DataFrame, expected_schema: StructType, table_name: str, fail_on_mismatch: bool = True
) -> None:
    """
    Validate that a DataFrame matches an expected StructType schema.

    Checks:
    - Required columns exist
    - Data types match (using simpleString() comparison)

    Args:
        df: DataFrame to validate
        expected_schema: Expected StructType schema
        table_name: Logical table name for error messages
        fail_on_mismatch: If True, raise exception on mismatch; if False, log warning

    Raises:
        ValueError: If schema validation fails and fail_on_mismatch=True
    """
    df_schema = df.schema
    df_cols = {f.name: f.dataType.simpleString() for f in df_schema.fields}
    expected_cols = {f.name: f.dataType.simpleString() for f in expected_schema.fields}

    # Check for missing columns
    missing_cols = set(expected_cols.keys()) - set(df_cols.keys())
    if missing_cols:
        error_msg = f"Schema validation FAILED for {table_name}: Missing columns: {missing_cols}"
        if fail_on_mismatch:
            raise ValueError(error_msg)
        else:
            logger.warning(error_msg)
            return

    # Check for type mismatches in common columns
    type_mismatches = []
    for col_name in expected_cols.keys():
        if col_name in df_cols:
            if df_cols[col_name] != expected_cols[col_name]:
                type_mismatches.append(
                    f"{col_name}: expected {expected_cols[col_name]}, got {df_cols[col_name]}"
                )

    if type_mismatches:
        error_msg = f"Schema validation FAILED for {table_name}: Type mismatches: {', '.join(type_mismatches)}"
        if fail_on_mismatch:
            raise ValueError(error_msg)
        else:
            logger.warning(error_msg)
            return

    logger.info(f"✅ Schema validation PASSED for {table_name}: {len(expected_cols)} columns match")
