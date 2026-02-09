"""Runtime contract manager used by tests and local validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from project_a.contracts.runtime_contracts import load_table_contracts, validate_contract


@dataclass
class ContractResult:
    """Result payload for contract checks."""

    overall_valid: bool
    error: str | None = None


class DataContractManager:
    """Lightweight contract manager for runtime validation."""

    def __init__(self, spark: SparkSession, contract_path: str | None = None) -> None:
        self.spark = spark
        self.contract_path = contract_path or "config/contracts/silver_contracts.yaml"
        try:
            self.contracts = load_table_contracts(self.contract_path)
        except Exception:
            self.contracts = {}

    def _get_contract(self, table_name: str) -> dict[str, Any] | None:
        return self.contracts.get(table_name)

    def validate_contract(self, df: DataFrame, table_name: str) -> dict[str, Any]:
        contract = self._get_contract(table_name)
        if not contract:
            return {
                "overall_valid": False,
                "error": f"No contract defined for table '{table_name}'",
            }

        try:
            validate_contract(df, table_name, contract)
            return {"overall_valid": True}
        except Exception as exc:
            return {"overall_valid": False, "error": str(exc)}

    def detect_schema_drift(self, df: DataFrame, table_name: str) -> dict[str, Any]:
        contract = self._get_contract(table_name)
        if not contract:
            return {"drift_detected": True, "error": f"No contract for table '{table_name}'"}

        expected_cols = set(contract.get("required_columns", []))
        actual_cols = set(df.columns)
        missing = sorted(expected_cols - actual_cols)
        new_cols = sorted(actual_cols - expected_cols)
        return {
            "drift_detected": bool(missing or new_cols),
            "missing_columns": missing,
            "new_columns": new_cols,
        }
