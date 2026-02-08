"""
Schema evolution policy validator.

This module enforces schema evolution policies across the data lakehouse:
- Bronze: permissive (accepts any schema)
- Silver: fixed schema (fails on breaking changes)
- Gold: contract schema (enforces JSON contracts)
"""

import json
import logging
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class SchemaValidator:
    """Validates schemas against evolution policies."""

    def __init__(self, spark: SparkSession, contracts_dir: str = "config/contracts"):
        self.spark = spark
        self.contracts_dir = Path(contracts_dir)

    def validate_bronze_schema(self, df: DataFrame, table_name: str) -> dict[str, Any]:
        """
        Bronze layer validation - permissive.

        Args:
            df: DataFrame to validate
            table_name: Name of the table

        Returns:
            Validation results
        """
        logger.info(f"Bronze validation for {table_name}: permissive mode")

        return {
            "layer": "bronze",
            "table": table_name,
            "valid": True,
            "message": "Bronze layer accepts any schema",
            "schema_info": {"column_count": len(df.columns), "columns": df.columns},
        }

    def validate_silver_schema(
        self, df: DataFrame, table_name: str, expected_schema: StructType | None = None
    ) -> dict[str, Any]:
        """
        Silver layer validation - fixed schema.

        Args:
            df: DataFrame to validate
            table_name: Name of the table
            expected_schema: Expected schema (if None, will be inferred from existing table)

        Returns:
            Validation results
        """
        logger.info(f"Silver validation for {table_name}: fixed schema mode")

        if expected_schema is None:
            # Try to infer from existing table
            try:
                existing_df = self.spark.read.format("delta").load(
                    f"data/lakehouse/silver/{table_name}"
                )
                expected_schema = existing_df.schema
            except Exception:
                # New table, accept any schema
                return {
                    "layer": "silver",
                    "table": table_name,
                    "valid": True,
                    "message": "New silver table - accepting schema",
                    "schema_info": {"column_count": len(df.columns), "columns": df.columns},
                }

        # Compare schemas
        actual_schema = df.schema
        differences = self._compare_schemas(expected_schema, actual_schema)

        if differences["breaking_changes"]:
            return {
                "layer": "silver",
                "table": table_name,
                "valid": False,
                "message": f"Breaking schema changes detected: {differences['breaking_changes']}",
                "differences": differences,
            }

        return {
            "layer": "silver",
            "table": table_name,
            "valid": True,
            "message": "Schema validation passed",
            "differences": differences,
        }

    def validate_gold_schema(self, df: DataFrame, table_name: str) -> dict[str, Any]:
        """
        Gold layer validation - contract schema.

        Args:
            df: DataFrame to validate
            table_name: Name of the table

        Returns:
            Validation results
        """
        logger.info(f"Gold validation for {table_name}: contract schema mode")

        # Load contract
        contract_path = self.contracts_dir / f"{table_name}.json"
        if not contract_path.exists():
            return {
                "layer": "gold",
                "table": table_name,
                "valid": False,
                "message": f"No contract found for {table_name}",
                "contract_path": str(contract_path),
            }

        try:
            with open(contract_path) as f:
                contract = json.load(f)
        except Exception as e:
            return {
                "layer": "gold",
                "table": table_name,
                "valid": False,
                "message": f"Failed to load contract: {e}",
                "contract_path": str(contract_path),
            }

        # Validate against contract
        validation_result = self._validate_against_contract(df, contract)

        return {
            "layer": "gold",
            "table": table_name,
            "valid": validation_result["valid"],
            "message": validation_result["message"],
            "contract": contract,
            "validation_details": validation_result,
        }

    def _compare_schemas(self, expected: StructType, actual: StructType) -> dict[str, Any]:
        """Compare two schemas and identify differences."""
        expected_fields = {f.name: f for f in expected.fields}
        actual_fields = {f.name: f for f in actual.fields}

        missing_columns = set(expected_fields.keys()) - set(actual_fields.keys())
        new_columns = set(actual_fields.keys()) - set(expected_fields.keys())

        type_changes = []
        for col in set(expected_fields.keys()) & set(actual_fields.keys()):
            if expected_fields[col].dataType != actual_fields[col].dataType:
                type_changes.append(
                    {
                        "column": col,
                        "expected": str(expected_fields[col].dataType),
                        "actual": str(actual_fields[col].dataType),
                    }
                )

        nullable_changes = []
        for col in set(expected_fields.keys()) & set(actual_fields.keys()):
            if expected_fields[col].nullable != actual_fields[col].nullable:
                nullable_changes.append(
                    {
                        "column": col,
                        "expected_nullable": expected_fields[col].nullable,
                        "actual_nullable": actual_fields[col].nullable,
                    }
                )

        # Breaking changes: missing required columns, type changes, nullable changes
        breaking_changes = []
        if missing_columns:
            breaking_changes.append(f"Missing columns: {missing_columns}")
        if type_changes:
            breaking_changes.append(f"Type changes: {type_changes}")
        if nullable_changes:
            breaking_changes.append(f"Nullable changes: {nullable_changes}")

        return {
            "missing_columns": list(missing_columns),
            "new_columns": list(new_columns),
            "type_changes": type_changes,
            "nullable_changes": nullable_changes,
            "breaking_changes": breaking_changes,
            "non_breaking_changes": list(new_columns),
        }

    def _validate_against_contract(self, df: DataFrame, contract: dict[str, Any]) -> dict[str, Any]:
        """Validate DataFrame against JSON schema contract."""
        schema = contract.get("schema", {})
        properties = schema.get("properties", {})
        required = schema.get("required", [])

        # Check required columns
        missing_required = [col for col in required if col not in df.columns]
        if missing_required:
            return {"valid": False, "message": f"Missing required columns: {missing_required}"}

        # Check for additional properties
        if not schema.get("additionalProperties", True):
            extra_columns = [col for col in df.columns if col not in properties]
            if extra_columns:
                return {"valid": False, "message": f"Extra columns not allowed: {extra_columns}"}

        # Basic type validation (simplified)
        type_violations = []
        for col, prop in properties.items():
            if col in df.columns:
                # This is a simplified check - in production you'd want more sophisticated type validation
                if prop.get("nullable") is False:
                    null_count = df.filter(df[col].isNull()).count()
                    if null_count > 0:
                        type_violations.append(
                            f"Column {col} has {null_count} null values but is not nullable"
                        )

        if type_violations:
            return {"valid": False, "message": f"Type violations: {type_violations}"}

        return {"valid": True, "message": "Contract validation passed"}

    def validate_table(self, df: DataFrame, table_name: str, layer: str) -> dict[str, Any]:
        """
        Validate table schema based on layer policy.

        Args:
            df: DataFrame to validate
            table_name: Name of the table
            layer: Data layer (bronze, silver, gold)

        Returns:
            Validation results
        """
        if layer.lower() == "bronze":
            return self.validate_bronze_schema(df, table_name)
        elif layer.lower() == "silver":
            return self.validate_silver_schema(df, table_name)
        elif layer.lower() == "gold":
            return self.validate_gold_schema(df, table_name)
        else:
            raise ValueError(f"Unknown layer: {layer}. Must be bronze, silver, or gold.")


def validate_schema_evolution(
    df: DataFrame, table_name: str, layer: str, spark: SparkSession
) -> dict[str, Any]:
    """
    Convenience function for schema validation.

    Args:
        df: DataFrame to validate
        table_name: Name of the table
        layer: Data layer (bronze, silver, gold)
        spark: SparkSession instance

    Returns:
        Validation results
    """
    validator = SchemaValidator(spark)
    return validator.validate_table(df, table_name, layer)
