"""
Schema Validator - Enforce Expected Schemas

Provides schema validation utilities to prevent schema drift and ensure
data contracts are maintained across ETL pipeline layers.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType,
)

from project_a.schema.registry import SCHEMAS as SCHEMA_REGISTRY

logger = logging.getLogger(__name__)


class SchemaValidator:
    """
    Validates DataFrames against expected schemas.

    Provides:
    - Column existence checks
    - Type enforcement
    - Nullability validation
    - Schema drift detection
    """

    # Single source-of-truth schema registry.
    SCHEMAS = SCHEMA_REGISTRY

    def __init__(self, spark=None):
        # Optional spark session for backward compatibility with older tests/utilities.
        self.spark = spark

    @staticmethod
    def validate_columns(
        df: DataFrame, expected_columns: list[str], table_name: str = "table"
    ) -> None:
        """
        Validate that DataFrame contains expected columns.

        Args:
            df: DataFrame to validate
            expected_columns: List of required column names
            table_name: Name for logging

        Raises:
            ValueError: If columns are missing
        """
        actual_columns = set(df.columns)
        expected_set = set(expected_columns)

        missing = expected_set - actual_columns
        if missing:
            raise ValueError(f"{table_name}: Missing required columns: {sorted(missing)}")

        extra = actual_columns - expected_set
        if extra:
            logger.warning(f"{table_name}: Found extra columns (not in contract): {sorted(extra)}")

    @staticmethod
    def validate_schema(
        df: DataFrame, expected_schema: StructType, table_name: str = "table", strict: bool = False
    ) -> None:
        """
        Validate DataFrame schema matches expected schema.

        Args:
            df: DataFrame to validate
            expected_schema: Expected StructType
            table_name: Name for logging
            strict: If True, fail on any mismatch; if False, warn only

        Raises:
            ValueError: If schema validation fails (strict=True)
        """
        actual_schema = df.schema
        expected_fields = {f.name: f for f in expected_schema.fields}
        actual_fields = {f.name: f for f in actual_schema.fields}

        issues = []

        # Check for missing columns
        missing = set(expected_fields.keys()) - set(actual_fields.keys())
        if missing:
            issues.append(f"Missing columns: {sorted(missing)}")

        # Check column types
        for col_name in set(expected_fields.keys()) & set(actual_fields.keys()):
            expected_field = expected_fields[col_name]
            actual_field = actual_fields[col_name]

            if not isinstance(actual_field.dataType, type(expected_field.dataType)):
                issues.append(
                    f"Column '{col_name}': Expected {expected_field.dataType}, "
                    f"got {actual_field.dataType}"
                )

            # Check nullability
            if not expected_field.nullable and actual_field.nullable:
                issues.append(f"Column '{col_name}': Expected non-nullable, got nullable")

        if issues:
            error_msg = f"{table_name} schema validation failed:\n" + "\n".join(issues)
            if strict:
                raise ValueError(error_msg)
            else:
                logger.warning(error_msg)

    @staticmethod
    def validate_primary_key(df: DataFrame, pk_column: str, table_name: str = "table") -> None:
        """
        Validate primary key constraints.

        Args:
            df: DataFrame to validate
            pk_column: Primary key column name
            table_name: Name for logging

        Raises:
            ValueError: If PK validation fails
        """
        from pyspark.sql.functions import col

        # Check for nulls
        null_count = df.filter(col(pk_column).isNull()).count()
        if null_count > 0:
            raise ValueError(
                f"{table_name}: Found {null_count} null values in primary key '{pk_column}'"
            )

        # Check for duplicates
        total_count = df.count()
        distinct_count = df.select(pk_column).distinct().count()

        if total_count != distinct_count:
            duplicate_count = total_count - distinct_count
            raise ValueError(
                f"{table_name}: Found {duplicate_count} duplicate values in "
                f"primary key '{pk_column}'"
            )

    @staticmethod
    def get_schema(table_name: str) -> StructType | None:
        """
        Get standard schema for a table.

        Args:
            table_name: Table name (e.g., 'orders_silver')

        Returns:
            StructType if defined, else None
        """
        return SchemaValidator.SCHEMAS.get(table_name)

    def validate_bronze_schema(self, df: DataFrame, table_name: str) -> bool:
        """Bronze is permissive; always returns True unless df is missing."""
        if df is None:
            logger.warning(f"{table_name}: Bronze schema validation skipped (no DataFrame)")
            return False
        return True

    def validate_silver_schema(self, df: DataFrame, table_name: str, existing_schema_path: str) -> bool:
        """
        Validate that silver schema is backward compatible with an existing schema.

        Rules:
        - All existing fields must remain with same type and nullability
        - New fields are allowed
        """
        try:
            from pyspark.sql.types import StructType
            import json

            with open(existing_schema_path) as f:
                existing_schema = StructType.fromJson(json.load(f))

            existing_fields = {f.name: f for f in existing_schema.fields}
            current_fields = {f.name: f for f in df.schema.fields}

            for name, field in existing_fields.items():
                if name not in current_fields:
                    logger.error(f"{table_name}: Missing existing field '{name}'")
                    return False
                current_field = current_fields[name]
                if type(current_field.dataType) is not type(field.dataType):
                    logger.error(
                        f"{table_name}: Type mismatch for '{name}': "
                        f"{current_field.dataType} vs {field.dataType}"
                    )
                    return False
                if field.nullable is False and current_field.nullable is True:
                    logger.error(f"{table_name}: Nullability relaxed for '{name}'")
                    return False
            return True
        except Exception as exc:
            logger.error(f"{table_name}: Failed silver schema validation: {exc}")
            return False

    def validate_gold_schema(self, df: DataFrame, table_name: str, contract_path: str) -> bool:
        """
        Validate gold schema against a contract file.
        """
        try:
            import json
            from pyspark.sql.types import (
                BooleanType,
                DateType,
                DecimalType,
                DoubleType,
                IntegerType,
                StringType,
                TimestampType,
            )

            with open(contract_path) as f:
                contract = json.load(f)

            type_map = {
                "string": StringType,
                "integer": IntegerType,
                "double": DoubleType,
                "date": DateType,
                "timestamp": TimestampType,
                "boolean": BooleanType,
                "decimal": DecimalType,
            }

            contract_fields = contract.get("fields", [])
            current_fields = {f.name: f for f in df.schema.fields}

            for field in contract_fields:
                name = field.get("name")
                type_name = field.get("type")
                nullable = field.get("nullable", True)

                if name not in current_fields:
                    logger.error(f"{table_name}: Missing required field '{name}'")
                    return False
                current_field = current_fields[name]

                expected_type = type_map.get(str(type_name).lower())
                if expected_type is None:
                    logger.error(f"{table_name}: Unsupported contract type '{type_name}'")
                    return False
                if not isinstance(current_field.dataType, expected_type):
                    logger.error(
                        f"{table_name}: Type mismatch for '{name}': "
                        f"{current_field.dataType} vs {type_name}"
                    )
                    return False
                if nullable is False and current_field.nullable is True:
                    logger.error(f"{table_name}: Nullability mismatch for '{name}'")
                    return False

            return True
        except Exception as exc:
            logger.error(f"{table_name}: Failed gold schema validation: {exc}")
            return False

    @staticmethod
    def apply_schema(df: DataFrame, table_name: str) -> DataFrame:
        """
        Apply and enforce standard schema on DataFrame.

        Args:
            df: DataFrame to apply schema to
            table_name: Table name (e.g., 'orders_silver')

        Returns:
            DataFrame with enforced schema

        Raises:
            ValueError: If schema not defined or cannot be applied
        """
        schema = SchemaValidator.get_schema(table_name)
        if schema is None:
            raise ValueError(f"No standard schema defined for '{table_name}'")

        # Select and cast columns according to schema
        from pyspark.sql.functions import col

        select_exprs = []
        for field in schema.fields:
            if field.name in df.columns:
                select_exprs.append(col(field.name).cast(field.dataType).alias(field.name))
            elif not field.nullable:
                raise ValueError(f"Required column '{field.name}' missing from DataFrame")
            else:
                # Add null column for optional missing fields
                from pyspark.sql.functions import lit

                select_exprs.append(lit(None).cast(field.dataType).alias(field.name))

        return df.select(*select_exprs)
