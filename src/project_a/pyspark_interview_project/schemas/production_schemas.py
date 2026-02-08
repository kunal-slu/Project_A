"""
Explicit schemas for all data sources - NO schema inference in production.

This module defines all schemas explicitly to:
- Prevent schema drift issues
- Ensure data type consistency
- Enable schema version control
- Support schema drift detection
"""

import json
import logging
from pathlib import Path
from typing import Any

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)


# Bronze Layer Schemas
BRONZE_CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("email", StringType(), nullable=True),
        StructField("phone", StringType(), nullable=True),
        StructField("address", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("zip_code", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("created_date", TimestampType(), nullable=True),
        StructField("_source", StringType(), nullable=False),
        StructField("_extracted_at", TimestampType(), nullable=False),
        StructField("_proc_date", DateType(), nullable=False),
    ]
)

BRONZE_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("order_date", TimestampType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("total_amount", DoubleType(), nullable=False),
        StructField("currency", StringType(), nullable=True),
        StructField("payment_method", StringType(), nullable=True),
        StructField("shipping_address", StringType(), nullable=True),
        StructField("_source", StringType(), nullable=False),
        StructField("_extracted_at", TimestampType(), nullable=False),
        StructField("_proc_date", DateType(), nullable=False),
    ]
)

BRONZE_PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
        StructField("category", StringType(), nullable=True),
        StructField("subcategory", StringType(), nullable=True),
        StructField("price", DoubleType(), nullable=False),
        StructField("cost", DoubleType(), nullable=True),
        StructField("supplier_id", StringType(), nullable=True),
        StructField("in_stock", BooleanType(), nullable=True),
        StructField("_source", StringType(), nullable=False),
        StructField("_extracted_at", TimestampType(), nullable=False),
        StructField("_proc_date", DateType(), nullable=False),
    ]
)

# Silver Layer Schemas
SILVER_CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("email", StringType(), nullable=True),
        StructField("phone", StringType(), nullable=True),
        StructField("address", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("zip_code", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("customer_segment", StringType(), nullable=True),
        StructField("lifetime_value", DoubleType(), nullable=True),
        StructField("first_order_date", TimestampType(), nullable=True),
        StructField("last_order_date", TimestampType(), nullable=True),
        StructField("total_orders", IntegerType(), nullable=True),
        StructField("is_active", BooleanType(), nullable=True),
        StructField("created_date", TimestampType(), nullable=True),
        StructField("updated_date", TimestampType(), nullable=False),
        StructField("_source", StringType(), nullable=False),
        StructField("_proc_date", DateType(), nullable=False),
    ]
)

SILVER_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("order_date", TimestampType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("total_amount", DoubleType(), nullable=False),
        StructField("tax_amount", DoubleType(), nullable=True),
        StructField("shipping_amount", DoubleType(), nullable=True),
        StructField("discount_amount", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("payment_method", StringType(), nullable=True),
        StructField("is_returned", BooleanType(), nullable=True),
        StructField("days_to_fulfill", IntegerType(), nullable=True),
        StructField("updated_date", TimestampType(), nullable=False),
        StructField("_source", StringType(), nullable=False),
        StructField("_proc_date", DateType(), nullable=False),
    ]
)

# Gold Layer Schemas
GOLD_CUSTOMER_ANALYTICS_SCHEMA = StructType(
    [
        StructField("customer_segment", StringType(), nullable=False),
        StructField("total_customers", LongType(), nullable=False),
        StructField("total_revenue", DoubleType(), nullable=False),
        StructField("avg_lifetime_value", DoubleType(), nullable=False),
        StructField("avg_order_value", DoubleType(), nullable=False),
        StructField("total_orders", LongType(), nullable=False),
        StructField("active_customers", LongType(), nullable=False),
        StructField("churned_customers", LongType(), nullable=False),
        StructField("report_date", DateType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

GOLD_MONTHLY_REVENUE_SCHEMA = StructType(
    [
        StructField("year", IntegerType(), nullable=False),
        StructField("month", IntegerType(), nullable=False),
        StructField("total_revenue", DoubleType(), nullable=False),
        StructField("total_orders", LongType(), nullable=False),
        StructField("unique_customers", LongType(), nullable=False),
        StructField("avg_order_value", DoubleType(), nullable=False),
        StructField("new_customers", LongType(), nullable=True),
        StructField("returning_customers", LongType(), nullable=True),
        StructField("report_date", DateType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)


class SchemaRegistry:
    """
    Central schema registry for all data sources.

    Features:
    - Version-controlled schemas
    - Schema drift detection
    - Schema validation
    - Schema evolution tracking
    """

    def __init__(self, schema_dir: Path = None):
        """
        Initialize schema registry.

        Args:
            schema_dir: Directory to store schema versions (default: config/schemas)
        """
        self.schema_dir = schema_dir or Path("config/schemas")
        self.schemas = self._load_all_schemas()

    def _load_all_schemas(self) -> dict[str, StructType]:
        """Load all defined schemas."""
        return {
            # Bronze
            "bronze.customers": BRONZE_CUSTOMERS_SCHEMA,
            "bronze.orders": BRONZE_ORDERS_SCHEMA,
            "bronze.products": BRONZE_PRODUCTS_SCHEMA,
            # Silver
            "silver.customers": SILVER_CUSTOMERS_SCHEMA,
            "silver.orders": SILVER_ORDERS_SCHEMA,
            # Gold
            "gold.customer_analytics": GOLD_CUSTOMER_ANALYTICS_SCHEMA,
            "gold.monthly_revenue": GOLD_MONTHLY_REVENUE_SCHEMA,
        }

    def get_schema(self, table_name: str) -> StructType:
        """
        Get schema for a table.

        Args:
            table_name: Fully qualified table name (e.g., 'bronze.customers')

        Returns:
            StructType schema

        Raises:
            ValueError: If schema not found
        """
        schema = self.schemas.get(table_name)
        if not schema:
            raise ValueError(f"Schema not found for table: {table_name}")
        return schema

    def validate_schema(
        self, actual_schema: StructType, expected_table: str, allow_additional_columns: bool = False
    ) -> dict[str, Any]:
        """
        Validate actual schema against expected schema.

        Args:
            actual_schema: Actual DataFrame schema
            expected_table: Expected table name
            allow_additional_columns: Whether to allow extra columns

        Returns:
            Dictionary with validation results
        """
        expected_schema = self.get_schema(expected_table)

        issues = []

        # Check for missing columns
        expected_fields = {f.name: f for f in expected_schema.fields}
        actual_fields = {f.name: f for f in actual_schema.fields}

        for col_name, expected_field in expected_fields.items():
            if col_name not in actual_fields:
                issues.append(
                    {
                        "type": "MISSING_COLUMN",
                        "column": col_name,
                        "message": f"Required column '{col_name}' is missing",
                    }
                )
            else:
                actual_field = actual_fields[col_name]

                # Check data type
                if actual_field.dataType != expected_field.dataType:
                    issues.append(
                        {
                            "type": "TYPE_MISMATCH",
                            "column": col_name,
                            "expected_type": str(expected_field.dataType),
                            "actual_type": str(actual_field.dataType),
                            "message": (
                                f"Column '{col_name}' type mismatch: "
                                f"expected {expected_field.dataType}, "
                                f"got {actual_field.dataType}"
                            ),
                        }
                    )

                # Check nullability (warn only if stricter than expected)
                if not expected_field.nullable and actual_field.nullable:
                    issues.append(
                        {
                            "type": "NULLABILITY_MISMATCH",
                            "column": col_name,
                            "severity": "WARNING",
                            "message": (
                                f"Column '{col_name}' is nullable but expected non-nullable"
                            ),
                        }
                    )

        # Check for additional columns
        if not allow_additional_columns:
            for col_name in actual_fields.keys():
                if col_name not in expected_fields:
                    issues.append(
                        {
                            "type": "UNEXPECTED_COLUMN",
                            "column": col_name,
                            "message": f"Unexpected column '{col_name}' found",
                        }
                    )

        is_valid = len([i for i in issues if i.get("severity") != "WARNING"]) == 0

        result = {
            "valid": is_valid,
            "table": expected_table,
            "issues": issues,
            "issue_count": len(issues),
        }

        if not is_valid:
            logger.error(f"Schema validation failed for {expected_table}", extra=result)
        elif issues:
            logger.warning(f"Schema validation warnings for {expected_table}", extra=result)
        else:
            logger.info(f"Schema validation passed for {expected_table}")

        return result

    def save_schema_version(self, table_name: str, schema: StructType, version: str) -> None:
        """
        Save a schema version to disk.

        Args:
            table_name: Table name
            schema: Schema to save
            version: Version identifier
        """
        self.schema_dir.mkdir(parents=True, exist_ok=True)

        schema_file = self.schema_dir / f"{table_name}.{version}.json"

        schema_dict = {
            "table": table_name,
            "version": version,
            "fields": [
                {"name": f.name, "type": str(f.dataType), "nullable": f.nullable}
                for f in schema.fields
            ],
        }

        with open(schema_file, "w") as f:
            json.dump(schema_dict, f, indent=2)

        logger.info(f"Saved schema version {version} for {table_name}")


# Create global registry instance
schema_registry = SchemaRegistry()


def get_schema(table_name: str) -> StructType:
    """
    Convenience function to get schema.

    Args:
        table_name: Fully qualified table name

    Returns:
        StructType schema
    """
    return schema_registry.get_schema(table_name)


def validate_dataframe_schema(
    df: "pyspark.sql.DataFrame", expected_table: str, fail_on_mismatch: bool = True
) -> dict[str, Any]:
    """
    Validate DataFrame schema against expected schema.

    Args:
        df: DataFrame to validate
        expected_table: Expected table name
        fail_on_mismatch: Whether to raise exception on mismatch

    Returns:
        Validation results

    Raises:
        RuntimeError: If validation fails and fail_on_mismatch is True
    """
    result = schema_registry.validate_schema(df.schema, expected_table)

    if not result["valid"] and fail_on_mismatch:
        error_msg = (
            f"Schema validation failed for {expected_table}. Found {result['issue_count']} issues."
        )
        raise RuntimeError(error_msg)

    return result


def validate_schema_drift(
    df: Any, expected_schema: StructType, table_name: str = "unknown"
) -> dict[str, Any]:
    """
    Validate schema drift for a DataFrame against expected schema.

    Args:
        df: DataFrame to validate
        expected_schema: Expected schema structure
        table_name: Name of the table for logging

    Returns:
        Dictionary with validation results
    """
    try:
        actual_schema = df.schema
        issues = []

        # Check for missing columns
        expected_fields = {field.name for field in expected_schema.fields}
        actual_fields = {field.name for field in actual_schema.fields}

        missing_fields = expected_fields - actual_fields
        extra_fields = actual_fields - expected_fields

        if missing_fields:
            issues.append(f"Missing columns: {missing_fields}")
        if extra_fields:
            issues.append(f"Extra columns: {extra_fields}")

        # Check field types for common fields
        common_fields = expected_fields & actual_fields
        for field_name in common_fields:
            expected_field = next(f for f in expected_schema.fields if f.name == field_name)
            actual_field = next(f for f in actual_schema.fields if f.name == field_name)

            if expected_field.dataType != actual_field.dataType:
                issues.append(
                    f"Column '{field_name}' type mismatch: "
                    f"expected {expected_field.dataType}, got {actual_field.dataType}"
                )

        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "issue_count": len(issues),
            "table_name": table_name,
        }

    except Exception as e:
        logger.error(f"Schema drift validation failed for {table_name}: {e}")
        return {
            "valid": False,
            "issues": [f"Validation error: {str(e)}"],
            "issue_count": 1,
            "table_name": table_name,
        }
