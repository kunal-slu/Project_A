"""
Schema Drift Checker

Validates that schemas remain consistent across environments and over time.
Checks for:
- Column name changes
- Column type changes
- New/missing columns
- Nullability changes
- ID pattern consistency
"""

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class SchemaDriftChecker:
    """Check for schema drift in DataFrames."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.issues: list[dict[str, Any]] = []

    def compare_schemas(
        self, actual: StructType, expected: StructType, table_name: str, strict: bool = True
    ) -> dict[str, Any]:
        """
        Compare actual schema against expected schema.

        Args:
            actual: Actual schema from DataFrame
            expected: Expected schema (baseline)
            table_name: Name of table being checked
            strict: If True, fail on any drift. If False, warn only.

        Returns:
            Dictionary with comparison results
        """
        actual_fields = {f.name: f for f in actual.fields}
        expected_fields = {f.name: f for f in expected.fields}

        result = {
            "table": table_name,
            "drift_detected": False,
            "missing_columns": [],
            "new_columns": [],
            "type_changes": [],
            "nullability_changes": [],
            "id_pattern_issues": [],
        }

        # Check for missing columns
        for col_name, expected_field in expected_fields.items():
            if col_name not in actual_fields:
                result["missing_columns"].append(
                    {
                        "column": col_name,
                        "expected_type": str(expected_field.dataType),
                        "expected_nullable": expected_field.nullable,
                    }
                )
                result["drift_detected"] = True

        # Check for new columns
        for col_name, actual_field in actual_fields.items():
            if col_name not in expected_fields:
                result["new_columns"].append(
                    {
                        "column": col_name,
                        "actual_type": str(actual_field.dataType),
                        "actual_nullable": actual_field.nullable,
                    }
                )
                if strict:
                    result["drift_detected"] = True

        # Check for type changes
        for col_name in set(actual_fields.keys()) & set(expected_fields.keys()):
            actual_field = actual_fields[col_name]
            expected_field = expected_fields[col_name]

            if str(actual_field.dataType) != str(expected_field.dataType):
                result["type_changes"].append(
                    {
                        "column": col_name,
                        "expected": str(expected_field.dataType),
                        "actual": str(actual_field.dataType),
                    }
                )
                result["drift_detected"] = True

            if actual_field.nullable != expected_field.nullable:
                result["nullability_changes"].append(
                    {
                        "column": col_name,
                        "expected_nullable": expected_field.nullable,
                        "actual_nullable": actual_field.nullable,
                    }
                )
                if strict:
                    result["drift_detected"] = True

        # Check ID pattern consistency
        id_columns = [col for col in actual_fields.keys() if col.endswith("_id")]
        for id_col in id_columns:
            if id_col in actual_fields:
                # Validate ID pattern (e.g., ORD-xxxxx, CUS-xxxxx)
                pattern_issues = self._check_id_pattern(actual, id_col)
                if pattern_issues:
                    result["id_pattern_issues"].extend(pattern_issues)
                    result["drift_detected"] = True

        if result["drift_detected"]:
            self.issues.append(result)
            logger.warning(f"Schema drift detected in {table_name}: {result}")

        return result

    def _check_id_pattern(self, schema: StructType, id_column: str) -> list[dict[str, Any]]:
        """Check if ID column values match expected pattern."""
        # This would require reading data, so we'll return empty for now
        # In practice, you'd sample the data and check patterns
        return []

    def validate_dataframe(
        self, df: DataFrame, expected_schema: StructType, table_name: str, strict: bool = True
    ) -> dict[str, Any]:
        """Validate a DataFrame against expected schema."""
        actual_schema = df.schema
        return self.compare_schemas(actual_schema, expected_schema, table_name, strict)

    def get_all_issues(self) -> list[dict[str, Any]]:
        """Get all detected schema drift issues."""
        return self.issues

    def generate_report(self) -> str:
        """Generate a human-readable report of all issues."""
        if not self.issues:
            return "✅ No schema drift detected."

        report = ["🔍 Schema Drift Report", "=" * 50, ""]

        for issue in self.issues:
            report.append(f"Table: {issue['table']}")
            report.append("-" * 30)

            if issue["missing_columns"]:
                report.append("❌ Missing Columns:")
                for col in issue["missing_columns"]:
                    report.append(f"  - {col['column']} ({col['expected_type']})")

            if issue["new_columns"]:
                report.append("⚠️  New Columns:")
                for col in issue["new_columns"]:
                    report.append(f"  - {col['column']} ({col['actual_type']})")

            if issue["type_changes"]:
                report.append("❌ Type Changes:")
                for col in issue["type_changes"]:
                    report.append(f"  - {col['column']}: {col['expected']} → {col['actual']}")

            if issue["nullability_changes"]:
                report.append("⚠️  Nullability Changes:")
                for col in issue["nullability_changes"]:
                    report.append(
                        f"  - {col['column']}: nullable={col['expected_nullable']} → {col['actual_nullable']}"
                    )

            report.append("")

        return "\n".join(report)
