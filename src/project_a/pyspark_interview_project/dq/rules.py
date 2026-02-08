"""
Data quality rules implementation.
"""

import logging
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)


class DataQualityRule:
    """Base class for data quality rules."""

    def __init__(self, name: str, severity: str):
        self.name = name
        self.severity = severity

    def check(self, df: DataFrame) -> dict[str, Any]:
        """Check rule against DataFrame."""
        raise NotImplementedError


class NotNullRule(DataQualityRule):
    """Check for null values in specified column."""

    def __init__(self, name: str, column: str, severity: str):
        super().__init__(name, severity)
        self.column = column

    def check(self, df: DataFrame) -> dict[str, Any]:
        """Check for null values."""
        null_count = df.filter(col(self.column).isNull()).count()
        total_count = df.count()

        return {
            "rule_name": self.name,
            "rule_type": "not_null",
            "column": self.column,
            "severity": self.severity,
            "null_count": null_count,
            "total_count": total_count,
            "passed": null_count == 0,
            "failure_rate": null_count / total_count if total_count > 0 else 0,
        }


class UniqueRule(DataQualityRule):
    """Check for unique values in specified columns."""

    def __init__(self, name: str, columns: list[str], severity: str):
        super().__init__(name, severity)
        self.columns = columns

    def check(self, df: DataFrame) -> dict[str, Any]:
        """Check for unique values."""
        total_count = df.count()
        unique_count = df.select(*self.columns).dropDuplicates().count()
        duplicate_count = total_count - unique_count

        return {
            "rule_name": self.name,
            "rule_type": "unique",
            "columns": self.columns,
            "severity": self.severity,
            "total_count": total_count,
            "unique_count": unique_count,
            "duplicate_count": duplicate_count,
            "passed": duplicate_count == 0,
            "failure_rate": duplicate_count / total_count if total_count > 0 else 0,
        }


class ExpressionRule(DataQualityRule):
    """Check custom SQL expression."""

    def __init__(self, name: str, sql: str, severity: str):
        super().__init__(name, severity)
        self.sql = sql

    def check(self, df: DataFrame) -> dict[str, Any]:
        """Check custom expression."""
        try:
            # Create temporary view for SQL
            df.createOrReplaceTempView("dq_check")

            # Execute SQL expression
            result_df = df.sparkSession.sql(
                f"SELECT COUNT(*) as total FROM dq_check WHERE NOT ({self.sql})"
            )
            failure_count = result_df.collect()[0]["total"]
            total_count = df.count()

            return {
                "rule_name": self.name,
                "rule_type": "expression",
                "sql": self.sql,
                "severity": self.severity,
                "total_count": total_count,
                "failure_count": failure_count,
                "passed": failure_count == 0,
                "failure_rate": failure_count / total_count if total_count > 0 else 0,
            }
        except Exception as e:
            logger.error(f"Expression rule failed: {e}")
            return {
                "rule_name": self.name,
                "rule_type": "expression",
                "sql": self.sql,
                "severity": self.severity,
                "error": str(e),
                "passed": False,
            }


def create_rule_from_config(rule_config: dict[str, Any]) -> DataQualityRule:
    """Create rule instance from configuration."""
    rule_type = rule_config["type"]
    name = rule_config["name"]
    severity = rule_config["severity"]

    if rule_type == "not_null":
        return NotNullRule(name, rule_config["column"], severity)
    elif rule_type == "unique":
        return UniqueRule(name, rule_config["columns"], severity)
    elif rule_type == "expression":
        return ExpressionRule(name, rule_config["sql"], severity)
    else:
        raise ValueError(f"Unknown rule type: {rule_type}")
