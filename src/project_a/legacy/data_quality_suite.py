"""
Data Quality Suite for comprehensive data validation and monitoring.

This module provides a complete data quality framework including:
- Schema validation
- Data completeness checks
- Statistical analysis
- Anomaly detection
- Quality scoring and reporting
"""

import json
from datetime import datetime, timedelta
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

from project_a.logging_setup import get_logger

logger = get_logger(__name__)


class DataQualitySuite:
    """
    Comprehensive data quality validation and monitoring suite.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.quality_metrics = {}
        self.validation_results = {}

    def validate_schema(self, df: DataFrame, expected_schema: StructType) -> dict[str, Any]:
        """
        Validate DataFrame schema against expected schema.

        Args:
            df: DataFrame to validate
            expected_schema: Expected schema

        Returns:
            Validation results
        """
        try:
            actual_schema = df.schema
            actual_fields = {field.name for field in actual_schema.fields}
            expected_fields = {field.name for field in expected_schema.fields}

            missing_fields = expected_fields - actual_fields
            extra_fields = actual_fields - expected_fields

            result = {
                "valid": len(missing_fields) == 0,
                "missing_fields": list(missing_fields),
                "extra_fields": list(extra_fields),
                "total_fields": len(actual_fields),
                "expected_fields": len(expected_fields),
            }

            self.validation_results["schema"] = result
            return result

        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            return {"valid": False, "error": str(e)}

    def check_completeness(
        self, df: DataFrame, columns: list[str] | None = None
    ) -> dict[str, Any]:
        """
        Check data completeness for specified columns.

        Args:
            df: DataFrame to check
            columns: Columns to check (if None, check all)

        Returns:
            Completeness results
        """
        try:
            if columns is None:
                columns = df.columns

            total_rows = df.count()
            completeness_results = {}

            for column in columns:
                if column not in df.columns:
                    completeness_results[column] = {
                        "completeness": 0.0,
                        "null_count": total_rows,
                        "total_count": total_rows,
                    }
                    continue

                # Use limit(1) instead of collect() to avoid driver OOM
                null_count = df.filter(F.col(column).isNull()).count()
                completeness = (total_rows - null_count) / total_rows if total_rows > 0 else 0.0

                completeness_results[column] = {
                    "completeness": completeness,
                    "null_count": null_count,
                    "total_count": total_rows,
                }

            self.validation_results["completeness"] = completeness_results
            return completeness_results

        except Exception as e:
            logger.error(f"Completeness check failed: {e}")
            return {"error": str(e)}

    def check_uniqueness(self, df: DataFrame, columns: list[str]) -> dict[str, Any]:
        """
        Check uniqueness of specified columns.

        Args:
            df: DataFrame to check
            columns: Columns to check for uniqueness

        Returns:
            Uniqueness results
        """
        try:
            total_rows = df.count()
            distinct_rows = df.select(*columns).distinct().count()
            uniqueness = distinct_rows / total_rows if total_rows > 0 else 0.0

            result = {
                "uniqueness": uniqueness,
                "total_rows": total_rows,
                "distinct_rows": distinct_rows,
                "duplicate_rows": total_rows - distinct_rows,
            }

            self.validation_results["uniqueness"] = result
            return result

        except Exception as e:
            logger.error(f"Uniqueness check failed: {e}")
            return {"error": str(e)}

    def check_consistency(self, df: DataFrame, rules: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Check data consistency against defined rules.

        Args:
            df: DataFrame to check
            rules: List of consistency rules

        Returns:
            Consistency results
        """
        try:
            consistency_results = {}

            for rule in rules:
                rule_name = rule.get("name", "unknown_rule")
                condition = rule.get("condition")
                expected_result = rule.get("expected_result", True)

                if not condition:
                    continue

                # Apply condition and count results
                filtered_df = df.filter(condition)
                actual_count = filtered_df.count()
                total_count = df.count()
                consistency_ratio = actual_count / total_count if total_count > 0 else 0.0

                consistency_results[rule_name] = {
                    "condition": condition,
                    "expected_result": expected_result,
                    "actual_count": actual_count,
                    "total_count": total_count,
                    "consistency_ratio": consistency_ratio,
                    "passed": (actual_count > 0) == expected_result,
                }

            self.validation_results["consistency"] = consistency_results
            return consistency_results

        except Exception as e:
            logger.error(f"Consistency check failed: {e}")
            return {"error": str(e)}

    def check_accuracy(self, df: DataFrame, accuracy_rules: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Check data accuracy against defined rules.

        Args:
            df: DataFrame to check
            accuracy_rules: List of accuracy rules

        Returns:
            Accuracy results
        """
        try:
            accuracy_results = {}

            for rule in accuracy_rules:
                rule_name = rule.get("name", "unknown_rule")
                column = rule.get("column")
                rule_type = rule.get("type")
                parameters = rule.get("parameters", {})

                if not column or column not in df.columns:
                    accuracy_results[rule_name] = {"error": f"Column {column} not found"}
                    continue

                if rule_type == "range":
                    min_val = parameters.get("min")
                    max_val = parameters.get("max")

                    # Use limit(1) instead of collect() to avoid driver OOM
                    if min_val is not None:
                        min_check = df.filter(F.col(column) < min_val).count()
                    else:
                        min_check = 0

                    if max_val is not None:
                        max_check = df.filter(F.col(column) > max_val).count()
                    else:
                        max_check = 0

                    total_count = df.count()
                    accuracy = (
                        (total_count - min_check - max_check) / total_count
                        if total_count > 0
                        else 0.0
                    )

                    accuracy_results[rule_name] = {
                        "type": "range",
                        "column": column,
                        "min": min_val,
                        "max": max_val,
                        "min_violations": min_check,
                        "max_violations": max_check,
                        "accuracy": accuracy,
                    }

                elif rule_type == "pattern":
                    pattern = parameters.get("pattern")
                    if pattern:
                        pattern_violations = df.filter(~F.col(column).rlike(pattern)).count()
                        total_count = df.count()
                        accuracy = (
                            (total_count - pattern_violations) / total_count
                            if total_count > 0
                            else 0.0
                        )

                        accuracy_results[rule_name] = {
                            "type": "pattern",
                            "column": column,
                            "pattern": pattern,
                            "violations": pattern_violations,
                            "accuracy": accuracy,
                        }

            self.validation_results["accuracy"] = accuracy_results
            return accuracy_results

        except Exception as e:
            logger.error(f"Accuracy check failed: {e}")
            return {"error": str(e)}

    def check_timeliness(
        self, df: DataFrame, timestamp_column: str, max_delay_hours: int = 24
    ) -> dict[str, Any]:
        """
        Check data timeliness.

        Args:
            df: DataFrame to check
            timestamp_column: Column containing timestamps
            max_delay_hours: Maximum allowed delay in hours

        Returns:
            Timeliness results
        """
        try:
            if timestamp_column not in df.columns:
                return {"error": f"Timestamp column {timestamp_column} not found"}

            # Calculate delay from current time
            current_time = datetime.now()
            max_delay = timedelta(hours=max_delay_hours)

            # Count records within acceptable delay
            timely_records = df.filter(
                F.col(timestamp_column) >= (current_time - max_delay)
            ).count()

            total_count = df.count()
            timeliness = timely_records / total_count if total_count > 0 else 0.0

            result = {
                "timeliness": timeliness,
                "total_records": total_count,
                "timely_records": timely_records,
                "delayed_records": total_count - timely_records,
                "max_delay_hours": max_delay_hours,
            }

            self.validation_results["timeliness"] = result
            return result

        except Exception as e:
            logger.error(f"Timeliness check failed: {e}")
            return {"error": str(e)}

    def calculate_statistics(
        self, df: DataFrame, columns: list[str] | None = None
    ) -> dict[str, Any]:
        """
        Calculate basic statistics for numeric columns.

        Args:
            df: DataFrame to analyze
            columns: Columns to analyze (if None, analyze all numeric columns)

        Returns:
            Statistical results
        """
        try:
            if columns is None:
                # Get numeric columns
                numeric_columns = [
                    field.name
                    for field in df.schema.fields
                    if field.dataType.typeName() in ["integer", "long", "float", "double"]
                ]
                columns = numeric_columns

            statistics = {}

            for column in columns:
                if column not in df.columns:
                    continue

                # Calculate statistics using Spark SQL functions
                stats_df = df.select(
                    F.count(F.col(column)).alias("count"),
                    F.mean(F.col(column)).alias("mean"),
                    F.stddev(F.col(column)).alias("stddev"),
                    F.min(F.col(column)).alias("min"),
                    F.max(F.col(column)).alias("max"),
                ).limit(1)  # Use limit(1) instead of collect()

                if stats_df.count() > 0:
                    row = stats_df.first()
                    statistics[column] = {
                        "count": row["count"],
                        "mean": row["mean"],
                        "stddev": row["stddev"],
                        "min": row["min"],
                        "max": row["max"],
                    }

            self.quality_metrics["statistics"] = statistics
            return statistics

        except Exception as e:
            logger.error(f"Statistics calculation failed: {e}")
            return {"error": str(e)}

    def detect_anomalies(
        self, df: DataFrame, column: str, method: str = "iqr", threshold: float = 1.5
    ) -> dict[str, Any]:
        """
        Detect anomalies in a numeric column.

        Args:
            df: DataFrame to analyze
            column: Column to check for anomalies
            method: Detection method ('iqr' or 'zscore')
            threshold: Threshold for anomaly detection

        Returns:
            Anomaly detection results
        """
        try:
            if column not in df.columns:
                return {"error": f"Column {column} not found"}

            if method == "iqr":
                # Calculate quartiles using window functions
                Window.orderBy(F.col(column))

                quartiles_df = df.select(
                    F.percentile_approx(F.col(column), 0.25).alias("q1"),
                    F.percentile_approx(F.col(column), 0.75).alias("q3"),
                ).limit(1)  # Use limit(1) instead of collect()

                if quartiles_df.count() > 0:
                    row = quartiles_df.first()
                    q1 = row["q1"]
                    q3 = row["q3"]
                    iqr = q3 - q1

                    lower_bound = q1 - threshold * iqr
                    upper_bound = q3 + threshold * iqr

                    anomalies = df.filter(
                        (F.col(column) < lower_bound) | (F.col(column) > upper_bound)
                    ).count()

                    total_count = df.count()
                    anomaly_ratio = anomalies / total_count if total_count > 0 else 0.0

                    return {
                        "method": "iqr",
                        "column": column,
                        "q1": q1,
                        "q3": q3,
                        "iqr": iqr,
                        "lower_bound": lower_bound,
                        "upper_bound": upper_bound,
                        "anomalies": anomalies,
                        "total_count": total_count,
                        "anomaly_ratio": anomaly_ratio,
                    }

            elif method == "zscore":
                # Calculate z-score based anomalies
                stats_df = df.select(
                    F.mean(F.col(column)).alias("mean"), F.stddev(F.col(column)).alias("stddev")
                ).limit(1)  # Use limit(1) instead of collect()

                if stats_df.count() > 0:
                    row = stats_df.first()
                    mean = row["mean"]
                    stddev = row["stddev"]

                    if stddev and stddev > 0:
                        anomalies = df.filter(
                            F.abs((F.col(column) - mean) / stddev) > threshold
                        ).count()

                        total_count = df.count()
                        anomaly_ratio = anomalies / total_count if total_count > 0 else 0.0

                        return {
                            "method": "zscore",
                            "column": column,
                            "mean": mean,
                            "stddev": stddev,
                            "threshold": threshold,
                            "anomalies": anomalies,
                            "total_count": total_count,
                            "anomaly_ratio": anomaly_ratio,
                        }

            return {"error": f"Unknown method: {method}"}

        except Exception as e:
            logger.error(f"Anomaly detection failed: {e}")
            return {"error": str(e)}

    def generate_quality_report(self, output_path: str | None = None) -> str:
        """
        Generate comprehensive data quality report.

        Args:
            output_path: Optional path to save report

        Returns:
            JSON report string
        """
        try:
            report = {
                "timestamp": datetime.now().isoformat(),
                "validation_results": self.validation_results,
                "quality_metrics": self.quality_metrics,
                "summary": {
                    "total_checks": len(self.validation_results),
                    "passed_checks": sum(
                        1
                        for result in self.validation_results.values()
                        if isinstance(result, dict) and result.get("valid", False)
                    ),
                    "failed_checks": sum(
                        1
                        for result in self.validation_results.values()
                        if isinstance(result, dict) and not result.get("valid", True)
                    ),
                },
            }

            report_json = json.dumps(report, indent=2, default=str)

            if output_path:
                with open(output_path, "w") as f:
                    f.write(report_json)
                logger.info(f"Quality report saved to {output_path}")

            return report_json

        except Exception as e:
            logger.error(f"Report generation failed: {e}")
            return json.dumps({"error": str(e)})

    def run_comprehensive_validation(
        self,
        df: DataFrame,
        expected_schema: StructType | None = None,
        completeness_columns: list[str] | None = None,
        uniqueness_columns: list[str] | None = None,
        consistency_rules: list[dict[str, Any]] | None = None,
        accuracy_rules: list[dict[str, Any]] | None = None,
        timestamp_column: str | None = None,
    ) -> dict[str, Any]:
        """
        Run comprehensive data quality validation.

        Args:
            df: DataFrame to validate
            expected_schema: Expected schema for validation
            completeness_columns: Columns to check for completeness
            uniqueness_columns: Columns to check for uniqueness
            consistency_rules: Rules for consistency checking
            accuracy_rules: Rules for accuracy checking
            timestamp_column: Column for timeliness checking

        Returns:
            Comprehensive validation results
        """
        try:
            results = {}

            # Schema validation
            if expected_schema:
                results["schema"] = self.validate_schema(df, expected_schema)

            # Completeness check
            results["completeness"] = self.check_completeness(df, completeness_columns)

            # Uniqueness check
            if uniqueness_columns:
                results["uniqueness"] = self.check_uniqueness(df, uniqueness_columns)

            # Consistency check
            if consistency_rules:
                results["consistency"] = self.check_consistency(df, consistency_rules)

            # Accuracy check
            if accuracy_rules:
                results["accuracy"] = self.check_accuracy(df, accuracy_rules)

            # Timeliness check
            if timestamp_column:
                results["timeliness"] = self.check_timeliness(df, timestamp_column)

            # Statistics calculation
            results["statistics"] = self.calculate_statistics(df)

            return results

        except Exception as e:
            logger.error(f"Comprehensive validation failed: {e}")
            return {"error": str(e)}
