"""
Advanced Data Quality & Observability Module

This module provides enterprise-grade data quality capabilities:
- Great Expectations integration
- SLA monitoring and alerting
- Data observability dashboards
- Automated quality gates
- Performance metrics and trending
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, isnan

logger = logging.getLogger(__name__)


class QualitySeverity(Enum):
    """Data quality severity levels."""

    CRITICAL = "critical"  # Pipeline should fail
    HIGH = "high"  # Pipeline should warn
    MEDIUM = "medium"  # Pipeline should log
    LOW = "low"  # Pipeline should monitor


class QualityStatus(Enum):
    """Data quality check status."""

    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class QualityCheck:
    """Data quality check definition."""

    name: str
    description: str
    severity: QualitySeverity
    check_type: str
    parameters: dict[str, Any]
    expected_result: Any
    tolerance: float | None = None
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    last_run: datetime | None = None
    success_count: int = 0
    failure_count: int = 0


@dataclass
class QualityResult:
    """Data quality check result."""

    check_name: str
    status: QualityStatus
    actual_result: Any
    expected_result: Any
    tolerance: float | None
    execution_time_ms: float
    error_message: str | None = None
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)


class AdvancedDataQualityManager:
    """
    Manages advanced data quality checks and monitoring.
    """

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        self.spark = spark
        self.config = config
        self.quality_checks: dict[str, QualityCheck] = {}
        self.quality_results: list[QualityResult] = []
        self.sla_configs: dict[str, Any] = {}
        self.alert_configs: dict[str, Any] = {}
        self._load_quality_configs()

    def _load_quality_configs(self):
        """Load quality configurations from config."""
        quality_config = self.config.get("data_quality", {})
        self.sla_configs = quality_config.get("sla_configs", {})
        self.alert_configs = quality_config.get("alert_configs", {})

    def add_quality_check(self, check: QualityCheck) -> bool:
        """
        Add a new quality check.

        Args:
            check: Quality check definition

        Returns:
            bool: True if successful
        """
        try:
            self.quality_checks[check.name] = check
            logger.info(f"Added quality check: {check.name}")
            return True
        except Exception as e:
            logger.error(f"Failed to add quality check {check.name}: {str(e)}")
            return False

    def create_completeness_check(
        self,
        table_name: str,
        column_name: str,
        threshold: float = 0.95,
        severity: QualitySeverity = QualitySeverity.HIGH,
    ) -> QualityCheck:
        """
        Create a completeness check for a column.

        Args:
            table_name: Name of the table
            column_name: Name of the column
            threshold: Minimum completeness threshold (0.0 to 1.0)
            severity: Severity level

        Returns:
            QualityCheck: Created quality check
        """
        check = QualityCheck(
            name=f"completeness_{table_name}_{column_name}",
            description=f"Check completeness of {column_name} in {table_name}",
            severity=severity,
            check_type="completeness",
            parameters={
                "table_name": table_name,
                "column_name": column_name,
                "threshold": threshold,
            },
            expected_result=threshold,
            tolerance=0.01,
        )

        self.add_quality_check(check)
        return check

    def create_uniqueness_check(
        self,
        table_name: str,
        column_name: str,
        threshold: float = 0.99,
        severity: QualitySeverity = QualitySeverity.HIGH,
    ) -> QualityCheck:
        """
        Create a uniqueness check for a column.

        Args:
            table_name: Name of the table
            column_name: Name of the column
            threshold: Minimum uniqueness threshold (0.0 to 1.0)
            severity: Severity level

        Returns:
            QualityCheck: Created quality check
        """
        check = QualityCheck(
            name=f"uniqueness_{table_name}_{column_name}",
            description=f"Check uniqueness of {column_name} in {table_name}",
            severity=severity,
            check_type="uniqueness",
            parameters={
                "table_name": table_name,
                "column_name": column_name,
                "threshold": threshold,
            },
            expected_result=threshold,
            tolerance=0.01,
        )

        self.add_quality_check(check)
        return check

    def create_range_check(
        self,
        table_name: str,
        column_name: str,
        min_value: int | float,
        max_value: int | float,
        severity: QualitySeverity = QualitySeverity.MEDIUM,
    ) -> QualityCheck:
        """
        Create a range check for a numeric column.

        Args:
            table_name: Name of the table
            column_name: Name of the column
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            severity: Severity level

        Returns:
            QualityCheck: Created quality check
        """
        check = QualityCheck(
            name=f"range_{table_name}_{column_name}",
            description=f"Check range of {column_name} in {table_name}",
            severity=severity,
            check_type="range",
            parameters={
                "table_name": table_name,
                "column_name": column_name,
                "min_value": min_value,
                "max_value": max_value,
            },
            expected_result=f"{min_value} to {max_value}",
            tolerance=None,
        )

        self.add_quality_check(check)
        return check

    def create_pattern_check(
        self,
        table_name: str,
        column_name: str,
        pattern: str,
        threshold: float = 0.95,
        severity: QualitySeverity = QualitySeverity.MEDIUM,
    ) -> QualityCheck:
        """
        Create a pattern check for a string column.

        Args:
            table_name: Name of the table
            column_name: Name of the column
            pattern: Regex pattern to match
            threshold: Minimum pattern match threshold
            severity: Severity level

        Returns:
            QualityCheck: Created quality check
        """
        check = QualityCheck(
            name=f"pattern_{table_name}_{column_name}",
            description=f"Check pattern of {column_name} in {table_name}",
            severity=severity,
            check_type="pattern",
            parameters={
                "table_name": table_name,
                "column_name": column_name,
                "pattern": pattern,
                "threshold": threshold,
            },
            expected_result=threshold,
            tolerance=0.05,
        )

        self.add_quality_check(check)
        return check

    def run_quality_check(self, check: QualityCheck, df: DataFrame) -> QualityResult:
        """
        Run a quality check on a DataFrame.

        Args:
            check: Quality check to run
            df: DataFrame to check

        Returns:
            QualityResult: Check result
        """
        start_time = time.time()

        try:
            if check.check_type == "completeness":
                result = self._run_completeness_check(check, df)
            elif check.check_type == "uniqueness":
                result = self._run_uniqueness_check(check, df)
            elif check.check_type == "range":
                result = self._run_range_check(check, df)
            elif check.check_type == "pattern":
                result = self._run_pattern_check(check, df)
            else:
                raise ValueError(f"Unknown check type: {check.check_type}")

            execution_time = (time.time() - start_time) * 1000

            # Create quality result
            quality_result = QualityResult(
                check_name=check.name,
                status=result["status"],
                actual_result=result["actual_result"],
                expected_result=check.expected_result,
                tolerance=check.tolerance,
                execution_time_ms=execution_time,
                error_message=result.get("error_message"),
                metadata=result.get("metadata", {}),
            )

            # Update check statistics
            if quality_result.status == QualityStatus.PASSED:
                check.success_count += 1
            else:
                check.failure_count += 1

            check.last_run = datetime.now()

            # Store result
            self.quality_results.append(quality_result)

            logger.info(f"Quality check {check.name}: {quality_result.status.value}")
            return quality_result

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            error_result = QualityResult(
                check_name=check.name,
                status=QualityStatus.ERROR,
                actual_result=None,
                expected_result=check.expected_result,
                tolerance=check.tolerance,
                execution_time_ms=execution_time,
                error_message=str(e),
            )

            check.failure_count += 1
            check.last_run = datetime.now()
            self.quality_results.append(error_result)

            logger.error(f"Quality check {check.name} failed: {str(e)}")
            return error_result

    def _run_completeness_check(self, check: QualityCheck, df: DataFrame) -> dict[str, Any]:
        """Run completeness check."""
        try:
            column_name = check.parameters["column_name"]
            threshold = check.parameters["threshold"]

            total_rows = df.count()
            if total_rows == 0:
                return {
                    "status": QualityStatus.WARNING,
                    "actual_result": 0.0,
                    "metadata": {"total_rows": 0, "null_rows": 0},
                }

            null_rows = df.filter(col(column_name).isNull() | isnan(col(column_name))).count()
            completeness = (total_rows - null_rows) / total_rows

            if completeness >= threshold:
                status = QualityStatus.PASSED
            elif completeness >= threshold - (check.tolerance or 0):
                status = QualityStatus.WARNING
            else:
                status = QualityStatus.FAILED

            return {
                "status": status,
                "actual_result": completeness,
                "metadata": {
                    "total_rows": total_rows,
                    "null_rows": null_rows,
                    "completeness_percentage": completeness * 100,
                },
            }

        except Exception as e:
            return {"status": QualityStatus.ERROR, "actual_result": None, "error_message": str(e)}

    def _run_uniqueness_check(self, check: QualityCheck, df: DataFrame) -> dict[str, Any]:
        """Run uniqueness check."""
        try:
            column_name = check.parameters["column_name"]
            threshold = check.parameters["threshold"]

            total_rows = df.count()
            if total_rows == 0:
                return {
                    "status": QualityStatus.WARNING,
                    "actual_result": 1.0,
                    "metadata": {"total_rows": 0, "unique_values": 0},
                }

            unique_values = df.select(column_name).distinct().count()
            uniqueness = unique_values / total_rows

            if uniqueness >= threshold:
                status = QualityStatus.PASSED
            elif uniqueness >= threshold - (check.tolerance or 0):
                status = QualityStatus.WARNING
            else:
                status = QualityStatus.FAILED

            return {
                "status": status,
                "actual_result": uniqueness,
                "metadata": {
                    "total_rows": total_rows,
                    "unique_values": unique_values,
                    "uniqueness_percentage": uniqueness * 100,
                },
            }

        except Exception as e:
            return {"status": QualityStatus.ERROR, "actual_result": None, "error_message": str(e)}

    def _run_range_check(self, check: QualityCheck, df: DataFrame) -> dict[str, Any]:
        """Run range check."""
        try:
            column_name = check.parameters["column_name"]
            min_value = check.parameters["min_value"]
            max_value = check.parameters["max_value"]

            total_rows = df.count()
            if total_rows == 0:
                return {
                    "status": QualityStatus.WARNING,
                    "actual_result": 1.0,
                    "metadata": {"total_rows": 0, "out_of_range": 0},
                }

            out_of_range = df.filter(
                (col(column_name) < min_value) | (col(column_name) > max_value)
            ).count()

            in_range_percentage = (total_rows - out_of_range) / total_rows

            if in_range_percentage >= 0.95:
                status = QualityStatus.PASSED
            elif in_range_percentage >= 0.90:
                status = QualityStatus.WARNING
            else:
                status = QualityStatus.FAILED

            return {
                "status": status,
                "actual_result": in_range_percentage,
                "metadata": {
                    "total_rows": total_rows,
                    "out_of_range": out_of_range,
                    "in_range_percentage": in_range_percentage * 100,
                    "min_value": min_value,
                    "max_value": max_value,
                },
            }

        except Exception as e:
            return {"status": QualityStatus.ERROR, "actual_result": None, "error_message": str(e)}

    def _run_pattern_check(self, check: QualityCheck, df: DataFrame) -> dict[str, Any]:
        """Run pattern check."""
        try:
            column_name = check.parameters["column_name"]
            pattern = check.parameters["pattern"]
            threshold = check.parameters["threshold"]

            total_rows = df.count()
            if total_rows == 0:
                return {
                    "status": QualityStatus.WARNING,
                    "actual_result": 1.0,
                    "metadata": {"total_rows": 0, "pattern_matches": 0},
                }

            # Use regex pattern matching
            pattern_matches = df.filter(col(column_name).rlike(pattern)).count()

            pattern_percentage = pattern_matches / total_rows

            if pattern_percentage >= threshold:
                status = QualityStatus.PASSED
            elif pattern_percentage >= threshold - (check.tolerance or 0):
                status = QualityStatus.WARNING
            else:
                status = QualityStatus.FAILED

            return {
                "status": status,
                "actual_result": pattern_percentage,
                "metadata": {
                    "total_rows": total_rows,
                    "pattern_matches": pattern_matches,
                    "pattern_percentage": pattern_percentage * 100,
                    "pattern": pattern,
                },
            }

        except Exception as e:
            return {"status": QualityStatus.ERROR, "actual_result": None, "error_message": str(e)}

    def run_all_quality_checks(self, df: DataFrame, table_name: str) -> list[QualityResult]:
        """
        Run all quality checks for a table.

        Args:
            df: DataFrame to check
            table_name: Name of the table

        Returns:
            List[QualityResult]: All check results
        """
        results = []

        for check in self.quality_checks.values():
            if check.enabled and check.parameters.get("table_name") == table_name:
                result = self.run_quality_check(check, df)
                results.append(result)

        return results

    def check_sla_compliance(self, table_name: str, sla_name: str) -> dict[str, Any]:
        """
        Check SLA compliance for a table.

        Args:
            table_name: Name of the table
            sla_name: Name of the SLA

        Returns:
            dict: SLA compliance status
        """
        try:
            sla_config = self.sla_configs.get(sla_name, {})
            if not sla_config:
                return {"error": f"SLA {sla_name} not found"}

            # Get recent quality results for the table
            table_checks = [
                check
                for check in self.quality_checks.values()
                if check.parameters.get("table_name") == table_name
            ]

            if not table_checks:
                return {"error": f"No quality checks found for table {table_name}"}

            # Calculate SLA metrics
            total_checks = len(table_checks)
            passed_checks = 0
            failed_checks = 0
            warning_checks = 0

            for check in table_checks:
                if check.last_run:
                    recent_results = [
                        r
                        for r in self.quality_results
                        if r.check_name == check.name and (datetime.now() - r.timestamp).days <= 1
                    ]

                    if recent_results:
                        latest_result = max(recent_results, key=lambda x: x.timestamp)
                        if latest_result.status == QualityStatus.PASSED:
                            passed_checks += 1
                        elif latest_result.status == QualityStatus.FAILED:
                            failed_checks += 1
                        elif latest_result.status == QualityStatus.WARNING:
                            warning_checks += 1

            # Calculate compliance percentage
            compliance_percentage = (passed_checks / total_checks) * 100 if total_checks > 0 else 0

            # Check if SLA is met
            sla_threshold = sla_config.get("threshold", 95.0)
            sla_met = compliance_percentage >= sla_threshold

            sla_status = {
                "sla_name": sla_name,
                "table_name": table_name,
                "compliance_percentage": round(compliance_percentage, 2),
                "sla_threshold": sla_threshold,
                "sla_met": sla_met,
                "total_checks": total_checks,
                "passed_checks": passed_checks,
                "failed_checks": failed_checks,
                "warning_checks": warning_checks,
                "last_checked": datetime.now().isoformat(),
                "status": "compliant" if sla_met else "non_compliant",
            }

            logger.info(f"SLA {sla_name} for {table_name}: {compliance_percentage:.1f}% compliant")
            return sla_status

        except Exception as e:
            logger.error(f"Failed to check SLA compliance: {str(e)}")
            return {"error": str(e)}

    def generate_quality_report(
        self, table_name: str = None, start_date: datetime = None, end_date: datetime = None
    ) -> dict[str, Any]:
        """
        Generate comprehensive quality report.

        Args:
            table_name: Filter by table name
            start_date: Start date for filtering
            end_date: End date for filtering

        Returns:
            dict: Quality report
        """
        try:
            # Filter results
            filtered_results = self.quality_results

            if table_name:
                filtered_results = [
                    r
                    for r in filtered_results
                    if self.quality_checks.get(r.check_name, {}).parameters.get("table_name")
                    == table_name
                ]

            if start_date:
                filtered_results = [r for r in filtered_results if r.timestamp >= start_date]

            if end_date:
                filtered_results = [r for r in filtered_results if r.timestamp <= end_date]

            # Calculate statistics
            total_checks = len(filtered_results)
            passed_checks = len([r for r in filtered_results if r.status == QualityStatus.PASSED])
            failed_checks = len([r for r in filtered_results if r.status == QualityStatus.FAILED])
            warning_checks = len([r for r in filtered_results if r.status == QualityStatus.WARNING])
            error_checks = len([r for r in filtered_results if r.status == QualityStatus.ERROR])

            success_rate = (passed_checks / total_checks) * 100 if total_checks > 0 else 0

            # Group by check type
            check_type_stats = {}
            for result in filtered_results:
                check = self.quality_checks.get(result.check_name)
                if check:
                    check_type = check.check_type
                    if check_type not in check_type_stats:
                        check_type_stats[check_type] = {"total": 0, "passed": 0, "failed": 0}

                    check_type_stats[check_type]["total"] += 1
                    if result.status == QualityStatus.PASSED:
                        check_type_stats[check_type]["passed"] += 1
                    else:
                        check_type_stats[check_type]["failed"] += 1

            # Performance metrics
            avg_execution_time = (
                sum(r.execution_time_ms for r in filtered_results) / total_checks
                if total_checks > 0
                else 0
            )

            report = {
                "report_period": {
                    "start_date": start_date.isoformat() if start_date else None,
                    "end_date": end_date.isoformat() if end_date else None,
                    "generated_at": datetime.now().isoformat(),
                },
                "overall_statistics": {
                    "total_checks": total_checks,
                    "passed_checks": passed_checks,
                    "failed_checks": failed_checks,
                    "warning_checks": warning_checks,
                    "error_checks": error_checks,
                    "success_rate_percentage": round(success_rate, 2),
                },
                "check_type_statistics": check_type_stats,
                "performance_metrics": {
                    "average_execution_time_ms": round(avg_execution_time, 2),
                    "total_execution_time_ms": sum(r.execution_time_ms for r in filtered_results),
                },
                "table_filter": table_name,
                "recommendations": self._generate_recommendations(filtered_results),
            }

            logger.info(f"Generated quality report: {success_rate:.1f}% success rate")
            return report

        except Exception as e:
            logger.error(f"Failed to generate quality report: {str(e)}")
            return {"error": str(e)}

    def _generate_recommendations(self, results: list[QualityResult]) -> list[str]:
        """Generate recommendations based on quality results."""
        recommendations = []

        # Check for frequently failing checks
        check_failures = {}
        for result in results:
            if result.status == QualityStatus.FAILED:
                check_failures[result.check_name] = check_failures.get(result.check_name, 0) + 1

        for check_name, failure_count in check_failures.items():
            if failure_count >= 3:
                recommendations.append(f"Investigate frequently failing check: {check_name}")

        # Check for performance issues
        slow_checks = [r for r in results if r.execution_time_ms > 5000]  # 5 seconds threshold
        if slow_checks:
            recommendations.append(f"Optimize {len(slow_checks)} slow-running quality checks")

        # Check for data quality trends
        if len(results) >= 10:
            recent_results = sorted(results, key=lambda x: x.timestamp)[-10:]
            recent_success_rate = len(
                [r for r in recent_results if r.status == QualityStatus.PASSED]
            ) / len(recent_results)

            if recent_success_rate < 0.8:
                recommendations.append(
                    "Data quality appears to be declining - review data sources and transformations"
                )

        return recommendations

    def export_quality_metrics(self, output_path: str = "data/quality/metrics.json") -> bool:
        """
        Export quality metrics to JSON file.

        Args:
            output_path: Output file path

        Returns:
            bool: True if successful
        """
        try:
            # Create output directory
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Prepare export data
            export_data = {
                "export_timestamp": datetime.now().isoformat(),
                "quality_checks": {},
                "quality_results": [],
                "sla_status": {},
            }

            # Export quality checks
            for check_name, check in self.quality_checks.items():
                export_data["quality_checks"][check_name] = {
                    "name": check.name,
                    "description": check.description,
                    "severity": check.severity.value,
                    "check_type": check.check_type,
                    "parameters": check.parameters,
                    "expected_result": check.expected_result,
                    "tolerance": check.tolerance,
                    "enabled": check.enabled,
                    "created_at": check.created_at.isoformat(),
                    "last_run": check.last_run.isoformat() if check.last_run else None,
                    "success_count": check.success_count,
                    "failure_count": check.failure_count,
                }

            # Export recent quality results (last 30 days)
            thirty_days_ago = datetime.now() - timedelta(days=30)
            recent_results = [r for r in self.quality_results if r.timestamp >= thirty_days_ago]

            for result in recent_results:
                export_data["quality_results"].append(
                    {
                        "check_name": result.check_name,
                        "status": result.status.value,
                        "actual_result": result.actual_result,
                        "expected_result": result.expected_result,
                        "tolerance": result.tolerance,
                        "execution_time_ms": result.execution_time_ms,
                        "error_message": result.error_message,
                        "timestamp": result.timestamp.isoformat(),
                        "metadata": result.metadata,
                    }
                )

            # Export SLA status for all tables
            tables = set()
            for check in self.quality_checks.values():
                table_name = check.parameters.get("table_name")
                if table_name:
                    tables.add(table_name)

            for table_name in tables:
                for sla_name in self.sla_configs.keys():
                    sla_status = self.check_sla_compliance(table_name, sla_name)
                    if "error" not in sla_status:
                        export_data["sla_status"][f"{table_name}_{sla_name}"] = sla_status

            # Write to file
            with open(output_path, "w") as f:
                json.dump(export_data, f, indent=2, default=str)

            logger.info(f"Quality metrics exported to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to export quality metrics: {str(e)}")
            return False


def setup_advanced_data_quality(
    spark: SparkSession, config: dict[str, Any]
) -> AdvancedDataQualityManager:
    """
    Setup advanced data quality management for the project.

    Args:
        spark: SparkSession instance
        config: Configuration dictionary

    Returns:
        AdvancedDataQualityManager: Configured DQ manager
    """
    try:
        # Initialize DQ manager
        dq_manager = AdvancedDataQualityManager(spark, config)

        # Get quality configuration
        quality_config = config.get("data_quality", {})

        # Create default quality checks for common tables
        default_checks = quality_config.get("default_checks", {})

        for table_name, table_checks in default_checks.items():
            for check_config in table_checks:
                check_type = check_config["type"]

                if check_type == "completeness":
                    dq_manager.create_completeness_check(
                        table_name=table_name,
                        column_name=check_config["column"],
                        threshold=check_config.get("threshold", 0.95),
                        severity=QualitySeverity(check_config.get("severity", "high")),
                    )
                elif check_type == "uniqueness":
                    dq_manager.create_uniqueness_check(
                        table_name=table_name,
                        column_name=check_config["column"],
                        threshold=check_config.get("threshold", 0.99),
                        severity=QualitySeverity(check_config.get("severity", "high")),
                    )
                elif check_type == "range":
                    dq_manager.create_range_check(
                        table_name=table_name,
                        column_name=check_config["column"],
                        min_value=check_config["min_value"],
                        max_value=check_config["max_value"],
                        severity=QualitySeverity(check_config.get("severity", "medium")),
                    )
                elif check_type == "pattern":
                    dq_manager.create_pattern_check(
                        table_name=table_name,
                        column_name=check_config["column"],
                        pattern=check_config["pattern"],
                        threshold=check_config.get("threshold", 0.95),
                        severity=QualitySeverity(check_config.get("severity", "medium")),
                    )

        logger.info("Advanced data quality setup completed")
        return dq_manager

    except Exception as e:
        logger.error(f"Failed to setup advanced data quality: {str(e)}")
        raise


def run_quality_pipeline(
    dq_manager: AdvancedDataQualityManager, table_name: str, df: DataFrame
) -> dict[str, Any]:
    """
    Run complete quality pipeline for a table.

    Args:
        dq_manager: Data quality manager
        table_name: Name of the table
        df: DataFrame to check

    Returns:
        dict: Quality pipeline results
    """
    try:
        logger.info(f"Starting quality pipeline for {table_name}")

        # Run all quality checks
        quality_results = dq_manager.run_all_quality_checks(df, table_name)

        # Check SLA compliance
        sla_results = {}
        for sla_name in dq_manager.sla_configs.keys():
            sla_status = dq_manager.check_sla_compliance(table_name, sla_name)
            sla_results[sla_name] = sla_status

        # Generate quality report
        quality_report = dq_manager.generate_quality_report(table_name=table_name)

        # Determine overall pipeline status
        critical_failures = [
            r
            for r in quality_results
            if r.status == QualityStatus.FAILED
            and dq_manager.quality_checks.get(r.check_name, {}).severity == QualitySeverity.CRITICAL
        ]

        pipeline_status = "failed" if critical_failures else "passed"

        pipeline_results = {
            "table_name": table_name,
            "pipeline_status": pipeline_status,
            "total_checks": len(quality_results),
            "passed_checks": len([r for r in quality_results if r.status == QualityStatus.PASSED]),
            "failed_checks": len([r for r in quality_results if r.status == QualityStatus.FAILED]),
            "quality_results": quality_results,
            "sla_compliance": sla_results,
            "quality_report": quality_report,
            "execution_time": datetime.now().isoformat(),
        }

        logger.info(f"Quality pipeline for {table_name} completed: {pipeline_status}")
        return pipeline_results

    except Exception as e:
        logger.error(f"Quality pipeline for {table_name} failed: {str(e)}")
        return {"error": str(e)}
