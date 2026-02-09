"""
Enterprise Data Platform Integration Module

This module provides a unified interface for all enterprise data platform components:
- Unity Catalog & Governance
- Azure Security & Compliance
- Disaster Recovery & High Availability
- Advanced Data Quality & Observability
- Performance Optimization & Monitoring
- Multi-environment CI/CD Integration
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from .advanced_dq_monitoring import AdvancedDataQualityManager, QualitySeverity
from .azure_security import AzureSecurityManager
from .delta_utils import DeltaUtils
from .disaster_recovery import DisasterRecoveryExecutor
from .monitoring import PipelineMonitor

# Import our enterprise modules
from .unity_catalog import UnityCatalogManager

logger = logging.getLogger(__name__)


@dataclass
class PlatformHealth:
    """Platform health status."""

    overall_status: str
    unity_catalog_status: str
    security_status: str
    dr_status: str
    data_quality_status: str
    performance_status: str
    last_check: datetime = field(default_factory=datetime.now)
    issues: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)


@dataclass
class PlatformMetrics:
    """Platform performance metrics."""

    total_tables: int
    total_users: int
    active_pipelines: int
    data_quality_score: float
    replication_lag_minutes: float
    security_compliance_score: float
    performance_score: float
    last_updated: datetime = field(default_factory=datetime.now)


class EnterpriseDataPlatform:
    """
    Unified enterprise data platform that integrates all components.
    """

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        self.spark = spark
        self.config = config
        self.platform_id = f"platform_{int(time.time())}"

        # Initialize all components
        self.unity_catalog = None
        self.security_manager = None
        self.dr_manager = None
        self.data_quality_manager = None
        self.monitor = None
        self.delta_utils = None

        # Platform state
        self.health_status = PlatformHealth(
            overall_status="initializing",
            unity_catalog_status="initializing",
            security_status="initializing",
            dr_status="initializing",
            data_quality_status="initializing",
            performance_status="initializing",
        )
        self.metrics = PlatformMetrics(
            total_tables=0,
            total_users=0,
            active_pipelines=0,
            data_quality_score=0.0,
            replication_lag_minutes=0.0,
            security_compliance_score=0.0,
            performance_score=0.0,
        )

        self._initialize_platform()

    def _initialize_platform(self):
        """Initialize all platform components."""
        try:
            logger.info("Initializing Enterprise Data Platform...")

            # Initialize Unity Catalog
            catalog_name = self.config.get("governance", {}).get("catalog_name", "main")
            self.unity_catalog = UnityCatalogManager(self.spark, catalog_name)
            logger.info("Unity Catalog initialized")

            # Initialize Azure Security
            self.security_manager = AzureSecurityManager(self.spark, self.config)
            logger.info("Azure Security Manager initialized")

            # Initialize Disaster Recovery
            self.config.get("disaster_recovery", {})
            self.dr_manager = DisasterRecoveryExecutor(self.spark, self.config)
            logger.info("Disaster Recovery Manager initialized")

            # Initialize Data Quality
            self.data_quality_manager = AdvancedDataQualityManager(self.spark, self.config)
            logger.info("Advanced Data Quality Manager initialized")

            # Initialize Monitoring
            self.monitor = PipelineMonitor(self.spark, self.config)
            logger.info("Pipeline Monitor initialized")

            # Initialize Delta Utils
            self.delta_utils = DeltaUtils(self.spark)
            logger.info("Delta Utils initialized")

            # Setup platform governance
            self._setup_platform_governance()

            # Check initial health
            self._check_platform_health()

            logger.info("Enterprise Data Platform initialization completed successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Enterprise Data Platform: {str(e)}")
            self.health_status.overall_status = "failed"
            self.health_status.issues.append(f"Initialization failed: {str(e)}")
            raise

    def _setup_platform_governance(self):
        """Setup platform governance and policies."""
        try:
            # Setup Unity Catalog governance
            if self.unity_catalog:
                self.unity_catalog.setup_unity_catalog_governance()

            # Setup Azure security
            if self.security_manager:
                from .azure_security import setup_azure_security

                setup_azure_security(self.config)

            # Setup disaster recovery
            if self.dr_manager:
                dr_config = self.config.get("disaster_recovery", {})
                if dr_config.get("enable_cross_region_replication", True):
                    storage_accounts = dr_config.get("storage_accounts", [])
                    if storage_accounts:
                        self.dr_manager.setup_cross_region_replication(storage_accounts)

                tables_to_backup = dr_config.get("tables_to_backup", [])
                if tables_to_backup:
                    self.dr_manager.create_backup_strategy(tables_to_backup)

            logger.info("Platform governance setup completed")

        except Exception as e:
            logger.error(f"Failed to setup platform governance: {str(e)}")
            self.health_status.issues.append(f"Governance setup failed: {str(e)}")

    def _check_platform_health(self):
        """Check the health of all platform components."""
        try:
            issues = []
            recommendations = []

            # Check Unity Catalog
            uc_status = "healthy"
            if self.unity_catalog:
                try:
                    # Basic health check
                    catalogs = self.unity_catalog.list_catalogs()
                    if not catalogs:
                        issues.append("No catalogs found in Unity Catalog")
                        recommendations.append("Create initial catalog and schema")
                        uc_status = "warning"
                except Exception as e:
                    issues.append(f"Unity Catalog health check failed: {str(e)}")
                    recommendations.append("Verify Unity Catalog connectivity and permissions")
                    uc_status = "error"
            else:
                uc_status = "not_initialized"

            # Check Security
            security_status = "healthy"
            if self.security_manager:
                try:
                    security_summary = self.security_manager.get_security_summary()
                    if security_summary.get("overall_status") != "compliant":
                        issues.append(
                            f"Security compliance issues: {security_summary.get('issues', [])}"
                        )
                        recommendations.append("Review and fix security compliance issues")
                        security_status = "warning"
                except Exception as e:
                    issues.append(f"Security health check failed: {str(e)}")
                    recommendations.append("Verify Azure security configuration")
                    security_status = "error"
            else:
                security_status = "not_initialized"

            # Check Disaster Recovery
            dr_status = "healthy"
            if self.dr_manager:
                try:
                    dr_health = self.dr_manager.check_replication_health()
                    if dr_health.get("overall_status") != "healthy":
                        issues.append(f"DR health issues: {dr_health.get('recommendations', [])}")
                        recommendations.append("Review DR configuration and replication status")
                        dr_status = "warning"
                except Exception as e:
                    issues.append(f"DR health check failed: {str(e)}")
                    recommendations.append("Verify DR configuration")
                    dr_status = "error"
            else:
                dr_status = "not_initialized"

            # Check Data Quality
            dq_status = "healthy"
            if self.data_quality_manager:
                try:
                    # Basic quality check
                    if not self.data_quality_manager.quality_checks:
                        recommendations.append("Configure data quality checks for critical tables")
                        dq_status = "warning"
                except Exception as e:
                    issues.append(f"Data quality health check failed: {str(e)}")
                    recommendations.append("Verify data quality configuration")
                    dq_status = "error"
            else:
                dq_status = "not_initialized"

            # Check Performance (basic check)
            performance_status = "healthy"
            if self.metrics.performance_score < 0.8:
                performance_status = "degraded"
                recommendations.append("Performance optimization recommended")

            # Determine overall status
            if issues:
                if len(issues) > 3:
                    overall_status = "critical"
                elif len(issues) > 1:
                    overall_status = "degraded"
                else:
                    overall_status = "warning"
            else:
                overall_status = "healthy"

            # Update all status fields
            self.health_status.overall_status = overall_status
            self.health_status.unity_catalog_status = uc_status
            self.health_status.security_status = security_status
            self.health_status.dr_status = dr_status
            self.health_status.data_quality_status = dq_status
            self.health_status.performance_status = performance_status
            self.health_status.issues = issues
            self.health_status.recommendations = recommendations
            self.health_status.last_check = datetime.now()

            logger.info(f"Platform health check completed: {overall_status}")

        except Exception as e:
            logger.error(f"Failed to check platform health: {str(e)}")
            self.health_status.overall_status = "error"
            self.health_status.issues.append(f"Health check failed: {str(e)}")

    def create_data_pipeline(
        self, pipeline_name: str, pipeline_config: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Create a new data pipeline with all enterprise features.

        Args:
            pipeline_name: Name of the pipeline
            pipeline_config: Pipeline configuration

        Returns:
            dict: Pipeline creation results
        """
        try:
            logger.info(f"Creating enterprise data pipeline: {pipeline_name}")

            # Start pipeline monitoring
            if self.monitor:
                self.monitor.start_pipeline(pipeline_name, pipeline_config)

            # Create Unity Catalog objects if specified
            if self.unity_catalog and pipeline_config.get("create_catalog_objects", True):
                catalog_name = pipeline_config.get("catalog_name", "default")
                schema_name = pipeline_config.get("schema_name", "default")

                # Create catalog and schema if they don't exist
                self.unity_catalog.create_catalog(catalog_name)
                self.unity_catalog.create_schema(catalog_name, schema_name)

                # Create tables if specified
                tables = pipeline_config.get("tables", [])
                for table_config in tables:
                    table_name = table_config["name"]
                    table_path = table_config["path"]
                    table_schema = table_config.get("schema")

                    self.unity_catalog.create_table(
                        catalog_name, schema_name, table_name, table_path, table_schema
                    )

            # Setup data quality checks
            if self.data_quality_manager and pipeline_config.get("setup_quality_checks", True):
                tables = pipeline_config.get("tables", [])
                for table_config in tables:
                    table_name = table_config["name"]
                    columns = table_config.get("columns", [])

                    for column in columns:
                        column_name = column["name"]
                        quality_checks = column.get("quality_checks", [])

                        for check_config in quality_checks:
                            check_type = check_config["type"]

                            if check_type == "completeness":
                                threshold = check_config.get("threshold", 0.95)
                                severity = QualitySeverity(check_config.get("severity", "high"))
                                self.data_quality_manager.create_completeness_check(
                                    table_name, column_name, threshold, severity
                                )

                            elif check_type == "uniqueness":
                                threshold = check_config.get("threshold", 0.99)
                                severity = QualitySeverity(check_config.get("severity", "high"))
                                self.data_quality_manager.create_uniqueness_check(
                                    table_name, column_name, threshold, severity
                                )

                            elif check_type == "range":
                                min_val = check_config["min_value"]
                                max_val = check_config["max_value"]
                                severity = QualitySeverity(check_config.get("severity", "medium"))
                                self.data_quality_manager.create_range_check(
                                    table_name, column_name, min_val, max_val, severity
                                )

            # Setup disaster recovery if specified
            if self.dr_manager and pipeline_config.get("setup_dr", True):
                tables = pipeline_config.get("tables", [])
                table_paths = [table["path"] for table in tables]
                recovery_tier = pipeline_config.get("recovery_tier", "high")

                self.dr_manager.replicate_data_to_secondary(table_paths, recovery_tier)

            # End pipeline monitoring
            if self.monitor:
                self.monitor.end_pipeline()

            # Update platform metrics
            self._update_platform_metrics()

            result = {
                "pipeline_name": pipeline_name,
                "status": "created",
                "catalog_objects_created": pipeline_config.get("create_catalog_objects", True),
                "quality_checks_setup": pipeline_config.get("setup_quality_checks", True),
                "dr_setup": pipeline_config.get("setup_dr", True),
                "created_at": datetime.now().isoformat(),
            }

            logger.info(f"Enterprise data pipeline {pipeline_name} created successfully")
            return result

        except Exception as e:
            logger.error(f"Failed to create enterprise data pipeline {pipeline_name}: {str(e)}")

            # End pipeline monitoring with error
            if self.monitor:
                self.monitor.end_pipeline(error=str(e))

            return {"error": str(e)}

    def run_data_quality_checks(self, table_name: str, df: DataFrame) -> dict[str, Any]:
        """
        Run comprehensive data quality checks on a table.

        Args:
            table_name: Name of the table
            df: DataFrame to check

        Returns:
            dict: Quality check results
        """
        try:
            if not self.data_quality_manager:
                return {"error": "Data quality manager not initialized"}

            logger.info(f"Running data quality checks for table: {table_name}")

            # Run all quality checks
            results = self.data_quality_manager.run_all_quality_checks(df, table_name)

            # Check SLA compliance
            sla_results = {}
            for sla_name in self.data_quality_manager.sla_configs.keys():
                sla_result = self.data_quality_manager.check_sla_compliance(table_name, sla_name)
                sla_results[sla_name] = sla_result

            # Generate quality report
            quality_report = self.data_quality_manager.generate_quality_report(table_name)

            # Update platform metrics
            if results:
                passed_checks = len([r for r in results if r.status.value == "passed"])
                total_checks = len(results)
                quality_score = passed_checks / total_checks if total_checks > 0 else 0.0
                self.metrics.data_quality_score = quality_score

            result = {
                "table_name": table_name,
                "quality_checks": results,
                "sla_compliance": sla_results,
                "quality_report": quality_report,
                "overall_quality_score": self.metrics.data_quality_score,
                "completed_at": datetime.now().isoformat(),
            }

            logger.info(f"Data quality checks completed for {table_name}")
            return result

        except Exception as e:
            logger.error(f"Failed to run data quality checks for {table_name}: {str(e)}")
            return {"error": str(e)}

    def optimize_table_performance(
        self, table_path: str, optimization_config: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Optimize table performance using Delta Lake features.

        Args:
            table_path: Path to the table
            optimization_config: Optimization configuration

        Returns:
            dict: Optimization results
        """
        try:
            if not self.delta_utils:
                return {"error": "Delta Utils not initialized"}

            logger.info(f"Optimizing table performance: {table_path}")

            results = {}

            # Optimize table
            if optimization_config.get("optimize", True):
                optimize_result = self.delta_utils.optimize_table(table_path)
                results["optimize"] = optimize_result

            # Set retention policy
            if optimization_config.get("set_retention", True):
                retention_days = optimization_config.get("retention_days", 30)
                retention_result = self.delta_utils.set_retention_policy(table_path, retention_days)
                results["retention"] = retention_result

            # Cleanup old partitions
            if optimization_config.get("cleanup_partitions", True):
                partition_column = optimization_config.get("partition_column", "date")
                cleanup_days = optimization_config.get("cleanup_days", 90)
                cleanup_result = self.delta_utils.cleanup_old_partitions(
                    table_path, partition_column, cleanup_days
                )
                results["cleanup"] = cleanup_result

            # Get table statistics
            stats_result = self.delta_utils.get_table_stats(table_path)
            results["statistics"] = stats_result

            # Run maintenance routine
            maintenance_result = self.delta_utils.run_maintenance_routine(table_path)
            results["maintenance"] = maintenance_result

            logger.info(f"Table performance optimization completed for {table_path}")
            return results

        except Exception as e:
            logger.error(f"Failed to optimize table performance for {table_path}: {str(e)}")
            return {"error": str(e)}

    def get_platform_summary(self) -> dict[str, Any]:
        """
        Get comprehensive platform summary.

        Returns:
            dict: Platform summary
        """
        try:
            # Check platform health
            self._check_platform_health()

            # Get component summaries
            unity_summary = {}
            if self.unity_catalog:
                try:
                    catalogs = self.unity_catalog.list_catalogs()
                    unity_summary = {"total_catalogs": len(catalogs), "status": "healthy"}
                except Exception as e:
                    unity_summary = {"status": "error", "error": str(e)}

            security_summary = {}
            if self.security_manager:
                try:
                    security_summary = self.security_manager.get_security_summary()
                except Exception as e:
                    security_summary = {"status": "error", "error": str(e)}

            dr_summary = {}
            if self.dr_manager:
                try:
                    dr_summary = self.dr_manager.get_dr_summary()
                except Exception as e:
                    dr_summary = {"status": "error", "error": str(e)}

            # Update platform metrics
            self._update_platform_metrics()

            summary = {
                "platform_id": self.platform_id,
                "health_status": {
                    "overall": self.health_status.overall_status,
                    "last_check": self.health_status.last_check.isoformat(),
                    "issues": self.health_status.issues,
                    "recommendations": self.health_status.recommendations,
                },
                "components": {
                    "unity_catalog": unity_summary,
                    "security": security_summary,
                    "disaster_recovery": dr_summary,
                    "data_quality": {
                        "total_checks": len(self.data_quality_manager.quality_checks)
                        if self.data_quality_manager
                        else 0,
                        "status": "healthy" if self.data_quality_manager else "not_initialized",
                    },
                    "monitoring": {"status": "healthy" if self.monitor else "not_initialized"},
                },
                "metrics": {
                    "total_tables": self.metrics.total_tables,
                    "total_users": self.metrics.total_users,
                    "active_pipelines": self.metrics.active_pipelines,
                    "data_quality_score": self.metrics.data_quality_score,
                    "replication_lag_minutes": self.metrics.replication_lag_minutes,
                    "security_compliance_score": self.metrics.security_compliance_score,
                    "performance_score": self.metrics.performance_score,
                    "last_updated": self.metrics.last_updated.isoformat(),
                },
                "configuration": {
                    "primary_region": self.dr_manager.primary_region if self.dr_manager else None,
                    "secondary_region": self.dr_manager.secondary_region
                    if self.dr_manager
                    else None,
                    "recovery_tiers": ["high", "medium", "low"] if self.dr_manager else [],
                    "quality_severities": list(QualitySeverity.__members__.keys()),
                },
                "last_updated": datetime.now().isoformat(),
            }

            return summary

        except Exception as e:
            logger.error(f"Failed to get platform summary: {str(e)}")
            return {"error": str(e)}

    def _update_platform_metrics(self):
        """Update platform metrics from all components."""
        try:
            # Update table count from Unity Catalog
            if self.unity_catalog:
                try:
                    catalogs = self.unity_catalog.list_catalogs()
                    total_tables = 0
                    for catalog in catalogs:
                        schemas = self.unity_catalog.list_schemas(catalog)
                        for schema in schemas:
                            tables = self.unity_catalog.list_tables(catalog, schema)
                            total_tables += len(tables)
                    self.metrics.total_tables = total_tables
                except Exception as e:
                    logger.warning(f"Failed to update table count: {str(e)}")

            # Update DR metrics
            if self.dr_manager:
                try:
                    dr_health = self.dr_manager.check_replication_health()
                    self.metrics.replication_lag_minutes = dr_health.get(
                        "replication_lag_minutes", 0.0
                    )
                except Exception as e:
                    logger.warning(f"Failed to update DR metrics: {str(e)}")

            # Update security metrics
            if self.security_manager:
                try:
                    security_summary = self.security_manager.get_security_summary()
                    self.metrics.security_compliance_score = security_summary.get(
                        "compliance_score", 0.0
                    )
                except Exception as e:
                    logger.warning(f"Failed to update security metrics: {str(e)}")

            # Update data quality metrics
            if self.data_quality_manager:
                try:
                    if self.data_quality_manager.quality_results:
                        recent_results = [
                            r
                            for r in self.data_quality_manager.quality_results
                            if (datetime.now() - r.timestamp).days <= 7
                        ]
                        if recent_results:
                            passed_checks = len(
                                [r for r in recent_results if r.status.value == "passed"]
                            )
                            total_checks = len(recent_results)
                            self.metrics.data_quality_score = (
                                passed_checks / total_checks if total_checks > 0 else 0.0
                            )
                except Exception as e:
                    logger.warning(f"Failed to update data quality metrics: {str(e)}")

            self.metrics.last_updated = datetime.now()

        except Exception as e:
            logger.error(f"Failed to update platform metrics: {str(e)}")

    def export_platform_config(
        self, output_path: str = "data/platform/platform_config.json"
    ) -> bool:
        """
        Export platform configuration to file.

        Args:
            output_path: Output file path

        Returns:
            bool: True if successful
        """
        try:
            # Get platform summary
            summary = self.get_platform_summary()

            # Create output directory
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Export configuration
            with open(output_path, "w") as f:
                json.dump(summary, f, indent=2)

            logger.info(f"Platform configuration exported to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to export platform configuration: {str(e)}")
            return False


def setup_enterprise_data_platform(
    spark: SparkSession, config: dict[str, Any]
) -> EnterpriseDataPlatform:
    """
    Setup the complete enterprise data platform.

    Args:
        spark: SparkSession instance
        config: Configuration dictionary

    Returns:
        EnterpriseDataPlatform: Configured platform instance
    """
    try:
        logger.info("Setting up Enterprise Data Platform...")

        platform = EnterpriseDataPlatform(spark, config)

        logger.info("Enterprise Data Platform setup completed successfully")
        return platform

    except Exception as e:
        logger.error(f"Failed to setup Enterprise Data Platform: {str(e)}")
        raise


def run_platform_health_check(platform: EnterpriseDataPlatform) -> dict[str, Any]:
    """
    Run a comprehensive platform health check.

    Args:
        platform: Enterprise data platform instance

    Returns:
        dict: Health check results
    """
    try:
        logger.info("Running platform health check...")

        # Get platform summary (includes health check)
        summary = platform.get_platform_summary()

        # Run additional component-specific checks
        additional_checks = {}

        # Check Unity Catalog connectivity
        if platform.unity_catalog:
            try:
                catalogs = platform.unity_catalog.list_catalogs()
                additional_checks["unity_catalog_connectivity"] = {
                    "status": "healthy",
                    "catalogs_found": len(catalogs),
                }
            except Exception as e:
                additional_checks["unity_catalog_connectivity"] = {
                    "status": "failed",
                    "error": str(e),
                }

        # Check DR replication
        if platform.dr_manager:
            try:
                dr_health = platform.dr_manager.check_replication_health()
                additional_checks["dr_replication"] = dr_health
            except Exception as e:
                additional_checks["dr_replication"] = {"status": "failed", "error": str(e)}

        # Check data quality
        if platform.data_quality_manager:
            try:
                quality_summary = {
                    "total_checks": len(platform.data_quality_manager.quality_checks),
                    "recent_results": len(platform.data_quality_manager.quality_results),
                    "status": "healthy",
                }
                additional_checks["data_quality"] = quality_summary
            except Exception as e:
                additional_checks["data_quality"] = {"status": "failed", "error": str(e)}

        health_check_result = {
            "platform_summary": summary,
            "additional_checks": additional_checks,
            "health_check_time": datetime.now().isoformat(),
            "overall_status": summary.get("health_status", {}).get("overall", "unknown"),
        }

        logger.info(f"Platform health check completed: {health_check_result['overall_status']}")
        return health_check_result

    except Exception as e:
        logger.error(f"Failed to run platform health check: {str(e)}")
        return {"error": str(e)}
