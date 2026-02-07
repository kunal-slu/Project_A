"""
Production Monitoring and Alerting System

This module provides comprehensive monitoring, metrics collection, health checks,
and alerting capabilities for production PySpark data engineering pipelines.
"""

import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

import psutil
import urllib.error
import urllib.request
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


@dataclass
class PipelineMetrics:
    """Pipeline execution metrics."""

    pipeline_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "running"
    total_records_processed: int = 0
    records_per_second: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    error_count: int = 0
    warning_count: int = 0
    stage_metrics: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data["start_time"] = self.start_time.isoformat()
        if self.end_time:
            data["end_time"] = self.end_time.isoformat()
        return data


@dataclass
class Alert:
    """Alert definition."""

    alert_id: str
    severity: str  # info, warning, error, critical
    message: str
    timestamp: datetime
    source: str
    context: Dict[str, Any] = None
    acknowledged: bool = False
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None

    def __post_init__(self):
        if self.context is None:
            self.context = {}


class MetricsCollector:
    """Collects and manages pipeline metrics."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.metrics: Dict[str, PipelineMetrics] = {}
        self._lock = threading.Lock()

    def start_pipeline(self, pipeline_name: str) -> str:
        """Start monitoring a pipeline."""
        with self._lock:
            pipeline_id = f"{pipeline_name}_{int(time.time())}"
            self.metrics[pipeline_id] = PipelineMetrics(
                pipeline_name=pipeline_name, start_time=datetime.utcnow()
            )
            logger.info(f"Started monitoring pipeline: {pipeline_id}")
            return pipeline_id

    def end_pipeline(self, pipeline_id: str, status: str = "completed"):
        """End monitoring a pipeline."""
        with self._lock:
            if pipeline_id in self.metrics:
                self.metrics[pipeline_id].end_time = datetime.utcnow()
                self.metrics[pipeline_id].status = status

                # Calculate final metrics
                end_time = self.metrics[pipeline_id].end_time
                if end_time is not None:
                    duration = (end_time - self.metrics[pipeline_id].start_time).total_seconds()
                    if duration > 0:
                        self.metrics[pipeline_id].records_per_second = (
                            self.metrics[pipeline_id].total_records_processed / duration
                        )

                logger.info(
                    f"Completed monitoring pipeline: {pipeline_id} - Status: {status}"
                )

    def update_metrics(self, pipeline_id: str, **kwargs):
        """Update pipeline metrics."""
        with self._lock:
            if pipeline_id in self.metrics:
                for key, value in kwargs.items():
                    if hasattr(self.metrics[pipeline_id], key):
                        setattr(self.metrics[pipeline_id], key, value)

    def get_pipeline_metrics(self, pipeline_id: str) -> Optional[PipelineMetrics]:
        """Get metrics for a specific pipeline."""
        return self.metrics.get(pipeline_id)

    def get_all_metrics(self) -> List[PipelineMetrics]:
        """Get all pipeline metrics."""
        return list(self.metrics.values())

    def collect_system_metrics(self) -> Dict[str, float]:
        """Collect system-level metrics."""
        try:
            # Spark metrics
            spark_metrics = {}
            if hasattr(self.spark, "sparkContext"):
                sc = self.spark.sparkContext
                spark_metrics = {
                    "executor_count": len(
                        sc._jsc.sc().statusTracker().getExecutorMetrics()
                    ),
                    "active_jobs": len(sc._jsc.sc().statusTracker().getActiveJobIds()),
                    "active_stages": len(
                        sc._jsc.sc().statusTracker().getActiveStageIds()
                    ),
                }

            # System metrics
            system_metrics = {
                "cpu_percent": psutil.cpu_percent(interval=1),
                "memory_percent": psutil.virtual_memory().percent,
                "disk_percent": psutil.disk_usage("/").percent,
                "network_io": psutil.net_io_counters()._asdict(),
            }

            return {**spark_metrics, **system_metrics}
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")
            return {}


class HealthChecker:
    """Performs health checks on various components."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.health_checks: Dict[str, Callable] = {}
        self.register_default_checks()

    def register_default_checks(self):
        """Register default health checks."""
        self.register_check("spark_session", self._check_spark_session)
        self.register_check("data_sources", self._check_data_sources)
        self.register_check("data_quality", self._check_data_quality)
        self.register_check("performance", self._check_performance)

    def register_check(self, name: str, check_function: Callable):
        """Register a custom health check."""
        self.health_checks[name] = check_function

    def run_health_checks(self) -> Dict[str, Dict[str, Any]]:
        """Run all registered health checks."""
        results = {}

        for name, check_func in self.health_checks.items():
            try:
                start_time = time.time()
                result = check_func()
                duration = time.time() - start_time

                results[name] = {
                    "status": (
                        "healthy" if result.get("healthy", False) else "unhealthy"
                    ),
                    "duration": duration,
                    "details": result,
                }
            except Exception as e:
                results[name] = {"status": "error", "duration": 0, "error": str(e)}

        return results

    def _check_spark_session(self) -> Dict[str, Any]:
        """Check Spark session health."""
        try:
            # Test basic operations
            test_df = self.spark.range(1)
            count = test_df.count()

            # Check available executors
            sc = self.spark.sparkContext
            executor_count = len(sc._jsc.sc().statusTracker().getExecutorMetrics())

            return {
                "healthy": True,
                "executor_count": executor_count,
                "test_operation": "successful",
                "test_count": count,
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}

    def _check_data_sources(self) -> Dict[str, Any]:
        """Check data source accessibility."""
        # This would check actual data sources in production
        # For now, return a mock check
        return {
            "healthy": True,
            "sources_accessible": True,
            "message": "Data sources check not implemented in development",
        }

    def _check_data_quality(self) -> Dict[str, Any]:
        """Check data quality metrics."""
        # This would run actual DQ checks in production
        return {
            "healthy": True,
            "quality_score": 0.95,
            "message": "Data quality check not implemented in development",
        }

    def _check_performance(self) -> Dict[str, Any]:
        """Check performance metrics."""
        try:
            system_metrics = {
                "cpu_percent": psutil.cpu_percent(interval=1),
                "memory_percent": psutil.virtual_memory().percent,
                "disk_percent": psutil.disk_usage("/").percent,
            }

            # Define thresholds
            thresholds = {"cpu_percent": 80, "memory_percent": 85, "disk_percent": 90}

            healthy = all(
                system_metrics[key] < threshold for key, threshold in thresholds.items()
            )

            return {
                "healthy": healthy,
                "metrics": system_metrics,
                "thresholds": thresholds,
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}


class AlertManager:
    """Manages alerts and notifications."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.alerts: List[Alert] = []
        self.alert_handlers: Dict[str, Callable] = {}
        self._setup_handlers()

    def _setup_handlers(self):
        """Setup alert handlers based on configuration."""
        if (
            self.config.get("monitoring", {})
            .get("notifications", {})
            .get("slack", {})
            .get("enabled")
        ):
            self.alert_handlers["slack"] = self._send_slack_alert

        if (
            self.config.get("monitoring", {})
            .get("notifications", {})
            .get("email", {})
            .get("enabled")
        ):
            self.alert_handlers["email"] = self._send_email_alert

    def create_alert(
        self,
        severity: str,
        message: str,
        source: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Alert:
        """Create a new alert."""
        alert = Alert(
            alert_id=f"alert_{int(time.time())}",
            severity=severity,
            message=message,
            timestamp=datetime.utcnow(),
            source=source,
            context=context or {},
        )

        self.alerts.append(alert)
        logger.warning(f"Alert created: {alert.alert_id} - {severity}: {message}")

        # Send notifications
        self._send_notifications(alert)

        return alert

    def _send_notifications(self, alert: Alert):
        """Send notifications for an alert."""
        for handler_name, handler_func in self.alert_handlers.items():
            try:
                handler_func(alert)
            except Exception as e:
                logger.error(f"Failed to send {handler_name} notification: {e}")

    def _send_slack_alert(self, alert: Alert):
        """Send Slack notification."""
        webhook_url = (
            self.config.get("monitoring", {})
            .get("notifications", {})
            .get("slack", {})
            .get("webhook_url")
        )
        if not webhook_url:
            return

        # Format Slack message
        color_map = {
            "info": "#36a64f",
            "warning": "#ffcc00",
            "error": "#ff6b6b",
            "critical": "#cc0000",
        }

        payload = {
            "attachments": [
                {
                    "color": color_map.get(alert.severity, "#36a64f"),
                    "title": f"Data Pipeline Alert: {alert.severity.upper()}",
                    "text": alert.message,
                    "fields": [
                        {"title": "Source", "value": alert.source, "short": True},
                        {
                            "title": "Time",
                            "value": alert.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC"),
                            "short": True,
                        },
                    ],
                    "footer": "PySpark Data Engineering Pipeline",
                }
            ]
        }

        try:
            data = json.dumps(payload).encode("utf-8")
            headers = {"Content-Type": "application/json"}
            req = urllib.request.Request(webhook_url, data=data, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=10) as resp:
                _ = resp.read()
            logger.info("Slack notification sent for alert: %s", alert.alert_id)
        except urllib.error.URLError as exc:
            logger.error("Failed to send Slack notification: %s", exc)

    def _send_email_alert(self, alert: Alert):
        """Send email notification."""
        # This would integrate with your email service (SES, SendGrid, etc.)
        logger.info(f"Email notification would be sent for alert: {alert.alert_id}")

    def get_alerts(
        self, severity: Optional[str] = None, acknowledged: Optional[bool] = None
    ) -> List[Alert]:
        """Get filtered alerts."""
        filtered_alerts = self.alerts

        if severity:
            filtered_alerts = [a for a in filtered_alerts if a.severity == severity]

        if acknowledged is not None:
            filtered_alerts = [
                a for a in filtered_alerts if a.acknowledged == acknowledged
            ]

        return filtered_alerts

    def acknowledge_alert(self, alert_id: str, user: str):
        """Acknowledge an alert."""
        for alert in self.alerts:
            if alert.alert_id == alert_id:
                alert.acknowledged = True
                alert.acknowledged_by = user
                alert.acknowledged_at = datetime.utcnow()
                logger.info(f"Alert {alert_id} acknowledged by {user}")
                break


class PipelineMonitor:
    """Main monitoring orchestrator."""

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.metrics_collector = MetricsCollector(spark)
        self.health_checker = HealthChecker(spark)
        self.alert_manager = AlertManager(config)
        self.monitoring_enabled = config.get("monitoring", {}).get("enabled", False)

        if not self.monitoring_enabled:
            logger.info("Monitoring is disabled in configuration")

    @contextmanager
    def monitor_pipeline(self, pipeline_name: str):
        """Context manager for monitoring pipeline execution."""
        if not self.monitoring_enabled:
            yield None
            return

        pipeline_id = self.metrics_collector.start_pipeline(pipeline_name)

        try:
            yield pipeline_id
            self.metrics_collector.end_pipeline(pipeline_id, "completed")
        except Exception as e:
            self.metrics_collector.end_pipeline(pipeline_id, "failed")
            self.alert_manager.create_alert(
                severity="error",
                message=f"Pipeline {pipeline_name} failed: {str(e)}",
                source="pipeline_execution",
                context={"pipeline_id": pipeline_id, "error": str(e)},
            )
            raise

    def run_health_check(self) -> Dict[str, Any]:
        """Run comprehensive health check."""
        if not self.monitoring_enabled:
            return {"status": "monitoring_disabled"}

        try:
            health_results = self.health_checker.run_health_checks()

            # Check if any checks failed
            failed_checks = [
                name
                for name, result in health_results.items()
                if result["status"] != "healthy"
            ]

            overall_healthy = len(failed_checks) == 0

            if not overall_healthy:
                self.alert_manager.create_alert(
                    severity="warning",
                    message=f"Health check failed for: {', '.join(failed_checks)}",
                    source="health_check",
                    context={"failed_checks": failed_checks, "results": health_results},
                )

            return {
                "status": "healthy" if overall_healthy else "unhealthy",
                "timestamp": datetime.utcnow().isoformat(),
                "results": health_results,
                "failed_checks": failed_checks,
            }

        except Exception as e:
            self.alert_manager.create_alert(
                severity="error",
                message=f"Health check failed: {str(e)}",
                source="health_check",
                context={"error": str(e)},
            )
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            }

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics."""
        if not self.monitoring_enabled:
            return {"status": "monitoring_disabled"}

        all_metrics = self.metrics_collector.get_all_metrics()
        system_metrics = self.metrics_collector.collect_system_metrics()

        # Calculate summary statistics
        total_pipelines = len(all_metrics)
        completed_pipelines = len([m for m in all_metrics if m.status == "completed"])
        failed_pipelines = len([m for m in all_metrics if m.status == "failed"])
        running_pipelines = len([m for m in all_metrics if m.status == "running"])

        total_records = sum(m.total_records_processed for m in all_metrics)
        avg_records_per_second = sum(m.records_per_second for m in all_metrics) / max(
            total_pipelines, 1
        )

        return {
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "pipeline_summary": {
                "total": total_pipelines,
                "completed": completed_pipelines,
                "failed": failed_pipelines,
                "running": running_pipelines,
            },
            "data_summary": {
                "total_records_processed": total_records,
                "average_records_per_second": avg_records_per_second,
            },
            "system_metrics": system_metrics,
            "recent_alerts": len(self.alert_manager.get_alerts(acknowledged=False)),
        }

    def cleanup_old_metrics(self, days_to_keep: int = 30):
        """Clean up old metrics data."""
        if not self.monitoring_enabled:
            return

        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)

        # Clean up old metrics
        old_metrics = [
            pipeline_id
            for pipeline_id, metrics in self.metrics_collector.metrics.items()
            if metrics.end_time and metrics.end_time < cutoff_date
        ]

        for pipeline_id in old_metrics:
            del self.metrics_collector.metrics[pipeline_id]

        # Clean up old alerts
        old_alerts = [
            alert
            for alert in self.alert_manager.alerts
            if alert.timestamp < cutoff_date
        ]

        for alert in old_alerts:
            self.alert_manager.alerts.remove(alert)

        logger.info(
            f"Cleaned up {len(old_metrics)} old metrics and {len(old_alerts)} old alerts"
        )


# Utility functions for easy monitoring
def create_monitor(spark: SparkSession, config: Dict[str, Any]) -> PipelineMonitor:
    """Create a monitoring instance."""
    return PipelineMonitor(spark, config)


def monitor_pipeline_execution(monitor: PipelineMonitor, pipeline_name: str):
    """Decorator for monitoring pipeline execution."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            with monitor.monitor_pipeline(pipeline_name):
                return func(*args, **kwargs)

        return wrapper

    return decorator
