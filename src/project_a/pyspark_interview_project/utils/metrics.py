"""
Metrics collection utilities for runtime monitoring.

Provides functions for emitting metrics to CloudWatch, Prometheus,
and other monitoring systems.
"""

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


def emit_metric(
    metric_name: str, value: float, unit: str = "Count", dimensions: dict[str, str] = None
) -> None:
    """
    Emit a metric to monitoring systems.

    Args:
        metric_name: Name of the metric
        value: Metric value
        unit: Unit of measurement
        dimensions: Metric dimensions/tags
    """
    dimensions = dimensions or {}

    metric_data = {
        "timestamp": datetime.utcnow().isoformat(),
        "metric_name": metric_name,
        "value": value,
        "unit": unit,
        "dimensions": dimensions,
    }

    logger.info(f"Metric: {metric_data}")


def track_job_start(job_name: str, config: dict[str, Any]) -> str:
    """Track job start event and return job ID."""
    job_id = f"{job_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    logger.info(f"Job started: {job_name}")
    emit_metric("jobs.started", value=1.0, dimensions={"job_name": job_name})
    return job_id


def track_job_complete(
    job_id: str, status: str, record_count: int = 0, error_msg: str = None
) -> None:
    """Track job completion event."""
    logger.info(f"Job completed: {job_id} with status: {status}")
    emit_metric("jobs.completed", value=1.0, dimensions={"job_id": job_id, "status": status})
    if record_count > 0:
        emit_metric("records.processed", value=float(record_count), dimensions={"job_id": job_id})


def track_records_processed(job_id: str, table_name: str, record_count: int) -> None:
    """Track records processed."""
    emit_metric(
        "records.processed",
        value=float(record_count),
        dimensions={"job_id": job_id, "table_name": table_name},
    )


def track_dq_check(suite_name: str, passed: bool) -> None:
    """Track data quality check result."""
    emit_metric(
        "dq.checks",
        value=1.0,
        dimensions={"suite_name": suite_name, "status": "passed" if passed else "failed"},
    )
