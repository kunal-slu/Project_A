"""
CloudWatch Metrics Emission Utility

Emits custom metrics to CloudWatch for EMR job monitoring.
"""

import logging
import os

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def _should_emit_cloudwatch(dimensions: dict[str, str] | None) -> bool:
    """Skip CloudWatch API calls for local runs unless explicitly enabled."""
    if os.environ.get("PROJECT_A_DISABLE_CLOUDWATCH", "0").lower() in {"1", "true", "yes"}:
        return False
    env = (dimensions or {}).get("Env", "").lower()
    if env in {"local", "dev_local"}:
        return False
    return True


def put_metric(
    namespace: str,
    metric_name: str,
    value: float,
    dimensions: dict[str, str] | None = None,
    unit: str = "Count",
) -> None:
    """
    Put a custom metric to CloudWatch.

    Args:
        namespace: CloudWatch namespace (e.g., "ProjectA/EMR")
        metric_name: Name of the metric
        value: Metric value
        dimensions: Optional dimensions (e.g., {"JobName": "bronze_to_silver", "Env": "dev"})
        unit: Unit of measurement (Count, Seconds, Bytes, etc.)
    """
    if not _should_emit_cloudwatch(dimensions):
        logger.debug("Skipping CloudWatch metric in local mode: %s", metric_name)
        return

    try:
        cw = boto3.client("cloudwatch", region_name="us-east-1")

        metric_data = {
            "MetricName": metric_name,
            "Value": value,
            "Unit": unit,
        }

        if dimensions:
            metric_data["Dimensions"] = [{"Name": k, "Value": v} for k, v in dimensions.items()]

        cw.put_metric_data(Namespace=namespace, MetricData=[metric_data])

        logger.debug(f"✅ Emitted metric: {namespace}/{metric_name}={value}")

    except ClientError as e:
        logger.warning(f"⚠️  Failed to emit metric {metric_name}: {e}")
    except Exception as e:
        logger.warning(f"⚠️  Unexpected error emitting metric: {e}")


def emit_job_success(
    job_name: str, duration_seconds: float, env: str = "dev", rows_processed: int | None = None
) -> None:
    """
    Emit metrics for a successful job run.

    Args:
        job_name: Name of the job (e.g., "bronze_to_silver")
        duration_seconds: Job duration in seconds
        env: Environment (dev/staging/prod)
        rows_processed: Optional number of rows processed
    """
    dimensions = {"JobName": job_name, "Env": env}

    # Emit success metric
    put_metric(
        namespace="ProjectA/EMR", metric_name="EMRJobSuccess", value=1.0, dimensions=dimensions
    )

    # Emit duration metric
    put_metric(
        namespace="ProjectA/EMR",
        metric_name="EMRJobDurationSeconds",
        value=duration_seconds,
        dimensions=dimensions,
        unit="Seconds",
    )

    # Emit rows processed if provided
    if rows_processed is not None:
        put_metric(
            namespace="ProjectA/EMR",
            metric_name="EMRJobRowsProcessed",
            value=float(rows_processed),
            dimensions=dimensions,
            unit="Count",
        )


def emit_job_failure(job_name: str, env: str = "dev", error_type: str | None = None) -> None:
    """
    Emit metrics for a failed job run.

    Args:
        job_name: Name of the job
        env: Environment (dev/staging/prod)
        error_type: Optional error type/class name
    """
    dimensions = {"JobName": job_name, "Env": env}

    if error_type:
        dimensions["ErrorType"] = error_type

    # Emit failure metric
    put_metric(
        namespace="ProjectA/EMR", metric_name="EMRJobFailures", value=1.0, dimensions=dimensions
    )


def emit_data_quality_metric(
    table_name: str, check_name: str, passed: bool, env: str = "dev"
) -> None:
    """
    Emit data quality check metrics.

    Args:
        table_name: Name of the table checked
        check_name: Name of the DQ check
        passed: Whether the check passed
        env: Environment
    """
    dimensions = {"TableName": table_name, "CheckName": check_name, "Env": env}

    metric_name = "DQCheckPassed" if passed else "DQCheckFailed"

    put_metric(namespace="ProjectA/DQ", metric_name=metric_name, value=1.0, dimensions=dimensions)
