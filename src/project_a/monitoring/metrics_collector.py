"""Metrics emitter for CloudWatch/local execution."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import boto3


def _cw_client(config: dict[str, Any]):
    region = config.get("aws", {}).get("region", "us-east-1")
    return boto3.client("cloudwatch", region_name=region)


def _namespace(config: dict[str, Any]) -> str:
    return (
        config.get("monitoring", {})
        .get("cloudwatch", {})
        .get("namespace", "ETL/Pipeline")
    )


def emit_rowcount(
    metric_name: str,
    value: int,
    labels: dict[str, str] | None = None,
    config: dict[str, Any] | None = None,
):
    config = config or {}
    if not config.get("monitoring", {}).get("metrics_enabled", False):
        return

    metric = {
        "MetricName": metric_name,
        "Timestamp": datetime.utcnow(),
        "Value": float(value),
        "Unit": "Count",
    }
    if labels:
        metric["Dimensions"] = [{"Name": k, "Value": str(v)} for k, v in labels.items()][:10]
    _cw_client(config).put_metric_data(Namespace=_namespace(config), MetricData=[metric])


def emit_duration(
    metric_name: str,
    duration_seconds: float,
    labels: dict[str, str] | None = None,
    config: dict[str, Any] | None = None,
):
    config = config or {}
    if not config.get("monitoring", {}).get("metrics_enabled", False):
        return

    metric = {
        "MetricName": metric_name,
        "Timestamp": datetime.utcnow(),
        "Value": float(duration_seconds),
        "Unit": "Seconds",
    }
    if labels:
        metric["Dimensions"] = [{"Name": k, "Value": str(v)} for k, v in labels.items()][:10]
    _cw_client(config).put_metric_data(Namespace=_namespace(config), MetricData=[metric])


def emit_metrics(
    job_name: str,
    rows_in: int,
    rows_out: int,
    duration_seconds: float,
    dq_status: str,
    config: dict[str, Any] | None = None,
) -> None:
    labels = {"job_name": job_name, "dq_status": dq_status}
    emit_rowcount("rows_in", rows_in, labels, config)
    emit_rowcount("rows_out", rows_out, labels, config)
    emit_duration("duration_seconds", duration_seconds, labels, config)

