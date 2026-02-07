"""
CloudWatch metrics emitter for ETL pipeline monitoring.

Sends custom metrics to CloudWatch for observability and alerting.
"""
import os
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict

try:
    import boto3
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

logger = logging.getLogger(__name__)

# CloudWatch namespace for all ETL metrics
NAMESPACE = "etl/data-pipeline"


def put_metric(
    name: str,
    value: float,
    unit: str = "Count",
    dims: Optional[List[Dict[str, str]]] = None
):
    """
    Put a custom metric to CloudWatch.
    
    Args:
        name: Metric name
        value: Metric value
        unit: Unit type (Count, Bytes, Seconds, etc.)
        dims: List of dimension dictionaries
        
    Example:
        put_metric("customer_behavior_records", 10000, dims=[{"Name": "env", "Value": "dev"}])
    """
    if not BOTO3_AVAILABLE:
        logger.warning("boto3 not available, skipping CloudWatch metric")
        return
    
    try:
        cw = boto3.client(
            "cloudwatch",
            region_name=os.getenv("AWS_REGION", "us-east-1")
        )
        
        metric = {
            "MetricName": name,
            "Timestamp": datetime.now(timezone.utc),
            "Value": value,
            "Unit": unit
        }
        
        if dims:
            metric["Dimensions"] = dims
        
        cw.put_metric_data(
            Namespace=NAMESPACE,
            MetricData=[metric]
        )
        
        logger.debug(f"Metric emitted: {name}={value}")
        
    except Exception as e:
        logger.warning(f"Failed to emit CloudWatch metric: {e}")


def emit_duration(name: str, duration_seconds: float, dims: Optional[List[Dict[str, str]]] = None):
    """Emit duration metric to CloudWatch."""
    put_metric(name, duration_seconds, unit="Seconds", dims=dims)


def emit_count(name: str, count: int, dims: Optional[List[Dict[str, str]]] = None):
    """Emit count metric to CloudWatch."""
    put_metric(name, float(count), unit="Count", dims=dims)


def emit_size(name: str, size_bytes: int, dims: Optional[List[Dict[str, str]]] = None):
    """Emit size metric to CloudWatch."""
    put_metric(name, float(size_bytes), unit="Bytes", dims=dims)


def emit_dq_metrics(results: dict, suite_name: str, layer: str):
    """
    Emit DQ validation metrics to CloudWatch.
    
    Args:
        results: DQ validation results dict
        suite_name: DQ suite name
        layer: Data layer (bronze, silver, gold)
    """
    dims = [{"Name": "suite", "Value": suite_name}, {"Name": "layer", "Value": layer}]
    
    emit_count(
        "dq_validations_total",
        len(results.get("expectations", [])),
        dims=dims
    )
    
    emit_count(
        "dq_critical_failures",
        results.get("critical_failures", 0),
        dims=dims
    )
    
    emit_count(
        "dq_warning_failures",
        results.get("warning_failures", 0),
        dims=dims
    )
    
    # Pass rate percentage
    if results.get("expectations"):
        pass_rate = (results.get("passed", 0) / len(results["expectations"])) * 100
        put_metric("dq_pass_rate", pass_rate, unit="Percent", dims=dims)
