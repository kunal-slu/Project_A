"""
Metrics collection utility for observability.

Emits metrics to CloudWatch (AWS) or logs locally.
"""

import os
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from pathlib import Path

try:
    import boto3
    CLOUDWATCH_AVAILABLE = True
except ImportError:
    CLOUDWATCH_AVAILABLE = False
    boto3 = None

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collects and emits metrics for pipeline observability."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize metrics collector.
        
        Args:
            config: Configuration dict with AWS settings
        """
        self.config = config or {}
        self.environment = self.config.get('environment', os.getenv('ENV', 'local'))
        self.cloudwatch_enabled = self.environment != 'local' and CLOUDWATCH_AVAILABLE
        
        # CloudWatch client
        self.cloudwatch = None
        if self.cloudwatch_enabled:
            try:
                self.cloudwatch = boto3.client('cloudwatch', region_name=self.config.get('aws', {}).get('region', 'us-east-1'))
            except Exception as e:
                logger.warning(f"Could not initialize CloudWatch: {e}")
                self.cloudwatch_enabled = False
        
        # Local metrics file
        self.local_metrics_file = Path("data/metrics/pipeline_metrics.log")
        self.local_metrics_file.parent.mkdir(parents=True, exist_ok=True)
    
    def emit_rowcount(self, metric_name: str, value: int, labels: Optional[Dict[str, str]] = None):
        """
        Emit row count metric.
        
        Args:
            metric_name: Name of metric (e.g., 'records_extracted', 'records_bronze')
            value: Row count value
            labels: Additional labels (e.g., {'source': 'snowflake', 'layer': 'bronze'})
        """
        labels = labels or {}
        labels['environment'] = self.environment
        
        if self.cloudwatch_enabled and self.cloudwatch:
            try:
                namespace = self.config.get('monitoring', {}).get('cloudwatch_namespace', 'ETLPipelineMetrics')
                
                metric_data = {
                    'MetricName': metric_name,
                    'Value': float(value),
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                }
                
                # Add dimensions
                dimensions = [{'Name': k, 'Value': str(v)} for k, v in labels.items()]
                if dimensions:
                    metric_data['Dimensions'] = dimensions[:10]  # CloudWatch limit
                
                self.cloudwatch.put_metric_data(
                    Namespace=namespace,
                    MetricData=[metric_data]
                )
                
                logger.debug(f"Emitted CloudWatch metric: {metric_name}={value}")
            except Exception as e:
                logger.warning(f"Failed to emit CloudWatch metric: {e}")
        
        # Also log locally
        self._log_local_metric(metric_name, value, labels, "rowcount")
    
    def emit_duration(self, metric_name: str, milliseconds: float, labels: Optional[Dict[str, str]] = None):
        """
        Emit duration metric.
        
        Args:
            metric_name: Name of metric (e.g., 'extraction_duration', 'transformation_duration')
            milliseconds: Duration in milliseconds
            labels: Additional labels
        """
        labels = labels or {}
        labels['environment'] = self.environment
        
        if self.cloudwatch_enabled and self.cloudwatch:
            try:
                namespace = self.config.get('monitoring', {}).get('cloudwatch_namespace', 'ETLPipelineMetrics')
                
                metric_data = {
                    'MetricName': metric_name,
                    'Value': milliseconds,
                    'Unit': 'Milliseconds',
                    'Timestamp': datetime.utcnow()
                }
                
                dimensions = [{'Name': k, 'Value': str(v)} for k, v in labels.items()]
                if dimensions:
                    metric_data['Dimensions'] = dimensions[:10]
                
                self.cloudwatch.put_metric_data(
                    Namespace=namespace,
                    MetricData=[metric_data]
                )
                
                logger.debug(f"Emitted CloudWatch metric: {metric_name}={milliseconds}ms")
            except Exception as e:
                logger.warning(f"Failed to emit CloudWatch metric: {e}")
        
        # Also log locally
        self._log_local_metric(metric_name, milliseconds, labels, "duration")
    
    def _log_local_metric(self, metric_name: str, value: Any, labels: Dict[str, str], metric_type: str):
        """Log metric to local JSON file."""
        metric_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "metric_name": metric_name,
            "metric_type": metric_type,
            "value": value,
            "labels": labels
        }
        
        try:
            with open(self.local_metrics_file, "a") as f:
                f.write(json.dumps(metric_entry) + "\n")
        except Exception as e:
            logger.warning(f"Could not write local metric: {e}")


# Global instance (can be initialized per job)
_metrics_collector = None


def get_metrics_collector(config: Optional[Dict[str, Any]] = None) -> MetricsCollector:
    """Get or create global metrics collector."""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector(config)
    return _metrics_collector


def emit_rowcount(metric_name: str, value: int, labels: Optional[Dict[str, str]] = None, config: Optional[Dict[str, Any]] = None):
    """Convenience function to emit row count."""
    collector = get_metrics_collector(config)
    collector.emit_rowcount(metric_name, value, labels)


def emit_duration(metric_name: str, milliseconds: float, labels: Optional[Dict[str, str]] = None, config: Optional[Dict[str, Any]] = None):
    """Convenience function to emit duration."""
    collector = get_metrics_collector(config)
    collector.emit_duration(metric_name, milliseconds, labels)


def emit_metrics(
    job_name: str,
    rows_in: int,
    rows_out: int,
    duration_seconds: float,
    dq_status: str,
    config: Optional[Dict[str, Any]] = None
) -> None:
    """Emit standard metrics for a job run.

    Default sink is CloudWatch via put_metric_data; fallback logs a JSON line.
    """
    labels = {"job": job_name, "dq_status": dq_status}
    emit_rowcount("rows_in", rows_in, labels, config)
    emit_rowcount("rows_out", rows_out, labels, config)
    emit_duration("duration_ms", duration_seconds * 1000.0, labels, config)

