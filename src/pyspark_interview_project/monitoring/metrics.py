"""
Prometheus metrics for ETL pipeline monitoring.
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Summary,
    CollectorRegistry,
    push_to_gateway,
    generate_latest,
)
import time
from functools import wraps

logger = logging.getLogger(__name__)

# Create a custom registry
registry = CollectorRegistry()

# ============================================
# Pipeline Metrics
# ============================================

# Job execution metrics
job_executions_total = Counter(
    'etl_job_executions_total',
    'Total number of ETL job executions',
    ['job_name', 'status', 'environment'],
    registry=registry
)

job_duration_seconds = Histogram(
    'etl_job_duration_seconds',
    'ETL job execution duration in seconds',
    ['job_name', 'stage', 'environment'],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600],
    registry=registry
)

job_last_success_timestamp = Gauge(
    'etl_job_last_success_timestamp',
    'Timestamp of last successful job execution',
    ['job_name', 'environment'],
    registry=registry
)

# Data quality metrics
dq_checks_total = Counter(
    'etl_dq_checks_total',
    'Total number of data quality checks',
    ['table', 'check_type', 'status'],
    registry=registry
)

dq_violations_total = Counter(
    'etl_dq_violations_total',
    'Total number of data quality violations',
    ['table', 'check_type', 'severity'],
    registry=registry
)

# Data processing metrics
records_processed_total = Counter(
    'etl_records_processed_total',
    'Total number of records processed',
    ['job_name', 'stage', 'table'],
    registry=registry
)

records_failed_total = Counter(
    'etl_records_failed_total',
    'Total number of records that failed processing',
    ['job_name', 'stage', 'table', 'error_type'],
    registry=registry
)

# Delta Lake metrics
delta_table_size_bytes = Gauge(
    'delta_table_size_bytes',
    'Size of Delta Lake table in bytes',
    ['table', 'layer'],
    registry=registry
)

delta_table_versions = Gauge(
    'delta_table_versions',
    'Number of versions for Delta Lake table',
    ['table', 'layer'],
    registry=registry
)

delta_table_files = Gauge(
    'delta_table_files',
    'Number of files in Delta Lake table',
    ['table', 'layer'],
    registry=registry
)

delta_write_duration_seconds = Histogram(
    'delta_write_duration_seconds',
    'Delta Lake write operation duration',
    ['table', 'operation', 'layer'],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60],
    registry=registry
)

# Schema metrics
schema_drift_detected_total = Counter(
    'etl_schema_drift_detected_total',
    'Total number of schema drift detections',
    ['table', 'drift_type'],
    registry=registry
)

# Resource metrics
memory_usage_bytes = Gauge(
    'etl_memory_usage_bytes',
    'Memory usage in bytes',
    ['job_name', 'stage'],
    registry=registry
)

cpu_usage_percent = Gauge(
    'etl_cpu_usage_percent',
    'CPU usage percentage',
    ['job_name', 'stage'],
    registry=registry
)

# Error metrics
errors_total = Counter(
    'etl_errors_total',
    'Total number of errors',
    ['job_name', 'stage', 'error_type', 'severity'],
    registry=registry
)

# ============================================
# Metric Helper Functions
# ============================================

def track_job_execution(job_name: str, environment: str = "local"):
    """
    Decorator to track job execution metrics.
    
    Usage:
        @track_job_execution("bronze_to_silver", "prod")
        def my_etl_job():
            # job logic
            pass
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            status = "success"
            
            try:
                result = func(*args, **kwargs)
                job_last_success_timestamp.labels(
                    job_name=job_name,
                    environment=environment
                ).set_to_current_time()
                return result
                
            except Exception as e:
                status = "failure"
                errors_total.labels(
                    job_name=job_name,
                    stage="execution",
                    error_type=type(e).__name__,
                    severity="critical"
                ).inc()
                raise
                
            finally:
                duration = time.time() - start_time
                job_executions_total.labels(
                    job_name=job_name,
                    status=status,
                    environment=environment
                ).inc()
                
                job_duration_seconds.labels(
                    job_name=job_name,
                    stage="full",
                    environment=environment
                ).observe(duration)
                
                logger.info(
                    f"Job {job_name} completed with status {status} "
                    f"in {duration:.2f}s"
                )
        
        return wrapper
    return decorator


def track_stage_duration(job_name: str, stage: str, environment: str = "local"):
    """
    Context manager to track stage duration.
    
    Usage:
        with track_stage_duration("etl_pipeline", "extract", "prod"):
            # extraction logic
            pass
    """
    class StageTracker:
        def __enter__(self):
            self.start_time = time.time()
            logger.info(f"Starting stage: {stage}")
            return self
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            duration = time.time() - self.start_time
            job_duration_seconds.labels(
                job_name=job_name,
                stage=stage,
                environment=environment
            ).observe(duration)
            
            if exc_type:
                errors_total.labels(
                    job_name=job_name,
                    stage=stage,
                    error_type=exc_type.__name__,
                    severity="error"
                ).inc()
                logger.error(f"Stage {stage} failed after {duration:.2f}s: {exc_val}")
            else:
                logger.info(f"Stage {stage} completed in {duration:.2f}s")
    
    return StageTracker()


def record_dq_check(table: str, check_type: str, passed: bool, 
                   violations: int = 0, severity: str = "warning"):
    """Record data quality check results."""
    status = "passed" if passed else "failed"
    
    dq_checks_total.labels(
        table=table,
        check_type=check_type,
        status=status
    ).inc()
    
    if violations > 0:
        dq_violations_total.labels(
            table=table,
            check_type=check_type,
            severity=severity
        ).inc(violations)
    
    logger.info(
        f"DQ check {check_type} for {table}: {status} "
        f"(violations: {violations})"
    )


def record_records_processed(job_name: str, stage: str, table: str, count: int):
    """Record number of records processed."""
    records_processed_total.labels(
        job_name=job_name,
        stage=stage,
        table=table
    ).inc(count)
    
    logger.info(f"Processed {count} records in {table} ({stage})")


def record_records_failed(job_name: str, stage: str, table: str, 
                         count: int, error_type: str):
    """Record number of records that failed processing."""
    records_failed_total.labels(
        job_name=job_name,
        stage=stage,
        table=table,
        error_type=error_type
    ).inc(count)
    
    logger.warning(f"Failed {count} records in {table} ({stage}): {error_type}")


def record_delta_table_metrics(table: str, layer: str, metrics: Dict[str, Any]):
    """Record Delta Lake table metrics."""
    if "size_bytes" in metrics:
        delta_table_size_bytes.labels(table=table, layer=layer).set(metrics["size_bytes"])
    
    if "version_count" in metrics:
        delta_table_versions.labels(table=table, layer=layer).set(metrics["version_count"])
    
    if "file_count" in metrics:
        delta_table_files.labels(table=table, layer=layer).set(metrics["file_count"])
    
    logger.info(f"Recorded Delta metrics for {layer}.{table}: {metrics}")


def record_delta_write(table: str, operation: str, layer: str, duration: float):
    """Record Delta Lake write operation duration."""
    delta_write_duration_seconds.labels(
        table=table,
        operation=operation,
        layer=layer
    ).observe(duration)
    
    logger.info(f"Delta {operation} on {layer}.{table} took {duration:.2f}s")


def record_schema_drift(table: str, drift_type: str):
    """Record schema drift detection."""
    schema_drift_detected_total.labels(
        table=table,
        drift_type=drift_type
    ).inc()
    
    logger.warning(f"Schema drift detected in {table}: {drift_type}")


def record_error(job_name: str, stage: str, error_type: str, severity: str = "error"):
    """Record an error occurrence."""
    errors_total.labels(
        job_name=job_name,
        stage=stage,
        error_type=error_type,
        severity=severity
    ).inc()
    
    logger.error(f"Error in {job_name}/{stage}: {error_type} (severity: {severity})")


def push_metrics_to_gateway(gateway_url: str, job_name: str):
    """
    Push metrics to Prometheus Pushgateway.
    
    Args:
        gateway_url: URL of the Pushgateway (e.g., "localhost:9091")
        job_name: Job name for metric grouping
    """
    try:
        push_to_gateway(gateway_url, job=job_name, registry=registry)
        logger.info(f"Pushed metrics to gateway for job {job_name}")
    except Exception as e:
        logger.error(f"Failed to push metrics to gateway: {e}")


def get_metrics_text() -> bytes:
    """
    Get metrics in Prometheus text format.
    
    Returns:
        Metrics in Prometheus exposition format
    """
    return generate_latest(registry)


def export_metrics_to_file(file_path: str):
    """
    Export metrics to a file.
    
    Args:
        file_path: Path to write metrics file
    """
    try:
        with open(file_path, 'wb') as f:
            f.write(get_metrics_text())
        logger.info(f"Exported metrics to {file_path}")
    except Exception as e:
        logger.error(f"Failed to export metrics to file: {e}")


# ============================================
# Resource Monitoring
# ============================================

def update_resource_metrics(job_name: str, stage: str):
    """Update resource usage metrics."""
    try:
        import psutil
        
        process = psutil.Process()
        memory_info = process.memory_info()
        cpu_percent = process.cpu_percent(interval=0.1)
        
        memory_usage_bytes.labels(
            job_name=job_name,
            stage=stage
        ).set(memory_info.rss)
        
        cpu_usage_percent.labels(
            job_name=job_name,
            stage=stage
        ).set(cpu_percent)
        
        logger.debug(
            f"Resource usage - Memory: {memory_info.rss / 1024 / 1024:.2f}MB, "
            f"CPU: {cpu_percent:.1f}%"
        )
        
    except ImportError:
        logger.debug("psutil not available, skipping resource metrics")
    except Exception as e:
        logger.warning(f"Failed to update resource metrics: {e}")


# ============================================
# Example Usage
# ============================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Example: Track a job
    @track_job_execution("example_etl", "dev")
    def example_job():
        with track_stage_duration("example_etl", "extract", "dev"):
            record_records_processed("example_etl", "extract", "customers", 1000)
            time.sleep(1)
        
        with track_stage_duration("example_etl", "transform", "dev"):
            record_records_processed("example_etl", "transform", "customers", 1000)
            record_dq_check("customers", "null_check", True)
            time.sleep(0.5)
        
        with track_stage_duration("example_etl", "load", "dev"):
            record_delta_table_metrics("customers", "bronze", {
                "size_bytes": 1024 * 1024,
                "version_count": 5,
                "file_count": 10
            })
            time.sleep(0.3)
    
    # Run example
    example_job()
    
    # Export metrics
    print("\nGenerated Metrics:")
    print(get_metrics_text().decode('utf-8'))

