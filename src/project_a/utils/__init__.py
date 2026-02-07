"""
Project A Utilities

Core utilities for data platform operations.
"""

from .cloudwatch_metrics import emit_job_failure, emit_job_success, put_metric
from .config import load_config, load_config_resolved
from .error_lanes import ErrorLaneHandler
from .logging import get_trace_id, setup_json_logging
from .run_audit import read_run_audit, write_run_audit
from .spark_session import build_spark, get_spark
from .schema_validator import SchemaValidator

__all__ = [
    "build_spark",
    "get_spark",
    "load_config",
    "load_config_resolved",
    "setup_json_logging",
    "get_trace_id",
    "write_run_audit",
    "read_run_audit",
    "emit_job_success",
    "emit_job_failure",
    "put_metric",
    "ErrorLaneHandler",
    "SchemaValidator",
]
