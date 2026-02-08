"""
Utility module for PySpark Interview Project.

This module provides core utilities for:
- Spark session management
- Configuration loading
- Logging setup
- IO operations
- Path resolution
- Data quality checks
- Metrics collection
"""

from .config import load_conf
from .dq_utils import generate_dq_report, run_dq_suite, validate_not_null, validate_unique
from .io import read_delta, write_delta
from .logging import setup_json_logging
from .metrics import (
    emit_metric,
    track_dq_check,
    track_job_complete,
    track_job_start,
    track_records_processed,
)
from .path_resolver import bronze_path, gold_path, resolve_lake_path, silver_path
from .spark_session import build_spark

# Add alias for backward compatibility
get_spark_session = build_spark

__all__ = [
    "build_spark",
    "get_spark_session",  # Add alias
    "load_conf",
    "setup_json_logging",
    "read_delta",
    "write_delta",
    "resolve_lake_path",
    "bronze_path",
    "silver_path",
    "gold_path",
    "run_dq_suite",
    "validate_not_null",
    "validate_unique",
    "generate_dq_report",
    "emit_metric",
    "track_job_start",
    "track_job_complete",
    "track_records_processed",
    "track_dq_check",
]
