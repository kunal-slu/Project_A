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

from .spark_session import build_spark
from .config import load_conf
from .logging import setup_json_logging
from .io import read_delta, write_delta
from .path_resolver import (
    resolve_lake_path,
    bronze_path,
    silver_path,
    gold_path
)
from .dq_utils import (
    run_dq_suite,
    validate_not_null,
    validate_unique,
    generate_dq_report
)
from .metrics import (
    emit_metric,
    track_job_start,
    track_job_complete,
    track_records_processed,
    track_dq_check
)

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
