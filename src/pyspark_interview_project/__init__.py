"""
PySpark Data Engineering Project

A production-ready data engineering platform with Bronze/Silver/Gold architecture.
"""

__version__ = "0.1.0"
__author__ = "Data Engineering Team"

# Core utilities
from .utils.config import load_conf
from .utils.spark import get_spark_session, is_local
from .utils.io import read_delta, write_delta
from .utils.logging import setup_json_logging, get_trace_id, log_with_trace

# Data quality
from .dq.runner import run_yaml_policy, print_dq_summary

# Schema validation
from .schema.validator import SchemaValidator

__all__ = [
    "load_conf",
    "get_spark_session", 
    "is_local",
    "read_delta",
    "write_delta",
    "setup_json_logging",
    "get_trace_id",
    "log_with_trace",
    "run_yaml_policy",
    "print_dq_summary",
    "SchemaValidator"
]