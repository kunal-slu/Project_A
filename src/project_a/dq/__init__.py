"""
Project A Data Quality Modules

DQ gates, validation utilities, and automation.
"""

from .gate import (
    DQCheckResult,
    run_dq_gate,
    run_not_null_checks,
    run_range_check,
    run_uniqueness_check,
    write_dq_result,
)
from .automation import (
    DataQualityProfiler,
    DataQualityChecker,
    get_dq_profiler,
    get_dq_checker
)

__all__ = [
    "DQCheckResult",
    "run_not_null_checks",
    "run_uniqueness_check",
    "run_range_check",
    "write_dq_result",
    "run_dq_gate",
    "DataQualityProfiler",
    "DataQualityChecker",
    "get_dq_profiler",
    "get_dq_checker"
]
