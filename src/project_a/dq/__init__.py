"""
Project A Data Quality Modules

DQ gates, validation utilities, and automation.
"""

from .automation import (
    DataQualityChecker,
    DataQualityProfiler,
)
from .automation import (
    get_checker as get_dq_checker,
)
from .automation import (
    get_profiler as get_dq_profiler,
)
from .gate import (
    DQCheckResult,
    DQGate,
    run_dq_gate,
    run_not_null_checks,
    run_range_check,
    run_uniqueness_check,
    write_dq_result,
)

__all__ = [
    "DQCheckResult",
    "DQGate",
    "run_not_null_checks",
    "run_uniqueness_check",
    "run_range_check",
    "write_dq_result",
    "run_dq_gate",
    "DataQualityProfiler",
    "DataQualityChecker",
    "get_dq_profiler",
    "get_dq_checker",
]
