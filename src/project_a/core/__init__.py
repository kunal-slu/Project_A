"""
Core platform classes for Project A.

Provides:
- ProjectConfig: Configuration management
- JobContext: Spark + Delta builder context
- BaseJob: Abstract base class for all ETL jobs
"""

from project_a.core.config import ProjectConfig
from project_a.core.context import JobContext
from project_a.core.base_job import BaseJob

__all__ = ["ProjectConfig", "JobContext", "BaseJob"]

