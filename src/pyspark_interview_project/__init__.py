"""
PySpark Interview Project - AWS Production ETL Pipeline
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

# Core modules
from .pipeline_core import main as run_pipeline
from .utils.spark_session import build_spark

# Available functions
__all__ = [
    "run_pipeline",
    "build_spark",
]

# Package metadata
__package_name__ = "pyspark_interview_project"
__description__ = "AWS Production ETL Pipeline with PySpark"
