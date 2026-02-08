"""
Project A - Enterprise Data Platform

PySpark + Delta + Airflow data platform for production use.
"""

from project_a import extract
from project_a.config_loader import load_config_resolved
from project_a.utils.spark_session import build_spark

__version__ = "0.1.0"
__author__ = "Data Engineering Team"

__all__ = [
    "__version__",
    "__author__",
    "build_spark",
    "load_config_resolved",
    "extract",
    "extract_customers",
]


def extract_customers(spark, path: str):
    """Compatibility helper used by legacy integration tests."""
    return extract.extract_customers(spark, path)
