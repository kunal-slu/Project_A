"""
PySpark Interview Project - AWS Production ETL Pipeline
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

# Core modules
from .pipeline_core import main as run_pipeline
from .utils.spark_session import build_spark
from .config_loader import load_config_resolved
from .extract import (
    extract_customers,
    extract_products,
    extract_orders_json,
    extract_returns,
    extract_exchange_rates,
    extract_inventory_snapshots
)
from .load import write_delta, write_parquet

# Available functions
__all__ = [
    "run_pipeline",
    "build_spark",
    "load_config_resolved",
    "extract_customers",
    "extract_products",
    "extract_orders_json",
    "extract_returns",
    "extract_exchange_rates",
    "extract_inventory_snapshots",
    "write_delta",
    "write_parquet",
]

# Package metadata
__package_name__ = "pyspark_interview_project"
__description__ = "AWS Production ETL Pipeline with PySpark"
