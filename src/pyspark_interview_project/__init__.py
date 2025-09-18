"""
PySpark Interview Project - Enterprise Data Platform

A comprehensive Azure data engineering platform with:
- Unity Catalog integration
- Azure Security & Compliance
- Disaster Recovery & High Availability
- Advanced Data Quality Monitoring
- CI/CD Pipeline Management
- Delta Lake utilities
- Streaming & Batch processing
"""

__version__ = "0.1.0"
__author__ = "Data Engineering Team"

# Core platform components
from .enterprise_data_platform import EnterpriseDataPlatform
from .unity_catalog import UnityCatalogManager
from .azure_security import AzureSecurityManager
from .disaster_recovery import DisasterRecoveryExecutor
from .advanced_dq_monitoring import AdvancedDataQualityManager, QualitySeverity, QualityStatus
from .cicd_manager import CICDManager, Environment, DeploymentType
from .monitoring import PipelineMonitor, create_monitor
from .delta_utils import DeltaUtils
from .performance_optimizer import PerformanceOptimizer, create_performance_optimizer

# Data processing components
from .pipeline import run_pipeline
from .extract import (
    extract_customers,
    extract_products,
    extract_orders_json,
    extract_returns,
    extract_exchange_rates,
    extract_inventory_snapshots
)
from .transform import (
    select_and_filter,
    join_examples,
    broadcast_join_demo,
    skew_mitigation_demo,
    window_functions_demo,
    udf_examples,
    data_cleaning_examples,
    enrich_customers,
    enrich_products,
    clean_orders,
    build_fact_orders,
    normalize_currency,
    join_returns,
    join_inventory,
    optimize_for_analytics,
    adaptive_join_strategy,
    build_customers_scd2
)
from .load import (
    write_parquet,
    write_delta,
    read_delta,
    write_avro,
    write_json
)
from .io_utils import write_delta as write_delta_safe, write_parquet as write_parquet_safe, read_delta_or_parquet
from .validate import ValidateOutput

# Configuration and utilities
from .config_loader import ConfigLoader, load_config_resolved
from .utils.spark_session import build_spark

# Legacy components (kept for backward compatibility)
from .dq_checks import DQChecks
from .modeling import (
    add_surrogate_key,
    build_dim_date,
    build_dim_products_scd2_base,
    build_dim_category,
    build_dim_brand,
    normalize_dim_products,
    build_dim_geography,
    normalize_dim_customers
)

__all__ = [
    # Core platform
    "EnterpriseDataPlatform",
    "UnityCatalogManager",
    "AzureSecurityManager",
    "DisasterRecoveryExecutor",
    "AdvancedDataQualityManager",
    "QualitySeverity",
    "QualityStatus",
    "CICDManager",
    "Environment",
    "DeploymentType",
    "PipelineMonitor",
    "create_monitor",
    "DeltaUtils",
    "PerformanceOptimizer",
    "create_performance_optimizer",

    # Data processing
    "run_pipeline",
    "extract_customers",
    "extract_products",
    "extract_orders_json",
    "extract_returns",
    "extract_exchange_rates",
    "extract_inventory_snapshots",
    "select_and_filter",
    "join_examples",
    "broadcast_join_demo",
    "skew_mitigation_demo",
    "window_functions_demo",
    "udf_examples",
    "data_cleaning_examples",
    "enrich_customers",
    "enrich_products",
    "clean_orders",
    "build_fact_orders",
    "normalize_currency",
    "join_returns",
    "join_inventory",
    "optimize_for_analytics",
    "adaptive_join_strategy",
    "build_customers_scd2",
    "write_parquet",
    "write_delta",
    "read_delta",
    "write_avro",
    "write_json",
    "write_delta_safe",
    "write_parquet_safe",
    "read_delta_or_parquet",
    "ValidateOutput",

    # Configuration
    "ConfigLoader",
    "load_config_resolved",
    "build_spark",

    # Legacy
    "DQChecks",
    "add_surrogate_key",
    "build_dim_date",
    "build_dim_products_scd2_base",
    "build_dim_category",
    "build_dim_brand",
    "normalize_dim_products",
    "build_dim_geography",
    "normalize_dim_customers",
]
