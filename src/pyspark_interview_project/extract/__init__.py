"""
Extract module for data source-specific extraction functions.

Consolidated module-based architecture:
- BaseExtractor: Base class for all extractors  
- SalesforceExtractor, CRMExtractor, SnowflakeExtractor, RedshiftExtractor, FXRatesExtractor, HubSpotExtractor
- Factory function: get_extractor() for easy access
- Legacy wrapper functions for backward compatibility
"""

from .base_extractor import (
    BaseExtractor,
    SalesforceExtractor,
    CRMExtractor,
    SnowflakeExtractor,
    RedshiftExtractor,
    FXRatesExtractor,
    HubSpotExtractor,
    get_extractor,
    extract_incremental
)

# Legacy wrapper functions for backward compatibility
from .snowflake_orders import extract_snowflake_orders
from .redshift_behavior import extract_redshift_behavior
from .kafka_orders_stream import get_kafka_stream

# Wrapper for old extract_fx_rates function
def extract_fx_rates(spark, config, **kwargs):
    """Legacy wrapper for FX rates extraction."""
    from pyspark.sql import SparkSession
    from typing import Dict, Any
    
    extractor = FXRatesExtractor(config)
    return extractor.extract_with_metrics(spark, **kwargs)

__all__ = [
    'BaseExtractor',
    'SalesforceExtractor',
    'CRMExtractor',
    'SnowflakeExtractor',
    'RedshiftExtractor',
    'FXRatesExtractor',
    'HubSpotExtractor',
    'get_extractor',
    'extract_snowflake_orders',
    'extract_redshift_behavior',
    'extract_fx_rates',
    'get_kafka_stream',
    'extract_incremental',
]
