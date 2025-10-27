"""
Path resolver utilities for logical to physical path mapping.

Converts logical paths like 'lake://bronze/...' to physical S3 paths
like 's3a://bucket/bronze/...' based on configuration.
"""

import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


def resolve_lake_path(logical_path: str, config: Dict[str, Any]) -> str:
    """
    Resolve a logical lake path to physical S3 path.
    
    Args:
        logical_path: Logical path like 'lake://bronze/source/table'
        config: Configuration dictionary with lake paths
        
    Returns:
        Physical S3 path like 's3a://bucket/bronze/source/table'
    """
    try:
        if logical_path.startswith("lake://"):
            layer = logical_path.split("/")[1]  # bronze, silver, gold
            rest_of_path = "/".join(logical_path.split("/")[2:])
            
            lake_root = config.get("lake", {}).get("root", "s3://default")
            if lake_root.endswith("/"):
                lake_root = lake_root[:-1]
                
            physical_path = f"{lake_root}/{layer}/{rest_of_path}"
            
            logger.debug(f"Resolved {logical_path} → {physical_path}")
            return physical_path
        else:
            # Already a physical path
            return logical_path
            
    except Exception as e:
        logger.error(f"Failed to resolve path {logical_path}: {e}")
        raise


def bronze_path(bucket: str, source: str, object_name: str) -> str:
    """
    Generate bronze layer S3 path.
    
    Args:
        bucket: S3 bucket name
        source: Source system name (e.g., 'salesforce', 'snowflake')
        object_name: Object/table name
        
    Returns:
        S3 path string
    """
    return f"s3://{bucket}/bronze/{source}/{object_name}/"


def silver_path(bucket: str, table_name: str) -> str:
    """
    Generate silver layer S3 path.
    
    Args:
        bucket: S3 bucket name
        table_name: Table name
        
    Returns:
        S3 path string
    """
    return f"s3://{bucket}/silver/{table_name}/"


def gold_path(bucket: str, table_name: str) -> str:
    """
    Generate gold layer S3 path.
    
    Args:
        bucket: S3 bucket name
        table_name: Table name
        
    Returns:
        S3 path string
    """
    return f"s3://{bucket}/gold/{table_name}/"

