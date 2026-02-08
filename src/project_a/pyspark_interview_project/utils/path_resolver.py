"""
Path resolver utilities for logical to physical path mapping.

Converts logical paths like 'lake://bronze/...' to physical S3 paths
like 's3a://bucket/bronze/...' based on configuration.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def resolve_path(logical_path: str, *args, **kwargs) -> str:
    """
    Resolve a logical path to physical path.

    Args:
        logical_path: Logical path like 'lake://bronze', 'lake://silver/customer_behavior', or absolute path
        *args: Optional table name (e.g., 'customer_behavior')
        **kwargs: Optional 'run_id' for timestamped paths

    Returns:
        Physical path like 's3a://bucket/bronze/customer_behavior' or 'data/lakehouse_delta/bronze/customer_behavior'
    """
    config = kwargs.get("config")
    if config is None:
        # If no config, return as-is (likely already physical)
        result = logical_path
        if args:
            result = f"{result}/{'/'.join(args)}"
        return result

    try:
        # Handle lake:// prefix
        if logical_path.startswith("lake://"):
            layer = (
                logical_path.split("/")[1]
                if "/" in logical_path[7:]
                else logical_path.replace("lake://", "")
            )
            rest_of_path = "/".join(logical_path.split("/")[2:]) if "/" in logical_path[7:] else ""

            # Get base path for layer from config
            paths_cfg = config.get("paths", {})
            base_path = paths_cfg.get(layer, "")

            if not base_path:
                # Fallback: try data_lake paths
                data_lake = config.get("data_lake", {})
                if layer == "bronze":
                    base_path = data_lake.get("bronze_path", "data/lakehouse_delta/bronze")
                elif layer == "silver":
                    base_path = data_lake.get("silver_path", "data/lakehouse_delta/silver")
                elif layer == "gold":
                    base_path = data_lake.get("gold_path", "data/lakehouse_delta/gold")

            # Clean trailing slashes
            if base_path.endswith("/"):
                base_path = base_path[:-1]
            if rest_of_path and not rest_of_path.startswith("/"):
                rest_of_path = "/" + rest_of_path

            physical_path = f"{base_path}{rest_of_path}"

            # Add any additional args (table names)
            if args:
                for arg in args:
                    physical_path = f"{physical_path}/{arg}"

            logger.debug(f"Resolved {logical_path} → {physical_path}")
            return physical_path
        else:
            # Already a physical path, just append args
            result = logical_path
            if args:
                result = f"{result}/{'/'.join(args)}"
            return result

    except Exception as e:
        logger.error(f"Failed to resolve path {logical_path}: {e}")
        raise


def resolve_lake_path(logical_path: str, config: dict[str, Any]) -> str:
    """
    Legacy alias for resolve_path - maintained for backward compatibility.

    Args:
        logical_path: Logical path like 'lake://bronze/source/table'
        config: Configuration dictionary with lake paths

    Returns:
        Physical S3 path like 's3a://bucket/bronze/source/table'
    """
    return resolve_path(logical_path, config=config)


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
