"""
Unified Path Resolver for Local and AWS Environments

This module provides a single abstraction for resolving logical paths
to physical paths that work identically on local and AWS environments.

Usage:
    from project_a.utils.path_resolver import resolve_data_path

    # Works on both local and AWS
    bronze_path = resolve_data_path(config, "bronze", "crm", "accounts")
    # Local: file:///path/to/data/bronze/crm/accounts
    # AWS: s3://bucket/bronze/crm/accounts
"""

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def resolve_data_path(
    config: dict[str, Any], layer: str, source: str | None = None, table: str | None = None
) -> str:
    """
    Resolve a logical data path to physical path (local or S3).

    Args:
        config: Configuration dictionary with paths section
        layer: Data layer ('bronze', 'silver', 'gold')
        source: Source system name (for bronze layer, e.g., 'crm', 'snowflake')
        table: Table name (e.g., 'customers', 'orders')

    Returns:
        Physical path (file:// for local, s3:// for AWS)

    Examples:
        >>> resolve_data_path(config, "bronze", "crm", "accounts")
        's3://bucket/bronze/crm/accounts'  # or 'file:///path/to/bronze/crm/accounts'

        >>> resolve_data_path(config, "silver", table="customers")
        's3://bucket/silver/customers'  # or 'file:///path/to/silver/customers'
    """
    paths_cfg = config.get("paths", {})
    environment = config.get("environment", config.get("env", "local"))

    # Get base path for layer
    layer_key = f"{layer}_root"
    base_path = paths_cfg.get(layer_key) or paths_cfg.get(layer) or ""

    if not base_path:
        raise ValueError(f"Path configuration missing for {layer_key}")

    # Build path components
    path_parts = [base_path.rstrip("/")]

    if source:
        path_parts.append(source)
    if table:
        path_parts.append(table)

    physical_path = "/".join(path_parts)

    # Ensure proper protocol and absolute paths for local
    if not physical_path.startswith(("s3://", "file://", "s3a://")):
        if environment == "local":
            # Local: convert relative paths to absolute file:// paths
            if not Path(physical_path).is_absolute():
                # Try to find project root (go up from this file)
                project_root = Path(__file__).parent.parent.parent.parent
                physical_path = str(project_root / physical_path)
            # Ensure file:// protocol
            if not physical_path.startswith("file://"):
                physical_path = f"file://{physical_path}"
        else:
            # Default to S3 for AWS
            physical_path = f"s3://{physical_path}"

    logger.debug(f"Resolved {layer}/{source}/{table} → {physical_path}")
    return physical_path


def resolve_source_file_path(config: dict[str, Any], source_name: str, file_key: str) -> str:
    """
    Resolve a source file path from config.

    Works identically on local and AWS by handling relative paths correctly.

    Args:
        config: Configuration dictionary
        source_name: Source system name (e.g., 'crm', 'snowflake', 'fx')
        file_key: File key from sources.{source_name}.files (e.g., 'accounts', 'orders')

    Returns:
        Full path to source file (file:// for local, s3:// for AWS)

    Examples:
        >>> resolve_source_file_path(config, "crm", "accounts")
        's3://bucket/bronze/crm/accounts.csv'  # AWS
        'file:///absolute/path/to/aws/data/samples/crm/accounts.csv'  # Local
    """
    sources_cfg = config.get("sources", {})
    source_cfg = sources_cfg.get(source_name, {})

    base_path = source_cfg.get("base_path", "")
    files = source_cfg.get("files", {})
    filename = files.get(file_key, "")

    if not base_path:
        raise ValueError(f"Source {source_name} has no base_path in config")
    if not filename:
        raise ValueError(f"Source {source_name} has no file for key {file_key}")

    full_path = f"{base_path.rstrip('/')}/{filename}"

    # Handle local vs AWS path resolution
    environment = config.get("environment", config.get("env", "local"))
    is_local = environment in ("local", "dev_local")

    if is_local:
        # Local: convert relative paths to absolute file:// paths
        if not full_path.startswith(("s3://", "file://", "s3a://")):
            # Relative path - make it absolute
            if not Path(full_path).is_absolute():
                project_root = Path(__file__).parent.parent.parent.parent
                full_path = str(project_root / full_path)
            # Ensure file:// protocol
            if not full_path.startswith("file://"):
                full_path = f"file://{full_path}"
    else:
        # AWS: ensure S3 protocol
        if not full_path.startswith(("s3://", "s3a://")):
            full_path = f"s3://{full_path}"

    logger.debug(f"Resolved source file {source_name}.{file_key} → {full_path} (env={environment})")
    return full_path


def is_local_environment(config: dict[str, Any]) -> bool:
    """
    Check if running in local environment.

    Args:
        config: Configuration dictionary

    Returns:
        True if local, False if AWS/EMR
    """
    environment = config.get("environment", config.get("env", "local"))
    return environment in ("local", "dev_local")


def get_kafka_bootstrap_servers(config: dict[str, Any]) -> str:
    """
    Get Kafka bootstrap servers from config (local or MSK).

    Args:
        config: Configuration dictionary

    Returns:
        Bootstrap servers string (e.g., 'localhost:9092' or 'broker1:9092,broker2:9092')
    """
    kafka_cfg = config.get("sources", {}).get("kafka", {})

    if is_local_environment(config):
        # Local Kafka
        return kafka_cfg.get("local_bootstrap_servers", "localhost:9092")
    else:
        # AWS MSK
        msk_bootstrap = kafka_cfg.get("msk_bootstrap_servers")
        if not msk_bootstrap:
            raise ValueError("MSK bootstrap servers not configured for AWS environment")
        return msk_bootstrap
