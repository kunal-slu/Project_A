"""
Watermark management for incremental ingestion.

Supports SSM Parameter Store (AWS) and state store (S3/local).
"""

import logging
from datetime import datetime

try:
    import boto3
    from botocore.exceptions import ClientError

    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None

logger = logging.getLogger(__name__)


def read_watermark(key: str, config: dict = None, use_ssm: bool = False) -> str | None:
    """
    Read watermark value from SSM Parameter Store or state store.

    Args:
        key: Watermark key (e.g., 'snowflake_orders_max_ts')
        config: Configuration dict
        use_ssm: If True, use SSM; otherwise use state store

    Returns:
        ISO8601 timestamp string or None if not found
    """
    if use_ssm and BOTO3_AVAILABLE:
        # Use AWS Systems Manager Parameter Store
        try:
            ssm = boto3.client("ssm", region_name=config.get("aws", {}).get("region", "us-east-1"))
            response = ssm.get_parameter(Name=f"/etl/watermarks/{key}")
            watermark = response["Parameter"]["Value"]
            logger.info(f"✅ Read watermark from SSM: {key} = {watermark}")
            return watermark
        except ClientError as e:
            if e.response["Error"]["Code"] == "ParameterNotFound":
                logger.info(f"No watermark found in SSM for {key} (first run)")
                return None
            else:
                logger.warning(f"SSM error reading watermark: {e}")
                return None
    else:
        # Use state store (S3/local)
        from project_a.utils.state_store import get_state_store

        state_store = get_state_store(config)
        watermark = state_store.get_watermark(key)

        if watermark:
            logger.info(f"✅ Read watermark from state store: {key} = {watermark}")
        else:
            logger.info(f"No watermark found for {key} (first run)")

        return watermark


def write_watermark(key: str, value: datetime, config: dict = None, use_ssm: bool = False) -> None:
    """
    Write watermark value to SSM Parameter Store or state store.

    Args:
        key: Watermark key
        value: Datetime value
        config: Configuration dict
        use_ssm: If True, use SSM; otherwise use state store
    """
    iso_value = value.isoformat()

    if use_ssm and BOTO3_AVAILABLE:
        # Use AWS Systems Manager Parameter Store
        try:
            ssm = boto3.client("ssm", region_name=config.get("aws", {}).get("region", "us-east-1"))
            ssm.put_parameter(
                Name=f"/etl/watermarks/{key}", Value=iso_value, Type="String", Overwrite=True
            )
            logger.info(f"✅ Wrote watermark to SSM: {key} = {iso_value}")
        except Exception as e:
            logger.warning(f"Failed to write watermark to SSM: {e}, falling back to state store")
            use_ssm = False

    if not use_ssm:
        # Use state store (S3/local)
        from project_a.utils.state_store import get_state_store

        state_store = get_state_store(config)
        state_store.set_watermark(key, iso_value)
        logger.info(f"✅ Wrote watermark to state store: {key} = {iso_value}")
