"""
AWS Secrets Manager and SSM Parameter Store utilities.

Provides secure credential retrieval for Snowflake, Redshift, and other external sources.
"""

import json
import logging
import os
from typing import Any

try:
    import boto3
    from botocore.exceptions import ClientError

    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False
    boto3 = None
    ClientError = Exception

logger = logging.getLogger(__name__)


def get_secret_from_manager(secret_name: str, region_name: str = "us-east-1") -> dict[str, Any]:
    """
    Retrieve secret from AWS Secrets Manager.

    Args:
        secret_name: Name of secret in Secrets Manager
        region_name: AWS region

    Returns:
        Secret dictionary (typically JSON-parsed)

    Raises:
        ValueError: If secret not found or access denied
    """
    if not AWS_AVAILABLE:
        logger.warning("boto3 not available, using environment variables")
        return _get_secret_from_env(secret_name)

    try:
        client = boto3.client("secretsmanager", region_name=region_name)

        response = client.get_secret_value(SecretId=secret_name)

        secret_string = response.get("SecretString")
        if secret_string:
            return json.loads(secret_string)
        else:
            # Binary secret
            return {"secret_binary": response.get("SecretBinary")}

    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        if error_code == "ResourceNotFoundException":
            logger.warning(f"Secret {secret_name} not found, falling back to environment variables")
            return _get_secret_from_env(secret_name)
        elif error_code == "AccessDeniedException":
            logger.error(f"Access denied to secret {secret_name}")
            raise ValueError(f"Cannot access secret {secret_name}: Access Denied")
        else:
            logger.error(f"Error retrieving secret {secret_name}: {e}")
            raise ValueError(f"Failed to retrieve secret: {e}")
    except Exception as e:
        logger.warning(f"Error accessing Secrets Manager: {e}, falling back to env vars")
        return _get_secret_from_env(secret_name)


def get_parameter_from_ssm(
    parameter_name: str, region_name: str = "us-east-1", decrypt: bool = True
) -> str:
    """
    Retrieve parameter from AWS SSM Parameter Store.

    Args:
        parameter_name: Name of parameter (e.g., '/data-platform/snowflake/password')
        region_name: AWS region
        decrypt: Whether to decrypt SecureString parameters

    Returns:
        Parameter value as string
    """
    if not AWS_AVAILABLE:
        logger.warning("boto3 not available, using environment variable")
        env_key = parameter_name.replace("/", "_").upper()
        return os.getenv(env_key, "")

    try:
        client = boto3.client("ssm", region_name=region_name)

        response = client.get_parameter(Name=parameter_name, WithDecryption=decrypt)

        return response["Parameter"]["Value"]

    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        if error_code == "ParameterNotFound":
            logger.warning(f"Parameter {parameter_name} not found, checking environment")
            env_key = parameter_name.replace("/", "_").upper()
            return os.getenv(env_key, "")
        else:
            logger.error(f"Error retrieving parameter {parameter_name}: {e}")
            raise ValueError(f"Failed to retrieve parameter: {e}")


def _get_secret_from_env(secret_name: str) -> dict[str, Any]:
    """Fallback to environment variables if Secrets Manager unavailable."""
    # Map common secret names to env var patterns
    env_mapping = {
        "snowflake": {
            "account": "SNOWFLAKE_ACCOUNT",
            "user": "SNOWFLAKE_USER",
            "password": "SNOWFLAKE_PASSWORD",
            "warehouse": "SNOWFLAKE_WAREHOUSE",
            "database": "SNOWFLAKE_DATABASE",
            "schema": "SNOWFLAKE_SCHEMA",
        },
        "redshift": {
            "host": "REDSHIFT_HOST",
            "port": "REDSHIFT_PORT",
            "database": "REDSHIFT_DATABASE",
            "user": "REDSHIFT_USER",
            "password": "REDSHIFT_PASSWORD",
        },
    }

    # Try to infer secret type from name
    secret_type = None
    if "snowflake" in secret_name.lower():
        secret_type = "snowflake"
    elif "redshift" in secret_name.lower():
        secret_type = "redshift"

    if secret_type and secret_type in env_mapping:
        result = {}
        for key, env_var in env_mapping[secret_type].items():
            value = os.getenv(env_var)
            if value:
                result[key] = value
        return result if result else {}

    # Generic fallback: try secret name as env var
    env_key = secret_name.replace("-", "_").upper()
    value = os.getenv(env_key)
    if value:
        try:
            return json.loads(value)
        except:
            return {"value": value}

    return {}


def get_snowflake_credentials(config: dict[str, Any]) -> dict[str, str]:
    """
    Get Snowflake credentials from Secrets Manager (Phase 2 format).

    Args:
        config: Configuration dictionary

    Returns:
        Dictionary with Snowflake connection parameters
    """

    region = config.get("region", "us-east-1")
    secret_name = "project-a-dev/snowflake/conn"

    try:
        logger.info(f"Retrieving Snowflake credentials from Secrets Manager: {secret_name}")
        return get_secret_from_manager(secret_name, region_name=region)
    except Exception as e:
        logger.warning(f"Failed to get secret from SM: {e}, checking config fallback")
        # Fallback to config
        return config.get("data_sources", {}).get("snowflake", {})


def get_redshift_credentials(config: dict[str, Any]) -> dict[str, str]:
    """
    Get Redshift credentials from Secrets Manager (Phase 2 format).

    Args:
        config: Configuration dictionary

    Returns:
        Dictionary with Redshift connection parameters
    """

    region = config.get("region", "us-east-1")
    secret_name = "project-a-dev/redshift/conn"

    try:
        logger.info(f"Retrieving Redshift credentials from Secrets Manager: {secret_name}")
        return get_secret_from_manager(secret_name, region_name=region)
    except Exception as e:
        logger.warning(f"Failed to get secret from SM: {e}, checking config fallback")
        # Fallback to config
        return config.get("data_sources", {}).get("redshift", {})
