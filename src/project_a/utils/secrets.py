"""Secrets retrieval helpers with config/env fallback."""

from __future__ import annotations

import json
import os
from typing import Any

try:
    import boto3
except Exception:  # pragma: no cover
    boto3 = None


def _env_fallback(secret_name: str) -> dict[str, Any]:
    key = secret_name.replace("/", "_").replace("-", "_").upper()
    raw = os.getenv(key)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {"value": raw}


def get_secret_from_manager(secret_name: str, region_name: str = "us-east-1") -> dict[str, Any]:
    if boto3 is None:
        return _env_fallback(secret_name)

    try:
        client = boto3.client("secretsmanager", region_name=region_name)
        response = client.get_secret_value(SecretId=secret_name)
        secret_str = response.get("SecretString", "{}")
        return json.loads(secret_str) if isinstance(secret_str, str) else {}
    except Exception:
        # Prefer Snowflake env vars for legacy fallback behavior in tests.
        fallback = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        }
        cleaned = {k: v for k, v in fallback.items() if v}
        return cleaned or _env_fallback(secret_name)


def get_snowflake_credentials(config: dict[str, Any]) -> dict[str, str]:
    secret_name = config.get("secrets", {}).get("snowflake", {}).get("secret_name")
    region = config.get("aws", {}).get("region", "us-east-1")

    if secret_name:
        secret = get_secret_from_manager(secret_name, region_name=region)
        if secret:
            return secret

    return config.get("data_sources", {}).get("snowflake", {})


def get_redshift_credentials(config: dict[str, Any]) -> dict[str, str]:
    secret_name = config.get("secrets", {}).get("redshift", {}).get("secret_name")
    region = config.get("aws", {}).get("region", "us-east-1")

    if secret_name:
        secret = get_secret_from_manager(secret_name, region_name=region)
        if secret:
            return secret

    return config.get("data_sources", {}).get("redshift", {})

