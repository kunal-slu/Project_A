"""
Lightweight schema registry utilities for Kafka events.

Loads a JSON schema and returns required fields for Spark validation.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_schema(schema_path: str) -> dict[str, Any]:
    if schema_path.startswith("s3://") or schema_path.startswith("s3a://"):
        import boto3

        s3_path = schema_path.replace("s3a://", "s3://")
        _, _, bucket_key = s3_path.partition("s3://")
        bucket, _, key = bucket_key.partition("/")
        if not bucket or not key:
            raise FileNotFoundError(f"Invalid S3 schema path: {schema_path}")
        client = boto3.client("s3")
        obj = client.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))

    path = Path(schema_path)
    if not path.exists():
        raise FileNotFoundError(f"Schema registry file not found: {schema_path}")
    return json.loads(path.read_text(encoding="utf-8"))


def required_fields(schema: dict[str, Any]) -> list[str]:
    required = schema.get("required") or []
    if isinstance(required, list):
        return [str(field) for field in required]
    return []
