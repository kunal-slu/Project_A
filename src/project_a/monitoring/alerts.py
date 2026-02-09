"""
Lightweight alert emitter.

Writes alerts to a local JSONL file to simulate on-call notifications.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


def emit_alert(
    title: str,
    message: str,
    level: str = "WARN",
    config: dict | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    cfg = config or {}

    def _get(key: str, default: Any = None):
        if hasattr(cfg, "get"):
            try:
                return cfg.get(key, default)
            except TypeError:
                return default
        return default

    monitoring_cfg = _get("monitoring", {}) or {}
    if not monitoring_cfg.get("alerts_enabled", True):
        return

    payload = {
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "level": level,
        "title": title,
        "message": message,
        "environment": _get("environment", _get("env", "local")),
        "metadata": metadata or {},
    }
    output_path = monitoring_cfg.get("alerts_path", "artifacts/alerts/alerts.jsonl")
    if output_path.startswith("s3://") or output_path.startswith("s3a://"):
        import boto3

        s3_path = output_path.replace("s3a://", "s3://")
        _, _, bucket_key = s3_path.partition("s3://")
        bucket, _, key = bucket_key.partition("/")
        if not key:
            key = "alerts/alerts"
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        object_key = f"{key.rstrip('/')}/{timestamp}.json"
        boto3.client("s3").put_object(
            Bucket=bucket,
            Key=object_key,
            Body=json.dumps(payload, default=str).encode("utf-8"),
        )
        return

    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with out_file.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, default=str) + "\n")
