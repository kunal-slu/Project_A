"""
Audit logger for pipeline runs.

Writes structured job/run metadata to JSONL so it can be queried later.
This simulates a production audit table without requiring a database.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


def record_job_run(
    job_name: str,
    status: str,
    duration_seconds: float,
    config: dict | None = None,
    result: dict[str, Any] | None = None,
    error: str | None = None,
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
    audit_path = monitoring_cfg.get("audit_path", "artifacts/audit/pipeline_run_audit.jsonl")
    payload = {
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "job_name": job_name,
        "status": status,
        "duration_seconds": duration_seconds,
        "environment": _get("environment", _get("env", "local")),
        "run_id": _get("run_id"),
        "result": result or {},
        "error": error,
    }

    if audit_path.startswith("s3://") or audit_path.startswith("s3a://"):
        import boto3

        s3_path = audit_path.replace("s3a://", "s3://")
        _, _, bucket_key = s3_path.partition("s3://")
        bucket, _, key = bucket_key.partition("/")
        if not key:
            key = "audit/pipeline_run_audit"
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        object_key = f"{key.rstrip('/')}/{job_name}_{timestamp}.json"
        boto3.client("s3").put_object(
            Bucket=bucket,
            Key=object_key,
            Body=json.dumps(payload, default=str).encode("utf-8"),
        )
        return

    audit_file = Path(audit_path)
    audit_file.parent.mkdir(parents=True, exist_ok=True)
    with audit_file.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, default=str) + "\n")
