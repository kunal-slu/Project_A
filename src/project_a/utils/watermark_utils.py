"""Watermark helpers for incremental processing."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql.functions import col
from pyspark.sql.functions import max as spark_max


def _watermark_dir(config: dict | None) -> Path:
    prefix = (config or {}).get("data_lake", {}).get("state_prefix")
    if prefix:
        return Path(prefix)
    return Path("data/checkpoints/watermarks")


def get_watermark(source_name: str, config: dict | None = None, spark=None) -> datetime | None:
    base = _watermark_dir(config)
    path = base / f"{source_name}_watermark.json"
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            payload = json.load(f)
        value = payload.get("watermark_value")
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def upsert_watermark(
    source_name: str, value: datetime, config: dict | None = None, spark=None
) -> None:
    base = _watermark_dir(config)
    base.mkdir(parents=True, exist_ok=True)
    path = base / f"{source_name}_watermark.json"
    with open(path, "w") as f:
        json.dump(
            {
                "source_name": source_name,
                "watermark_value": value.astimezone(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            f,
        )


def get_latest_timestamp_from_df(df, timestamp_col: str = "_ingestion_ts") -> datetime | None:
    try:
        if df is None or df.isEmpty():
            return None
        value = df.agg(spark_max(col(timestamp_col)).alias("mx")).collect()[0]["mx"]
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                return None
        return value
    except Exception:
        return None
