"""Pipeline metrics ingestion and aggregation helpers."""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


class MetricsCollector:
    """
    Lightweight metrics collector used by integration workflows.

    This intentionally keeps storage file-based for local reproducibility.
    """

    def __init__(self, spark=None, config: dict[str, Any] | None = None):
        self.spark = spark
        self.config = config or {}
        metrics_root = (
            (self.config.get("paths") or {}).get("metrics_root")
            or (self.config.get("monitoring") or {}).get("metrics_path")
            or "data/metrics"
        )
        self.metrics_root = Path(metrics_root)
        self.metrics_root.mkdir(parents=True, exist_ok=True)
        self.store_path = self.metrics_root / "pipeline_metrics_store.jsonl"

    def ingest_pipeline_metrics(self, metrics_path: str) -> dict[str, Any]:
        """Ingest one JSON metrics payload into local JSONL storage."""
        source = Path(metrics_path)
        if not source.exists():
            return {"success": False, "error": f"Metrics file not found: {metrics_path}"}

        payload = json.loads(source.read_text(encoding="utf-8"))
        payload.setdefault("ingested_at", datetime.utcnow().isoformat())
        with self.store_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload) + "\n")

        return {"success": True, "store_path": str(self.store_path), "records_ingested": 1}

    def aggregate_metrics(self) -> dict[str, Any]:
        """Aggregate ingested metrics into a compact summary."""
        if not self.store_path.exists():
            return {
                "total_pipelines": 0,
                "status_counts": {},
                "avg_processing_time_seconds": 0.0,
                "total_records_processed": 0,
            }

        rows: list[dict[str, Any]] = []
        with self.store_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

        if not rows:
            return {
                "total_pipelines": 0,
                "status_counts": {},
                "avg_processing_time_seconds": 0.0,
                "total_records_processed": 0,
            }

        statuses = Counter(str(row.get("status", "unknown")) for row in rows)
        durations = [
            float(row.get("processing_time_seconds", 0.0))
            for row in rows
            if row.get("processing_time_seconds") is not None
        ]
        processed = sum(int(row.get("records_processed", 0)) for row in rows)

        return {
            "total_pipelines": len(rows),
            "status_counts": dict(statuses),
            "avg_processing_time_seconds": (sum(durations) / len(durations) if durations else 0.0),
            "total_records_processed": processed,
        }


__all__ = ["MetricsCollector"]
