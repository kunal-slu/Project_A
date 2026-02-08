"""Lightweight ingestion pipeline compatibility shim."""

from __future__ import annotations

from datetime import datetime
from typing import Any


class IngestionPipeline:
    """Minimal orchestrator retained for legacy imports/tests."""

    def __init__(self, spark, config: dict[str, Any]):
        self.spark = spark
        self.config = config

    def run_full_pipeline(self) -> dict[str, Any]:
        return {
            "status": "success",
            "started_at": datetime.utcnow().isoformat(),
            "metrics": {},
        }

