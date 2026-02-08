"""
Base Job Abstract Class

All ETL jobs inherit from BaseJob to ensure consistent:
- Configuration loading
- SparkSession management
- Error handling
- Logging
- Metrics collection
"""

import json
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from project_a.core.config import ProjectConfig
from project_a.core.context import JobContext

logger = logging.getLogger(__name__)


class BaseJob(ABC):
    """
    Abstract base class for all ETL jobs.

    Provides:
    - Configuration management
    - SparkSession context
    - Standardized error handling
    - Logging setup
    """

    def __init__(self, config: ProjectConfig):
        """
        Initialize job.

        Args:
            config: ProjectConfig instance
        """
        self.config = config
        self.ctx: JobContext | None = None

    @abstractmethod
    def run(self, ctx: JobContext) -> dict[str, Any]:
        """
        Execute the job logic.

        Args:
            ctx: JobContext with SparkSession

        Returns:
            Dictionary with job results (e.g., row counts, paths)
        """
        pass

    def execute(self) -> dict[str, Any]:
        """
        Execute the job with context management.

        Returns:
            Dictionary with job results
        """
        job_name = getattr(self, "job_name", self.__class__.__name__.lower())
        logger.info(f"Starting job: {self.__class__.__name__}")
        start_ts = time.perf_counter()

        try:
            with JobContext(self.config, app_name=self.__class__.__name__) as ctx:
                self.ctx = ctx
                result = self.run(ctx)
                duration_seconds = round(time.perf_counter() - start_ts, 3)
                self._emit_run_metric(job_name, "success", duration_seconds, result)
                self._check_duration_threshold(job_name, duration_seconds)
                logger.info(f"Job completed successfully: {self.__class__.__name__}")
                return result
        except Exception:
            duration_seconds = round(time.perf_counter() - start_ts, 3)
            self._emit_run_metric(job_name, "failed", duration_seconds, {})
            logger.error(f"Job failed: {self.__class__.__name__}", exc_info=True)
            raise

    def _emit_run_metric(
        self,
        job_name: str,
        status: str,
        duration_seconds: float,
        result: dict[str, Any],
    ) -> None:
        """Write a structured run metric artifact for observability."""
        metrics_dir = Path("artifacts/metrics")
        metrics_dir.mkdir(parents=True, exist_ok=True)
        metrics_file = metrics_dir / "pipeline_runs.jsonl"
        payload = {
            "timestamp_utc": datetime.utcnow().isoformat() + "Z",
            "job_name": job_name,
            "status": status,
            "duration_seconds": duration_seconds,
            "result": result,
        }
        with open(metrics_file, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, default=str) + "\n")

    def _check_duration_threshold(self, job_name: str, duration_seconds: float) -> None:
        """Check configured duration threshold and optionally fail."""
        monitoring_cfg = self.config.get("monitoring", {}) or {}
        thresholds = monitoring_cfg.get("thresholds", {}) or {}
        threshold_key = f"{job_name}_duration_seconds"
        threshold = thresholds.get(threshold_key)
        if threshold is None:
            return

        if duration_seconds > float(threshold):
            msg = (
                f"{job_name}: duration {duration_seconds:.2f}s exceeded threshold "
                f"{float(threshold):.2f}s"
            )
            if monitoring_cfg.get("fail_on_threshold_breach", False):
                raise ValueError(msg)
            logger.warning(msg)

    @property
    def spark(self) -> SparkSession:
        """Get SparkSession from context."""
        if self.ctx is None or self.ctx.spark is None:
            raise RuntimeError("JobContext not initialized. Call execute() first.")
        return self.ctx.spark

    def apply_dq_rules(self, df, table_name: str):
        """Apply data quality rules to a DataFrame."""
        # Optional DQ integration - can be overridden by subclasses
        logger.debug(f"DQ check for {table_name} - implement if needed")
        pass

    def log_lineage(self, source: str, target: str, records_processed: dict[str, Any]):
        """Log data lineage information."""
        # Optional lineage tracking - can be overridden by subclasses
        logger.info(f"Lineage: {source} -> {target}, records: {records_processed}")
        pass
