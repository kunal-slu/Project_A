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
from project_a.monitoring.alerts import emit_alert
from project_a.observability.audit_logger import record_job_run

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
                record_job_run(
                    job_name=job_name,
                    status="success",
                    duration_seconds=duration_seconds,
                    config=self.config,
                    result=result,
                )
                self._check_duration_threshold(job_name, duration_seconds)
                logger.info(f"Job completed successfully: {self.__class__.__name__}")
                return result
        except Exception:
            duration_seconds = round(time.perf_counter() - start_ts, 3)
            self._emit_run_metric(job_name, "failed", duration_seconds, {})
            record_job_run(
                job_name=job_name,
                status="failed",
                duration_seconds=duration_seconds,
                config=self.config,
                result={},
                error="exception",
            )
            emit_alert(
                title=f"Job failed: {job_name}",
                message=f"{self.__class__.__name__} failed after {duration_seconds:.2f}s",
                level="ERROR",
                config=self.config,
            )
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
            if monitoring_cfg.get("alert_on_threshold_breach", True):
                emit_alert(
                    title=f"Duration SLA breach: {job_name}",
                    message=msg,
                    level="WARN",
                    config=self.config,
                )
            logger.warning(msg)

    @property
    def spark(self) -> SparkSession:
        """Get SparkSession from context."""
        if self.ctx is None or self.ctx.spark is None:
            raise RuntimeError("JobContext not initialized. Call execute() first.")
        return self.ctx.spark

    def apply_dq_rules(self, df, table_name: str):
        """
        Apply runtime data-contract checks.

        Behavior:
        - Silver tables: validate via `config/contracts/silver_contracts.yaml`
        - Gold fact_orders: validate required + unique keys from JSON contract
        - If `dq.fail_on_error=false`, log and continue instead of raising
        """
        if df is None:
            logger.info("Skipping DQ rules for %s (no DataFrame provided)", table_name)
            return

        dq_cfg = self.config.get("dq", {}) or {}
        fail_on_error = bool(dq_cfg.get("fail_on_error", True))

        try:
            parts = [p for p in str(table_name).split(".") if p]
            if not parts:
                logger.debug("Skipping DQ rules for empty table name")
                return

            layer = parts[0].lower()
            logical_table = parts[-1]

            if layer == "silver":
                from project_a.contracts.runtime_contracts import (
                    load_table_contracts,
                    validate_contract,
                )

                contracts_cfg = self.config.get("contracts", {}) or {}
                contracts_path = contracts_cfg.get("path", "config/contracts/silver_contracts.yaml")
                contracts = load_table_contracts(contracts_path)

                contract = contracts.get(logical_table)
                if not contract:
                    logger.debug("No Silver contract found for %s", logical_table)
                    return

                validate_contract(df, logical_table, contract)
                logger.info("DQ contract passed for %s", table_name)
                return

            if layer == "gold" and logical_table == "fact_orders":
                contract_path = Path("config/contracts/fact_orders.json")
                if not contract_path.exists():
                    logger.debug("Gold contract not found at %s", contract_path)
                    return

                contract = json.loads(contract_path.read_text(encoding="utf-8"))
                required_cols = (
                    ((contract.get("schema") or {}).get("required") or [])
                    if isinstance(contract, dict)
                    else []
                )
                missing = [col for col in required_cols if col not in df.columns]
                if missing:
                    raise ValueError(f"{table_name}: Missing required columns: {missing}")

                null_violations = {}
                for column in required_cols:
                    count = df.filter(df[column].isNull()).count()
                    if count > 0:
                        null_violations[column] = count
                if null_violations:
                    raise ValueError(
                        f"{table_name}: Null violations in required columns: {null_violations}"
                    )

                unique_keys = (contract.get("constraints") or {}).get("unique_keys") or []
                for key in unique_keys:
                    duplicate_count = (
                        df.groupBy(key).count().filter("count > 1").count()
                    )
                    if duplicate_count > 0:
                        raise ValueError(
                            f"{table_name}: Duplicate key violations for {key}: {duplicate_count}"
                        )

                logger.info("DQ contract passed for %s", table_name)
                return

            logger.debug("No runtime DQ contract configured for %s", table_name)
        except Exception as exc:
            if fail_on_error:
                raise
            logger.warning("DQ check failed for %s but continuing: %s", table_name, exc)

    def log_lineage(self, source: str, target: str, records_processed: dict[str, Any]):
        """Log data lineage information."""
        logger.info(f"Lineage: {source} -> {target}, records: {records_processed}")
        try:
            from project_a.lineage.tracking import LineageTracker

            lineage_cfg = self.config.get("lineage", {})
            storage_path = lineage_cfg.get("storage_path", "data/lineage")
            tracker = LineageTracker(storage_path)
            tracker.track_transformation(
                source_dataset=source,
                target_dataset=target,
                transformation=self.__class__.__name__,
                job_id=getattr(self, "job_name", self.__class__.__name__.lower()),
                records_processed=int(
                    records_processed.get("row_count", 0) if isinstance(records_processed, dict) else 0
                ),
                duration_ms=0,
                success=True,
                metadata=records_processed if isinstance(records_processed, dict) else {},
            )
        except Exception:
            logger.debug("Lineage tracking skipped (tracker error)", exc_info=True)
