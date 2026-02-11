"""High-level data quality checks for local/integration workflows."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualitySuite:
    """Simple table-level DQ checks with a deterministic score output."""

    def __init__(self, spark, config: dict[str, Any] | None = None):
        self.spark = spark
        self.config = config or {}
        dq_cfg = self.config.get("dq") or {}
        self.fail_on_error = bool(dq_cfg.get("fail_on_error", False))
        self.max_null_pct = float(dq_cfg.get("max_null_pct", 0.05))

    def run_data_quality_checks(self, df: DataFrame, dataset_name: str) -> dict[str, Any]:
        """
        Run baseline DQ checks and return a compact summary.

        Output contract intentionally matches integration test expectations:
        - `overall_score`
        - `failed_checks`
        """
        row_count = df.count()
        total_checks = 0
        failed_checks: list[str] = []

        # Check 1: non-empty dataset
        total_checks += 1
        if row_count <= 0:
            failed_checks.append("non_empty_dataset")

        # Check 2: null-rate thresholds on key-ish columns
        key_candidates = [c for c in df.columns if c.endswith("_id")] or df.columns[:3]
        for col_name in key_candidates:
            total_checks += 1
            null_count = df.filter(F.col(col_name).isNull()).count()
            null_pct = (null_count / row_count) if row_count else 1.0
            if null_pct > self.max_null_pct:
                failed_checks.append(f"null_rate:{col_name}")

        # Check 3: duplicate guard on primary key candidate
        if key_candidates:
            primary_key = key_candidates[0]
            total_checks += 1
            duplicates = df.groupBy(primary_key).count().filter(F.col("count") > 1).limit(1).count()
            if duplicates > 0:
                failed_checks.append(f"duplicates:{primary_key}")

        passed_checks = max(total_checks - len(failed_checks), 0)
        overall_score = (passed_checks / total_checks) if total_checks else 1.0

        result = {
            "dataset": dataset_name,
            "row_count": row_count,
            "overall_score": round(overall_score, 4),
            "total_checks": total_checks,
            "failed_checks": failed_checks,
            "passed": len(failed_checks) == 0,
        }
        if self.fail_on_error and failed_checks:
            raise ValueError(f"DQ checks failed for {dataset_name}: {failed_checks}")
        return result


__all__ = ["DataQualitySuite"]
