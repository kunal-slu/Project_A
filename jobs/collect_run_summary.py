"""Collect run summary metrics from Bronze/Silver/Gold paths."""

from __future__ import annotations

from datetime import datetime


def _safe_count(spark, path: str):
    try:
        return spark.read.format("delta").load(path).count(), None
    except Exception as exc:
        return 0, str(exc)


def collect_run_summary(spark, config: dict, run_id: str, execution_date: str) -> dict:
    lake = config.get("data_lake", {})
    bronze_base = lake.get("bronze_path", "")
    silver_base = lake.get("silver_path", "")
    gold_base = lake.get("gold_path", "")

    stages = {
        "bronze_behavior": f"{bronze_base}/redshift/behavior",
        "silver_behavior": f"{silver_base}/behavior",
        "gold_customer_360": f"{gold_base}/customer_360",
    }

    metrics = {}
    errors = []
    for name, path in stages.items():
        count, err = _safe_count(spark, path)
        metrics[name] = {"row_count": count, "path": path}
        if err:
            errors.append(f"{name}: {err}")

    return {
        "run_id": run_id,
        "execution_date": execution_date,
        "status": "success" if not errors else "partial",
        "generated_at": datetime.utcnow().isoformat(),
        "stages": stages,
        "metrics": metrics,
        "errors": errors,
    }
