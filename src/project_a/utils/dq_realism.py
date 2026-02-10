"""Lightweight realism checks for ingestion.

Validates date/timestamp columns for future or too-old values.
"""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def check_date_realism(df: DataFrame, table_name: str, config: dict[str, Any]) -> dict[str, Any]:
    dq_cfg = config.get("dq", {})
    realism_cfg = dq_cfg.get("realism", {}) if isinstance(dq_cfg, dict) else {}
    if not realism_cfg.get("enabled", False):
        return {"enabled": False, "table": table_name}

    max_future_days = int(realism_cfg.get("max_future_days", 3))
    max_past_years = int(realism_cfg.get("max_past_years", 20))
    fail_on_violation = bool(realism_cfg.get("fail_on_violation", False))

    date_cols = [c for c in df.columns if "date" in c.lower() or "timestamp" in c.lower()]
    if not date_cols:
        return {"enabled": True, "table": table_name, "checked": False, "reason": "no date columns"}

    today = F.current_date()
    max_future = F.date_add(today, max_future_days)
    min_past = F.date_sub(today, max_past_years * 365)

    results: dict[str, Any] = {
        "enabled": True,
        "table": table_name,
        "violations": False,
        "columns": {},
    }

    for col_name in date_cols:
        col = F.col(col_name)
        parsed = F.coalesce(
            F.to_date(col),
            F.to_date(col, "yyyy-MM-dd HH:mm:ss"),
            F.to_date(col, "yyyy-MM-dd'T'HH:mm:ss"),
        )
        invalid = df.filter(col.isNotNull() & parsed.isNull()).count()
        future = df.filter(parsed > max_future).count()
        old = df.filter(parsed < min_past).count()

        results["columns"][col_name] = {
            "invalid_format": invalid,
            "future_dates": future,
            "too_old_dates": old,
        }
        if invalid > 0 or future > 0 or old > 0:
            results["violations"] = True

    if results["violations"]:
        logger.warning("Date realism violations detected for %s: %s", table_name, results)
        if fail_on_violation:
            raise ValueError(f"Date realism violations for {table_name}")

    return results
