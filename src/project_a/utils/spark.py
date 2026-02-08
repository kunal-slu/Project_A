"""
Compatibility Spark helpers.

This module exists for older imports like:
    from project_a.utils.spark import get_spark_session
"""

from __future__ import annotations

from typing import Any

from .spark_session import build_spark, get_spark


def get_spark_session(
    app_name: str = "project_a",
    config: dict[str, Any] | None = None,
    extra_conf: dict[str, Any] | None = None,
):
    """
    Backward-compatible alias around build_spark.
    """
    merged = dict(config or {})
    if extra_conf:
        spark_cfg = dict(merged.get("spark", {}))
        spark_cfg.update(extra_conf)
        merged["spark"] = spark_cfg
    return build_spark(app_name=app_name, config=merged)


__all__ = ["build_spark", "get_spark", "get_spark_session"]
