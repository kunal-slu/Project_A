"""Compatibility pipeline stage wrapper."""

from project_a.utils.spark_session import build_spark


def run(config=None):
    spark = build_spark(app_name="bronze_to_silver_stage", config=config or {})
    return {"status": "ok", "spark": spark}
