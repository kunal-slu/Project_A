"""Bronze behavior -> Silver behavior transformation shim used in tests."""

from __future__ import annotations

import os

from pyspark.sql import functions as F

from project_a.monitoring.lineage_emitter import emit_complete, emit_fail, emit_start
from project_a.monitoring.metrics_collector import emit_metrics


class GreatExpectationsRunner:
    """Minimal GE runner shim for tests."""

    def __init__(self, *_args, **_kwargs):
        pass

    def init_context(self):
        return None

    def run_checkpoint(self, *_args, **_kwargs):
        return {"success": True}


def transform_bronze_to_silver_behavior(spark, config: dict):
    bronze_base = config.get("data_lake", {}).get("bronze_path", "")
    silver_base = config.get("data_lake", {}).get("silver_path", "")
    bronze_path = f"{bronze_base}/redshift/behavior"
    silver_path = f"{silver_base}/behavior"

    emit_start("bronze_to_silver_behavior", [{"name": bronze_path}], [{"name": silver_path}], config)

    try:
        local_bronze = bronze_path
        if bronze_path.startswith("file://"):
            local_bronze = bronze_path.replace("file://", "", 1)
        if local_bronze.startswith("/") and not os.path.exists(local_bronze):
            raise FileNotFoundError(f"Bronze path not found: {bronze_path}")

        bronze_df = spark.read.format("delta").load(bronze_path)
        if bronze_df.count() == 0:
            raise ValueError("Bronze behavior input is empty")

        result_df = (
            bronze_df.withColumn("event_name", F.lower(F.col("event_name")))
            .dropDuplicates(["event_id"])
            .withColumn("_processing_ts", F.current_timestamp())
        )

        ge_runner = GreatExpectationsRunner()
        ge_runner.init_context()
        ge_result = ge_runner.run_checkpoint("silver_behavior")
        if not ge_result.get("success", False):
            raise RuntimeError("Great Expectations checkpoint failed")

        result_df.write.format("delta").mode("overwrite").save(silver_path)

        emit_metrics(
            job_name="bronze_to_silver_behavior",
            rows_in=bronze_df.count(),
            rows_out=result_df.count(),
            duration_seconds=0.0,
            dq_status="pass",
            config=config,
        )
        emit_complete(
            "bronze_to_silver_behavior",
            [{"name": bronze_path}],
            [{"name": silver_path}],
            config,
            {"rows_out": result_df.count()},
        )
        return result_df
    except Exception as exc:
        emit_fail(
            "bronze_to_silver_behavior",
            [{"name": bronze_path}],
            [{"name": silver_path}],
            config,
            str(exc),
        )
        raise
