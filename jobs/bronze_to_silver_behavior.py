"""Bronze behavior -> Silver behavior transformation."""

from __future__ import annotations

import logging
import os
from typing import Any

from pyspark.sql import functions as F

from project_a.dq.great_expectations_runner import GreatExpectationsRunner
from project_a.monitoring.lineage_emitter import emit_complete, emit_fail, emit_start
from project_a.monitoring.metrics_collector import emit_metrics

logger = logging.getLogger(__name__)


def _storage_format(config: dict[str, Any]) -> str:
    storage_cfg = config.get("storage", {}) or {}
    fmt = storage_cfg.get("format")
    if fmt:
        return str(fmt).lower()
    return "delta"


def _read_bronze_behavior(spark, bronze_path: str, config: dict[str, Any]):
    fmt = _storage_format(config)
    read_attempts: list[str] = []

    # Prefer configured storage format first, then fallback.
    if fmt == "parquet":
        read_attempts.extend(["parquet", "delta"])
    elif fmt == "iceberg":
        read_attempts.extend(["iceberg", "delta", "parquet"])
    else:
        read_attempts.extend(["delta", "parquet"])

    last_error: Exception | None = None
    for attempt in read_attempts:
        try:
            if attempt == "delta":
                return spark.read.format("delta").load(bronze_path)
            if attempt == "parquet":
                return spark.read.parquet(bronze_path)
            if attempt == "iceberg":
                iceberg_cfg = config.get("iceberg", {}) or {}
                catalog = iceberg_cfg.get("catalog_name", "local")
                table_name = (
                    config.get("tables", {}).get("bronze_behavior")
                    or config.get("tables", {}).get("customer_behavior_bronze")
                    or "customer_behavior_bronze"
                )
                return spark.read.format("iceberg").load(f"{catalog}.{table_name}")
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Read attempt failed for bronze behavior (%s): %s", attempt, exc
            )

    raise RuntimeError(
        f"Unable to read bronze behavior dataset from {bronze_path} "
        f"using formats {read_attempts}"
    ) from last_error


def _write_silver_behavior(df, silver_path: str, config: dict[str, Any]) -> None:
    fmt = _storage_format(config)
    if fmt == "parquet":
        df.write.mode("overwrite").parquet(silver_path)
        return
    if fmt == "iceberg":
        try:
            iceberg_cfg = config.get("iceberg", {}) or {}
            catalog = iceberg_cfg.get("catalog_name", "local")
            table_name = (
                config.get("tables", {}).get("silver_behavior")
                or config.get("tables", {}).get("customer_behavior_silver")
                or "customer_behavior_silver"
            )
            df.writeTo(f"{catalog}.{table_name}").using("iceberg").createOrReplace()
            return
        except Exception as exc:
            logger.warning("Iceberg write failed, falling back to delta: %s", exc)

    # Default write format for compatibility with existing tests/scripts.
    try:
        df.write.format("delta").mode("overwrite").save(silver_path)
    except Exception as exc:
        logger.warning("Delta write failed, falling back to parquet: %s", exc)
        df.write.mode("overwrite").parquet(silver_path)


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

        bronze_df = _read_bronze_behavior(spark, bronze_path, config)
        if bronze_df.count() == 0:
            raise ValueError("Bronze behavior input is empty")

        result_df = (
            bronze_df.withColumn("event_name", F.lower(F.col("event_name")))
            .dropDuplicates(["event_id"])
            .withColumn("_processing_ts", F.current_timestamp())
        )

        ge_runner = GreatExpectationsRunner()
        ge_runner.init_context()
        dq_cfg = config.get("dq", {}) or {}
        ge_checkpoint = dq_cfg.get("checkpoint", "silver_behavior")
        fail_on_error = bool(dq_cfg.get("fail_on_error", True))
        ge_result = ge_runner.run_checkpoint(ge_checkpoint, fail_on_error=fail_on_error)
        if not ge_result.get("success", False):
            raise RuntimeError("Great Expectations checkpoint failed")

        _write_silver_behavior(result_df, silver_path, config)

        emit_metrics(
            job_name="bronze_to_silver_behavior",
            rows_in=bronze_df.count(),
            rows_out=result_df.count(),
            duration_seconds=0.0,
            dq_status="pass" if ge_result.get("success", False) else "fail",
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
