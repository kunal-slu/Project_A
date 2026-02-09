"""Standardized SCD2 utilities used by tests and local pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dataclass
class SCD2Config:
    """Configuration for SCD2 behavior."""

    business_key: str
    change_columns: list[str]
    effective_from_column: str = "effective_from"
    effective_to_column: str = "effective_to"
    is_current_column: str = "is_current"
    surrogate_key_column: str = "surrogate_key"
    hash_column: str = "hash_diff"
    updated_at_column: str = "updated_at"
    ts_column: str | None = None


def _hash_diff(df: DataFrame, cols: list[str]) -> F.Column:
    safe_cols = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]
    return F.sha2(F.concat_ws("||", *safe_cols), 256)


def _read_target(spark: SparkSession, path: str) -> DataFrame | None:
    try:
        return spark.read.format("delta").load(path)
    except Exception:
        try:
            return spark.read.parquet(path)
        except Exception:
            return None


def _write_target(df: DataFrame, path: str) -> None:
    try:
        df.write.format("delta").mode("overwrite").save(path)
    except Exception:
        df.write.mode("overwrite").parquet(path)


def _validate_required_columns(df: DataFrame, config: SCD2Config) -> list[str]:
    required = [config.business_key] + list(config.change_columns)
    if config.ts_column:
        required.append(config.ts_column)
    missing = [col for col in required if col not in df.columns]
    return missing


def apply_scd2(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    config: SCD2Config,
    effective_from: datetime | None = None,
) -> dict[str, Any]:
    """Apply SCD2 logic for the given source DataFrame."""
    missing = _validate_required_columns(source_df, config)
    if missing:
        return {
            "success": False,
            "records_processed": 0,
            "error": f"Missing required columns: {missing}",
        }

    if source_df.rdd.isEmpty():
        return {
            "success": True,
            "records_processed": 0,
            "message": "No source data to process",
        }

    ts_expr = None
    if effective_from is not None:
        ts_expr = F.lit(effective_from)
    elif config.ts_column and config.ts_column in source_df.columns:
        ts_expr = F.col(config.ts_column)
    else:
        ts_expr = F.current_timestamp()

    source_prepped = (
        source_df.withColumn(config.effective_from_column, ts_expr)
        .withColumn(config.hash_column, _hash_diff(source_df, config.change_columns))
        .withColumn(config.updated_at_column, F.current_timestamp())
    )

    target_df = _read_target(spark, target_path)

    if target_df is None or target_df.rdd.isEmpty():
        # Initial load
        initial = (
            source_prepped.withColumn(config.effective_to_column, F.lit(None).cast("timestamp"))
            .withColumn(config.is_current_column, F.lit(True))
            .withColumn(
                config.surrogate_key_column,
                F.sha2(
                    F.concat_ws(
                        "||", F.col(config.business_key).cast("string"), F.col(config.effective_from_column).cast("string")
                    ),
                    256,
                ),
            )
        )
        _write_target(initial, target_path)
        return {"success": True, "records_processed": initial.count()}

    # Current records only
    current_df = target_df.filter(F.col(config.is_current_column) == F.lit(True))

    # Drop already materialized versions (same business_key + effective_from)
    existing_versions = target_df.select(
        F.col(config.business_key),
        F.col(config.effective_from_column),
    ).distinct()
    source_candidates = source_prepped.join(
        existing_versions,
        [config.business_key, config.effective_from_column],
        "left_anti",
    )

    # Join on business key for current comparison
    joined = source_candidates.join(
        current_df.select(
            F.col(config.business_key).alias("_bk"),
            F.col(config.hash_column).alias("_current_hash"),
            F.col(config.effective_from_column).alias("_current_effective_from"),
        ),
        source_candidates[config.business_key] == F.col("_bk"),
        "left",
    )

    # New keys (no current record)
    new_rows = joined.filter(F.col("_bk").isNull()).drop(
        "_bk", "_current_hash", "_current_effective_from"
    )

    # Changed keys (hash differs)
    changed_rows = joined.filter(
        F.col("_bk").isNotNull() & (F.col("_current_hash") != F.col(config.hash_column))
    )

    # Determine late-arriving updates (effective_from < current effective_from)
    late_rows = joined.filter(
        (F.col("_bk").isNotNull())
        & (F.col(config.effective_from_column) < F.col("_current_effective_from"))
    )
    normal_updates = changed_rows.filter(
        F.col(config.effective_from_column) >= F.col("_current_effective_from")
    )

    # Close current records for normal updates only
    to_close_keys = normal_updates.select(
        F.col(config.business_key),
        F.col(config.effective_from_column).alias("_new_effective_from"),
    )
    closed_current = (
        current_df.join(to_close_keys, config.business_key, "inner")
        .withColumn(config.is_current_column, F.lit(False))
        .withColumn(
            config.effective_to_column,
            F.coalesce(F.col("_new_effective_from"), F.col(config.updated_at_column)),
        )
        .drop("_new_effective_from")
    )

    # Build new current rows for normal updates + new rows
    def _as_current(df: DataFrame) -> DataFrame:
        return (
            df.drop("_bk", "_current_hash", "_current_effective_from")
            .withColumn(config.effective_to_column, F.lit(None).cast("timestamp"))
            .withColumn(config.is_current_column, F.lit(True))
            .withColumn(
                config.surrogate_key_column,
                F.sha2(
                    F.concat_ws(
                        "||", F.col(config.business_key).cast("string"), F.col(config.effective_from_column).cast("string")
                    ),
                    256,
                ),
            )
        )

    new_current = _as_current(new_rows)
    updated_current = _as_current(normal_updates)

    # Late-arriving records: insert as historical, not current
    late_historical = (
        late_rows.drop("_bk", "_current_hash", "_current_effective_from")
        .withColumn(config.is_current_column, F.lit(False))
        .withColumn(config.effective_to_column, F.lit(None).cast("timestamp"))
        .withColumn(
            config.surrogate_key_column,
            F.sha2(
                F.concat_ws(
                    "||", F.col(config.business_key).cast("string"), F.col(config.effective_from_column).cast("string")
                ),
                256,
            ),
        )
    )

    # Adjust late historical effective_to to current effective_from
    late_historical = late_historical.join(
        current_df.select(
            F.col(config.business_key).alias("_bk"),
            F.col(config.effective_from_column).alias("_current_effective_from"),
        ),
        late_historical[config.business_key] == F.col("_bk"),
        "left",
    ).withColumn(
        config.effective_to_column,
        F.coalesce(F.col("_current_effective_from"), F.col(config.effective_to_column)),
    ).drop("_bk", "_current_effective_from")

    unchanged_current = current_df.join(to_close_keys, config.business_key, "left_anti")
    historical_df = target_df.filter(F.col(config.is_current_column) == F.lit(False))

    final_df = (
        historical_df.unionByName(unchanged_current)
        .unionByName(closed_current)
        .unionByName(new_current)
        .unionByName(updated_current)
        .unionByName(late_historical)
    )

    records_processed = new_current.count() + updated_current.count() + late_historical.count()
    if records_processed == 0:
        return {
            "success": True,
            "records_processed": 0,
            "message": "No SCD2 changes detected",
        }

    _write_target(final_df, target_path)
    return {"success": True, "records_processed": records_processed}


def validate_scd2_table(
    spark: SparkSession,
    target_path: str,
    config: SCD2Config,
) -> dict[str, Any]:
    """Validate SCD2 table consistency."""
    df = _read_target(spark, target_path)
    if df is None:
        return {"valid": False, "message": "SCD2 table not found"}

    required = [
        config.business_key,
        config.effective_from_column,
        config.effective_to_column,
        config.is_current_column,
        config.surrogate_key_column,
        config.hash_column,
        config.updated_at_column,
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        return {"valid": False, "message": f"Missing columns: {missing}"}

    current_dupes = (
        df.filter(F.col(config.is_current_column) == F.lit(True))
        .groupBy(config.business_key)
        .count()
        .filter(F.col("count") > F.lit(1))
        .count()
    )
    if current_dupes > 0:
        return {"valid": False, "message": "Multiple current records per business key"}

    current_with_effective_to = (
        df.filter(F.col(config.is_current_column) == F.lit(True))
        .filter(F.col(config.effective_to_column).isNotNull())
        .count()
    )
    if current_with_effective_to > 0:
        return {"valid": False, "message": "Current records have non-null effective_to"}

    return {"valid": True, "message": "SCD2 validation passed"}
