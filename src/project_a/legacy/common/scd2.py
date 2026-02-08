"""
Standardized SCD2 (Slowly Changing Dimension Type 2) implementation.

This module provides a reusable `apply_scd2()` helper that can be used across
all dimension tables with consistent semantics and behavior.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

logger = logging.getLogger(__name__)


def _delta_available(spark: SparkSession) -> bool:
    try:
        _ = spark._jvm.java.lang.Class.forName("io.delta.tables.DeltaTable")
        _ = spark._jvm.java.lang.Class.forName("io.delta.sql.DeltaSparkSessionExtension")
        return True
    except Exception:
        return False


def _table_format(spark: SparkSession) -> str:
    return "delta" if _delta_available(spark) else "parquet"


def _read_table(spark: SparkSession, path: str) -> DataFrame:
    fmt = _table_format(spark)
    return spark.read.format(fmt).load(path)


def _write_table(df: DataFrame, path: str) -> None:
    fmt = _table_format(df.sparkSession)
    df.write.format(fmt).mode("overwrite").save(path)


@dataclass
class SCD2Config:
    """Configuration for SCD Type 2 operations."""

    business_key: str
    change_columns: list[str]
    effective_from_column: str = "effective_from"
    effective_to_column: str = "effective_to"
    is_current_column: str = "is_current"
    surrogate_key_column: str | None = "surrogate_key"
    hash_column: str = "hash_diff"
    updated_at_column: str = "updated_at"
    ts_column: str | None = None
    add_surrogate_key: bool = True
    handle_late_arriving: bool = True
    strict_mode: bool = True


def apply_scd2(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    config: SCD2Config,
    effective_from: datetime | None = None,
) -> dict[str, Any]:
    """
    Apply SCD Type 2 logic to handle slowly changing dimensions.

    Args:
        spark: SparkSession instance
        source_df: Source DataFrame with dimension data
        target_path: Path to the target Delta table
        config: SCD2 configuration
        effective_from: Optional effective from timestamp

    Returns:
        Dictionary with processing results
    """
    try:
        required_columns = [config.business_key, *config.change_columns]
        missing_columns = [col for col in required_columns if col not in source_df.columns]
        if missing_columns:
            return {
                "success": False,
                "error": f"Missing required columns: {missing_columns}",
            }

        if source_df.rdd.isEmpty():
            logger.info("No source data to process")
            return {
                "success": True,
                "records_processed": 0,
                "message": "No source data to process",
            }

        # Create hash for change detection
        hash_expr = F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(F.col(col).cast("string"), F.lit(""))
                    for col in config.change_columns
                ],
            ),
            256,
        )

        if effective_from is not None:
            effective_from_expr = F.lit(effective_from).cast(TimestampType())
        elif config.ts_column and config.ts_column in source_df.columns:
            effective_from_expr = F.col(config.ts_column).cast(TimestampType())
        elif config.updated_at_column in source_df.columns:
            effective_from_expr = F.col(config.updated_at_column).cast(TimestampType())
        elif "created_at" in source_df.columns:
            effective_from_expr = F.col("created_at").cast(TimestampType())
        else:
            effective_from_expr = F.current_timestamp().cast(TimestampType())

        # Prepare staged data
        staged_df = (
            source_df.withColumn(config.effective_from_column, effective_from_expr)
            .withColumn(config.effective_to_column, F.lit(None).cast(TimestampType()))
            .withColumn(config.is_current_column, F.lit(True))
            .withColumn(config.hash_column, hash_expr)
        )

        if config.updated_at_column not in staged_df.columns:
            staged_df = staged_df.withColumn(config.updated_at_column, effective_from_expr)

        # Add surrogate key if requested
        if config.add_surrogate_key:
            staged_df = staged_df.withColumn(config.surrogate_key_column, F.expr("uuid()"))

        # Check if target table exists
        table_exists = True
        try:
            _read_table(spark, target_path)
        except Exception:
            table_exists = False

        if not table_exists:
            # Create new table
            _write_table(staged_df, target_path)
            return {
                "success": True,
                "records_processed": staged_df.count(),
                "message": "New SCD2 table created",
            }

        existing_df = _read_table(spark, target_path)
        current_flag = F.col(config.is_current_column)
        current_expr = (current_flag == F.lit(True)) | (
            F.lower(current_flag.cast("string")) == F.lit("true")
        )

        # Get current records for comparison
        current_records = (
            existing_df.filter(current_expr)
            .select(config.business_key, config.hash_column, config.effective_from_column)
            .withColumnRenamed(config.hash_column, "t_hash_diff")
            .withColumnRenamed(config.effective_from_column, "t_effective_from")
        )

        # Find changed records
        if effective_from is not None and config.handle_late_arriving:
            changes_df = staged_df
        else:
            changes_df = (
                staged_df.alias("s")
                .join(
                    current_records.alias("t"),
                    F.col(f"s.{config.business_key}") == F.col(f"t.{config.business_key}"),
                    "left",
                )
                .filter(
                    F.col("t.t_hash_diff").isNull()
                    | (F.col("t.t_hash_diff") != F.col(f"s.{config.hash_column}"))
                )
                .select("s.*")
            )

        changes_df = changes_df.persist()
        processed_count = changes_df.count()
        if processed_count == 0:
            changes_df.unpersist()
            logger.info("No SCD2 changes detected")
            return {
                "success": True,
                "records_processed": 0,
                "message": "No SCD2 changes detected",
            }

        # Manual SCD2 merge (avoid DeltaTable merge for test stability)
        current_df = existing_df.filter(current_expr)
        historical_df = existing_df.filter(~current_expr)

        changes_with_current = changes_df.alias("s").join(
            current_records.alias("t"),
            F.col(f"s.{config.business_key}") == F.col(f"t.{config.business_key}"),
            "left",
        )

        if config.handle_late_arriving:
            late_condition = (
                F.col("t.t_effective_from").isNotNull()
                & (F.col(f"s.{config.effective_from_column}") < F.col("t.t_effective_from"))
            )
        else:
            late_condition = F.lit(False)

        late_changes = changes_with_current.filter(late_condition)
        regular_changes = changes_with_current.filter(~late_condition).select("s.*")

        change_keys = (
            regular_changes.select(
                F.col(config.business_key).alias("_bk"),
                F.col(config.effective_from_column).alias("_new_effective_from"),
            )
            .dropDuplicates(["_bk"])
        )

        expired_df = (
            current_df.alias("t")
            .join(change_keys.alias("s"), F.col(f"t.{config.business_key}") == F.col("s._bk"), "inner")
            .withColumn(config.effective_to_column, F.col("s._new_effective_from"))
            .withColumn(config.is_current_column, F.lit(False))
            .withColumn(config.updated_at_column, F.current_timestamp())
            .drop("_bk", "_new_effective_from")
        )

        unchanged_current = current_df.join(
            change_keys.select("_bk"), current_df[config.business_key] == F.col("_bk"), "left_anti"
        )

        late_df = (
            late_changes.select("s.*", "t.t_effective_from")
            .withColumn(config.effective_to_column, F.col("t.t_effective_from"))
            .withColumn(config.is_current_column, F.lit(False))
            .withColumn(config.updated_at_column, F.current_timestamp())
            .drop("t_effective_from")
        )

        merged_df = (
            historical_df.unionByName(unchanged_current, allowMissingColumns=True)
            .unionByName(expired_df, allowMissingColumns=True)
            .unionByName(regular_changes, allowMissingColumns=True)
            .unionByName(late_df, allowMissingColumns=True)
        )

        # Break lineage before overwriting the same path
        merged_df = merged_df.persist()
        merged_df.count()
        _write_table(merged_df, target_path)
        merged_df.unpersist()

        logger.info("SCD2 updated %s rows", processed_count)
        changes_df.unpersist()

        return {
            "success": True,
            "records_processed": processed_count,
            "message": "SCD2 incremental update completed",
        }

    except Exception as e:
        logger.error(f"SCD Type 2 operation failed: {e}")
        return {"success": False, "error": str(e)}


def validate_scd2_table(spark: SparkSession, table_path: str, config: SCD2Config) -> dict[str, Any]:
    """
    Validate SCD2 table integrity and constraints.

    Args:
        spark: SparkSession instance
        table_path: Path to the SCD2 table
        config: SCD2 configuration used

    Returns:
        Dictionary with validation results
    """
    try:
        df = _read_table(spark, table_path)

        validation_results = {
            "valid": True,
            "message": "validation passed",
            "errors": [],
            "warnings": [],
        }

        # Check required columns exist
        required_columns = [
            config.business_key,
            config.effective_from_column,
            config.effective_to_column,
            config.is_current_column,
            config.hash_column,
        ]

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            validation_results["errors"].append(f"Missing required columns: {missing_columns}")
            validation_results["valid"] = False

        # Check only one current record per business key
        current_flag = F.col(config.is_current_column)
        current_expr = (current_flag == F.lit(True)) | (
            F.lower(current_flag.cast("string")) == F.lit("true")
        )
        current_records = df.filter(current_expr)
        duplicate_current = (
            current_records.groupBy(config.business_key).count().filter(F.col("count") > 1)
        )

        if duplicate_current.count() > 0:
            validation_results["errors"].append(
                "Multiple current records found for some business keys"
            )
            validation_results["valid"] = False

        # Check no overlapping effective periods
        if config.handle_late_arriving:
            # This is a simplified check - in production you'd want more sophisticated logic
            overlapping = df.alias("a").join(
                df.alias("b"),
                (F.col(f"a.{config.business_key}") == F.col(f"b.{config.business_key}"))
                & (
                    F.col(f"a.{config.effective_from_column}")
                    < F.col(f"b.{config.effective_to_column}")
                )
                & (
                    F.col(f"a.{config.effective_to_column}")
                    > F.col(f"b.{config.effective_from_column}")
                ),
            )

            if overlapping.count() > 0:
                validation_results["warnings"].append(
                    "Potential overlapping effective periods detected"
                )

        # Check data quality
        null_business_keys = df.filter(F.col(config.business_key).isNull()).count()

        if null_business_keys > 0:
            validation_results["errors"].append(
                f"Found {null_business_keys} records with null business keys"
            )
            validation_results["valid"] = False

        # Check effective date logic
        invalid_dates = df.filter(
            (F.col(config.effective_from_column).isNotNull())
            & (F.col(config.effective_to_column).isNotNull())
            & (F.col(config.effective_from_column) >= F.col(config.effective_to_column))
        ).count()

        if invalid_dates > 0:
            validation_results["errors"].append(
                f"Found {invalid_dates} records with invalid effective date ranges"
            )
            validation_results["valid"] = False

        if not validation_results["valid"]:
            validation_results["message"] = "validation failed"

        return validation_results

    except Exception as e:
        logger.error(f"SCD2 validation failed: {e}")
        return {
            "valid": False,
            "message": "validation failed",
            "errors": [f"Validation error: {str(e)}"],
            "warnings": [],
        }


def get_scd2_history(
    spark: SparkSession, table_path: str, config: SCD2Config, business_key_value: Any
) -> DataFrame:
    """
    Get complete history for a specific business key.

    Args:
        spark: SparkSession instance
        table_path: Path to the SCD2 table
        config: SCD2 configuration
        business_key_value: Value of the business key to get history for

    Returns:
        DataFrame with complete history for the business key
    """
    df = _read_table(spark, table_path)

    return df.filter(F.col(config.business_key) == business_key_value).orderBy(
        config.effective_from_column
    )


def get_current_records(spark: SparkSession, table_path: str, config: SCD2Config) -> DataFrame:
    """
    Get all current records from SCD2 table.

    Args:
        spark: SparkSession instance
        table_path: Path to the SCD2 table
        config: SCD2 configuration

    Returns:
        DataFrame with only current records
    """
    df = _read_table(spark, table_path)

    current_flag = F.col(config.is_current_column)
    current_expr = (current_flag == F.lit(True)) | (
        F.lower(current_flag.cast("string")) == F.lit("true")
    )
    return df.filter(current_expr)
