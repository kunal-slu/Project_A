"""
Standardized SCD2 (Slowly Changing Dimension Type 2) implementation.

This module provides a reusable `apply_scd2()` helper that can be used across
all dimension tables with consistent semantics and behavior.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)


@dataclass
class SCD2Config:
    """Configuration for SCD Type 2 operations."""
    business_key: str
    change_columns: List[str]
    effective_from_column: str = "effective_from"
    effective_to_column: str = "effective_to"
    is_current_column: str = "is_current"
    surrogate_key_column: Optional[str] = "surrogate_key"
    hash_column: str = "hash_diff"
    updated_at_column: str = "updated_at"
    ts_column: Optional[str] = None
    add_surrogate_key: bool = True
    handle_late_arriving: bool = True
    strict_mode: bool = True


def apply_scd2(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    config: SCD2Config,
    effective_from: Optional[datetime] = None
) -> Dict[str, Any]:
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
        if source_df.rdd.isEmpty():
            logger.info("No source data to process")
            return {
                "success": True,
                "records_processed": 0,
                "message": "No source data"
            }

        # Create hash for change detection
        hash_expr = F.sha2(
            F.concat_ws("||", *[
                F.coalesce(F.col(col).cast("string"), F.lit(""))
                for col in config.change_columns
            ]),
            256
        )

        # Prepare staged data
        staged_df = source_df.withColumn(
            config.effective_from_column,
            F.col(config.ts_column or config.updated_at_column).cast(TimestampType())
            if effective_from is None else F.lit(effective_from).cast(TimestampType())
        ).withColumn(
            config.effective_to_column,
            F.lit(None).cast(TimestampType())
        ).withColumn(
            config.is_current_column,
            F.lit(True)
        ).withColumn(
            config.hash_column,
            hash_expr
        )

        # Add surrogate key if requested
        if config.add_surrogate_key:
            staged_df = staged_df.withColumn(
                config.surrogate_key_column,
                F.expr("uuid()")
            )

        # Check if target table exists
        try:
            target_table = DeltaTable.forPath(spark, target_path)
            table_exists = True
        except Exception:
            table_exists = False

        if not table_exists:
            # Create new table
            staged_df.write.format("delta").mode("overwrite").save(target_path)
            return {
                "success": True,
                "records_processed": staged_df.count(),
                "message": "New SCD2 table created"
            }

        # Get current records for comparison
        current_records = target_table.toDF().filter(
            F.col(config.is_current_column).isTrue()
        ).select(
            config.business_key,
            config.hash_column
        ).withColumnRenamed(config.hash_column, "t_hash_diff")

        # Find changed records
        changes_df = staged_df.alias("s").join(
            current_records.alias("t"),
            F.col(f"s.{config.business_key}") == F.col(f"t.{config.business_key}"),
            "left"
        ).filter(
            F.col("t.t_hash_diff").isNull() |
            (F.col("t.t_hash_diff") != F.col(f"s.{config.hash_column}"))
        ).select("s.*")

        if changes_df.rdd.isEmpty():
            logger.info("No SCD2 changes detected")
            return {
                "success": True,
                "records_processed": 0,
                "message": "No changes"
            }

        # Apply MERGE operation
        merge_condition = (
            f"t.{config.business_key} = s.{config.business_key} AND "
            f"t.{config.is_current_column} = true"
        )

        target_table.alias("t").merge(
            changes_df.alias("s"),
            merge_condition
        ).whenMatchedUpdate(
            condition=f"t.{config.hash_column} <> s.{config.hash_column}",
            set={
                config.effective_to_column: f"s.{config.effective_from_column}",
                config.is_current_column: "false",
                config.updated_at_column: "current_timestamp()"
            }
        ).whenNotMatchedInsert(
            values={col: f"s.{col}" for col in changes_df.columns}
        ).execute()

        processed_count = changes_df.count()
        logger.info(f"SCD2 updated {processed_count} rows")

        return {
            "success": True,
            "records_processed": processed_count,
            "message": "SCD2 incremental update completed"
        }

    except Exception as e:
        logger.error(f"SCD Type 2 operation failed: {e}")
        return {"success": False, "error": str(e)}


def validate_scd2_table(
    spark: SparkSession,
    table_path: str,
    config: SCD2Config
) -> Dict[str, Any]:
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
        df = spark.read.format("delta").load(table_path)
        
        validation_results = {
            "success": True,
            "errors": [],
            "warnings": []
        }

        # Check required columns exist
        required_columns = [
            config.business_key,
            config.effective_from_column,
            config.effective_to_column,
            config.is_current_column,
            config.hash_column
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            validation_results["errors"].append(
                f"Missing required columns: {missing_columns}"
            )
            validation_results["success"] = False

        # Check only one current record per business key
        current_records = df.filter(F.col(config.is_current_column).isTrue())
        duplicate_current = current_records.groupBy(config.business_key).count().filter(
            F.col("count") > 1
        )
        
        if duplicate_current.count() > 0:
            validation_results["errors"].append(
                "Multiple current records found for some business keys"
            )
            validation_results["success"] = False

        # Check no overlapping effective periods
        if config.handle_late_arriving:
            # This is a simplified check - in production you'd want more sophisticated logic
            overlapping = df.alias("a").join(
                df.alias("b"),
                (F.col(f"a.{config.business_key}") == F.col(f"b.{config.business_key}")) &
                (F.col(f"a.{config.effective_from_column}") < F.col(f"b.{config.effective_to_column}")) &
                (F.col(f"a.{config.effective_to_column}") > F.col(f"b.{config.effective_from_column}"))
            )
            
            if overlapping.count() > 0:
                validation_results["warnings"].append(
                    "Potential overlapping effective periods detected"
                )

        # Check data quality
        null_business_keys = df.filter(
            F.col(config.business_key).isNull()
        ).count()
        
        if null_business_keys > 0:
            validation_results["errors"].append(
                f"Found {null_business_keys} records with null business keys"
            )
            validation_results["success"] = False

        # Check effective date logic
        invalid_dates = df.filter(
            (F.col(config.effective_from_column).isNotNull()) &
            (F.col(config.effective_to_column).isNotNull()) &
            (F.col(config.effective_from_column) >= F.col(config.effective_to_column))
        ).count()
        
        if invalid_dates > 0:
            validation_results["errors"].append(
                f"Found {invalid_dates} records with invalid effective date ranges"
            )
            validation_results["success"] = False

        return validation_results

    except Exception as e:
        logger.error(f"SCD2 validation failed: {e}")
        return {
            "success": False,
            "errors": [f"Validation error: {str(e)}"],
            "warnings": []
        }


def get_scd2_history(
    spark: SparkSession,
    table_path: str,
    config: SCD2Config,
    business_key_value: Any
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
    df = spark.read.format("delta").load(table_path)
    
    return df.filter(
        F.col(config.business_key) == business_key_value
    ).orderBy(config.effective_from_column)


def get_current_records(
    spark: SparkSession,
    table_path: str,
    config: SCD2Config
) -> DataFrame:
    """
    Get all current records from SCD2 table.
    
    Args:
        spark: SparkSession instance
        table_path: Path to the SCD2 table
        config: SCD2 configuration
        
    Returns:
        DataFrame with only current records
    """
    df = spark.read.format("delta").load(table_path)
    
    return df.filter(F.col(config.is_current_column).isTrue())
