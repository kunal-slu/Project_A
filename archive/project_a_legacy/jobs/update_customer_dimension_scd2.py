"""
SCD Type 2 implementation for customer dimension.

Tracks historical changes to customer records with effective dates.
"""

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    when,
)
from pyspark.sql.functions import (
    max as spark_max,
)

logger = logging.getLogger(__name__)


def hash_record(df: DataFrame, key_cols: list[str], hash_col_name: str = "_hash") -> DataFrame:
    """
    Calculate hash of key columns to detect changes.

    Args:
        df: Input DataFrame
        key_cols: Columns to include in hash
        hash_col_name: Name of hash column

    Returns:
        DataFrame with hash column
    """
    from pyspark.sql.functions import concat_ws, sha2

    # Create hash from key columns
    hash_expr = sha2(concat_ws("|", *[col(c) for c in key_cols]), 256)

    return df.withColumn(hash_col_name, hash_expr)


def update_customer_dimension_scd2(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    key_column: str = "customer_id",
    version_col: str = "version",
    effective_from_col: str = "effective_from",
    effective_to_col: str = "effective_to",
    is_current_col: str = "is_current",
) -> int:
    """
    Update customer dimension with SCD Type 2 logic.

    Process:
    1. Add hash to source records
    2. Compare with target (existing records)
    3. Identify new records, changed records, unchanged records
    4. Close old versions (set effective_to)
    5. Insert new versions

    Args:
        spark: SparkSession
        source_df: Source DataFrame with customer data
        target_path: Path to target Delta table
        key_column: Primary key column
        version_col: Version number column
        effective_from_col: Effective from date column
        effective_to_col: Effective to date column
        is_current_col: Is current record flag column

    Returns:
        Number of records inserted/updated
    """
    logger.info("Starting SCD2 update for customer dimension")

    # Add hash to source
    source_with_hash = hash_record(
        source_df,
        key_cols=[col for col in source_df.columns if col != key_column],
        hash_col_name="_source_hash",
    ).withColumn("_source_ts", current_timestamp())

    try:
        # Read existing dimension table
        target = DeltaTable.forPath(spark, target_path)
        existing_df = target.toDF()

        logger.info(f"Existing records: {existing_df.count():,}")

        # Add hash to existing records
        existing_with_hash = hash_record(
            existing_df,
            key_cols=[
                col
                for col in existing_df.columns
                if col
                not in [
                    key_column,
                    version_col,
                    effective_from_col,
                    effective_to_col,
                    is_current_col,
                    "_hash",
                ]
            ],
            hash_col_name="_target_hash",
        )

        # Get current versions only
        current_versions = existing_with_hash.filter(col(is_current_col) == lit(True))

        # Join source with current versions
        comparison_df = source_with_hash.alias("source").join(
            current_versions.alias("target"),
            col("source.customer_id") == col("target.customer_id"),
            "full_outer",
        )

        # Classify records
        # New records: in source, not in target
        new_records = comparison_df.filter(col("target.customer_id").isNull()).select(
            [col(f"source.{c}") for c in source_with_hash.columns if not c.startswith("_")]
        )

        # Changed records: in both, but hash differs
        changed_records = comparison_df.filter(
            col("source.customer_id").isNotNull()
            & col("target.customer_id").isNotNull()
            & (col("source._source_hash") != col("target._target_hash"))
        ).select([col(f"source.{c}") for c in source_with_hash.columns if not c.startswith("_")])

        # Unchanged records: hash matches (no action needed)
        unchanged_count = comparison_df.filter(
            col("source._source_hash") == col("target._target_hash")
        ).count()

        logger.info("📊 SCD2 Analysis:")
        logger.info(f"   New records: {new_records.count():,}")
        logger.info(f"   Changed records: {changed_records.count():,}")
        logger.info(f"   Unchanged records: {unchanged_count:,}")

        # Prepare new/changed records for insertion
        records_to_insert = new_records.unionByName(changed_records, allowMissingColumns=True)

        if records_to_insert.count() == 0:
            logger.info("✅ No changes detected, SCD2 update complete")
            return 0

        # Add SCD2 metadata
        records_with_scd2 = (
            records_to_insert.withColumn(version_col, lit(1))
            .withColumn(effective_from_col, current_timestamp())
            .withColumn(effective_to_col, lit(None).cast("timestamp"))
            .withColumn(is_current_col, lit(True))
        )

        # For changed records, we need to:
        # 1. Close old versions (set effective_to and is_current=False)
        # 2. Insert new versions

        # Close old versions for changed records
        changed_customer_ids = changed_records.select(key_column).distinct()

        update_condition = (
            f"target.{key_column} = source.{key_column} AND target.{is_current_col} = true"
        )

        (
            target.alias("target")
            .merge(changed_customer_ids.alias("source"), update_condition)
            .whenMatchedUpdate(
                set={effective_to_col: current_timestamp(), is_current_col: lit(False)}
            )
            .execute()
        )

        logger.info("✅ Closed old versions for changed records")

        # Insert new versions
        # Get next version numbers
        max_version_df = existing_df.groupBy(key_column).agg(
            spark_max(version_col).alias("max_version")
        )

        records_with_versions = (
            records_with_scd2.alias("new")
            .join(
                max_version_df.alias("max"),
                col(f"new.{key_column}") == col(f"max.{key_column}"),
                "left",
            )
            .withColumn(
                version_col,
                when(col("max.max_version").isNotNull(), col("max.max_version") + 1).otherwise(
                    lit(1)
                ),
            )
            .drop("max_version")
        )

        # Insert new records
        records_with_versions.write.format("delta").mode("append").save(target_path)

        inserted_count = records_with_versions.count()
        logger.info(f"✅ Inserted {inserted_count:,} new/updated records")

        return inserted_count

    except Exception:
        # Table doesn't exist, create initial version
        logger.info(f"Creating new SCD2 dimension table: {target_path}")

        initial_scd2 = (
            source_with_hash.withColumn(version_col, lit(1))
            .withColumn(effective_from_col, current_timestamp())
            .withColumn(effective_to_col, lit(None).cast("timestamp"))
            .withColumn(is_current_col, lit(True))
            .drop("_source_hash", "_source_ts")
        )

        initial_scd2.write.format("delta").mode("overwrite").save(target_path)

        count = initial_scd2.count()
        logger.info(f"✅ Created initial SCD2 dimension with {count:,} records")

        return count


if __name__ == "__main__":
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent.parent.parent))

    from project_a.utils.config import load_conf
    from project_a.utils.spark_session import build_spark

    config = load_conf("config/local.yaml")
    spark = build_spark(app_name="scd2_customer_dim", config=config)

    # Load source data (from silver)
    silver_path = config.get("data_lake", {}).get("silver_path", "data/lakehouse_delta/silver")
    source_df = spark.read.format("delta").load(f"{silver_path}/customers")

    # Update dimension
    gold_path = config.get("data_lake", {}).get("gold_path", "data/lakehouse_delta/gold")
    target_path = f"{gold_path}/dim_customer_scd2"

    update_customer_dimension_scd2(spark, source_df, target_path)

    spark.stop()
