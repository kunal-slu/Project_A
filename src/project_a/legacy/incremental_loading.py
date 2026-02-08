"""
Incremental loading strategies for Delta Lake tables.

This module provides various incremental loading patterns including:
- Upsert operations with MERGE
- Append operations
- Aggregation updates
- SCD Type 1 and Type 2 implementations
- Change Data Capture (CDC)
"""

import logging
from datetime import datetime
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

logger = logging.getLogger(__name__)


class IncrementalLoader:
    """
    Handles incremental loading strategies for Delta Lake tables.
    Supports SCD Type 2, CDC, and watermark-based incremental processing.
    """

    def __init__(self, spark: SparkSession, watermark_column: str = "updated_at"):
        self.spark = spark
        self.watermark_column = watermark_column

    def get_latest_watermark(self, table_path: str) -> datetime | None:
        """
        Get the latest watermark from a Delta table.

        Args:
            table_path: Path to the Delta table

        Returns:
            Latest watermark timestamp or None if table is empty
        """
        try:
            df = self.spark.read.format("delta").load(table_path)

            if df.rdd.isEmpty():
                return None

            result = df.agg(F.max(self.watermark_column)).limit(1).first()
            return result[0] if result and result[0] else None

        except Exception as e:
            logger.warning(f"Could not get watermark from {table_path}: {e}")
            return None

    def filter_incremental_data(
        self,
        source_df: DataFrame,
        last_watermark: datetime | None = None
    ) -> DataFrame:
        """
        Filter source data to only include records newer than the watermark.

        Args:
            source_df: Source DataFrame
            last_watermark: Last processed watermark timestamp

        Returns:
            Filtered DataFrame with only new records
        """
        if last_watermark is None:
            return source_df

        return source_df.filter(F.col(self.watermark_column) > last_watermark)

    def apply_scd_type2(
        self,
        source_df: DataFrame,
        target_path: str,
        business_key: str,
        change_columns: list[str],
        last_watermark: datetime | None = None
    ) -> dict[str, Any]:
        """
        Apply SCD Type 2 logic to handle slowly changing dimensions.

        Args:
            source_df: Source DataFrame with dimension data
            target_path: Path to the target Delta table
            business_key: Column name for the business key
            change_columns: List of columns to monitor for changes
            last_watermark: Last processed watermark timestamp

        Returns:
            Dictionary with processing results
        """
        try:
            # Filter for incremental data
            incremental_df = self.filter_incremental_data(source_df, last_watermark)

            if incremental_df.rdd.isEmpty():
                logger.info("No new dimension changes to process")
                return {
                    "success": True,
                    "records_processed": 0,
                    "message": "No new data"
                }

            # Create hash for change detection
            hash_expr = F.sha2(
                F.concat_ws("||", *[
                    F.coalesce(F.col(col).cast("string"), F.lit(""))
                    for col in change_columns
                ]),
                256
            )

            # Prepare staged data
            staged_df = incremental_df.withColumn(
                "effective_from",
                F.col(self.watermark_column).cast(TimestampType())
            ).withColumn(
                "effective_to",
                F.lit(None).cast(TimestampType())
            ).withColumn(
                "is_current",
                F.lit(True)
            ).withColumn(
                "hash_diff",
                hash_expr
            ).withColumn(
                "surrogate_key",
                F.expr("uuid()")
            )

            # Check if target table exists
            try:
                target_table = DeltaTable.forPath(self.spark, target_path)
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
                F.col("is_current").isTrue()
            ).select(
                business_key,
                "hash_diff"
            ).withColumnRenamed("hash_diff", "t_hash_diff")

            # Find changed records
            changes_df = staged_df.alias("s").join(
                current_records.alias("t"),
                F.col(f"s.{business_key}") == F.col(f"t.{business_key}"),
                "left"
            ).filter(
                F.col("t.t_hash_diff").isNull() |
                (F.col("t.t_hash_diff") != F.col("s.hash_diff"))
            ).select("s.*")

            if changes_df.rdd.isEmpty():
                logger.info("No SCD2 changes detected")
                return {
                    "success": True,
                    "records_processed": 0,
                    "message": "No changes"
                }

            # Apply MERGE operation
            merge_condition = f"t.{business_key} = s.{business_key} AND t.is_current = true"

            target_table.alias("t").merge(
                changes_df.alias("s"),
                merge_condition
            ).whenMatchedUpdate(
                condition="t.hash_diff <> s.hash_diff",
                set={
                    "effective_to": "s.effective_from",
                    "is_current": "false",
                    "updated_at": "current_timestamp()"
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
            logger.error(f"SCD Type 2 incremental update failed: {e}")
            return {"success": False, "error": str(e)}

    def detect_changes(
        self,
        source_df: DataFrame,
        target_df: DataFrame,
        key_columns: list[str]
    ) -> dict[str, DataFrame]:
        """
        Detect changes between source and target DataFrames.

        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            key_columns: Columns to use as keys for comparison

        Returns:
            Dictionary with 'inserts', 'updates', and 'deletes' DataFrames
        """
        # Find inserts (records in source but not in target)
        inserts = source_df.alias("s").join(
            target_df.alias("t"),
            [F.col(f"s.{col}") == F.col(f"t.{col}") for col in key_columns],
            "left_anti"
        )

        # Find deletes (records in target but not in source)
        deletes = target_df.alias("t").join(
            source_df.alias("s"),
            [F.col(f"t.{col}") == F.col(f"s.{col}") for col in key_columns],
            "left_anti"
        )

        # Find updates (records in both but with different values)
        # Join on key columns and filter for non-key column differences
        non_key_columns = [col for col in source_df.columns if col not in key_columns]

        if non_key_columns:
            update_conditions = [
                F.col(f"s.{col}") != F.col(f"t.{col}")
                for col in non_key_columns
            ]

            updates = source_df.alias("s").join(
                target_df.alias("t"),
                [F.col(f"s.{col}") == F.col(f"t.{col}") for col in key_columns],
                "inner"
            ).filter(F.or_(*update_conditions)).select("s.*")
        else:
            # No non-key columns to compare
            updates = self.spark.createDataFrame([], source_df.schema)

        return {
            "inserts": inserts,
            "updates": updates,
            "deletes": deletes
        }

    def apply_cdc_changes(
        self,
        source_df: DataFrame,
        target_path: str,
        key_columns: list[str],
        last_watermark: datetime | None = None
    ) -> dict[str, Any]:
        """
        Apply Change Data Capture (CDC) changes to target table.

        Args:
            source_df: Source DataFrame with CDC data
            target_path: Path to target Delta table
            key_columns: Columns to use as keys
            last_watermark: Last processed watermark timestamp

        Returns:
            Dictionary with processing results
        """
        try:
            # Filter for incremental data
            incremental_df = self.filter_incremental_data(source_df, last_watermark)

            if incremental_df.rdd.isEmpty():
                return {
                    "success": True,
                    "records_processed": 0,
                    "message": "No new CDC data"
                }

            # Check if target table exists
            try:
                target_table = DeltaTable.forPath(self.spark, target_path)
                target_df = target_table.toDF()
            except Exception:
                # Create new table with all records as inserts
                incremental_df.write.format("delta").mode("overwrite").save(target_path)
                return {
                    "success": True,
                    "records_processed": incremental_df.count(),
                    "message": "New CDC table created"
                }

            # Detect changes
            changes = self.detect_changes(incremental_df, target_df, key_columns)

            # Apply changes using MERGE
            merge_condition = " AND ".join([
                f"t.{col} = s.{col}" for col in key_columns
            ])

            # Handle inserts and updates
            target_table.alias("t").merge(
                changes["inserts"].union(changes["updates"]).alias("s"),
                merge_condition
            ).whenMatchedUpdate(
                set={col: f"s.{col}" for col in incremental_df.columns if col in key_columns}
            ).whenNotMatchedInsert(
                values={col: f"s.{col}" for col in incremental_df.columns}
            ).execute()

            # Handle deletes (if supported by your CDC format)
            if not changes["deletes"].rdd.isEmpty():
                # Limit the number of deletes to process to avoid memory issues
                limited_deletes = changes["deletes"].limit(1000)
                delete_condition = " OR ".join([
                    f"({' AND '.join([f't.{col} = d.{col}' for col in key_columns])})"
                    for row in limited_deletes.collect()
                ])

                if delete_condition:
                    target_table.delete(delete_condition)

            total_processed = (
                changes["inserts"].count() +
                changes["updates"].count() +
                changes["deletes"].count()
            )

            return {
                "success": True,
                "records_processed": total_processed,
                "message": f"CDC changes applied: {total_processed} records"
            }

        except Exception as e:
            logger.error(f"CDC changes application failed: {e}")
            return {"success": False, "error": str(e)}

    def create_watermark_table(
        self,
        table_path: str,
        watermark_column: str,
        partition_columns: list[str] | None = None
    ) -> None:
        """
        Create a watermark tracking table for incremental processing.

        Args:
            table_path: Path for the watermark table
            watermark_column: Column name for watermark tracking
            partition_columns: Optional partition columns
        """
        schema = self.spark.createDataFrame([], self.spark.read.format("delta").load(table_path).schema)

        watermark_df = schema.withColumn(
            watermark_column,
            F.current_timestamp()
        ).withColumn(
            "processed_at",
            F.current_timestamp()
        )

        write_opts = watermark_df.write.format("delta").mode("overwrite")

        if partition_columns:
            write_opts = write_opts.partitionBy(*partition_columns)

        write_opts.save(table_path)

    def optimize_for_incremental_processing(
        self,
        table_path: str,
        partition_columns: list[str],
        z_order_columns: list[str] | None = None
    ) -> None:
        """
        Optimize Delta table for incremental processing.

        Args:
            table_path: Path to the Delta table
            partition_columns: Columns to partition by
            z_order_columns: Columns for Z-ordering optimization
        """
        try:
            target_table = DeltaTable.forPath(self.spark, table_path)

            # Optimize the table
            target_table.optimize().execute()

            # Z-order if specified
            if z_order_columns:
                target_table.optimize().executeZOrderBy(z_order_columns)

            # Vacuum to remove old files
            target_table.vacuum(168)  # Keep 7 days of history

            logger.info(f"Optimized table {table_path} for incremental processing")

        except Exception as e:
            logger.error(f"Failed to optimize table {table_path}: {e}")
            raise
