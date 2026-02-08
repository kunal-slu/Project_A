"""
Safe Delta Lake writer utilities with data safety guarantees.

This module provides production-ready patterns for writing to Delta Lake
with proper safety mechanisms to prevent data loss.
"""

import logging
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class SafeDeltaWriter:
    """
    Production-safe Delta Lake writer with multiple write strategies.

    Features:
    - Prevents unsafe overwrites
    - Validates row counts before/after writes
    - Supports partition-scoped overwrites
    - Implements safe MERGE operations
    - Emits data quality metrics
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write_with_merge(
        self,
        df: DataFrame,
        target_path: str,
        merge_keys: list[str] | None = None,
        mode: str = "merge",
        replace_where_condition: str | None = None,
        partition_cols: list[str] | None = None,
        pre_write_hook: Any | None = None,
        post_write_hook: Any | None = None,
    ) -> dict[str, Any]:
        """
        Safe Delta write helper supporting append, merge, and partition overwrite.

        Args:
            df: Source DataFrame
            target_path: Path to Delta table
            merge_keys: Columns to use as merge keys (required for merge mode)
            mode: 'append', 'merge', or 'overwrite'
            replace_where_condition: Required for overwrite mode
            partition_cols: Optional partition columns
            pre_write_hook: Optional hook to transform df before write
            post_write_hook: Optional hook called after write (df, row_count)
        """
        try:
            if pre_write_hook:
                df = pre_write_hook(df)

            source_count = df.count()
            logger.info("Starting %s operation: %s source records", mode, source_count)

            # Determine if target exists
            try:
                target_table = DeltaTable.forPath(self.spark, target_path)
                target_exists = True
                before_count = target_table.toDF().count()
            except Exception as exc:
                target_table = None
                target_exists = False
                before_count = 0
                logger.debug("DeltaTable.forPath(%s) failed; treating as missing: %s", target_path, exc)
                # Fallback: table may exist but DeltaTable is unavailable
                try:
                    existing_df = self.spark.read.format("delta").load(target_path)
                    target_exists = True
                    before_count = existing_df.count()
                except Exception:
                    pass

            if mode == "append":
                writer = df.write.format("delta").mode("append" if target_exists else "overwrite")
                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)
                writer.save(target_path)

            elif mode == "merge":
                if not merge_keys:
                    raise ValueError("Merge keys must be provided")

                if not target_exists:
                    writer = df.write.format("delta").mode("overwrite")
                    if partition_cols:
                        writer = writer.partitionBy(*partition_cols)
                    writer.save(target_path)
                else:
                    if target_table is not None:
                        merge_condition = " AND ".join(
                            [f"target.{key} = source.{key}" for key in merge_keys]
                        )
                        (
                            target_table.alias("target")
                            .merge(df.alias("source"), merge_condition)
                            .whenMatchedUpdateAll()
                            .whenNotMatchedInsertAll()
                            .execute()
                        )
                    else:
                        # Manual merge fallback (prefer source rows)
                        existing_df = self.spark.read.format("delta").load(target_path)
                        incoming = df.withColumn("_is_source", F.lit(1))
                        existing = existing_df.withColumn("_is_source", F.lit(0))
                        combined = existing.unionByName(incoming, allowMissingColumns=True)
                        window = Window.partitionBy(*merge_keys).orderBy(
                            F.col("_is_source").desc()
                        )
                        merged = (
                            combined.withColumn("_rn", F.row_number().over(window))
                            .filter(F.col("_rn") == 1)
                            .drop("_rn", "_is_source")
                        )
                        merged.write.format("delta").mode("overwrite").save(target_path)

            elif mode == "overwrite":
                if not replace_where_condition:
                    raise ValueError("Unsupported write mode")
                writer = (
                    df.write.format("delta")
                    .mode("overwrite")
                    .option("replaceWhere", replace_where_condition)
                )
                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)
                writer.save(target_path)

            else:
                raise ValueError("Unsupported write mode")

            after_count = self.spark.read.format("delta").load(target_path).count()

            if post_write_hook:
                post_write_hook(df, source_count)

            metrics = {
                "success": True,
                "initial_row_count": before_count,
                "final_row_count": after_count,
                "records_written": source_count,
            }
            logger.info("%s completed successfully: %s", mode, metrics)
            return metrics

        except Exception as e:
            logger.error("%s operation failed: %s", mode, e)
            raise

    def write_partition_overwrite(
        self,
        df: DataFrame,
        target_path: str,
        partition_col: str,
        partition_value: str,
        validate_partition: bool = True,
    ) -> dict[str, Any]:
        """
        Safely overwrite a single partition using replaceWhere.

        Args:
            df: Source DataFrame (should contain only target partition data)
            target_path: Path to Delta table
            partition_col: Partition column name
            partition_value: Partition value to overwrite
            validate_partition: Whether to validate source data matches partition

        Returns:
            Dictionary with write metrics
        """
        try:
            source_count = df.count()
            logger.info(
                f"Starting partition overwrite: {partition_col}={partition_value}, "
                f"{source_count} records"
            )

            # Validate source data only contains target partition
            if validate_partition:
                distinct_partitions = df.select(partition_col).distinct().collect()

                if len(distinct_partitions) == 0:
                    raise ValueError("Source DataFrame is empty")

                if len(distinct_partitions) > 1:
                    raise ValueError(
                        f"Source DataFrame contains multiple partition values: "
                        f"{[row[0] for row in distinct_partitions]}"
                    )

                actual_value = str(distinct_partitions[0][0])
                if actual_value != partition_value:
                    raise ValueError(
                        f"Source partition value '{actual_value}' does not match "
                        f"target partition '{partition_value}'"
                    )

            # Get before count for this partition
            try:
                before_df = self.spark.read.format("delta").load(target_path)
                before_count = before_df.filter(
                    F.col(partition_col) == F.lit(partition_value)
                ).count()
            except Exception:
                before_count = 0

            # Perform safe partition overwrite
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("replaceWhere", f"{partition_col} = '{partition_value}'")
                .save(target_path)
            )

            # Validate after write
            after_df = self.spark.read.format("delta").load(target_path)
            after_count = after_df.filter(F.col(partition_col) == F.lit(partition_value)).count()

            if after_count != source_count:
                logger.error(f"Row count mismatch: wrote {source_count}, found {after_count}")
                raise RuntimeError(
                    f"Row count validation failed: expected {source_count}, got {after_count}"
                )

            metrics = {
                "success": True,
                "operation": "PARTITION_OVERWRITE",
                "partition_col": partition_col,
                "partition_value": partition_value,
                "source_records": source_count,
                "before_count": before_count,
                "after_count": after_count,
                "records_replaced": before_count,
                "records_written": source_count,
            }

            logger.info(f"Partition overwrite completed successfully: {metrics}")
            return metrics

        except Exception as e:
            logger.error(f"Partition overwrite failed: {e}")
            raise RuntimeError(f"Delta partition overwrite failed: {e}") from e

    def write_append(
        self,
        df: DataFrame,
        target_path: str,
        partition_cols: list[str] | None = None,
        validate_duplicates: bool = False,
        duplicate_keys: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Safely append data with optional duplicate checking.

        Args:
            df: Source DataFrame
            target_path: Path to Delta table
            partition_cols: Optional partition columns
            validate_duplicates: Whether to check for duplicates
            duplicate_keys: Keys to use for duplicate checking

        Returns:
            Dictionary with write metrics
        """
        try:
            source_count = df.count()
            logger.info(f"Starting append operation: {source_count} records")

            # Check for duplicates if requested
            if validate_duplicates and duplicate_keys:
                duplicates = df.groupBy(*duplicate_keys).count().filter(F.col("count") > 1)

                duplicate_count = duplicates.count()
                if duplicate_count > 0:
                    logger.error(f"Found {duplicate_count} duplicate keys in source data")
                    raise ValueError(f"Source data contains {duplicate_count} duplicate keys")

            # Get before count
            try:
                before_count = self.spark.read.format("delta").load(target_path).count()
            except Exception:
                before_count = 0

            # Append data
            writer = df.write.format("delta").mode("append")
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.save(target_path)

            # Validate after write
            after_count = self.spark.read.format("delta").load(target_path).count()
            expected_count = before_count + source_count

            if after_count != expected_count:
                logger.error(f"Row count mismatch: expected {expected_count}, got {after_count}")
                raise RuntimeError(
                    f"Row count validation failed: expected {expected_count}, got {after_count}"
                )

            metrics = {
                "success": True,
                "operation": "APPEND",
                "source_records": source_count,
                "before_count": before_count,
                "after_count": after_count,
                "records_appended": source_count,
            }

            logger.info(f"Append completed successfully: {metrics}")
            return metrics

        except Exception as e:
            logger.error(f"Append operation failed: {e}")
            raise RuntimeError(f"Delta append failed: {e}") from e

    def validate_and_log_metrics(
        self, df: DataFrame, table_name: str, metrics: dict[str, Any]
    ) -> None:
        """
        Validate data quality and log metrics.

        Args:
            df: DataFrame to validate
            table_name: Name of the table for logging
            metrics: Additional metrics to log
        """
        try:
            # Basic metrics
            row_count = df.count()
            column_count = len(df.columns)

            # Null counts
            null_counts = {}
            for col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                if null_count > 0:
                    null_counts[col] = null_count

            # Log comprehensive metrics
            logger.info(
                f"Data Quality Metrics for {table_name}:",
                extra={
                    "table": table_name,
                    "row_count": row_count,
                    "column_count": column_count,
                    "null_counts": null_counts,
                    **metrics,
                },
            )

        except Exception as e:
            logger.warning(f"Failed to collect metrics: {e}")


def write_delta_safe(
    spark: SparkSession,
    df: DataFrame,
    target_path: str,
    mode: str = "merge",
    merge_keys: list[str] | None = None,
    partition_col: str | None = None,
    partition_value: str | None = None,
    partition_cols: list[str] | None = None,
) -> dict[str, Any]:
    """
    Convenience function for safe Delta writes.

    Args:
        spark: SparkSession
        df: Source DataFrame
        target_path: Path to Delta table
        mode: Write mode ('merge', 'partition_overwrite', 'append')
        merge_keys: Keys for merge operation
        partition_col: Partition column for partition_overwrite
        partition_value: Partition value for partition_overwrite
        partition_cols: Partition columns for initial table creation

    Returns:
        Dictionary with write metrics
    """
    writer = SafeDeltaWriter(spark)

    if mode == "merge":
        if not merge_keys:
            raise ValueError("merge_keys required for merge mode")
        return writer.write_with_merge(
            df=df,
            target_path=target_path,
            merge_keys=merge_keys,
            mode="merge",
            partition_cols=partition_cols,
        )

    elif mode == "partition_overwrite":
        if not partition_col or not partition_value:
            raise ValueError("partition_col and partition_value required for partition_overwrite")
        return writer.write_partition_overwrite(df, target_path, partition_col, partition_value)

    elif mode == "append":
        return writer.write_append(df, target_path, partition_cols)

    else:
        raise ValueError(f"Unsupported mode: {mode}")
