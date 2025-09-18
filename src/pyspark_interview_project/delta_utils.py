"""
Delta Lake utilities for table operations and maintenance.
"""

import logging
import os
from typing import Iterable, Dict, Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class DeltaUtils:
    """
    Delta Lake utilities class for table operations and maintenance.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def optimize_table(self, table_path: str) -> Dict[str, Any]:
        """
        Optimize Delta table for better performance.

        Args:
            table_path: Path to the Delta table

        Returns:
            dict: Optimization result
        """
        try:
            if not table_exists(self.spark, table_path):
                return {"success": False, "error": "Table does not exist"}

            # Optimize table
            self.spark.sql(f"OPTIMIZE '{table_path}'")

            # Z-order optimization if columns are specified
            # This would typically be configured based on query patterns
            logger.info(f"Table {table_path} optimized successfully")
            return {"success": True, "message": "Table optimized successfully"}

        except Exception as e:
            logger.error(f"Failed to optimize table {table_path}: {str(e)}")
            return {"success": False, "error": str(e)}

    def set_retention_policy(self, table_path: str, retention_days: int) -> Dict[str, Any]:
        """
        Set retention policy for Delta table.

        Args:
            table_path: Path to the Delta table
            retention_days: Number of days to retain data

        Returns:
            dict: Retention policy result
        """
        try:
            if not table_exists(self.spark, table_path):
                return {"success": False, "error": "Table does not exist"}

            # Set retention policy
            self.spark.sql(f"ALTER TABLE delta.`{table_path}` SET TBLPROPERTIES ('delta.logRetentionDuration' = '{retention_days} days')")

            logger.info(f"Retention policy set to {retention_days} days for table {table_path}")
            return {"success": True, "message": f"Retention policy set to {retention_days} days"}

        except Exception as e:
            logger.error(f"Failed to set retention policy for table {table_path}: {str(e)}")
            return {"success": False, "error": str(e)}

    def cleanup_old_partitions(self, table_path: str, partition_column: str,
                               retention_days: int) -> Dict[str, Any]:
        """
        Clean up old partitions from Delta table.

        Args:
            table_path: Path to the Delta table
            partition_column: Name of the partition column
            retention_days: Number of days to retain partitions

        Returns:
            dict: Cleanup result
        """
        try:
            if not table_exists(self.spark, table_path):
                return {"success": False, "error": "Table does not exist"}

            # Clean up old partitions
            self.spark.sql(f"DELETE FROM delta.`{table_path}` WHERE {partition_column} < date_sub(current_date(), {retention_days})")

            logger.info(f"Cleaned up old partitions from table {table_path}")
            return {"success": True, "message": "Old partitions cleaned up successfully"}

        except Exception as e:
            logger.error(f"Failed to cleanup old partitions from table {table_path}: {str(e)}")
            return {"success": False, "error": str(e)}

    def get_table_stats(self, table_path: str) -> Dict[str, Any]:
        """
        Get statistics for Delta table.

        Args:
            table_path: Path to the Delta table

        Returns:
            dict: Table statistics
        """
        try:
            if not table_exists(self.spark, table_path):
                return {"success": False, "error": "Table does not exist"}

            # Get table statistics
            stats_df = self.spark.sql(f"DESCRIBE DETAIL '{table_path}'")
            stats = stats_df.limit(1).first().asDict()

            logger.info(f"Retrieved statistics for table {table_path}")
            return {"success": True, "stats": stats}

        except Exception as e:
            logger.error(f"Failed to get statistics for table {table_path}: {str(e)}")
            return {"success": False, "error": str(e)}

    def run_maintenance_routine(self, table_path: str) -> Dict[str, Any]:
        """
        Run maintenance routine on Delta table.

        Args:
            table_path: Path to the Delta table

        Returns:
            dict: Maintenance result
        """
        try:
            if not table_exists(self.spark, table_path):
                return {"success": False, "error": "Table does not exist"}

            # Run maintenance routine
            # 1. Optimize table
            self.optimize_table(table_path)

            # 2. Vacuum table to remove old files
            self.spark.sql(f"VACUUM '{table_path}' RETAIN 168 HOURS")

            # 3. Generate statistics
            self.spark.sql(f"ANALYZE TABLE delta.`{table_path}` COMPUTE STATISTICS FOR ALL COLUMNS")

            logger.info(f"Maintenance routine completed for table {table_path}")
            return {"success": True, "message": "Maintenance routine completed successfully"}

        except Exception as e:
            logger.error(f"Failed to run maintenance routine on table {table_path}: {str(e)}")
            return {"success": False, "error": str(e)}


def table_exists(spark: SparkSession, path: str) -> bool:
    """
    Check if Delta table exists at the specified path.

    This function checks if a Delta table exists, with fallback to Parquet
    if Delta Lake is not available.

    Args:
        spark: SparkSession instance
        path: Table path

    Returns:
        bool: True if table exists, False otherwise
    """
    try:
        DeltaTable.forPath(spark, path)
        return True
    except Exception:
        # Fallback: check if Parquet files exist
        try:
            if os.path.exists(path):
                # Try to read as Parquet
                spark.read.format("parquet").load(path).limit(1).count()
                return True
        except Exception:
            pass
        return False


def merge_upsert(
    spark: SparkSession,
    incoming_df: DataFrame,
    target_path: str,
    keys: Iterable[str],
    partition_by: Iterable[str] | None = None,
) -> None:
    """
    Idempotent upsert into a Delta table using MERGE with fallback to overwrite.

    This function performs an idempotent upsert operation. If Delta Lake is not
    available, it falls back to a simple overwrite operation.

    Args:
        spark: SparkSession instance
        incoming_df: DataFrame to upsert
        target_path: Target table path
        keys: List of key columns for merge
        partition_by: List of partition columns
    """
    if not table_exists(spark, target_path):
        # Create new table
        try:
            writer = (
                incoming_df.write.format("delta")
                .mode("overwrite")
                .option("mergeSchema", "true")
            )
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.save(target_path)
            return
        except Exception as e:
            # Fallback to Parquet
            logger.warning(f"Delta Lake write failed: {str(e)}")
            writer = incoming_df.write.format("parquet").mode("overwrite")
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.save(target_path)
            return

    try:
        # Try Delta Lake merge
        target = DeltaTable.forPath(spark, target_path)
        on_expr = " AND ".join([f"t.{k} = s.{k}" for k in keys])
        # Alias for clarity
        target.alias("t").merge(
            source=incoming_df.alias("s"),
            condition=on_expr,
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    except Exception as e:
        # Fallback to overwrite if Delta Lake merge fails
        logger.warning(f"Delta Lake merge failed: {str(e)}")
        logger.info("Falling back to overwrite operation")
        writer = incoming_df.write.format("parquet").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(target_path)


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """
    Read data from Delta table with fallback to Parquet.

    Args:
        spark: SparkSession instance
        path: Table path

    Returns:
        DataFrame: Loaded data
    """
    try:
        return spark.read.format("delta").load(path)
    except Exception:
        # Fallback to Parquet
        logger.warning(f"Delta Lake read failed, falling back to Parquet for {path}")
        return spark.read.format("parquet").load(path)


def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_by: Iterable[str] | None = None,
    merge_schema: bool = True
) -> None:
    """
    Write DataFrame to Delta table with fallback to Parquet.

    Args:
        df: DataFrame to write
        path: Target path
        mode: Write mode (overwrite, append, etc.)
        partition_by: Partition columns
        merge_schema: Whether to merge schema
    """
    try:
        writer = (
            df.write.format("delta")
            .mode(mode)
            .option("mergeSchema", str(merge_schema).lower())
        )
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
    except Exception as e:
        # Fallback to Parquet
        logger.warning(f"Delta Lake write failed: {str(e)}, falling back to Parquet")
        writer = df.write.format("parquet").mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
