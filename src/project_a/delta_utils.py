"""
Delta Lake utilities for enterprise data platform.
Provides staging, validation, and publish patterns.
"""

import logging
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def spark_session(app: str = "projecta", config: dict[str, Any] | None = None) -> SparkSession:
    """Create Spark session with Delta Lake extensions."""
    builder = (
        SparkSession.builder.appName(app)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    # Add custom configs
    if config:
        for key, value in config.items():
            builder = builder.config(key, value)

    # Set default optimizations
    builder = (
        builder.config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "64MB")
    )

    spark = builder.getOrCreate()
    logger.info(f"Spark session created: {app}")
    return spark


def write_staging(df: DataFrame, path: str, partition_cols: list[str] | None = None) -> None:
    """Write DataFrame to staging path with audit columns."""
    staging_df = df.withColumn("ingest_ts", F.current_timestamp()).withColumn(
        "batch_id", F.monotonically_increasing_id()
    )

    writer = staging_df.write.format("delta").mode("overwrite")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(path)
    logger.info(f"Staging data written to: {path}")


def merge_publish(
    spark: SparkSession,
    staging_path: str,
    publish_path: str,
    key: str,
    additional_conditions: str | None = None,
) -> None:
    """Merge staging data into publish table with SCD2 pattern."""
    try:
        dst = DeltaTable.forPath(spark, publish_path)
        src = spark.read.format("delta").load(staging_path)

        # Build merge condition
        condition = f"t.{key} = s.{key}"
        if additional_conditions:
            condition += f" AND {additional_conditions}"

        # SCD2 merge pattern
        merge_operation = (
            dst.alias("t")
            .merge(src.alias("s"), condition)
            .whenMatchedUpdate(
                set={
                    "email": "s.email",
                    "name": "s.name",
                    "updated_at": "s.ingest_ts",
                    "is_current": "true",
                }
            )
            .whenNotMatchedInsertAll()
        )

        merge_operation.execute()
        logger.info(f"Data published to: {publish_path}")

    except Exception as e:
        logger.error(f"Merge publish failed: {e}")
        raise


def optimize_table(
    spark: SparkSession, table_path: str, zorder_columns: list[str] | None = None
) -> None:
    """Optimize Delta table with Z-ordering."""
    try:
        if zorder_columns:
            spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({', '.join(zorder_columns)})")
        else:
            spark.sql(f"OPTIMIZE delta.`{table_path}`")
        logger.info(f"Table optimized: {table_path}")
    except Exception as e:
        logger.error(f"Table optimization failed: {e}")
        raise


def vacuum_table(spark: SparkSession, table_path: str, retain_hours: int = 168) -> None:
    """Vacuum Delta table to remove old files."""
    try:
        spark.sql(f"VACUUM delta.`{table_path}` RETAIN {retain_hours} HOURS")
        logger.info(f"Table vacuumed: {table_path}")
    except Exception as e:
        logger.error(f"Table vacuum failed: {e}")
        raise


def get_table_history(spark: SparkSession, table_path: str, limit: int = 10) -> DataFrame:
    """Get table history for audit purposes."""
    return spark.sql(f"DESCRIBE HISTORY delta.`{table_path}` LIMIT {limit}")


def get_table_details(spark: SparkSession, table_path: str) -> DataFrame:
    """Get detailed table information."""
    return spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`")
