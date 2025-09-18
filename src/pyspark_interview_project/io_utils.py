from __future__ import annotations
import logging
from typing import Optional, Sequence, Literal
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def _delta_supported() -> bool:
    """Check if Delta Lake is available."""
    try:
        import delta  # noqa
        return True
    except Exception:
        return False


def _is_delta_table(spark: SparkSession, path: str) -> bool:
    """Check if path contains a Delta table."""
    try:
        from delta.tables import DeltaTable
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        try:
            jvm = spark._jvm
            hconf = spark._jsc.hadoopConfiguration()
            p = jvm.org.apache.hadoop.fs.Path(path.rstrip('/') + '/_delta_log')
            return p.getFileSystem(hconf).exists(p)
        except Exception:
            return False


def write_parquet(
    df: DataFrame,
    path: str,
    mode: Literal["overwrite", "append"] = "overwrite",
    compression: str = "snappy",
    partition_by: Optional[Sequence[str]] = None
) -> None:
    """Write DataFrame to Parquet format with safety checks."""
    w = df.write.mode(mode).format("parquet").option("compression", compression)
    if partition_by:
        w = w.partitionBy(*partition_by)
    logger.info("Writing Parquet -> %s", path)
    w.save(path)


def write_delta(
    df: DataFrame,
    path: str,
    mode: Literal["overwrite", "append"] = "overwrite",
    partition_by: Optional[Sequence[str]] = None,
    merge_schema: bool = True,
    strict_delta: bool = True
) -> None:
    """Write DataFrame to Delta format with fallback to Parquet if Delta unavailable."""
    if not _delta_supported():
        if strict_delta:
            raise RuntimeError("Delta not available; refusing to write Parquet silently.")
        return write_parquet(df, path, mode=mode, partition_by=partition_by)

    w = df.write.format("delta").mode(mode).option("mergeSchema", str(merge_schema).lower())
    if partition_by:
        w = w.partitionBy(*partition_by)
    logger.info("Writing Delta -> %s", path)
    w.save(path)


def read_delta_or_parquet(
    spark: SparkSession,
    path: str,
    strict_delta: bool = False
) -> DataFrame:
    """Read from Delta table with fallback to Parquet."""
    if _is_delta_table(spark, path):
        logger.info("Reading Delta <- %s", path)
        return spark.read.format("delta").load(path)

    if strict_delta:
        raise RuntimeError(f"Expected Delta table at {path}")

    logger.info("Reading Parquet fallback <- %s", path)
    return spark.read.format("parquet").load(path)
