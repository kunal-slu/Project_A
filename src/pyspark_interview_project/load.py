import logging
from pyspark.sql import DataFrame, SparkSession
from .io_utils import read_delta_or_parquet, write_delta as _write_delta, write_parquet as _write_parquet

logger = logging.getLogger(__name__)


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """
    Read DataFrame from Delta Lake format with fallback to Parquet.

    Args:
        spark: SparkSession instance
        path: Input path

    Returns:
        DataFrame: Loaded DataFrame
    """
    return read_delta_or_parquet(spark, path, strict_delta=True)


def write_delta(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """
    Write DataFrame to Delta Lake format.

    Args:
        df: DataFrame to write
        path: Output path
        mode: Write mode (overwrite, append, etc.)
    """
    _write_delta(df, path, mode=mode)


def write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """
    Write DataFrame to Parquet format.

    Args:
        df: DataFrame to write
        path: Output path
        mode: Write mode (overwrite, append, etc.)
    """
    _write_parquet(df, path, mode=mode)


def write_avro(df: DataFrame, path: str) -> None:
    logger.info(f"Writing Avro to {path}")
    df.write.format("avro").mode("overwrite").save(path)


def write_json(df: DataFrame, path: str) -> None:
    logger.info(f"Writing JSON to {path}")
    df.write.mode("overwrite").json(path)
