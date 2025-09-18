import logging
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, current_timestamp, when
from ..logging_setup import get_logger
from ..schemas.returns_raw import returns_raw_schema
from ..config.paths import BRONZE, SILVER
from ..metrics.metrics_exporter import PIPELINE_RUNS, ROWS_PROCESSED, STAGE_DURATION
from ..utils.spark_session import build_spark
from ..io_utils import write_delta
from ..config_loader import load_config_resolved

log = get_logger("bronze_to_silver")


def build_spark(app="bronze_to_silver"):
    config = load_config_resolved()
    return get_spark_session(config)


def validate(df: DataFrame) -> DataFrame:
    # Basic DQ: not nulls, positive qty/amount, trimmed cols
    df2 = (df
           .withColumn("sku", trim(col("sku")))
           .withColumn("reason", trim(col("reason")))
           .withColumn("qty", when(col("qty").isNull() | (col("qty") < 0), None).otherwise(col("qty")))
           .withColumn("amount", when(col("amount") < 0, None).otherwise(col("amount")))
           .withColumn("dq_ingested_ts", current_timestamp()))
    return df2


def run(input_path: str = None, output_path: str = None) -> str:
    PIPELINE_RUNS.labels(stage="bronze_to_silver").inc()
    t0 = time.time()
    spark = build_spark()

    src = input_path or str(BRONZE / "returns_raw")
    dst = output_path or str(SILVER / "returns_clean")

    log.info(f"Reading bronze: {src}")
    df = spark.read.schema(returns_raw_schema).parquet(src)

    before = df.count()
    df_clean = validate(df)
    after = df_clean.count()
    ROWS_PROCESSED.labels(stage="bronze_to_silver").inc(after)

    # Partition by return_date if present
    writer = df_clean.write.mode("overwrite")
    if "return_date" in df_clean.columns:
        writer = writer.partitionBy("return_date")
    log.info(f"Writing silver: {dst}")
    writer.parquet(dst)

    duration = time.time() - t0
    STAGE_DURATION.labels(stage="bronze_to_silver").set(duration)
    log.info("bronze_to_silver done", extra={"rows_in": before, "rows_out": after, "sec": duration})
    return dst
