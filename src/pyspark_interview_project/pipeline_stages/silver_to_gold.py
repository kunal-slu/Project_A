import time
from pyspark.sql.functions import sum as ssum, count as scount
from ..logging_setup import get_logger
from ..config.paths import SILVER, GOLD
from ..metrics.metrics_exporter import PIPELINE_RUNS, ROWS_PROCESSED, STAGE_DURATION
from ..utils import get_spark_session
from ..config_loader import load_config_resolved

log = get_logger("silver_to_gold")


def build_spark(app="silver_to_gold"):
    config = load_config_resolved()
    return get_spark_session(config)


def run(input_path: str = None, output_path: str = None) -> str:
    PIPELINE_RUNS.labels(stage="silver_to_gold").inc()
    t0 = time.time()
    spark = build_spark()

    src = input_path or str(SILVER / "returns_clean")
    dst = output_path or str(GOLD / "returns_metrics")

    log.info(f"Reading silver: {src}")
    df = spark.read.parquet(src)

    # Example: gold metrics per sku and reason
    gold = (df.groupBy("sku", "reason")
              .agg(ssum("amount").alias("total_amount"),
                   scount("*").alias("return_count")))

    out_rows = gold.count()
    ROWS_PROCESSED.labels(stage="silver_to_gold").inc(out_rows)

    log.info(f"Writing gold: {dst}")
    gold.write.mode("overwrite").parquet(dst)

    duration = time.time() - t0
    STAGE_DURATION.labels(stage="silver_to_gold").set(duration)
    log.info("silver_to_gold done", extra={"rows_out": out_rows, "sec": duration})
    return dst
