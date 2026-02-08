import logging
from typing import Any

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def build_spark(
    app_name: str = "project_a",
    config: dict[str, Any] | None = None,
) -> SparkSession:
    """
    Create a real SparkSession for both local and EMR Serverless.

    - Disables Spark event logging to avoid "Target log directory already exists"
      errors on EMR Serverless.
    - Does NOT fall back to a Mock() session – if Spark can't start, we raise.
    """
    logger.info("Creating SparkSession (app_name=%s)", app_name)

    # Read optional spark config from YAML
    cfg = (config or {}).get("spark", {}) if config else {}

    builder = (
        SparkSession.builder.appName(app_name)
        # Critical: disable event logs that were causing IOExceptions on EMR
        .config("spark.eventLog.enabled", "false")
    )

    # Determine if Delta should be enabled
    # Default: False for local, True for AWS/EMR
    environment = (config or {}).get("environment") or (config or {}).get("env", "local")
    is_local = environment in ("local", "dev_local")

    # Enable Delta only if explicitly requested OR if not local
    enable_delta = cfg.get("enable_delta", not is_local)

    if enable_delta:
        builder = builder.config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        logger.info("Delta Lake extensions enabled")
    else:
        logger.info("Delta Lake extensions disabled for local execution (using Parquet)")

    # Apply any extra configs from YAML (spark.extra_conf)
    extra_conf: dict[str, str] = cfg.get("extra_conf", {}) if cfg else {}
    for k, v in extra_conf.items():
        builder = builder.config(k, v)

    try:
        spark = builder.getOrCreate()
        logger.info("SparkSession created successfully (master=%s)", spark.sparkContext.master)
        return spark
    except Exception:
        # On EMR we WANT the job to fail loudly, not silently use a Mock
        logger.exception("Failed to create SparkSession – aborting job instead of using mock.")
        raise
