"""
FX JSON Reader - Extract and normalize FX rates from JSON Lines format.

Reads JSON Lines (NDJSON) from S3, enforces schema, cleans nulls, and normalizes.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def read_fx_json(
    spark: SparkSession,
    config: dict,
) -> DataFrame:
    """
    Read FX JSON lines from S3, enforce schema, clean nulls.

    Expected config:
      config["sources"]["fx"]["raw_path"]

    Args:
        spark: SparkSession instance
        config: Configuration dictionary

    Returns:
        DataFrame with normalized FX rates
    """
    sources_cfg = config.get("sources", {})
    fx_cfg = sources_cfg.get("fx", {})
    raw_path = fx_cfg.get("raw_path", "")
    files = fx_cfg.get("files", {})

    if not raw_path:
        raise ValueError("FX raw_path not found in config")

    # Construct full path to JSON file
    json_file = files.get("daily_rates_json", "fx_rates_historical.json")
    json_path = f"{raw_path.rstrip('/')}/{json_file}"

    logger.info(f"📥 Reading FX JSON from: {json_path}")

    # Read JSON with explicit schema (no inference)
    from project_a.schemas.bronze_schemas import FX_RATES_SCHEMA

    df = spark.read.schema(FX_RATES_SCHEMA).json(json_path)

    initial_count = df.count()
    logger.info(f"📊 Read {initial_count:,} raw JSON records")

    # Normalize column names defensively (handle variations)
    if "ccy" in df.columns and "base_ccy" not in df.columns:
        df = df.withColumnRenamed("ccy", "base_ccy")
    if "fx_rate" in df.columns and "rate" not in df.columns:
        df = df.withColumnRenamed("fx_rate", "rate")
    if "currency" in df.columns and "quote_ccy" not in df.columns:
        df = df.withColumnRenamed("currency", "quote_ccy")

    # Standardize and clean
    # Rename JSON columns (base_ccy, quote_ccy, rate) to schema standard (base_currency, target_currency, exchange_rate)
    df = (
        df.withColumn(
            "date",
            F.coalesce(F.to_date("date", "yyyy-MM-dd"), F.to_date("trade_date", "yyyy-MM-dd")),
        )
        .withColumn("base_currency", F.upper(F.coalesce(F.col("base_ccy"), F.col("base_currency"))))
        .withColumn("target_currency", F.upper(F.coalesce(F.col("quote_ccy"), F.col("target_currency"))))
        .withColumn("exchange_rate", F.coalesce(F.col("rate"), F.col("exchange_rate")).cast("double"))
        .withColumn("record_source", F.coalesce(F.col("source"), F.lit("fx-json-demo")))
        .withColumn("ingest_timestamp", F.current_timestamp())
    )

    # Drop obviously bad rows
    df = df.filter(
        F.col("date").isNotNull()
        & F.col("base_currency").isNotNull()
        & F.col("target_currency").isNotNull()
        & F.col("exchange_rate").isNotNull()
        & (F.col("exchange_rate") > 0)
        & (F.length(F.trim(F.col("base_currency"))) > 0)
        & (F.length(F.trim(F.col("target_currency"))) > 0)
    )

    # Select standardized columns matching fx_rates_bronze schema
    df_clean = df.select(
        F.col("date"),
        F.col("base_currency"),
        F.col("target_currency"),
        F.col("exchange_rate"),
        F.col("record_source"),
        F.col("ingest_timestamp"),
        F.coalesce(F.col("bid_rate"), F.col("exchange_rate")).alias("bid_rate"),
        F.coalesce(F.col("ask_rate"), F.col("exchange_rate")).alias("ask_rate"),
        F.coalesce(F.col("mid_rate"), F.col("exchange_rate")).alias("mid_rate"),
    )

    final_count = df_clean.count()
    logger.info(
        f"✅ Cleaned FX data: {final_count:,} records (dropped {initial_count - final_count:,} invalid)"
    )

    return df_clean


def read_fx_rates_from_bronze(spark: SparkSession, bronze_root_or_config) -> DataFrame:
    """
    Read FX rates from Bronze layer (CSV / JSON / Delta) and normalize columns.

    This helper is designed for use on EMR Serverless where FX data is already
    landed to Bronze by a separate integration job. It avoids any HTTP calls
    or external dependencies like ``requests``.

    Expected layout:
      - s3://.../bronze/fx/fx_rates_historical.json
      - s3://.../bronze/fx/fx_rates_historical.csv
      - data/samples/fx/fx_rates_historical.json (local)

    Args:
        spark: SparkSession instance
        bronze_root_or_config: Either:
            - Root path to the Bronze layer (e.g. s3://.../bronze or data/samples/fx)
            - Config dictionary (for backward compatibility)

    Returns:
        DataFrame containing normalized FX rates.
    """
    from project_a.schemas.bronze_schemas import FX_RATES_SCHEMA
    
    # Handle both config dict and string path
    if isinstance(bronze_root_or_config, dict):
        # Backward compatibility: extract path from config
        from project_a.utils.path_resolver import resolve_data_path
        config = bronze_root_or_config
        fx_cfg = config.get("sources", {}).get("fx", {})
        bronze_root = fx_cfg.get("raw_path", fx_cfg.get("base_path", ""))
        if not bronze_root:
            bronze_root = resolve_data_path(config, "bronze")
    else:
        bronze_root = bronze_root_or_config

    # Handle both bronze paths (s3://.../bronze) and source paths (local file paths)
    if "/fx/" in bronze_root or bronze_root.endswith("/fx"):
        # Already includes /fx, use as-is
        fx_base = bronze_root.rstrip('/')
    elif bronze_root.endswith(".json") or bronze_root.endswith(".csv"):
        # Direct file path, use as-is
        fx_base = str(Path(bronze_root).parent)
    else:
        # Add /fx suffix
        fx_base = f"{bronze_root.rstrip('/')}/fx"
    
    # Try multiple possible JSON locations
    json_paths = [
        f"{fx_base}/json/fx_rates_historical.json",  # Actual location
        f"{fx_base}/fx_rates_historical.json",  # Alternative (most common for local)
    ]
    csv_path = f"{fx_base}/fx_rates_historical.csv"

    logger.info("📥 Reading FX rates from Bronze layer")

    df_raw: DataFrame = None

    # Try Delta first (in case a normalized Delta table already exists)
    delta_path = fx_base
    try:
        df_raw = spark.read.format("delta").load(delta_path)
        logger.info("✅ Loaded FX rates from Delta at %s", delta_path)
    except Exception as e:
        logger.debug("Delta not found: %s", e)
        # Try JSON paths
        for json_path in json_paths:
            try:
                df_raw = spark.read.schema(FX_RATES_SCHEMA).json(json_path)
                logger.info("✅ Loaded FX rates from JSON at %s", json_path)
                break
            except Exception as e2:
                logger.debug("JSON not found at %s: %s", json_path, e2)
                continue
        
        # If JSON failed, try CSV
        if df_raw is None:
            try:
                logger.info("Trying CSV: %s", csv_path)
                df_raw = (
                    spark.read.schema(FX_RATES_SCHEMA)
                    .option("header", "true")
                    .csv(csv_path)
                )
                logger.info("✅ Loaded FX rates from CSV at %s", csv_path)
            except Exception as e3:
                logger.warning("❌ No FX data found at any location. Creating empty DataFrame: %s", e3)
                # Create empty DataFrame with schema
                df_raw = spark.createDataFrame([], FX_RATES_SCHEMA)

    # Standardize date column
    df = df_raw.withColumn(
        "date",
        F.coalesce(
            F.to_date("date"),
            F.to_date("trade_date"),
        ),
    )

    # Ensure fetched_at is a timestamp when present
    if "fetched_at" in df.columns:
        df = df.withColumn("fetched_at", F.to_timestamp("fetched_at"))

    # Normalize currency and rate columns defensively
    if "ccy" in df.columns and "base_ccy" not in df.columns:
        df = df.withColumnRenamed("ccy", "base_ccy")
    if "currency" in df.columns and "quote_ccy" not in df.columns:
        df = df.withColumnRenamed("currency", "quote_ccy")
    if "fx_rate" in df.columns and "rate" not in df.columns:
        df = df.withColumnRenamed("fx_rate", "rate")

    df = df.withColumn(
        "base_ccy",
        F.upper(F.coalesce(F.col("base_ccy"), F.col("base_currency"))),
    ).withColumn(
        "quote_ccy",
        F.upper(F.coalesce(F.col("quote_ccy"), F.col("target_currency"))),
    ).withColumn(
        "rate",
        F.coalesce(F.col("rate"), F.col("exchange_rate")).cast("double"),
    )

    # Provide normalized "fx_*" aliases while keeping original columns
    if "rate" in df.columns and "fx_rate" not in df.columns:
        df = df.withColumn("fx_rate", F.col("rate"))
    if "mid_rate" in df.columns and "fx_mid_rate" not in df.columns:
        df = df.withColumn("fx_mid_rate", F.col("mid_rate"))
    if "bid_rate" in df.columns and "fx_bid_rate" not in df.columns:
        df = df.withColumn("fx_bid_rate", F.col("bid_rate"))
    if "ask_rate" in df.columns and "fx_ask_rate" not in df.columns:
        df = df.withColumn("fx_ask_rate", F.col("ask_rate"))

    # Basic sanity filtering
    df = df.filter(
        F.col("date").isNotNull()
        & F.col("base_ccy").isNotNull()
        & F.col("quote_ccy").isNotNull()
        & F.col("rate").isNotNull()
        & (F.col("rate") > 0)
    )

    logger.info("✅ Loaded FX Bronze DataFrame with %s rows", df.count())
    return df
