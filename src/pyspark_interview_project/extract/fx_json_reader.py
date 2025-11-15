"""
FX JSON Reader - Extract and normalize FX rates from JSON Lines format.

Reads JSON Lines (NDJSON) from S3, enforces schema, cleans nulls, and normalizes.
"""
from __future__ import annotations

import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def read_fx_json(
    spark: SparkSession,
    config: Dict[str, Any],
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
    from pyspark_interview_project.schemas.bronze_schemas import FX_RATES_SCHEMA
    
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
    df = df.withColumn(
        "date",
        F.coalesce(
            F.to_date("date", "yyyy-MM-dd"),
            F.to_date("trade_date", "yyyy-MM-dd")
        )
    ).withColumn(
        "base_ccy",
        F.upper(F.coalesce(F.col("base_ccy"), F.col("base_currency")))
    ).withColumn(
        "quote_ccy",
        F.upper(F.coalesce(F.col("quote_ccy"), F.col("target_currency")))
    ).withColumn(
        "rate",
        F.coalesce(F.col("rate"), F.col("exchange_rate")).cast("double")
    ).withColumn(
        "source_system",
        F.coalesce(F.col("source"), F.lit("fx-json-demo"))
    )
    
    # Drop obviously bad rows
    df = df.filter(
        F.col("date").isNotNull() &
        F.col("base_ccy").isNotNull() &
        F.col("quote_ccy").isNotNull() &
        F.col("rate").isNotNull() &
        (F.col("rate") > 0) &
        (F.length(F.trim(F.col("base_ccy"))) > 0) &
        (F.length(F.trim(F.col("quote_ccy"))) > 0)
    )
    
    # Select standardized columns
    df_clean = df.select(
        F.col("date").alias("trade_date"),
        F.col("base_ccy"),
        F.col("quote_ccy"),
        F.col("rate"),
        F.col("source_system").alias("source"),
        F.coalesce(F.col("bid_rate"), F.col("rate")).alias("bid_rate"),
        F.coalesce(F.col("ask_rate"), F.col("rate")).alias("ask_rate"),
        F.coalesce(F.col("mid_rate"), F.col("rate")).alias("mid_rate")
    )
    
    final_count = df_clean.count()
    logger.info(f"✅ Cleaned FX data: {final_count:,} records (dropped {initial_count - final_count:,} invalid)")
    
    return df_clean

