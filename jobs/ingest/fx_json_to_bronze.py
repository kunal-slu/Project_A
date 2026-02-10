#!/usr/bin/env python3
"""
FX JSON to Bronze Ingestion Job
Enterprise-grade JSON ingestion with explicit schemas, error handling, and config-driven paths.

Features:
- Reads JSON Lines (NDJSON) format from S3
- Uses explicit schema (no inference)
- Config-driven paths
- Structured logging
- Error handling with context
- Metadata columns
- Delta Lake output
"""

import argparse
import logging
import sys
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from project_a.config_loader import load_config_resolved
from project_a.extract.fx_json_reader import read_fx_json
from project_a.monitoring.lineage_decorator import lineage_job
from project_a.monitoring.metrics_collector import emit_duration, emit_rowcount
from project_a.utils.dq_realism import check_date_realism
from project_a.utils.error_lanes import ErrorLaneHandler
from project_a.utils.logging import get_trace_id, setup_json_logging
from project_a.utils.run_audit import write_run_audit
from project_a.utils.spark_session import build_spark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@lineage_job(
    name="bronze_fx_json",
    inputs=["s3://bucket/bronze/fx/json/"],
    outputs=["s3://bucket/bronze/fx/normalized/"],
)
def extract_fx_json_to_bronze(
    spark: SparkSession, config: dict[str, Any], run_id: str = None
) -> DataFrame:
    """
    Extract FX rates from JSON format and write to Bronze layer.

    Args:
        spark: SparkSession instance
        config: Configuration dictionary
        run_id: Optional run identifier (UUID)

    Returns:
        DataFrame with validated, enriched FX rates data
    """
    if run_id is None:
        run_id = str(uuid.uuid4())

    logger.info(f"🚀 Starting FX JSON extraction (run_id={run_id})")
    start_time = time.time()

    try:
        # Get paths from config
        sources_cfg = config.get("sources", {})
        fx_cfg = sources_cfg.get("fx", {})
        bronze_fx_path = fx_cfg.get("bronze_path", "")
        lake_bucket = config.get("buckets", {}).get("lake", "")

        if not bronze_fx_path:
            raise ValueError("FX bronze_path not found in config")

        # Setup error lane handler
        error_handler = ErrorLaneHandler(f"s3://{lake_bucket}")
        error_handler.get_error_lane_path("bronze", "fx", run_id)

        # Read and normalize JSON using extractor (contract-driven)
        df_raw = read_fx_json(spark, config)
        df_raw.count()

        # Apply contract-driven validation:
        # 1. Enforce non-null primary key columns (date, base_ccy, quote_ccy, rate)
        # 2. Apply range checks
        # 3. Route bad rows to error lane
        df_clean = df_raw.filter(
            F.col("trade_date").isNotNull()
            & F.col("base_ccy").isNotNull()
            & F.col("quote_ccy").isNotNull()
            & F.col("rate").isNotNull()
            & (F.length(F.trim(F.col("base_ccy"))) > 0)
            & (F.length(F.trim(F.col("quote_ccy"))) > 0)
            & (F.col("rate") > 0.0001)
            & (F.col("rate") < 1000)
        )

        bad = df_raw.exceptAll(df_clean)
        bad_count = bad.count()

        if bad_count > 0:
            logger.warning(f"⚠️  Routing {bad_count:,} contract-violating rows to error lane")
            error_handler.quarantine(bad, "bronze", "fx", run_id, reason="contract_violation")

        # Add metadata columns
        run_date = datetime.utcnow().strftime("%Y-%m-%d")
        df_clean = (
            df_clean.withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_run_date", F.lit(run_date).cast(DateType()))
            .withColumn("_batch_id", F.lit(run_id))
            .withColumn("_source_system", F.lit("fx_json"))
            .withColumn("_format", F.lit("json"))
        )

        final_count = df_clean.count()
        logger.info(
            f"✅ Cleaned data: {final_count:,} records (routed {bad_count:,} to error lane)"
        )

        # Realism checks (date range, invalid formats)
        check_date_realism(df_clean, "bronze.fx_json", config)

        # Emit metrics
        duration_ms = (time.time() - start_time) * 1000
        emit_rowcount(
            "records_processed_total",
            final_count,
            {"job": "bronze_fx_json", "source": "fx", "format": "json"},
            config,
        )

        emit_duration(
            "latency_seconds",
            duration_ms / 1000.0,
            {"job": "bronze_fx_json", "stage": "extraction"},
            config,
        )

        logger.info(f"✅ Extraction complete: {final_count:,} records in {duration_ms:.0f}ms")

        return df_clean

    except Exception as e:
        logger.error(
            f"❌ Extraction failed: {e}",
            exc_info=True,
            extra={"job": "bronze_fx_json", "run_id": run_id, "error_type": type(e).__name__},
        )
        raise


def main():
    """Main entry point for EMR job."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="FX JSON to Bronze ingestion job")
    parser.add_argument("--env", default="dev", help="Environment (dev/prod)")
    parser.add_argument("--config", help="Config file path (local or S3)")
    args = parser.parse_args()

    # Setup structured logging
    trace_id = get_trace_id()
    setup_json_logging(level="INFO", include_trace_id=True)
    logger.info(f"Job started (trace_id={trace_id}, env={args.env})")

    # Load configuration
    if args.config:
        config_path = args.config
        # If S3 path, read it using Spark's native S3 support
        if config_path.startswith("s3://"):
            logger.info(f"Reading config from S3 using Spark: {config_path}")
            from pyspark.sql import SparkSession

            spark_temp = (
                SparkSession.builder.appName("config_loader")
                .config("spark.sql.adaptive.enabled", "false")
                .getOrCreate()
            )

            try:
                config_lines = spark_temp.sparkContext.textFile(config_path).collect()
                config_content = "\n".join(config_lines)
                tmp_file = tempfile.NamedTemporaryFile(
                    mode="w", suffix=".yaml", delete=False, encoding="utf-8"
                )
                tmp_file.write(config_content)
                tmp_file.close()
                config_path = tmp_file.name
                logger.info(f"Loaded config from S3: {args.config} -> {config_path}")
            finally:
                spark_temp.stop()
        else:
            config_path = str(Path(config_path))
    else:
        config_path = Path(f"config/{args.env}.yaml")
        if not config_path.exists():
            config_path = Path("config/prod.yaml")
        if not config_path.exists():
            config_path = Path("config/local.yaml")
        config_path = str(config_path)

    logger.info(f"Loading config from: {config_path}")
    config = load_config_resolved(config_path)

    # Ensure environment is set correctly for EMR
    if not config.get("environment"):
        config["environment"] = "emr"
        logger.info("Set environment=emr in config (was missing)")

    # Generate run ID
    run_id = str(uuid.uuid4())
    start_time = time.time()

    # Build Spark session
    spark = build_spark(config)

    try:
        # Run extraction
        df = extract_fx_json_to_bronze(spark, config, run_id=run_id)

        if df.isEmpty():
            logger.warning("⚠️  No data to write")
            return 0

        # Get output path from config
        sources_cfg = config.get("sources", {})
        fx_cfg = sources_cfg.get("fx", {})
        bronze_fx_path = fx_cfg.get("bronze_path", "")

        if not bronze_fx_path:
            bronze_root = config["paths"]["bronze_root"]
            bronze_fx_path = f"{bronze_root}/fx/delta/"

        logger.info(f"💾 Writing to Bronze: {bronze_fx_path}")

        # Write to bronze as Delta with contract-driven partitioning
        # Partition by trade_date for downstream pruning (senior pattern)
        df_partitioned = df.repartition(F.col("trade_date"))

        df_partitioned.write.format("delta").mode("overwrite").option("mergeSchema", "true").option(
            "delta.autoOptimize.optimizeWrite", "true"
        ).partitionBy("trade_date", "_run_date").save(bronze_fx_path)

        rows_out = df.count()
        rows_in = rows_out
        duration_ms = (time.time() - start_time) * 1000

        # Write run audit trail
        lake_bucket = config.get("buckets", {}).get("lake", "")
        if lake_bucket:
            try:
                write_run_audit(
                    bucket=lake_bucket,
                    job_name="fx_json_to_bronze",
                    env=config.get("environment", "dev"),
                    source=fx_cfg.get("raw_path", ""),
                    target=bronze_fx_path,
                    rows_in=rows_in,
                    rows_out=rows_out,
                    status="SUCCESS",
                    run_id=run_id,
                    duration_ms=duration_ms,
                    config=config,
                )
            except Exception as e:
                logger.warning(f"⚠️  Failed to write run audit: {e}")

        logger.info(
            f"🎉 FX JSON to Bronze job completed: {rows_out:,} records in {duration_ms:.0f}ms"
        )
        return 0

    except Exception as e:
        logger.error(
            f"❌ Job failed: {e}",
            exc_info=True,
            extra={"job": "fx_json_to_bronze", "run_id": run_id, "trace_id": trace_id},
        )
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
