"""
FX JSON to Bronze Job

Ingest FX JSON from bronze/fx_json/ → normalized bronze/fx/
Contract-driven ingestion with explicit schema enforcement.
"""

import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import tempfile
import time
import uuid

from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp, lit, to_date
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from project_a.extract.fx_json_reader import read_fx_json
from project_a.monitoring.lineage_emitter import LineageEmitter, load_lineage_config
from project_a.utils.cloudwatch_metrics import emit_job_failure, emit_job_success
from project_a.utils.config import load_config_resolved
from project_a.utils.error_lanes import ErrorLaneHandler
from project_a.utils.logging import get_trace_id, setup_json_logging
from project_a.utils.run_audit import write_run_audit
from project_a.utils.spark_session import build_spark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


FX_SCHEMA = StructType(
    [
        StructField("date", StringType(), True),
        StructField("trade_date", StringType(), True),
        StructField("base_ccy", StringType(), True),
        StructField("quote_ccy", StringType(), True),
        StructField("counter_ccy", StringType(), True),
        StructField("rate", DoubleType(), True),
        StructField("source", StringType(), True),
        StructField("bid_rate", DoubleType(), True),
        StructField("ask_rate", DoubleType(), True),
        StructField("mid_rate", DoubleType(), True),
    ]
)


def main(args):
    """
    Ingest FX JSON from bronze/fx_json/ → normalized bronze/fx/

    Args:
        args: argparse.Namespace with --env, --config, --run-date
    """
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
    spark = build_spark(app_name="fx_json_to_bronze", config=config)

    try:
        # Get paths from config
        sources_cfg = config.get("sources", {})
        fx_cfg = sources_cfg.get("fx", {})
        bronze_root = config.get("paths", {}).get("bronze_root", "")
        lake_bucket = config.get("buckets", {}).get("lake", "")

        # Determine input and output paths
        input_path = fx_cfg.get("raw_path", f"{bronze_root}/fx_json/")
        output_path = fx_cfg.get("bronze_path", f"{bronze_root}/fx/delta/")

        logger.info(f"📥 Reading FX JSON from: {input_path}")
        logger.info(f"💾 Writing to Bronze: {output_path}")

        # Setup error lane handler
        error_handler = ErrorLaneHandler(spark, config)

        # Read and normalize JSON using extractor (contract-driven)
        df_raw = read_fx_json(spark, config)
        rows_in = df_raw.count()

        # Apply contract-driven validation:
        # 1. Enforce non-null primary key columns
        # 2. Apply range checks
        # 3. Route bad rows to error lane
        df_clean = df_raw.filter(
            col("date").isNotNull()
            & col("base_currency").isNotNull()
            & col("target_currency").isNotNull()
            & col("exchange_rate").isNotNull()
            & (col("exchange_rate") > 0.0001)
            & (col("exchange_rate") < 1000)
        )

        bad = df_raw.exceptAll(df_clean)
        bad_count = bad.count()

        if bad_count > 0:
            logger.warning(f"⚠️  Routing {bad_count:,} contract-violating rows to error lane")
            error_handler.quarantine(bad, "bronze", "fx", run_id, reason="contract_violation")

        # Add metadata columns
        run_date = args.run_date if hasattr(args, "run_date") and args.run_date else None
        if not run_date:
            from datetime import datetime

            run_date = datetime.utcnow().strftime("%Y-%m-%d")

        # Add metadata columns
        df_clean = df_clean.withColumn("as_of_date", to_date(col("date")))

        # Add ingest timestamp if not already present
        if "_ingest_ts" not in df_clean.columns:
            df_clean = df_clean.withColumn(
                "_ingest_ts",
                F.coalesce(col("ingest_timestamp").cast("timestamp"), current_timestamp()),
            )

        # Add other metadata
        df_clean = (
            df_clean.withColumn("_run_date", lit(run_date).cast(DateType()))
            .withColumn("_batch_id", lit(run_id))
            .withColumn("_source_system", lit("fx_json"))
        )

        # Add source column if not present
        if "source" not in df_clean.columns:
            df_clean = df_clean.withColumn("source", lit("fx_json"))

        # Write to bronze as Delta with contract-driven partitioning
        # Partition by trade_date for downstream pruning (senior pattern)
        df_partitioned = df_clean.repartition(col("date"))

        df_partitioned.write.format("delta").mode("overwrite").option("mergeSchema", "true").option(
            "delta.autoOptimize.optimizeWrite", "true"
        ).partitionBy("date", "_run_date").save(output_path)

        rows_out = df_clean.count()
        duration_seconds = time.time() - start_time
        duration_ms = duration_seconds * 1000

        # Emit CloudWatch metrics
        try:
            emit_job_success(
                job_name="fx_json_to_bronze",
                duration_seconds=duration_seconds,
                env=config.get("environment", "dev"),
                rows_processed=rows_out,
            )
        except Exception as e:
            logger.warning(f"⚠️  Failed to emit metrics: {e}")

        # Emit lineage event (skip when disabled or config path missing)
        try:
            lineage_cfg = config.get("lineage", {}) if isinstance(config, dict) else {}
            if lineage_cfg.get("enabled", False):
                lineage_config_path = lineage_cfg.get("config_path")
                if not lineage_config_path:
                    artifacts_bucket = config.get("buckets", {}).get("artifacts", "")
                    if artifacts_bucket:
                        lineage_config_path = f"s3://{artifacts_bucket}/config/lineage.yaml"

                if lineage_config_path:
                    lineage_config = load_lineage_config(lineage_config_path)
                    emitter = LineageEmitter(lineage_config)

                    emitter.emit_job(
                        job_name="fx_json_to_bronze",
                        run_id=run_id,
                        inputs=["fx_json.raw"],
                        outputs=["bronze.fx.rates"],
                        status="SUCCESS",
                        metadata={
                            "rows_in": rows_in,
                            "rows_out": rows_out,
                            "duration_seconds": duration_seconds,
                        },
                    )
                else:
                    logger.info("Lineage enabled but no config path provided; skipping emission")
        except Exception as e:
            logger.warning(f"⚠️  Failed to emit lineage: {e}")

        # Write run audit trail
        if lake_bucket:
            try:
                write_run_audit(
                    bucket=lake_bucket,
                    job_name="fx_json_to_bronze",
                    env=config.get("environment", "dev"),
                    source=input_path,
                    target=output_path,
                    rows_in=rows_in,
                    rows_out=rows_out,
                    status="SUCCESS",
                    run_id=run_id,
                    duration_ms=duration_ms,
                    config=config,
                )
            except Exception as e:
                logger.warning(f"⚠️  Failed to write run audit: {e}")

        logger.info(f"✅ Wrote FX bronze data to: {output_path}")
        logger.info(
            f"🎉 FX JSON to Bronze job completed: {rows_out:,} records in {duration_ms:.0f}ms"
        )

    except Exception as e:
        logger.error(
            f"❌ Job failed: {e}",
            exc_info=True,
            extra={"job": "fx_json_to_bronze", "run_id": run_id, "trace_id": trace_id},
        )

        # Emit failure metrics
        try:
            emit_job_failure(
                job_name="fx_json_to_bronze",
                env=config.get("environment", "dev"),
                error_type=type(e).__name__,
            )
        except Exception as e:
            logger.warning(f"Failed to emit metrics: {e}")

        # Emit failure lineage event
        try:
            lineage_cfg = config.get("lineage", {}) if isinstance(config, dict) else {}
            if lineage_cfg.get("enabled", False):
                lineage_config_path = lineage_cfg.get("config_path")
                if not lineage_config_path:
                    artifacts_bucket = config.get("buckets", {}).get("artifacts", "")
                    if artifacts_bucket:
                        lineage_config_path = f"s3://{artifacts_bucket}/config/lineage.yaml"

                if lineage_config_path:
                    lineage_config = load_lineage_config(lineage_config_path)
                    emitter = LineageEmitter(lineage_config)

                    emitter.emit_job(
                        job_name="fx_json_to_bronze",
                        run_id=run_id,
                        inputs=["fx_json.raw"],
                        outputs=["bronze.fx.rates"],
                        status="FAILED",
                        error_message=str(e),
                    )
        except Exception as e:
            logger.warning(f"Failed to emit metrics: {e}")

        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    # Keep this for local debugging
    import argparse

    parser = argparse.ArgumentParser(description="FX JSON to Bronze ingestion job")
    parser.add_argument("--env", default="dev", help="Environment (dev/prod)")
    parser.add_argument("--config", help="Config file path (local or S3)")
    parser.add_argument("--run-date", help="Processing date (YYYY-MM-DD)")
    main(parser.parse_args())
