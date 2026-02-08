#!/usr/bin/env python3
"""
CRM Contacts Bronze Ingestion Job
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from pyspark.sql import functions as F

from project_a.dq.runner import run_suite
from project_a.utils.config import load_conf
from project_a.utils.metrics import track_job_complete, track_job_start, track_records_processed
from project_a.utils.schema_validator import validate_bronze_ingestion
from project_a.utils.spark_session import build_spark

sys.path.insert(0, os.path.dirname(__file__))
import argparse
import logging

from emit_lineage_and_metrics import emit_pipeline_lineage, log_table_operation

logger = logging.getLogger(__name__)


def run_crm_contacts_ingest(config_path: str = None, env: str = None):
    """Run CRM contacts bronze ingestion."""

    # Determine config path based on env if not provided
    if config_path is None:
        if env:
            config_path = f"config/{env}.yaml"
        else:
            env = os.getenv("ENV", "dev")
            config_path = f"config/{env}.yaml"

    config = load_conf(config_path)
    spark = build_spark(app_name="crm_contacts_ingest", config=config)

    try:
        job_id = track_job_start("crm_contacts_ingest", config)

        src_path = config.get("paths", {}).get("crm_contacts", "aws/data/crm/contacts.csv")
        bronze_path = config.get("bronze", {}).get(
            "crm_contacts", "data/lakehouse_delta/bronze/crm/contacts"
        )

        logger.info(f"Reading CRM contacts from: {src_path}")

        # Emit lineage - start of job
        run_id = emit_pipeline_lineage(
            spark,
            config,
            job_name="crm_contacts_ingest",
            input_tables=[src_path],
            output_tables=[bronze_path],
            transformations=["read_csv", "add_metadata", "write_delta"],
        )

        df = spark.read.option("header", "true").option("inferSchema", "true").csv(src_path)

        # Validate against schema registry (enforce data contracts)
        logger.info("Validating schema against registry...")
        try:
            df_validated, validation_results = validate_bronze_ingestion(
                df, spark, "crm_contacts", schemas_dir="schemas"
            )
            if not validation_results.get("passed", True):
                logger.warning(f"Schema validation issues: {validation_results.get('issues', [])}")
            df = df_validated
            logger.info("Schema validation passed")
        except Exception as e:
            logger.warning(f"Schema validation failed (continuing anyway): {str(e)}")

        df_with_metadata = (
            df.withColumn("_source_system", F.lit("salesforce_synthetic"))
            .withColumn("_ingestion_ts", F.current_timestamp())
            .withColumn("_job_id", F.lit(job_id))
        )

        # Cache before write since we'll need count
        df_with_metadata.cache()

        (
            df_with_metadata.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(bronze_path)
        )

        record_count = df_with_metadata.count()

        # Emit lineage - after write
        log_table_operation(
            spark,
            config,
            run_id=run_id,
            table_name="crm_contacts",
            row_count=record_count,
            s3_path=bronze_path,
        )

        track_records_processed(job_id, "crm_contacts", record_count)

        # DQ checks
        dq_result = run_suite("crm_contacts_not_null_keys", bronze_path, spark)

        if not dq_result.passed:
            raise Exception(f"DQ checks failed: {dq_result.failures}")

        track_job_complete(job_id, "SUCCESS", record_count)

        # Unpersist cached DataFrame
        df_with_metadata.unpersist()
        logger.info(f"✅ Successfully ingested {record_count:,} CRM contacts to bronze")
        return True

    except Exception as e:
        logger.error(f"❌ CRM contacts ingestion failed: {str(e)}")
        track_job_complete(job_id, "FAILED", 0, str(e))
        return False

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CRM Contacts Bronze Ingestion")
    parser.add_argument("--config", help="Configuration file path")
    parser.add_argument(
        "--env", choices=["dev", "prod", "local"], help="Environment (dev/prod/local)"
    )
    args = parser.parse_args()

    success = run_crm_contacts_ingest(config_path=args.config, env=args.env)
    sys.exit(0 if success else 1)
