"""
Redshift to Bronze Ingestion Job

Ingests data from Redshift into the Bronze layer.
"""

import logging
from pathlib import Path
from typing import Any

from project_a.core.base_job import BaseJob
from project_a.core.config import ProjectConfig
from project_a.schemas.bronze_schemas import REDSHIFT_BEHAVIOR_SCHEMA
from project_a.utils.dq_realism import check_date_realism
from project_a.utils.path_resolver import resolve_source_file_path

logger = logging.getLogger(__name__)


def _local_path_missing(path: str) -> bool:
    if not path:
        return True
    if path.startswith(("s3://", "s3a://")):
        return False
    candidate = path.replace("file://", "", 1) if path.startswith("file://") else path
    return not Path(candidate).exists()


def _read_csv_with_incremental(spark, schema, snapshot_path: str, incremental_dir: str | None):
    reader = spark.read.schema(schema).option("header", "true").option("ignoreMissingFiles", "true")
    paths = [snapshot_path]
    if incremental_dir and not _local_path_missing(incremental_dir):
        reader = reader.option("recursiveFileLookup", "true")
        paths.append(incremental_dir)
    return reader.csv(paths)


class RedshiftToBronzeJob(BaseJob):
    """Job to ingest data from Redshift to Bronze layer."""

    def __init__(self, config: ProjectConfig):
        super().__init__(config)
        self.job_name = "redshift_to_bronze"

    def run(self, ctx) -> dict[str, Any]:
        """Execute the Redshift to Bronze ingestion."""
        logger.info("Starting Redshift to Bronze ingestion...")

        try:
            # Get Spark session from context
            spark = ctx.spark

            # Get source and target paths from config
            paths_cfg = self.config.get("paths", {})
            bronze_root = paths_cfg.get("bronze_root") or paths_cfg.get("bronze") or "data/bronze"
            bronze_path = f"{bronze_root.rstrip('/')}/redshift"

            # Ingest customer behavior data
            logger.info("Ingesting customer behavior data from Redshift...")
            try:
                behavior_path = resolve_source_file_path(self.config, "redshift", "behavior")
                if _local_path_missing(behavior_path):
                    raise ValueError(f"Missing local file: {behavior_path}")
            except ValueError:
                source_path = (
                    self.config.get("sources", {})
                    .get("redshift", {})
                    .get("base_path", "data/samples/redshift")
                )
                behavior_path = f"{source_path}/redshift_customer_behavior_50000.csv"
            incremental_dirs = (
                self.config.get("sources", {}).get("redshift", {}).get("incremental_dirs", {})
            )
            behavior_df = _read_csv_with_incremental(
                spark, REDSHIFT_BEHAVIOR_SCHEMA, behavior_path, incremental_dirs.get("behavior")
            ).cache()
            behavior_count = behavior_df.count()
            behavior_df.write.mode("overwrite").parquet(f"{bronze_path}/customer_behavior")

            # Apply data quality checks
            logger.info("Applying data quality checks...")
            self.apply_dq_rules(behavior_df, "bronze.redshift.customer_behavior")
            check_date_realism(behavior_df, "bronze.redshift.customer_behavior", self.config)

            # Log lineage
            self.log_lineage(
                source="redshift",
                target="bronze.redshift",
                records_processed={"customer_behavior": behavior_count},
            )

            result = {
                "status": "success",
                "records_processed": {"customer_behavior": behavior_count},
                "output_path": bronze_path,
            }

            logger.info(f"Redshift to Bronze ingestion completed: {result}")
            return result

        except Exception as e:
            logger.error(f"Redshift to Bronze ingestion failed: {e}")
            raise
