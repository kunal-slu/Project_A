"""
CRM to Bronze Ingestion Job

Ingests data from CRM systems into the Bronze layer.
"""

import logging
from pathlib import Path
from typing import Any

from project_a.core.base_job import BaseJob
from project_a.core.config import ProjectConfig
from project_a.schemas.bronze_schemas import (
    CRM_ACCOUNTS_SCHEMA,
    CRM_CONTACTS_SCHEMA,
    CRM_OPPORTUNITIES_SCHEMA,
)
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


class CrmToBronzeJob(BaseJob):
    """Job to ingest data from CRM to Bronze layer."""

    def __init__(self, config: ProjectConfig):
        super().__init__(config)
        self.job_name = "crm_to_bronze"

    def run(self, ctx) -> dict[str, Any]:
        """Execute the CRM to Bronze ingestion."""
        logger.info("Starting CRM to Bronze ingestion...")

        try:
            # Get Spark session from context
            spark = ctx.spark

            # Get source and target paths from config
            paths_cfg = self.config.get("paths", {})
            bronze_root = paths_cfg.get("bronze_root") or paths_cfg.get("bronze") or "data/bronze"
            bronze_path = f"{bronze_root.rstrip('/')}/crm"

            # Ingest accounts data
            logger.info("Ingesting accounts data from CRM...")
            try:
                accounts_path = resolve_source_file_path(self.config, "crm", "accounts")
                if _local_path_missing(accounts_path):
                    raise ValueError(f"Missing local file: {accounts_path}")
            except ValueError:
                source_path = (
                    self.config.get("sources", {})
                    .get("crm", {})
                    .get("base_path", "data/samples/crm")
                )
                accounts_path = f"{source_path}/accounts.csv"
            incremental_dirs = (
                self.config.get("sources", {}).get("crm", {}).get("incremental_dirs", {})
            )
            accounts_df = _read_csv_with_incremental(
                spark, CRM_ACCOUNTS_SCHEMA, accounts_path, incremental_dirs.get("accounts")
            )
            accounts_df.write.mode("overwrite").parquet(f"{bronze_path}/accounts")

            # Ingest contacts data
            logger.info("Ingesting contacts data from CRM...")
            try:
                contacts_path = resolve_source_file_path(self.config, "crm", "contacts")
                if _local_path_missing(contacts_path):
                    raise ValueError(f"Missing local file: {contacts_path}")
            except ValueError:
                source_path = (
                    self.config.get("sources", {})
                    .get("crm", {})
                    .get("base_path", "data/samples/crm")
                )
                contacts_path = f"{source_path}/contacts.csv"
            contacts_df = _read_csv_with_incremental(
                spark, CRM_CONTACTS_SCHEMA, contacts_path, incremental_dirs.get("contacts")
            )
            contacts_df.write.mode("overwrite").parquet(f"{bronze_path}/contacts")

            # Ingest opportunities data
            logger.info("Ingesting opportunities data from CRM...")
            try:
                opportunities_path = resolve_source_file_path(self.config, "crm", "opportunities")
                if _local_path_missing(opportunities_path):
                    raise ValueError(f"Missing local file: {opportunities_path}")
            except ValueError:
                source_path = (
                    self.config.get("sources", {})
                    .get("crm", {})
                    .get("base_path", "data/samples/crm")
                )
                opportunities_path = f"{source_path}/opportunities.csv"
            opportunities_df = _read_csv_with_incremental(
                spark,
                CRM_OPPORTUNITIES_SCHEMA,
                opportunities_path,
                incremental_dirs.get("opportunities"),
            )
            opportunities_df.write.mode("overwrite").parquet(f"{bronze_path}/opportunities")

            # Apply data quality checks
            logger.info("Applying data quality checks...")
            self.apply_dq_rules(accounts_df, "bronze.crm.accounts")
            self.apply_dq_rules(contacts_df, "bronze.crm.contacts")
            self.apply_dq_rules(opportunities_df, "bronze.crm.opportunities")
            check_date_realism(accounts_df, "bronze.crm.accounts", self.config)
            check_date_realism(contacts_df, "bronze.crm.contacts", self.config)
            check_date_realism(opportunities_df, "bronze.crm.opportunities", self.config)

            # Cache counts to avoid multiple computations
            accounts_count = accounts_df.count()
            contacts_count = contacts_df.count()
            opportunities_count = opportunities_df.count()

            # Log lineage
            self.log_lineage(
                source="crm",
                target="bronze.crm",
                records_processed={
                    "accounts": accounts_count,
                    "contacts": contacts_count,
                    "opportunities": opportunities_count,
                },
            )

            result = {
                "status": "success",
                "records_processed": {
                    "accounts": accounts_count,
                    "contacts": contacts_count,
                    "opportunities": opportunities_count,
                },
                "output_path": bronze_path,
            }

            logger.info(f"CRM to Bronze ingestion completed: {result}")
            return result

        except Exception as e:
            logger.error(f"CRM to Bronze ingestion failed: {e}")
            raise
