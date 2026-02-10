"""
Kafka Events to Bronze Ingestion Job

Ingests Kafka streaming events into the Bronze layer.
"""

import logging
from pathlib import Path
from typing import Any

from project_a.core.base_job import BaseJob
from project_a.core.config import ProjectConfig
from project_a.schemas.bronze_schemas import KAFKA_EVENTS_SCHEMA
from project_a.utils.dq_realism import check_date_realism

logger = logging.getLogger(__name__)


def _local_path_missing(path: str) -> bool:
    if not path:
        return True
    if path.startswith(("s3://", "s3a://")):
        return False
    candidate = path.replace("file://", "", 1) if path.startswith("file://") else path
    return not Path(candidate).exists()


class KafkaEventsToBronzeJob(BaseJob):
    """Job to ingest Kafka events to Bronze layer."""

    def __init__(self, config: ProjectConfig):
        super().__init__(config)
        self.job_name = "kafka_events_to_bronze"

    def run(self, ctx) -> dict[str, Any]:
        """Execute the Kafka events to Bronze ingestion."""
        logger.info("Starting Kafka events to Bronze ingestion...")

        try:
            # Get Spark session from context
            spark = ctx.spark

            # Get source and target paths from config
            source_cfg = self.config.get("sources", {}).get("kafka_sim", {})
            source_path = source_cfg.get("base_path", "data/samples/kafka")
            incremental_dir = source_cfg.get("incremental_dirs", {}).get("events")
            bronze_path = f"{self.config.get('paths', {}).get('bronze_root', 'data/bronze')}/kafka"

            # Ingest Kafka events data
            logger.info("Ingesting Kafka events data...")
            snapshot_path = f"{source_path}/stream_kafka_events_100000.csv"
            reader = (
                spark.read.schema(KAFKA_EVENTS_SCHEMA)
                .option("header", "true")
                .option("quote", '"')
                .option("escape", '"')
                .option("mode", "PERMISSIVE")
                .option("ignoreMissingFiles", "true")
            )
            paths = [snapshot_path]
            if incremental_dir and not _local_path_missing(incremental_dir):
                reader = reader.option("recursiveFileLookup", "true")
                paths.append(incremental_dir)
            kafka_events_df = reader.csv(paths).cache()
            kafka_count = kafka_events_df.count()
            kafka_events_df.write.mode("overwrite").parquet(f"{bronze_path}/events")

            # Apply data quality checks
            logger.info("Applying data quality checks...")
            self.apply_dq_rules(kafka_events_df, "bronze.kafka.events")
            check_date_realism(kafka_events_df, "bronze.kafka.events", self.config)

            # Log lineage
            self.log_lineage(
                source="kafka_events",
                target="bronze.kafka",
                records_processed={"events": kafka_count},
            )

            result = {
                "status": "success",
                "records_processed": {"events": kafka_count},
                "output_path": bronze_path,
            }

            logger.info(f"Kafka Events to Bronze ingestion completed: {result}")
            return result

        except Exception as e:
            logger.error(f"Kafka Events to Bronze ingestion failed: {e}")
            raise
