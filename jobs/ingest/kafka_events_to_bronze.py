"""
Kafka Events to Bronze Ingestion Job

Ingests Kafka streaming events into the Bronze layer.
"""
import logging
from typing import Dict, Any
from project_a.core.base_job import BaseJob
from project_a.core.config import ProjectConfig


logger = logging.getLogger(__name__)


class KafkaEventsToBronzeJob(BaseJob):
    """Job to ingest Kafka events to Bronze layer."""
    
    def __init__(self, config: ProjectConfig):
        super().__init__(config)
        self.job_name = "kafka_events_to_bronze"
    
    def run(self, ctx) -> Dict[str, Any]:
        """Execute the Kafka events to Bronze ingestion."""
        logger.info("Starting Kafka events to Bronze ingestion...")
            
        try:
            # Get Spark session from context
            spark = ctx.spark
            
            # Get source and target paths from config
            source_path = self.config.get('sources', {}).get('kafka_sim', {}).get('base_path', 'data/samples/kafka')
            bronze_path = f"{self.config.get('paths', {}).get('bronze_root', 'data/bronze')}/kafka"
            
            # Ingest Kafka events data
            logger.info("Ingesting Kafka events data...")
            kafka_events_df = spark.read.option("header", "true").csv(f"{source_path}/stream_kafka_events_100000.csv")
            kafka_events_df.write.mode("overwrite").parquet(f"{bronze_path}/events")
            
            # Apply data quality checks
            logger.info("Applying data quality checks...")
            self.apply_dq_rules(kafka_events_df, "bronze.kafka.events")
            
            # Log lineage
            self.log_lineage(
                source="kafka_events",
                target="bronze.kafka",
                records_processed={
                    "events": kafka_events_df.count()
                }
            )
            
            result = {
                "status": "success",
                "records_processed": {
                    "events": kafka_events_df.count()
                },
                "output_path": bronze_path
            }
            
            logger.info(f"Kafka Events to Bronze ingestion completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Kafka Events to Bronze ingestion failed: {e}")
            raise