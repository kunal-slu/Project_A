"""
Kafka Producer Job

Simulates Kafka event production for testing streaming pipelines.
"""

import logging
from typing import Any

from project_a.core.base_job import BaseJob
from project_a.core.config import ProjectConfig

logger = logging.getLogger(__name__)


class KafkaProducerJob(BaseJob):
    """Job to simulate Kafka event production."""

    def __init__(self, config: ProjectConfig):
        super().__init__(config)
        self.job_name = "kafka_producer"

    def run(self, ctx) -> dict[str, Any]:
        """Execute the Kafka Producer job."""
        logger.info("Starting Kafka Producer job...")

        try:
            # This job doesn't require Spark - it simulates event production
            logger.info("Simulating Kafka event production...")

            # In a real implementation, this would connect to Kafka and produce events
            # For now, we'll just simulate the process
            simulated_events = 1000
            logger.info(f"Simulated production of {simulated_events} events")

            result = {"status": "success", "events_produced": simulated_events, "simulation": True}

            logger.info(f"Kafka Producer job completed: {result}")
            return result

        except Exception as e:
            logger.error(f"Kafka Producer job failed: {e}")
            raise
