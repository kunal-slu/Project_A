"""
Compatibility wrapper for Kafka events CSV snapshot -> Bronze ingestion job.
"""

from __future__ import annotations

from ._compat import run_job_class


def main(args):
    """Run Kafka CSV snapshot ingestion using the canonical class-based job."""
    try:
        return run_job_class("jobs.ingest.kafka_events_to_bronze", "KafkaEventsToBronzeJob", args)
    except Exception as exc:  # pragma: no cover - defensive error wrapper
        raise RuntimeError(
            "Failed to run KafkaEventsToBronzeJob from jobs.ingest.kafka_events_to_bronze"
        ) from exc
