"""
Compatibility wrapper for Kafka CSV/events -> Bronze ingestion job.
"""

from __future__ import annotations

from ._compat import call_module_main, run_job_class


def main(args):
    """Run Kafka ingestion using class-based job, with legacy fallback."""
    try:
        return run_job_class("jobs.ingest.kafka_events_to_bronze", "KafkaEventsToBronzeJob", args)
    except (ImportError, AttributeError, ModuleNotFoundError):
        return call_module_main(
            "project_a.legacy.jobs.kafka_orders_stream",
            args,
            arg_keys=("config",),
        )
