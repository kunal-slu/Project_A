"""
Compatibility wrapper for Kafka Structured Streaming ingestion.
"""

from __future__ import annotations

from ._compat import call_module_main


def main(args):
    """Run Kafka stream -> Bronze ingestion."""
    return call_module_main(
        "jobs.streaming.kafka_orders_stream",
        args,
        arg_keys=("env", "config"),
    )

