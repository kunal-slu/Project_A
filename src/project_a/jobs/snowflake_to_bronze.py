"""
Compatibility wrapper for Snowflake -> Bronze ingestion job.
"""

from __future__ import annotations

from ._compat import run_job_class


def main(args):
    """Run Snowflake ingestion using the canonical class-based job."""
    try:
        return run_job_class("jobs.ingest.snowflake_to_bronze", "SnowflakeToBronzeJob", args)
    except Exception as exc:  # pragma: no cover - defensive error wrapper
        raise RuntimeError(
            "Failed to run SnowflakeToBronzeJob from jobs.ingest.snowflake_to_bronze"
        ) from exc
