"""
Compatibility wrapper for Snowflake -> Bronze ingestion job.
"""

from __future__ import annotations

from ._compat import call_module_main, run_job_class


def main(args):
    """Run Snowflake ingestion using class-based job, with legacy fallback."""
    try:
        return run_job_class("jobs.ingest.snowflake_to_bronze", "SnowflakeToBronzeJob", args)
    except (ImportError, AttributeError, ModuleNotFoundError):
        return call_module_main(
            "project_a.legacy.jobs.snowflake_to_bronze",
            args,
            arg_keys=("config",),
        )
