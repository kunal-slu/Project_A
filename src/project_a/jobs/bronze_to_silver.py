"""
Compatibility wrapper for Bronze -> Silver job entrypoint.
"""

from __future__ import annotations

from ._compat import call_module_main, run_job_class


def main(args):
    """Run Bronze -> Silver transformation using the best available implementation."""
    try:
        return run_job_class("jobs.transform.bronze_to_silver", "BronzeToSilverJob", args)
    except (ImportError, AttributeError, ModuleNotFoundError):
        return call_module_main(
            "project_a.legacy.jobs.snowflake_bronze_to_silver_merge",
            args,
            arg_keys=("config",),
        )
