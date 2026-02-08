"""
Compatibility wrapper for Silver -> Gold job entrypoint.
"""

from __future__ import annotations

from ._compat import call_module_main, run_job_class


def main(args):
    """Run Silver -> Gold transformation using the best available implementation."""
    try:
        return run_job_class("jobs.transform.silver_to_gold", "SilverToGoldJob", args)
    except (ImportError, AttributeError, ModuleNotFoundError):
        return call_module_main(
            "project_a.legacy.jobs.gold_star_schema",
            args,
            arg_keys=("config",),
        )
