"""
Compatibility wrapper for Bronze -> Silver job entrypoint.
"""

from __future__ import annotations

from ._compat import run_job_class


def main(args):
    """Run Bronze -> Silver transformation using the canonical class-based job."""
    try:
        return run_job_class("jobs.transform.bronze_to_silver", "BronzeToSilverJob", args)
    except Exception as exc:  # pragma: no cover - defensive error wrapper
        raise RuntimeError(
            "Failed to run BronzeToSilverJob from jobs.transform.bronze_to_silver"
        ) from exc
