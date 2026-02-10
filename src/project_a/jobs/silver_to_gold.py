"""
Compatibility wrapper for Silver -> Gold job entrypoint.
"""

from __future__ import annotations

from ._compat import run_job_class


def main(args):
    """Run Silver -> Gold transformation using the canonical class-based job."""
    try:
        return run_job_class("jobs.transform.silver_to_gold", "SilverToGoldJob", args)
    except Exception as exc:  # pragma: no cover - defensive error wrapper
        raise RuntimeError(
            "Failed to run SilverToGoldJob from jobs.transform.silver_to_gold"
        ) from exc
