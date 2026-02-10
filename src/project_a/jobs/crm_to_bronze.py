"""
Compatibility wrapper for CRM -> Bronze ingestion job.
"""

from __future__ import annotations

from ._compat import run_job_class


def main(args):
    """Run CRM ingestion using the canonical class-based job."""
    try:
        return run_job_class("jobs.ingest.crm_to_bronze", "CrmToBronzeJob", args)
    except Exception as exc:  # pragma: no cover - defensive error wrapper
        raise RuntimeError("Failed to run CrmToBronzeJob from jobs.ingest.crm_to_bronze") from exc
