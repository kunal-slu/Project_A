"""
Compatibility wrapper for Redshift -> Bronze ingestion job.
"""

from __future__ import annotations

from ._compat import run_job_class


def main(args):
    """Run Redshift ingestion."""
    return run_job_class("jobs.ingest.redshift_to_bronze", "RedshiftToBronzeJob", args)
