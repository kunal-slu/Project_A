"""
Compatibility wrapper for orders Silver -> Iceberg maintenance job.
"""

from __future__ import annotations

from ._compat import run_job_class


def main(args):
    """Run orders_silver_to_iceberg class-based job."""
    return run_job_class("jobs.iceberg.orders_silver_to_iceberg", "OrdersSilverToIcebergJob", args)

