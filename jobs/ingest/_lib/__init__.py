"""
Shared library for ingestion jobs.

Provides watermark management utilities for incremental ingestion.
Schema contracts and error lanes are available via utils modules.
"""

from .watermark import read_watermark, write_watermark

__all__ = [
    "read_watermark",
    "write_watermark",
]
