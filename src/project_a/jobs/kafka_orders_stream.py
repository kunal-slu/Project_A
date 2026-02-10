"""
Backward-compatible alias for Kafka stream -> Bronze job.
"""

from __future__ import annotations

from .kafka_stream_to_bronze import main

__all__ = ["main"]
