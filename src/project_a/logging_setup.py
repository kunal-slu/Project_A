"""Simple logging helpers for local runs and tests."""

from __future__ import annotations

import logging
import os
import uuid


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger with a sensible default format."""
    logger = logging.getLogger(name)
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
    return logger


def new_correlation_id() -> str:
    """Generate and set a correlation ID for the current process."""
    cid = str(uuid.uuid4())
    os.environ["CORRELATION_ID"] = cid
    return cid
