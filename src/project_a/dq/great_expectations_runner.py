"""Lightweight Great Expectations runner compatibility layer."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

try:
    from great_expectations.data_context import DataContext  # type: ignore
except Exception:  # pragma: no cover
    class DataContext:  # type: ignore
        def __init__(self, *_args, **_kwargs):
            pass


class GreatExpectationsRunner:
    def __init__(self, context_root: str | None = None):
        self.context_root = context_root or "great_expectations"
        self.context: Any | None = None

    def init_context(self) -> None:
        root = Path(self.context_root)
        if not root.exists():
            self.context = None
            return
        self.context = DataContext(str(root))

    def run_checkpoint(self, checkpoint_name: str, fail_on_error: bool = True) -> dict[str, Any]:
        if self.context is None:
            if fail_on_error and os.getenv("PROJECT_A_ALLOW_GE_SKIP") != "1":
                raise RuntimeError(
                    "Great Expectations context not found. "
                    "Set PROJECT_A_ALLOW_GE_SKIP=1 to allow skipping."
                )
            logging.getLogger(__name__).warning(
                "Great Expectations context not found; skipping checkpoint %s",
                checkpoint_name,
            )
            return {"success": True, "skipped": True, "checkpoint": checkpoint_name}

        checkpoint = self.context.get_checkpoint(checkpoint_name)
        result = checkpoint.run()
        success = bool(result.get("success", False))
        payload = {"success": success, "run_results": result.get("run_results", {})}
        if not success and fail_on_error:
            raise RuntimeError(f"DQ checkpoint failed: {checkpoint_name}")
        return payload


def run_dq_checkpoint(checkpoint_name: str, fail_on_error: bool = True) -> dict[str, Any]:
    runner = GreatExpectationsRunner()
    runner.init_context()
    return runner.run_checkpoint(checkpoint_name, fail_on_error=fail_on_error)
