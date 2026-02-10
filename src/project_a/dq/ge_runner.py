"""Compatibility wrapper for GE runner."""

from __future__ import annotations

from importlib import import_module

_legacy_module = import_module("project_a.pyspark_interview_project.dq.ge_runner")
__all__ = [name for name in dir(_legacy_module) if not name.startswith("_")]
globals().update({name: getattr(_legacy_module, name) for name in __all__})
