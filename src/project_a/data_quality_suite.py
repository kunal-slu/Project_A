"""Compatibility wrapper for legacy data quality suite module."""

from __future__ import annotations

from importlib import import_module

try:
    _legacy_module = import_module("project_a.legacy.data_quality_suite")
except Exception:  # pragma: no cover - optional legacy package
    _legacy_module = None
    __all__: list[str] = []
else:
    __all__ = [name for name in dir(_legacy_module) if not name.startswith("_")]
    globals().update({name: getattr(_legacy_module, name) for name in __all__})
