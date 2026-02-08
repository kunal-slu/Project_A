"""
Backward-compatible schema package.
"""

from __future__ import annotations

__all__ = ["SchemaValidator"]


def __getattr__(name: str):
    if name == "SchemaValidator":
        from .validator import SchemaValidator

        return SchemaValidator
    raise AttributeError(name)
