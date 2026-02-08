"""
Backward-compatible SchemaValidator import path.

Historical imports reference ``project_a.schema.validator.SchemaValidator``.
The maintained implementation lives in ``project_a.utils.schema_validator``.
"""

from project_a.utils.schema_validator import SchemaValidator

__all__ = ["SchemaValidator"]
