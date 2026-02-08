"""
Test that all schema contract JSON files are valid.
"""

import json
from pathlib import Path

import pytest


def test_schema_contracts_exist():
    """Verify schema_definitions directory exists."""
    schemas_dir = Path(__file__).parent.parent / "config" / "schema_definitions"
    assert schemas_dir.exists(), "schema_definitions/ directory not found"


def test_all_schemas_valid():
    """Test that all schema JSON files are valid."""
    schemas_dir = Path(__file__).parent.parent / "config" / "schema_definitions"

    schema_files = list(schemas_dir.glob("*.json"))
    assert len(schema_files) > 0, "No schema files found"

    required_fields = [
        "schema_name",
        "source",
        "layer",
        "version",
        "required_columns",
        "columns",
    ]

    for schema_file in schema_files:
        with open(schema_file) as f:
            schema = json.load(f)

        for field in required_fields:
            assert field in schema, f"{schema_file}: Missing {field}"

        # Validate columns structure
        assert "columns" in schema
        columns = schema["columns"]
        assert isinstance(columns, dict), "Columns must be a dictionary"

        # Check that columns match required_columns
        required_cols = schema.get("required_columns", [])
        for col in required_cols:
            assert col in columns, f"{schema_file}: Required column {col} not in columns"

        print(f"✅ Valid schema: {schema_file.name}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
