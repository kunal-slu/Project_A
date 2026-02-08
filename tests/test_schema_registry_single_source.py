"""Tests for single source-of-truth schema registry usage."""

from project_a.schema.registry import SCHEMAS, get_required_columns, get_schema
from project_a.utils.schema_validator import SchemaValidator


def test_registry_contains_core_silver_tables():
    assert get_schema("orders_silver") is not None
    assert get_schema("customers_silver") is not None
    assert get_schema("products_silver") is not None


def test_schema_validator_uses_registry_reference():
    # Should point to same shared mapping to avoid drift.
    assert SchemaValidator.SCHEMAS is SCHEMAS


def test_required_columns_from_registry():
    required = get_required_columns("orders_silver")
    assert "order_id" in required
    assert "customer_id" in required
    assert "total_amount" in required
