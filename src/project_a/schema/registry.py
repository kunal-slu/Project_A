"""Single source of truth for core table schemas and contract metadata."""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SCHEMAS: dict[str, StructType] = {
    "orders_silver": StructType(
        [
            StructField("order_id", StringType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=True),
            StructField("order_date", DateType(), nullable=False),
            StructField("currency", StringType(), nullable=True),
            StructField("unit_price", DecimalType(10, 2), nullable=True),
            StructField("total_amount", DecimalType(10, 2), nullable=False),
            StructField("quantity", IntegerType(), nullable=True),
            StructField("status", StringType(), nullable=True),
            StructField("payment_method", StringType(), nullable=True),
            StructField("updated_at", TimestampType(), nullable=True),
        ]
    ),
    "customers_silver": StructType(
        [
            StructField("customer_id", StringType(), nullable=False),
            StructField("first_name", StringType(), nullable=True),
            StructField("last_name", StringType(), nullable=True),
            StructField("email", StringType(), nullable=True),
            StructField("country", StringType(), nullable=True),
            StructField("registration_date", DateType(), nullable=True),
            StructField("updated_at", TimestampType(), nullable=True),
        ]
    ),
    "products_silver": StructType(
        [
            StructField("product_id", StringType(), nullable=False),
            StructField("product_name", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("price_usd", DecimalType(10, 2), nullable=True),
            StructField("cost_usd", DecimalType(10, 2), nullable=True),
            StructField("supplier_id", StringType(), nullable=True),
            StructField("updated_at", TimestampType(), nullable=True),
        ]
    ),
}

PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "orders_silver": ("order_id",),
    "customers_silver": ("customer_id",),
    "products_silver": ("product_id",),
}


def get_schema(table_name: str) -> StructType | None:
    """Return schema by logical table name."""
    return SCHEMAS.get(table_name)


def get_required_columns(table_name: str) -> list[str]:
    """Return required (non-nullable) columns for a table."""
    schema = get_schema(table_name)
    if schema is None:
        return []
    return [field.name for field in schema.fields if not field.nullable]
