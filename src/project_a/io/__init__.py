"""
Unified I/O operations for Project A.

Provides:
- Readers: Read from various sources (CSV, JSON, Delta, Parquet, Kafka)
- Writers: Write to various formats (Delta, Parquet)
- Schema validation
"""

from project_a.io.reader import (
    read_bronze_table,
    read_csv_with_schema,
    read_delta_table,
    read_json_with_schema,
)
from project_a.io.writer import (
    validate_schema_before_write,
    write_gold_table,
    write_silver_table,
)

__all__ = [
    "read_bronze_table",
    "read_csv_with_schema",
    "read_json_with_schema",
    "read_delta_table",
    "write_silver_table",
    "write_gold_table",
    "validate_schema_before_write",
]
