#!/usr/bin/env python3
"""
Auto-generate data catalog documentation from schema definitions.

Generates markdown tables for each layer (Bronze, Silver, Gold) with
columns, types, DQ rules, and descriptions.
"""

import json
import yaml
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime


def load_schema(schema_path: Path) -> Dict[str, Any]:
    """Load JSON schema file."""
    with open(schema_path) as f:
        return json.load(f)


def generate_table_doc(schema: Dict[str, Any], layer: str) -> str:
    """Generate markdown documentation for a table."""
    table_name = schema.get("table_name", "unknown")
    primary_key = schema.get("primary_key", "N/A")
    columns = schema.get("columns", [])
    dq_rules = schema.get("data_quality_rules", [])
    compliance = schema.get("compliance_notes", "")
    
    doc = f"## {table_name}\n\n"
    doc += f"**Layer**: {layer}\n\n"
    doc += f"**Primary Key**: `{primary_key}`\n\n"
    
    # Column table
    doc += "### Columns\n\n"
    doc += "| Column | Type | Nullable | Description | PII |\n"
    doc += "|--------|------|----------|-------------|-----|\n"
    
    for col in columns:
        col_name = col.get("name", "")
        col_type = col.get("type", "")
        nullable = "✅" if col.get("nullable", True) else "❌"
        desc = col.get("description", "")
        is_pii = "⚠️" if col.get("pii", False) else ""
        doc += f"| `{col_name}` | {col_type} | {nullable} | {desc} | {is_pii} |\n"
    
    # DQ Rules
    if dq_rules:
        doc += "\n### Data Quality Rules\n\n"
        for rule in dq_rules:
            field = rule.get("field", "")
            rule_type = rule.get("rule", "")
            severity = rule.get("severity", "warning")
            doc += f"- **{field}**: {rule_type} ({severity})\n"
    
    # Compliance
    if compliance:
        doc += f"\n### Compliance Notes\n\n{compliance}\n"
    
    return doc


def generate_catalog():
    """Generate data catalog for all layers."""
    output_dir = Path("docs/data_catalog")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    schemas_dir = Path("config/schema_definitions")
    contracts_dir = Path("src/pyspark_interview_project/contracts")
    
    # Bronze layer schemas
    bronze_schemas = list(schemas_dir.glob("*_bronze.schema.json"))
    
    # Generate Bronze catalog
    bronze_doc = f"# Bronze Layer Data Catalog\n\n"
    bronze_doc += f"*Auto-generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n"
    bronze_doc += "The Bronze layer contains raw, unprocessed data ingested from external sources.\n\n"
    
    for schema_path in bronze_schemas:
        schema = load_schema(schema_path)
        bronze_doc += generate_table_doc(schema, "Bronze") + "\n\n"
    
    with open(output_dir / "Bronze.md", "w") as f:
        f.write(bronze_doc)
    
    # Silver/Gold schemas (if available)
    # Add similar logic for Silver and Gold layers
    
    print(f"✅ Generated data catalog:")
    print(f"   - {output_dir / 'Bronze.md'}")
    
    return bronze_doc


if __name__ == "__main__":
    generate_catalog()

