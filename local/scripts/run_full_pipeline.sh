#!/bin/bash
# Quick validation check

set -e

echo "=== Step 1: Regenerating Source Data ==="
python scripts/regenerate_source_data.py
echo ""

echo "=== Step 2: Running Bronze → Silver ETL ==="
python jobs/transform/bronze_to_silver.py --env local --config local/config/local.yaml
echo ""

echo "=== Step 3: Running Silver → Gold ETL ==="
python jobs/transform/silver_to_gold.py --env local --config local/config/local.yaml
echo ""

echo "=== Step 4: Running Validation ==="
python tools/validate_local_etl.py --env local --config local/config/local.yaml
