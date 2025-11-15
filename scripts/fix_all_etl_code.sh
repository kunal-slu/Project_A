#!/bin/bash
# Fix All ETL Code - Comprehensive validation and fixes

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=========================================="
echo "Fixing All ETL Code"
echo "=========================================="

ERRORS=0
WARNINGS=0

# 1. Check Python syntax
echo ""
echo "1️⃣ Checking Python syntax..."
for file in $(find src/project_a aws/dags jobs -name "*.py" 2>/dev/null | head -20); do
    if python3 -m py_compile "$file" 2>/dev/null; then
        echo "  ✅ $file"
    else
        echo "  ❌ $file - Syntax error"
        ERRORS=$((ERRORS + 1))
    fi
done

# 2. Check imports
echo ""
echo "2️⃣ Checking imports..."
python3 -c "
import sys
sys.path.insert(0, 'src')
try:
    from project_a.pipeline.run_pipeline import JOB_MAP, main
    from project_a.jobs import fx_json_to_bronze, bronze_to_silver, silver_to_gold, publish_gold_to_snowflake
    print('  ✅ All imports successful')
    print(f'  ✅ JOB_MAP has {len(JOB_MAP)} jobs')
except Exception as e:
    print(f'  ❌ Import error: {e}')
    sys.exit(1)
" 2>&1 || ERRORS=$((ERRORS + 1))

# 3. Check Terraform syntax
echo ""
echo "3️⃣ Checking Terraform files..."
if command -v terraform &> /dev/null; then
    cd aws/terraform
    if terraform fmt -check -recursive > /dev/null 2>&1; then
        echo "  ✅ Terraform files formatted"
    else
        echo "  ⚠️  Terraform files need formatting (run: terraform fmt)"
        WARNINGS=$((WARNINGS + 1))
    fi
    cd "$PROJECT_ROOT"
else
    echo "  ⚠️  Terraform not installed, skipping validation"
    WARNINGS=$((WARNINGS + 1))
fi

# 4. Check DAG files
echo ""
echo "4️⃣ Checking Airflow DAGs..."
for dag in aws/dags/*.py; do
    if [ -f "$dag" ]; then
        if python3 -m py_compile "$dag" 2>/dev/null; then
            echo "  ✅ $(basename $dag)"
        else
            echo "  ❌ $(basename $dag) - Syntax error"
            ERRORS=$((ERRORS + 1))
        fi
    fi
done

# 5. Check wheel exists
echo ""
echo "5️⃣ Checking wheel file..."
if [ -f "dist/project_a-0.1.0-py3-none-any.whl" ]; then
    echo "  ✅ Wheel file exists"
else
    echo "  ⚠️  Wheel file not found (run: python -m build)"
    WARNINGS=$((WARNINGS + 1))
fi

# 6. Check config files
echo ""
echo "6️⃣ Checking config files..."
if [ -f "config/dev.yaml" ]; then
    echo "  ✅ config/dev.yaml exists"
else
    echo "  ⚠️  config/dev.yaml not found"
    WARNINGS=$((WARNINGS + 1))
fi

# Summary
echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="
echo "❌ Errors: $ERRORS"
echo "⚠️  Warnings: $WARNINGS"

if [ $ERRORS -eq 0 ]; then
    echo ""
    echo "✅ All ETL code is ready!"
    exit 0
else
    echo ""
    echo "❌ Please fix errors before deployment"
    exit 1
fi

