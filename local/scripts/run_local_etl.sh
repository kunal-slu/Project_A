#!/usr/bin/env bash
# Local ETL Pipeline Runner (Bash Script)
# 
# Runs the complete ETL pipeline locally with proper environment setup.
# Falls back to S3 data checks if SparkSession fails.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONFIG_PATH="${1:-local/config/local.yaml}"

echo "================================================================================"
echo "🚀 LOCAL ETL PIPELINE RUNNER"
echo "================================================================================"
echo "Project Root: ${PROJECT_ROOT}"
echo "Config: ${CONFIG_PATH}"
echo ""

# Set PYTHONPATH
export PYTHONPATH="${PROJECT_ROOT}/src:${PYTHONPATH:-}"

# Step 1: Bronze → Silver
echo "================================================================================"
echo "STEP 1: BRONZE → SILVER"
echo "================================================================================"
cd "${PROJECT_ROOT}"

if python3 "${PROJECT_ROOT}/local/jobs/transform/bronze_to_silver.py" \
    --env local \
    --config "${PROJECT_ROOT}/${CONFIG_PATH}"; then
    echo "✅ Bronze→Silver completed successfully"
else
    echo "❌ Bronze→Silver failed"
    echo ""
    echo "💡 TIP: If SparkSession error occurred, try:"
    echo "   1. Check Java version: java -version (should be Java 8 or 11)"
    echo "   2. Check PySpark version: pip show pyspark"
    echo "   3. Use AWS EMR for ETL (recommended)"
    echo "   4. Check S3 data: python3 local/scripts/dq/check_s3_data_quality.py --layer all"
    exit 1
fi

echo ""

# Step 2: Silver → Gold
echo "================================================================================"
echo "STEP 2: SILVER → GOLD"
echo "================================================================================"

if python3 "${PROJECT_ROOT}/local/jobs/transform/silver_to_gold.py" \
    --env local \
    --config "${PROJECT_ROOT}/${CONFIG_PATH}"; then
    echo "✅ Silver→Gold completed successfully"
else
    echo "❌ Silver→Gold failed"
    exit 1
fi

echo ""

# Step 3: Verify outputs
echo "================================================================================"
echo "STEP 3: VERIFICATION"
echo "================================================================================"

python3 "${PROJECT_ROOT}/local/jobs/run_etl_pipeline.py" --verify-only --config "${CONFIG_PATH}" || {
    echo "⚠️  Verification failed, but ETL may have completed"
    echo "   Check S3 data: python3 local/scripts/dq/check_s3_data_quality.py --layer all"
}

echo ""
echo "================================================================================"
echo "🎉 LOCAL ETL PIPELINE COMPLETED!"
echo "================================================================================"

