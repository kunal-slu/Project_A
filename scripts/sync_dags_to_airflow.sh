#!/bin/bash
# Sync DAGs from aws/dags/ to airflow/dags/
# This ensures local Airflow can see all DAGs

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DAGS_SOURCE="${PROJECT_ROOT}/aws/dags"
DAGS_TARGET="${PROJECT_ROOT}/airflow/dags"

echo "=========================================="
echo "Syncing DAGs to Local Airflow"
echo "=========================================="

# Create target directory if it doesn't exist
mkdir -p "$DAGS_TARGET"

# Copy DAGs
echo "📁 Copying DAGs from $DAGS_SOURCE to $DAGS_TARGET"
rsync -av --delete \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='.pytest_cache' \
    "$DAGS_SOURCE/" "$DAGS_TARGET/"

echo "✅ DAGs synced"

# List synced DAGs
echo ""
echo "📋 Synced DAGs:"
find "$DAGS_TARGET" -name "*.py" -type f | sed 's|.*/||' | sort

echo ""
echo "=========================================="
echo "✅ DAG sync complete!"
echo "=========================================="
echo ""
echo "If Airflow is running, restart the scheduler:"
echo "  docker compose -f docker-compose-airflow.yml restart airflow-scheduler"

