#!/usr/bin/env bash
# Sync data sources between aws/data/samples and data/samples
# Ensures both use identical source files

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AWS_SAMPLES="${PROJECT_ROOT}/aws/data/samples"
LOCAL_SAMPLES="${PROJECT_ROOT}/data/samples"

echo "🔄 Syncing data sources to ensure local and AWS use identical files..."
echo ""

# Create directory structure
for dir in crm snowflake redshift fx kafka; do
    mkdir -p "${AWS_SAMPLES}/${dir}"
    mkdir -p "${LOCAL_SAMPLES}/${dir}"
done

# Sync from aws/data/samples to data/samples (aws is source of truth for structure)
echo "📥 Syncing from aws/data/samples → data/samples..."
for file in $(find "${AWS_SAMPLES}" -type f \( -name "*.csv" -o -name "*.json" \) | sort); do
    rel_path="${file#${AWS_SAMPLES}/}"
    local_file="${LOCAL_SAMPLES}/${rel_path}"
    mkdir -p "$(dirname "$local_file")"
    cp "$file" "$local_file"
    echo "  ✅ Synced: $rel_path"
done

echo ""
echo "✅ Data sources synced!"
echo ""
echo "📊 File counts:"
for dir in crm snowflake redshift fx kafka; do
    aws_count=$(find "${AWS_SAMPLES}/${dir}" -type f \( -name "*.csv" -o -name "*.json" \) 2>/dev/null | wc -l | tr -d ' ')
    local_count=$(find "${LOCAL_SAMPLES}/${dir}" -type f \( -name "*.csv" -o -name "*.json" \) 2>/dev/null | wc -l | tr -d ' ')
    echo "  ${dir}: AWS=${aws_count}, Local=${local_count}"
done
