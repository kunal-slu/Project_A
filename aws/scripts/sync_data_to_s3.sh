#!/usr/bin/env bash
# Sync local sample data to S3 bronze layer (overwrite existing, delete old files)

set -euo pipefail

AWS_PROFILE="${AWS_PROFILE:-kunal21}"
AWS_REGION="${AWS_REGION:-us-east-1}"
S3_BRONZE_ROOT="s3://my-etl-lake-demo-424570854632/bronze"
LOCAL_SAMPLES_ROOT="./aws/data/samples"

echo "=========================================="
echo "Syncing Sample Data to S3 Bronze Layer"
echo "=========================================="
echo "Local source: ${LOCAL_SAMPLES_ROOT}"
echo "S3 target:    ${S3_BRONZE_ROOT}"
echo "Mode:         Overwrite existing, delete old files"
echo ""

# Function to sync with delete (removes files in S3 that don't exist locally)
sync_with_delete() {
    local local_dir="$1"
    local s3_path="$2"
    local source_name="$3"
    
    if [ -d "$local_dir" ]; then
        echo "📤 Syncing ${source_name}..."
        aws s3 sync "$local_dir" "$s3_path" \
            --delete \
            --profile "${AWS_PROFILE}" \
            --region "${AWS_REGION}"
        echo "✅ ${source_name} synced (old files deleted)"
    else
        echo "⚠️  Local directory not found: ${local_dir}"
    fi
}

# Sync CRM data
sync_with_delete \
    "${LOCAL_SAMPLES_ROOT}/crm/" \
    "${S3_BRONZE_ROOT}/crm/" \
    "CRM"

# Sync Snowflake data (note: S3 uses 'snowflakes' directory)
sync_with_delete \
    "${LOCAL_SAMPLES_ROOT}/snowflake/" \
    "${S3_BRONZE_ROOT}/snowflakes/" \
    "Snowflake"

# Sync Redshift data
sync_with_delete \
    "${LOCAL_SAMPLES_ROOT}/redshift/" \
    "${S3_BRONZE_ROOT}/redshift/" \
    "Redshift"

# Sync Kafka data
sync_with_delete \
    "${LOCAL_SAMPLES_ROOT}/kafka/" \
    "${S3_BRONZE_ROOT}/kafka/" \
    "Kafka"

# Sync FX data
sync_with_delete \
    "${LOCAL_SAMPLES_ROOT}/fx/" \
    "${S3_BRONZE_ROOT}/fx/" \
    "FX"

# Clean up old/duplicate directories
echo ""
echo "🧹 Cleaning up old/duplicate files in S3..."
echo ""

# Remove old fx/json/ directory if it exists (duplicate)
if aws s3 ls "${S3_BRONZE_ROOT}/fx/json/" --profile "${AWS_PROFILE}" --region "${AWS_REGION}" 2>/dev/null | grep -q .; then
    echo "  Removing old fx/json/ directory..."
    aws s3 rm "${S3_BRONZE_ROOT}/fx/json/" --recursive \
        --profile "${AWS_PROFILE}" \
        --region "${AWS_REGION}"
    echo "  ✅ Removed old fx/json/"
fi

# List any other unexpected directories
echo ""
echo "📋 Final S3 Bronze Structure:"
aws s3 ls "${S3_BRONZE_ROOT}/" --recursive \
    --profile "${AWS_PROFILE}" \
    --region "${AWS_REGION}" \
    | awk '{print $4}' \
    | sort

echo ""
echo "=========================================="
echo "✅ Sync Complete!"
echo "=========================================="
echo ""
echo "Summary:"
echo "  - Local samples synced to S3"
echo "  - Old files in S3 deleted"
echo "  - S3 bronze is now source of truth"
echo ""
echo "Both local and AWS ETL will use:"
echo "  ${S3_BRONZE_ROOT}/"

