#!/bin/bash
# Deploy DAGs to MWAA S3 bucket
# Usage: ./scripts/deploy_mwaa_dags.sh [MWAA_BUCKET]

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DAGS_SOURCE="${PROJECT_ROOT}/aws/dags"

# Get MWAA bucket from argument or Terraform output
if [ -n "$1" ]; then
    MWAA_BUCKET="$1"
else
    # Try to get from Terraform output
    cd "${PROJECT_ROOT}/aws/terraform"
    MWAA_BUCKET=$(terraform output -raw mwaa_s3_bucket 2>/dev/null || echo "")
    
    if [ -z "$MWAA_BUCKET" ]; then
        echo "❌ MWAA bucket not found. Please provide it as an argument:"
        echo "   ./scripts/deploy_mwaa_dags.sh s3://your-mwaa-bucket"
        exit 1
    fi
fi

# Remove s3:// prefix if present
MWAA_BUCKET="${MWAA_BUCKET#s3://}"

echo "=========================================="
echo "Deploying DAGs to MWAA"
echo "=========================================="
echo "MWAA Bucket: $MWAA_BUCKET"
echo "DAGs Source: $DAGS_SOURCE"
echo ""

# Check AWS credentials
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo "❌ AWS credentials not configured"
    echo "   Run: aws configure --profile kunal21"
    exit 1
fi

# Upload DAGs
echo "📁 Uploading DAGs..."
aws s3 sync "$DAGS_SOURCE/" "s3://${MWAA_BUCKET}/dags/" \
    --exclude "*.pyc" \
    --exclude "__pycache__/*" \
    --exclude ".pytest_cache/*" \
    --exclude "*.md" \
    --delete \
    --profile kunal21 \
    --region us-east-1

echo "✅ DAGs uploaded"

# Upload requirements.txt if it exists
if [ -f "${PROJECT_ROOT}/requirements.txt" ]; then
    echo "📦 Uploading requirements.txt..."
    aws s3 cp "${PROJECT_ROOT}/requirements.txt" "s3://${MWAA_BUCKET}/requirements.txt" \
        --profile kunal21 \
        --region us-east-1
    echo "✅ requirements.txt uploaded"
fi

# List uploaded DAGs
echo ""
echo "📋 Uploaded DAGs:"
aws s3 ls "s3://${MWAA_BUCKET}/dags/" \
    --profile kunal21 \
    --region us-east-1 \
    --recursive | grep "\.py$" | awk '{print $4}' | sed 's|dags/||'

echo ""
echo "=========================================="
echo "✅ MWAA DAG deployment complete!"
echo "=========================================="
echo ""
echo "MWAA will automatically sync DAGs within a few minutes."
echo "Check MWAA UI to verify DAGs are visible."

