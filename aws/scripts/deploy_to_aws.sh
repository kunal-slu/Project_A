#!/bin/bash
#
# Deploy Project_A to AWS
#
# This script:
# 1. Builds the Python wheel package
# 2. Uploads wheel to S3 artifacts bucket
# 3. Syncs job scripts to S3
# 4. Syncs configs to S3
#
# Usage:
#   export ARTIFACTS_BUCKET="my-etl-artifacts-demo-424570854632"
#   export AWS_REGION="us-east-1"
#   export AWS_PROFILE="${AWS_PROFILE:-default}"
#   bash scripts/deploy_to_aws.sh
#

set -euo pipefail

# Configuration
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-my-etl-artifacts-demo-424570854632}"
AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_PROFILE="${AWS_PROFILE:-default}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "🚀 Deploying Project_A to AWS"
echo "=========================================="
echo "Artifacts Bucket: ${ARTIFACTS_BUCKET}"
echo "AWS Region: ${AWS_REGION}"
echo "AWS Profile: ${AWS_PROFILE}"
echo ""

# Step 1: Build the wheel
echo "📦 Step 1: Building Python wheel..."
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ python3 not found${NC}"
    exit 1
fi

# Check if build is available
if python3 -m build --help &> /dev/null; then
    python3 -m build --wheel
else
    # Fallback to setuptools
    python3 setup.py bdist_wheel
fi

# Find the latest wheel
WHEEL_FILE=$(ls -t dist/*.whl 2>/dev/null | head -1)

if [ -z "$WHEEL_FILE" ]; then
    echo -e "${RED}❌ No wheel file found in dist/${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Built: ${WHEEL_FILE}${NC}"

# Step 2: Upload wheel to S3
echo ""
echo "📤 Step 2: Uploading wheel to S3..."
WHEEL_NAME=$(basename "$WHEEL_FILE")
S3_WHEEL_PATH="s3://${ARTIFACTS_BUCKET}/packages/${WHEEL_NAME}"

aws s3 cp "$WHEEL_FILE" "$S3_WHEEL_PATH" \
    --region "$AWS_REGION" \
    --profile "$AWS_PROFILE" \
    --quiet

echo -e "${GREEN}✓ Uploaded: ${S3_WHEEL_PATH}${NC}"

# Step 3: Sync job scripts
echo ""
echo "📤 Step 3: Syncing job scripts to S3..."
aws s3 sync jobs/ "s3://${ARTIFACTS_BUCKET}/jobs/" \
    --exclude "*.pyc" \
    --exclude "__pycache__/*" \
    --exclude "*.py~" \
    --delete \
    --region "$AWS_REGION" \
    --profile "$AWS_PROFILE" \
    --quiet

echo -e "${GREEN}✓ Synced jobs/ to s3://${ARTIFACTS_BUCKET}/jobs/${NC}"

# Step 4: Sync configs
echo ""
echo "📤 Step 4: Syncing configs to S3..."
aws s3 sync config/ "s3://${ARTIFACTS_BUCKET}/config/" \
    --exclude "*.pyc" \
    --exclude "__pycache__/*" \
    --exclude "*.py~" \
    --delete \
    --region "$AWS_REGION" \
    --profile "$AWS_PROFILE" \
    --quiet

echo -e "${GREEN}✓ Synced config/ to s3://${ARTIFACTS_BUCKET}/config/${NC}"

# Step 5: Upload validation script (optional, for EMR execution)
echo ""
echo "📤 Step 5: Uploading validation script to S3..."
aws s3 cp tools/validate_aws_etl.py "s3://${ARTIFACTS_BUCKET}/tools/validate_aws_etl.py" \
    --region "$AWS_REGION" \
    --profile "$AWS_PROFILE" \
    --quiet

echo -e "${GREEN}✓ Uploaded: s3://${ARTIFACTS_BUCKET}/tools/validate_aws_etl.py${NC}"

# Summary
echo ""
echo "=========================================="
echo -e "${GREEN}✅ Deployment Complete!${NC}"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Submit EMR job:"
echo "     aws emr-serverless start-job-run \\"
echo "       --application-id <APP_ID> \\"
echo "       --execution-role-arn <ROLE_ARN> \\"
echo "       --job-driver '{\"sparkSubmit\": {...}}'"
echo ""
echo "  2. Validate pipeline:"
echo "     python tools/validate_aws_etl.py --config aws/config/dev.yaml"
echo ""
echo "  3. Check S3 data:"
echo "     aws s3 ls s3://${ARTIFACTS_BUCKET}/packages/"
echo "     aws s3 ls s3://${ARTIFACTS_BUCKET}/jobs/"
echo "     aws s3 ls s3://${ARTIFACTS_BUCKET}/config/"
echo ""

