#!/bin/bash
# Build and upload Python package to S3 artifacts bucket

set -e

AWS_PROFILE="${AWS_PROFILE:-kunal21}"
AWS_REGION="${AWS_REGION:-us-east-1}"

# Get artifacts bucket from terraform outputs or env var
if [ -z "$ARTIFACTS_BUCKET" ]; then
    if [ -f "aws/terraform/terraform-outputs.dev.json" ]; then
        ARTIFACTS_BUCKET=$(jq -r '.s3_artifacts_bucket.value' aws/terraform/terraform-outputs.dev.json)
        echo "✅ Found artifacts bucket: $ARTIFACTS_BUCKET"
    else
        echo "❌ ARTIFACTS_BUCKET not set and terraform-outputs.dev.json not found"
        exit 1
    fi
fi

echo "📦 Building Python package..."
cd "$(dirname "$0")/../.."

# Clean previous builds
rm -rf dist/ build/ *.egg-info

# Build wheel
python3 -m pip install --upgrade build
python3 -m build

# Find the wheel file
WHEEL_FILE=$(ls -t dist/*.whl | head -1)
if [ -z "$WHEEL_FILE" ]; then
    echo "❌ No wheel file found in dist/"
    exit 1
fi

echo "✅ Built: $WHEEL_FILE"

# Upload to S3
echo "📤 Uploading to s3://${ARTIFACTS_BUCKET}/packages/..."
aws s3 cp "$WHEEL_FILE" \
  "s3://${ARTIFACTS_BUCKET}/packages/" \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION"

WHEEL_NAME=$(basename "$WHEEL_FILE")
echo ""
echo "✅ Upload complete!"
echo "   Package: s3://${ARTIFACTS_BUCKET}/packages/${WHEEL_NAME}"
echo ""
echo "📝 Update config/dev.yaml with:"
echo "   whl_path: s3://${ARTIFACTS_BUCKET}/packages/${WHEEL_NAME}"

