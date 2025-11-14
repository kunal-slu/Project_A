#!/bin/bash
# Build dependencies zip for EMR Serverless
# EMR Serverless needs dependencies in a zip file alongside the wheel

set -e

AWS_PROFILE="${AWS_PROFILE:-kunal21}"
AWS_REGION="${AWS_REGION:-us-east-1}"

# Get artifacts bucket
if [ -z "$ARTIFACTS_BUCKET" ]; then
    if [ -f "aws/terraform/terraform-outputs.dev.json" ]; then
        ARTIFACTS_BUCKET=$(jq -r '.s3_artifacts_bucket.value' aws/terraform/terraform-outputs.dev.json)
        echo "✅ Found artifacts bucket: $ARTIFACTS_BUCKET"
    else
        echo "❌ ARTIFACTS_BUCKET not set"
        exit 1
    fi
fi

echo "📦 Building dependencies zip for EMR Serverless..."
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# Create temp directory
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Install dependencies to temp directory
# Use EMR-specific requirements (minimal, no Airflow)
REQUIREMENTS_FILE="$PROJECT_ROOT/requirements-emr.txt"
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    REQUIREMENTS_FILE="$PROJECT_ROOT/requirements.txt"
fi

echo "📥 Installing dependencies from $REQUIREMENTS_FILE..."
python3 -m pip install -r "$REQUIREMENTS_FILE" --target "$TEMP_DIR" --upgrade

# Create zip file
DEPS_ZIP="$PROJECT_ROOT/dependencies.zip"
cd "$TEMP_DIR"
zip -r "$DEPS_ZIP" . -q
cd "$PROJECT_ROOT"

echo "✅ Created: $DEPS_ZIP"

# Upload to S3
echo "📤 Uploading to s3://${ARTIFACTS_BUCKET}/packages/..."
aws s3 cp "$DEPS_ZIP" \
  "s3://${ARTIFACTS_BUCKET}/packages/" \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION"

echo ""
echo "✅ Upload complete!"
echo "   Dependencies: s3://${ARTIFACTS_BUCKET}/packages/${DEPS_ZIP}"
echo ""
echo "💡 Update your job submission to include:"
echo "   --py-files s3://${ARTIFACTS_BUCKET}/packages/${DEPS_ZIP},s3://${ARTIFACTS_BUCKET}/packages/project_a-0.1.0-py3-none-any.whl"

