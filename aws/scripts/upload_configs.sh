#!/bin/bash
# Upload config files to S3 artifacts bucket

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

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

echo "📤 Uploading config files to s3://${ARTIFACTS_BUCKET}/config/..."

# Upload dev.yaml
if [ -f "$REPO_ROOT/config/dev.yaml" ]; then
    aws s3 cp "$REPO_ROOT/config/dev.yaml" \
      "s3://${ARTIFACTS_BUCKET}/config/dev.yaml" \
      --profile "$AWS_PROFILE" \
      --region "$AWS_REGION"
    echo "✅ Uploaded config/dev.yaml"
else
    echo "⚠️  config/dev.yaml not found"
fi

# Upload dq.yaml if exists
if [ -f "$REPO_ROOT/config/dq.yaml" ]; then
    aws s3 cp "$REPO_ROOT/config/dq.yaml" \
      "s3://${ARTIFACTS_BUCKET}/config/dq.yaml" \
      --profile "$AWS_PROFILE" \
      --region "$AWS_REGION"
    echo "✅ Uploaded config/dq.yaml"
fi

# Upload schema definitions
if [ -d "$REPO_ROOT/config/schema_definitions" ]; then
    aws s3 sync "$REPO_ROOT/config/schema_definitions/" \
      "s3://${ARTIFACTS_BUCKET}/config/schema_definitions/" \
      --profile "$AWS_PROFILE" \
      --region "$AWS_REGION" \
      --exclude "*.md"
    echo "✅ Uploaded schema definitions"
fi

echo ""
echo "✅ Config upload complete!"
echo "   Config: s3://${ARTIFACTS_BUCKET}/config/dev.yaml"

