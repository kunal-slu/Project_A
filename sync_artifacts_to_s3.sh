#!/bin/bash
# Sync artifacts to S3 for EMR on EC2
# Usage: ./sync_artifacts_to_s3.sh

set -e

export AWS_PROFILE="${AWS_PROFILE:-kunal21}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-my-etl-artifacts-demo-424570854632}"

echo "=== Syncing Artifacts to S3 ==="
echo "Bucket: s3://${ARTIFACTS_BUCKET}"
echo "Profile: $AWS_PROFILE"
echo "Region: $AWS_REGION"
echo ""

# 1. Build wheel
echo "📦 Step 1: Building wheel..."
rm -rf build dist src/*.egg-info 2>/dev/null || true
python3 -m build
if [ ! -f "dist/project_a-0.1.0-py3-none-any.whl" ]; then
  echo "❌ Wheel not found! Build failed."
  exit 1
fi
echo "✅ Wheel built: dist/project_a-0.1.0-py3-none-any.whl"
echo ""

# 2. Upload wheel
echo "📤 Step 2: Uploading wheel to S3..."
aws s3 cp dist/project_a-0.1.0-py3-none-any.whl \
  "s3://${ARTIFACTS_BUCKET}/packages/project_a-0.1.0-py3-none-any.whl" \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION"
echo "✅ Wheel uploaded"
echo ""

# 3. Upload job scripts
echo "📤 Step 3: Uploading job scripts to S3..."
aws s3 sync jobs/transform \
  "s3://${ARTIFACTS_BUCKET}/jobs/transform/" \
  --exclude "*" \
  --include "*.py" \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION"
echo "✅ Job scripts uploaded"
echo ""

# 4. Verify Delta JARs exist
echo "🔍 Step 4: Verifying Delta JARs..."
DELTA_SPARK="s3://${ARTIFACTS_BUCKET}/packages/delta-spark_2.12-3.2.0.jar"
DELTA_STORAGE="s3://${ARTIFACTS_BUCKET}/packages/delta-storage-3.2.0.jar"

if aws s3 ls "$DELTA_SPARK" --profile "$AWS_PROFILE" --region "$AWS_REGION" > /dev/null 2>&1; then
  echo "✅ Delta Spark JAR exists"
else
  echo "⚠️  Delta Spark JAR not found: $DELTA_SPARK"
  echo "   Download from: https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/"
fi

if aws s3 ls "$DELTA_STORAGE" --profile "$AWS_PROFILE" --region "$AWS_REGION" > /dev/null 2>&1; then
  echo "✅ Delta Storage JAR exists"
else
  echo "⚠️  Delta Storage JAR not found: $DELTA_STORAGE"
  echo "   Download from: https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/"
fi
echo ""

echo "🎉 Artifact sync complete!"
echo ""
echo "Next steps:"
echo "  1. Ensure EMR cluster is running: j-3N2JXYADSENNU"
echo "  2. Submit steps: ./run_emr_steps.sh"
echo "  3. Monitor: aws emr list-steps --cluster-id j-3N2JXYADSENNU --region $AWS_REGION --profile $AWS_PROFILE --output table"

