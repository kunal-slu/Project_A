#!/bin/bash
# Phase 4: Run Bronze → Silver → Gold jobs on EMR Serverless
# 
# Prerequisites:
# - EMR Serverless app created
# - Wheel uploaded to S3
# - Config uploaded to S3
# - Bronze data populated

set -e

# Configuration
export AWS_PROFILE="${AWS_PROFILE:-kunal21}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export EMR_APP_ID="${EMR_APP_ID:-00g0tm6kccmdcf09}"
export EMR_ROLE_ARN="${EMR_ROLE_ARN:-arn:aws:iam::424570854632:role/project-a-dev-emr-exec}"
export ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-my-etl-artifacts-demo-424570854632}"
export LAKE_BUCKET="${LAKE_BUCKET:-my-etl-lake-demo-424570854632}"

echo "🚀 Phase 4: Running Bronze → Silver → Gold Jobs"
echo "================================================"
echo "EMR App ID: $EMR_APP_ID"
echo "Artifacts Bucket: $ARTIFACTS_BUCKET"
echo "Lake Bucket: $LAKE_BUCKET"
echo ""

# Step 1: Rebuild and upload wheel
echo "📦 Step 1: Rebuilding wheel..."
cd "$(dirname "$0")/.."
python -m build

WHEEL_PATH=$(ls dist/project_a-*.whl | tail -n 1)
echo "✅ Built: $WHEEL_PATH"

echo "📤 Uploading wheel to S3..."
aws s3 cp "$WHEEL_PATH" \
  "s3://${ARTIFACTS_BUCKET}/packages/" \
  --profile "$AWS_PROFILE" --region "$AWS_REGION"

echo "✅ Wheel uploaded"
echo ""

# Step 2: Verify dependencies bundle exists
echo "🔍 Step 2: Verifying dependencies..."
aws s3 ls "s3://${ARTIFACTS_BUCKET}/packages/" \
  --profile "$AWS_PROFILE" --region "$AWS_REGION" | grep -E "(project_a|emr_deps)"

echo ""

# Step 3: Run Bronze → Silver job
echo "🔄 Step 3: Running Bronze → Silver job..."
BRONZE_TO_SILVER_JOB_ID=$(aws emr-serverless start-job-run \
  --application-id "$EMR_APP_ID" \
  --execution-role-arn "$EMR_ROLE_ARN" \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"s3://${ARTIFACTS_BUCKET}/packages/project_a-0.1.0-py3-none-any.whl\",
      \"entryPointArguments\": [
        \"--job\", \"bronze_to_silver\",
        \"--env\", \"dev\",
        \"--config\", \"s3://${ARTIFACTS_BUCKET}/config/dev.yaml\"
      ],
      \"sparkSubmitParameters\": \"--py-files s3://${ARTIFACTS_BUCKET}/packages/project_a-0.1.0-py3-none-any.whl,s3://${ARTIFACTS_BUCKET}/packages/emr_deps_pyyaml.zip --packages io.delta:delta-core_2.12:2.4.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog\"
    }
  }" \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "s3://'${ARTIFACTS_BUCKET}'/emr-logs/"
      }
    }
  }' \
  --query 'jobRunId' --output text)

echo "✅ Bronze → Silver job started: $BRONZE_TO_SILVER_JOB_ID"
echo "⏳ Waiting for job to complete..."

# Wait for job to complete
while true; do
  STATE=$(aws emr-serverless get-job-run \
    --application-id "$EMR_APP_ID" \
    --job-run-id "$BRONZE_TO_SILVER_JOB_ID" \
    --profile "$AWS_PROFILE" --region "$AWS_REGION" \
    --query 'jobRun.state' --output text)
  
  echo "  Current state: $STATE"
  
  if [ "$STATE" = "SUCCESS" ]; then
    echo "✅ Bronze → Silver job completed successfully!"
    break
  elif [ "$STATE" = "FAILED" ] || [ "$STATE" = "CANCELLED" ]; then
    echo "❌ Bronze → Silver job failed with state: $STATE"
    exit 1
  fi
  
  sleep 30
done

echo ""

# Step 4: Verify Silver tables
echo "🔍 Step 4: Verifying Silver tables..."
aws s3 ls "s3://${LAKE_BUCKET}/silver/" \
  --recursive --profile "$AWS_PROFILE" --region "$AWS_REGION" | head -20

echo ""

# Step 5: Run Silver → Gold job
echo "🔄 Step 5: Running Silver → Gold job..."
SILVER_TO_GOLD_JOB_ID=$(aws emr-serverless start-job-run \
  --application-id "$EMR_APP_ID" \
  --execution-role-arn "$EMR_ROLE_ARN" \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"s3://${ARTIFACTS_BUCKET}/packages/project_a-0.1.0-py3-none-any.whl\",
      \"entryPointArguments\": [
        \"--job\", \"silver_to_gold\",
        \"--env\", \"dev\",
        \"--config\", \"s3://${ARTIFACTS_BUCKET}/config/dev.yaml\"
      ],
      \"sparkSubmitParameters\": \"--py-files s3://${ARTIFACTS_BUCKET}/packages/project_a-0.1.0-py3-none-any.whl,s3://${ARTIFACTS_BUCKET}/packages/emr_deps_pyyaml.zip --packages io.delta:delta-core_2.12:2.4.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog\"
    }
  }" \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "s3://'${ARTIFACTS_BUCKET}'/emr-logs/"
      }
    }
  }' \
  --query 'jobRunId' --output text)

echo "✅ Silver → Gold job started: $SILVER_TO_GOLD_JOB_ID"
echo "⏳ Waiting for job to complete..."

# Wait for job to complete
while true; do
  STATE=$(aws emr-serverless get-job-run \
    --application-id "$EMR_APP_ID" \
    --job-run-id "$SILVER_TO_GOLD_JOB_ID" \
    --profile "$AWS_PROFILE" --region "$AWS_REGION" \
    --query 'jobRun.state' --output text)
  
  echo "  Current state: $STATE"
  
  if [ "$STATE" = "SUCCESS" ]; then
    echo "✅ Silver → Gold job completed successfully!"
    break
  elif [ "$STATE" = "FAILED" ] || [ "$STATE" = "CANCELLED" ]; then
    echo "❌ Silver → Gold job failed with state: $STATE"
    exit 1
  fi
  
  sleep 30
done

echo ""

# Step 6: Verify Gold tables
echo "🔍 Step 6: Verifying Gold tables..."
aws s3 ls "s3://${LAKE_BUCKET}/gold/" \
  --recursive --profile "$AWS_PROFILE" --region "$AWS_REGION" | head -20

echo ""
echo "🎉 Phase 4 Complete!"
echo "==================="
echo "✅ Bronze → Silver: SUCCESS"
echo "✅ Silver → Gold: SUCCESS"
echo ""
echo "Next steps:"
echo "  - Check run audit logs: s3://${LAKE_BUCKET}/_audit/"
echo "  - Query Gold tables in Athena/Spark"
echo "  - Set up Airflow DAG to run these jobs daily"

