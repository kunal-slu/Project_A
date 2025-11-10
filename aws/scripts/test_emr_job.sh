#!/bin/bash
# Test EMR Serverless job run
# Usage: ./test_emr_job.sh <job_name> [job_args...]

set -e

AWS_PROFILE="${AWS_PROFILE:-kunal21}"
AWS_REGION="${AWS_REGION:-us-east-1}"

# Load from terraform outputs
if [ -f "aws/terraform/terraform-outputs.dev.json" ]; then
    EMR_APP_ID=$(jq -r '.emr_app_id.value' aws/terraform/terraform-outputs.dev.json)
    EMR_EXEC_ROLE_ARN=$(jq -r '.emr_exec_role_arn.value' aws/terraform/terraform-outputs.dev.json)
    ARTIFACTS_BUCKET=$(jq -r '.s3_artifacts_bucket.value' aws/terraform/terraform-outputs.dev.json)
else
    echo "❌ terraform-outputs.dev.json not found. Set EMR_APP_ID, EMR_EXEC_ROLE_ARN, ARTIFACTS_BUCKET"
    exit 1
fi

JOB_NAME="${1:-dev_secret_probe}"
shift
JOB_ARGS=("$@")

echo "🚀 Starting EMR Serverless job..."
echo "   App ID: $EMR_APP_ID"
echo "   Role: $EMR_EXEC_ROLE_ARN"
echo "   Job: $JOB_NAME"
echo ""

# Determine entry point
if [ "$JOB_NAME" = "dev_secret_probe" ]; then
    ENTRY_POINT="s3://${ARTIFACTS_BUCKET}/jobs/dev_secret_probe.py"
elif [ "$JOB_NAME" = "snowflake_to_bronze" ]; then
    ENTRY_POINT="s3://${ARTIFACTS_BUCKET}/jobs/ingest/snowflake_to_bronze.py"
elif [ "$JOB_NAME" = "bronze_to_silver" ]; then
    ENTRY_POINT="s3://${ARTIFACTS_BUCKET}/jobs/transform/bronze_to_silver.py"
else
    ENTRY_POINT="s3://${ARTIFACTS_BUCKET}/jobs/${JOB_NAME}.py"
fi

# Build job driver JSON
JOB_DRIVER=$(cat <<EOF
{
  "sparkSubmit": {
    "entryPoint": "${ENTRY_POINT}",
    "entryPointArguments": $(printf '%s\n' "${JOB_ARGS[@]}" | jq -R . | jq -s .),
    "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  }
}
EOF
)

# Start job
JOB_RUN_ID=$(aws emr-serverless start-job-run \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --application-id "$EMR_APP_ID" \
  --execution-role-arn "$EMR_EXEC_ROLE_ARN" \
  --job-driver "$JOB_DRIVER" \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "s3://'${ARTIFACTS_BUCKET}'/emr-logs/"
      }
    }
  }' \
  --query 'jobRunId' \
  --output text)

echo "✅ Job started: $JOB_RUN_ID"
echo ""
echo "📊 Monitor job:"
echo "   aws emr-serverless get-job-run --application-id $EMR_APP_ID --job-run-id $JOB_RUN_ID --profile $AWS_PROFILE"
echo ""
echo "📋 View logs:"
echo "   aws s3 ls s3://${ARTIFACTS_BUCKET}/emr-logs/applications/${EMR_APP_ID}/jobs/${JOB_RUN_ID}/ --profile $AWS_PROFILE"

