#!/usr/bin/env bash
# Run data quality checks on AWS EMR

set -euo pipefail

AWS_PROFILE="${AWS_PROFILE:-kunal21}"
AWS_REGION="${AWS_REGION:-us-east-1}"
EMR_APP_ID="${EMR_APP_ID:-00g0tm6kccmdcf09}"
EMR_ROLE_ARN="${EMR_ROLE_ARN:-arn:aws:iam::424570854632:role/project-a-dev-emr-exec}"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-my-etl-artifacts-demo-424570854632}"

echo "=========================================="
echo "Running Data Quality Checks on AWS EMR"
echo "=========================================="

# Submit DQ job to EMR Serverless
JOB_OUTPUT=$(aws emr-serverless start-job-run \
  --application-id "${EMR_APP_ID}" \
  --execution-role-arn "${EMR_ROLE_ARN}" \
  --profile "${AWS_PROFILE}" \
  --region "${AWS_REGION}" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"s3://${ARTIFACTS_BUCKET}/scripts/dq/check_data_quality.py\",
      \"entryPointArguments\": [
        \"--env\", \"aws\",
        \"--config\", \"s3://${ARTIFACTS_BUCKET}/config/dev.yaml\",
        \"--layer\", \"all\"
      ],
      \"sparkSubmitParameters\": \"--py-files s3://${ARTIFACTS_BUCKET}/packages/project_a-0.1.0-py3-none-any.whl --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog\"
    }
  }" \
  --output json)

JOB_RUN_ID=$(echo "$JOB_OUTPUT" | grep -o '"jobRunId": "[^"]*"' | cut -d'"' -f4)

echo "Job submitted: ${JOB_RUN_ID}"
echo ""
echo "Check status with:"
echo "  aws emr-serverless get-job-run --application-id ${EMR_APP_ID} --job-run-id ${JOB_RUN_ID} --profile ${AWS_PROFILE} --region ${AWS_REGION}"

