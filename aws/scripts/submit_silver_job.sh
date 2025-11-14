#!/bin/bash
# Submit Bronze to Silver Transformation Job to EMR Serverless
# Usage: ./submit_silver_job.sh [--env dev] [--config-path config/dev.yaml]

set -e

# Default values
ENV="${ENV:-dev}"
CONFIG_PATH="${CONFIG_PATH:-config/dev.yaml}"
EMR_APP_ID="${EMR_APP_ID:-00g0tm6kccmdcf09}"
EMR_ROLE_ARN="${EMR_ROLE_ARN:-arn:aws:iam::424570854632:role/project-a-dev-emr-exec}"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-my-etl-artifacts-demo-424570854632}"
LAKE_BUCKET="${LAKE_BUCKET:-my-etl-lake-demo-424570854632}"
AWS_PROFILE="${AWS_PROFILE:-kunal21}"
AWS_REGION="${AWS_REGION:-us-east-1}"
WHEEL_FILE="project_a-0.1.0-py3-none-any.whl"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --env)
            ENV="$2"
            shift 2
            ;;
        --config-path)
            CONFIG_PATH="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Fixed wheel path
WHEEL_S3_URI="s3://${ARTIFACTS_BUCKET}/packages/${WHEEL_FILE}"

# Job-specific paths
ENTRY_POINT="s3://${ARTIFACTS_BUCKET}/jobs/transform/bronze_to_silver.py"
CONFIG_S3_URI="s3://${ARTIFACTS_BUCKET}/${CONFIG_PATH}"

echo "🥈 Submitting Bronze → Silver Job to EMR Serverless"
echo "=================================================="
echo "Entry Point: $ENTRY_POINT"
echo "Wheel File: $WHEEL_S3_URI"
echo "Config: $CONFIG_S3_URI"
echo "Environment: $ENV"
echo ""

# Submit job with reduced resource requirements to avoid vCPU quota issues
RESPONSE=$(aws emr-serverless start-job-run \
  --application-id "${EMR_APP_ID}" \
  --execution-role-arn "${EMR_ROLE_ARN}" \
  --region $AWS_REGION \
  --profile $AWS_PROFILE \
  --name "${ENV}-bronze-to-silver" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"${ENTRY_POINT}\",
      \"entryPointArguments\": [
        \"--env\", \"${ENV}\",
        \"--config\", \"${CONFIG_S3_URI}\"
      ],
      \"sparkSubmitParameters\": \"--packages io.delta:delta-core_2.12:2.4.0 --py-files s3://${ARTIFACTS_BUCKET}/packages/dependencies.zip,s3://${ARTIFACTS_BUCKET}/packages/project_a-0.1.0-py3-none-any.whl --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.executor.instances=1 --conf spark.executor.cores=1 --conf spark.executor.memory=2g --conf spark.driver.cores=1 --conf spark.driver.memory=2g\"
    }
  }" \
  --configuration-overrides "{
    \"monitoringConfiguration\": {
      \"s3MonitoringConfiguration\": {
        \"logUri\": \"s3://${LAKE_BUCKET}/logs/emr-serverless/\"
      }
    },
    \"applicationConfiguration\": [
      {
        \"classification\": \"spark-defaults\",
        \"properties\": {
          \"spark.executor.instances\": \"1\",
          \"spark.executor.cores\": \"1\",
          \"spark.executor.memory\": \"2g\",
          \"spark.driver.cores\": \"1\",
          \"spark.driver.memory\": \"2g\"
        }
      }
    ]
  }" \
  --output json)

JOB_RUN_ID=$(echo $RESPONSE | jq -r '.jobRunId')

if [ "$JOB_RUN_ID" != "null" ] && [ -n "$JOB_RUN_ID" ]; then
    echo "✅ Job submitted successfully!"
    echo ""
    echo "Job Run ID: $JOB_RUN_ID"
    echo "Application ID: $EMR_APP_ID"
    echo ""
    echo "📊 Monitor job status:"
    echo "   export JOB_RUN_ID=$JOB_RUN_ID"
    echo "   ./aws/scripts/monitor_job.sh $JOB_RUN_ID"
    echo ""
    echo "📋 Or check manually:"
    echo "   aws emr-serverless get-job-run --application-id $EMR_APP_ID --job-run-id $JOB_RUN_ID --profile $AWS_PROFILE --region $AWS_REGION --query 'jobRun.[state,stateDetails]' --output table"
    echo ""
    echo "🔍 Verify Silver data (after job completes):"
    echo "   aws s3 ls s3://${LAKE_BUCKET}/silver/ --recursive --profile $AWS_PROFILE --region $AWS_REGION | head -20"
else
    echo "❌ Failed to submit job"
    echo "$RESPONSE" | jq .
    exit 1
fi

