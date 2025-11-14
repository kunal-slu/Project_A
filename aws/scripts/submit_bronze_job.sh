#!/bin/bash
# Submit Bronze Ingestion Job to EMR Serverless
# Usage: ./submit_bronze_job.sh <job_name> [--env dev] [--config-path config/dev.yaml]
# Examples:
#   ./submit_bronze_job.sh snowflake_to_bronze
#   ./submit_bronze_job.sh redshift_to_bronze
#   ./submit_bronze_job.sh salesforce_to_bronze

set -e

# Default values
JOB_NAME="${1}"
ENV="${ENV:-dev}"
CONFIG_PATH="${CONFIG_PATH:-config/dev.yaml}"
EMR_APP_ID="${EMR_APP_ID:-00g0tm6kccmdcf09}"
EMR_ROLE_ARN="${EMR_ROLE_ARN:-arn:aws:iam::424570854632:role/project-a-dev-emr-exec}"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-my-etl-artifacts-demo-424570854632}"
AWS_PROFILE="${AWS_PROFILE:-kunal21}"
AWS_REGION="${AWS_REGION:-us-east-1}"
WHEEL_FILE="project_a-0.1.0-py3-none-any.whl"

# Parse arguments
shift || true
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

if [ -z "$JOB_NAME" ]; then
    echo "❌ Error: Job name required"
    echo ""
    echo "Usage: $0 <job_name> [options]"
    echo ""
    echo "Available jobs:"
    echo "  - snowflake_to_bronze"
    echo "  - redshift_to_bronze"
    echo "  - salesforce_to_bronze"
    echo "  - vendor_to_bronze"
    echo "  - kafka_orders_to_bronze"
    echo ""
    echo "Options:"
    echo "  --env <env>              Environment (default: dev)"
    echo "  --config-path <path>     Config file path (default: config/dev.yaml)"
    echo ""
    echo "Examples:"
    echo "  $0 snowflake_to_bronze"
    echo "  $0 redshift_to_bronze --env prod --config-path config/prod.yaml"
    exit 1
fi

# Determine job path
if [[ "$JOB_NAME" == *"snowflake"* ]] && [[ "$JOB_NAME" == *"bronze"* ]]; then
    JOB_PATH="jobs/ingest/snowflake_to_bronze.py"
elif [[ "$JOB_NAME" == *"redshift"* ]] && [[ "$JOB_NAME" == *"bronze"* ]]; then
    JOB_PATH="jobs/redshift_to_bronze.py"
elif [[ "$JOB_NAME" == *"salesforce"* ]] && [[ "$JOB_NAME" == *"bronze"* ]]; then
    JOB_PATH="jobs/salesforce_to_bronze.py"
elif [[ "$JOB_NAME" == *"vendor"* ]] && [[ "$JOB_NAME" == *"bronze"* ]]; then
    JOB_PATH="jobs/vendor_to_bronze.py"
elif [[ "$JOB_NAME" == *"kafka"* ]]; then
    JOB_PATH="jobs/kafka_orders_to_bronze.py"
else
    # Try to find the job file
    if [ -f "jobs/${JOB_NAME}.py" ]; then
        JOB_PATH="jobs/${JOB_NAME}.py"
    elif [ -f "jobs/ingest/${JOB_NAME}.py" ]; then
        JOB_PATH="jobs/ingest/${JOB_NAME}.py"
    else
        echo "❌ Error: Job file not found for: $JOB_NAME"
        echo "   Tried: jobs/${JOB_NAME}.py"
        echo "   Tried: jobs/ingest/${JOB_NAME}.py"
        exit 1
    fi
fi

# Fixed wheel path - reuse for all jobs
WHEEL_S3_URI="s3://${ARTIFACTS_BUCKET}/packages/${WHEEL_FILE}"

# Job-specific paths
ENTRY_POINT="s3://${ARTIFACTS_BUCKET}/${JOB_PATH}"
CONFIG_S3_URI="s3://${ARTIFACTS_BUCKET}/${CONFIG_PATH}"

echo "🚀 Submitting Bronze Job to EMR Serverless"
echo "=========================================="
echo "Job Name: $JOB_NAME"
echo "Entry Point: $ENTRY_POINT"
echo "Wheel File: $WHEEL_S3_URI (reused for all jobs)"
echo "Config: $CONFIG_S3_URI"
echo "Environment: $ENV"
echo ""

# Get lake bucket for monitoring config
LAKE_BUCKET="${LAKE_BUCKET:-my-etl-lake-demo-424570854632}"

# Submit job - template with consistent wheel path and monitoring
RESPONSE=$(aws emr-serverless start-job-run \
  --application-id "${EMR_APP_ID}" \
  --execution-role-arn "${EMR_ROLE_ARN}" \
  --region $AWS_REGION \
  --profile $AWS_PROFILE \
  --name "${ENV}-${JOB_NAME}" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"${ENTRY_POINT}\",
      \"entryPointArguments\": [
        \"--env\", \"${ENV}\",
        \"--config\", \"${CONFIG_S3_URI}\"
      ],
      \"sparkSubmitParameters\": \"--packages io.delta:delta-core_2.12:2.4.0 --py-files s3://${ARTIFACTS_BUCKET}/packages/dependencies.zip,s3://${ARTIFACTS_BUCKET}/packages/project_a-0.1.0-py3-none-any.whl --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog\"
    }
  }" \
  --configuration-overrides "{
    \"monitoringConfiguration\": {
      \"s3MonitoringConfiguration\": {
        \"logUri\": \"s3://${LAKE_BUCKET}/logs/emr-serverless/\"
      }
    }
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
    echo "🔍 Verify Bronze data (after job completes):"
    echo "   ./aws/scripts/verify_bronze_data.sh"
else
    echo "❌ Failed to submit job"
    echo "$RESPONSE" | jq .
    exit 1
fi

