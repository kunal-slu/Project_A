#!/bin/bash
# Complete Bronze Job Workflow: Submit, Wait, Verify
# Usage: ./run_and_verify_bronze.sh <job_name> [--env dev]

set -e

JOB_NAME="${1}"
ENV="${ENV:-dev}"
LAKE_BUCKET="${LAKE_BUCKET:-my-etl-lake-demo-424570854632}"
AWS_PROFILE="${AWS_PROFILE:-kunal21}"
AWS_REGION="${AWS_REGION:-us-east-1}"

if [ -z "$JOB_NAME" ]; then
    echo "❌ Error: Job name required"
    echo "Usage: $0 <job_name> [--env dev]"
    echo ""
    echo "Examples:"
    echo "  $0 snowflake_to_bronze"
    echo "  $0 redshift_to_bronze --env dev"
    exit 1
fi

echo "🚀 Complete Bronze Job Workflow"
echo "=========================================="
echo "Job: $JOB_NAME"
echo "Environment: $ENV"
echo ""

# Step 1: Submit job
echo "📤 Step 1: Submitting job..."
JOB_RUN_ID=$(./aws/scripts/submit_bronze_job.sh "$JOB_NAME" --env "$ENV" 2>&1 | grep "Job Run ID:" | awk '{print $4}')

if [ -z "$JOB_RUN_ID" ]; then
    echo "❌ Failed to get job run ID"
    exit 1
fi

echo "✅ Job submitted: $JOB_RUN_ID"
echo ""

# Step 2: Wait for completion
echo "⏳ Step 2: Waiting for job to complete..."
export JOB_RUN_ID
export EMR_APP_ID="${EMR_APP_ID:-00g0tm6kccmdcf09}"
./aws/scripts/wait_for_job_completion.sh "$JOB_RUN_ID" 30

if [ $? -ne 0 ]; then
    echo ""
    echo "❌ Job did not complete successfully"
    echo "📋 Check logs:"
    echo "   ./aws/scripts/check_job_logs.sh $JOB_RUN_ID"
    exit 1
fi

echo ""

# Step 3: Verify bronze data
echo "🔍 Step 3: Verifying bronze data..."
export LAKE_BUCKET
./aws/scripts/verify_bronze_data.sh

echo ""
echo "✅ Complete! Bronze data verified."
echo ""
echo "📊 Summary:"
echo "   Job Run ID: $JOB_RUN_ID"
echo "   Status: SUCCESS"
echo "   Bronze data: Verified in s3://${LAKE_BUCKET}/bronze/"

