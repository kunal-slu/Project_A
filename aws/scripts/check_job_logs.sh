#!/bin/bash
# Check EMR Serverless Job Logs and Help Interpret Errors
# Usage: ./check_job_logs.sh <JOB_RUN_ID>

set -e

JOB_RUN_ID="${1:-${JOB_RUN_ID}}"
EMR_APP_ID="${EMR_APP_ID:-00g0tm6kccmdcf09}"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-my-etl-artifacts-demo-424570854632}"
AWS_PROFILE="${AWS_PROFILE:-kunal21}"
AWS_REGION="${AWS_REGION:-us-east-1}"

if [ -z "$JOB_RUN_ID" ]; then
    echo "❌ Error: JOB_RUN_ID required"
    echo "Usage: $0 <JOB_RUN_ID>"
    exit 1
fi

echo "📋 Checking Job Logs for: $JOB_RUN_ID"
echo "=========================================="
echo ""

# Get job status first
echo "📊 Job Status:"
aws emr-serverless get-job-run \
  --application-id "${EMR_APP_ID}" \
  --job-run-id "${JOB_RUN_ID}" \
  --region $AWS_REGION \
  --profile $AWS_PROFILE \
  --query 'jobRun.[state,stateDetails]' \
  --output table

echo ""

# Get log location
LOG_PREFIX="s3://${ARTIFACTS_BUCKET}/emr-logs/applications/${EMR_APP_ID}/jobs/${JOB_RUN_ID}"

echo "📁 Log Location: ${LOG_PREFIX}/"
echo ""

# Check if logs exist
if aws s3 ls "${LOG_PREFIX}/" --profile $AWS_PROFILE --region $AWS_REGION 2>/dev/null | grep -q .; then
    echo "✅ Logs found. Listing log files:"
    aws s3 ls "${LOG_PREFIX}/" --recursive --profile $AWS_PROFILE --region $AWS_REGION
    
    echo ""
    echo "📄 Driver Logs (stdout):"
    DRIVER_STDOUT="${LOG_PREFIX}/driver/stdout.gz"
    if aws s3 ls "${DRIVER_STDOUT}" --profile $AWS_PROFILE --region $AWS_REGION 2>/dev/null; then
        echo "   Downloading and showing last 50 lines..."
        aws s3 cp "${DRIVER_STDOUT}" - --profile $AWS_PROFILE --region $AWS_REGION 2>/dev/null | gunzip | tail -50 || echo "   (Could not read compressed log)"
    else
        echo "   Checking for uncompressed stdout..."
        DRIVER_STDOUT_ALT="${LOG_PREFIX}/driver/stdout"
        if aws s3 ls "${DRIVER_STDOUT_ALT}" --profile $AWS_PROFILE --region $AWS_REGION 2>/dev/null; then
            aws s3 cp "${DRIVER_STDOUT_ALT}" - --profile $AWS_PROFILE --region $AWS_REGION 2>/dev/null | tail -50
        fi
    fi
    
    echo ""
    echo "📄 Driver Logs (stderr):"
    DRIVER_STDERR="${LOG_PREFIX}/driver/stderr.gz"
    if aws s3 ls "${DRIVER_STDERR}" --profile $AWS_PROFILE --region $AWS_REGION 2>/dev/null; then
        echo "   Downloading and showing last 50 lines..."
        aws s3 cp "${DRIVER_STDERR}" --profile $AWS_PROFILE --region $AWS_REGION 2>/dev/null | gunzip | tail -50 || echo "   (Could not read compressed log)"
    else
        echo "   Checking for uncompressed stderr..."
        DRIVER_STDERR_ALT="${LOG_PREFIX}/driver/stderr"
        if aws s3 ls "${DRIVER_STDERR_ALT}" --profile $AWS_PROFILE --region $AWS_REGION 2>/dev/null; then
            aws s3 cp "${DRIVER_STDERR_ALT}" - --profile $AWS_PROFILE --region $AWS_REGION 2>/dev/null | tail -50
        fi
    fi
    
    echo ""
    echo "💡 To download all logs locally:"
    echo "   aws s3 sync ${LOG_PREFIX}/ ./logs/${JOB_RUN_ID}/ --profile $AWS_PROFILE --region $AWS_REGION"
    
else
    echo "⚠️  Logs not yet available (job may still be starting)"
    echo ""
    echo "💡 Logs will appear at: ${LOG_PREFIX}/"
    echo "   Check again in a few moments"
fi

echo ""
echo "=========================================="
echo "🔍 Common Error Patterns to Check:"
echo "  - Import errors: Look for 'ModuleNotFoundError' or 'ImportError'"
echo "  - Config errors: Look for 'FileNotFoundError' or config parsing errors"
echo "  - S3 access errors: Look for 'AccessDenied' or 'NoSuchBucket'"
echo "  - Spark errors: Look for 'SparkException' or 'AnalysisException'"
echo ""
echo "💡 To get full job details:"
echo "   aws emr-serverless get-job-run --application-id ${EMR_APP_ID} --job-run-id ${JOB_RUN_ID} --profile $AWS_PROFILE --region $AWS_REGION --output json | jq ."

