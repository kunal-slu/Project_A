#!/bin/bash
# Monitor EMR Serverless Job Status
# Usage: ./monitor_job.sh <JOB_RUN_ID>

set -e

JOB_RUN_ID="${1:-${JOB_RUN_ID}}"
EMR_APP_ID="${EMR_APP_ID:-00g0tm6kccmdcf09}"
AWS_PROFILE="${AWS_PROFILE:-kunal21}"
AWS_REGION="${AWS_REGION:-us-east-1}"

if [ -z "$JOB_RUN_ID" ]; then
    echo "❌ Error: JOB_RUN_ID required"
    echo "Usage: $0 <JOB_RUN_ID>"
    echo "   or: export JOB_RUN_ID=xxx && $0"
    exit 1
fi

echo "📊 Monitoring Job Run: $JOB_RUN_ID"
echo "=========================================="
echo ""

# Get job status
aws emr-serverless get-job-run \
  --application-id "${EMR_APP_ID}" \
  --job-run-id "${JOB_RUN_ID}" \
  --region $AWS_REGION \
  --profile $AWS_PROFILE \
  --query 'jobRun.[state,stateDetails,createdAt,updatedAt]' \
  --output table

echo ""
echo "💡 To check logs, run:"
echo "   aws s3 ls s3://my-etl-artifacts-demo-424570854632/emr-logs/applications/${EMR_APP_ID}/jobs/${JOB_RUN_ID}/ --profile $AWS_PROFILE"
echo ""
echo "💡 To get full details:"
echo "   aws emr-serverless get-job-run --application-id ${EMR_APP_ID} --job-run-id ${JOB_RUN_ID} --profile $AWS_PROFILE --region $AWS_REGION --output json | jq ."
echo ""

# Check if job is still running
STATE=$(aws emr-serverless get-job-run \
  --application-id "${EMR_APP_ID}" \
  --job-run-id "${JOB_RUN_ID}" \
  --region $AWS_REGION \
  --profile $AWS_PROFILE \
  --query 'jobRun.state' \
  --output text)

if [ "$STATE" = "SUCCESS" ]; then
    echo "✅ Job completed successfully!"
elif [ "$STATE" = "FAILED" ]; then
    echo "❌ Job failed. Check logs for details."
elif [ "$STATE" = "CANCELLED" ]; then
    echo "⚠️  Job was cancelled."
elif [ "$STATE" = "PENDING" ] || [ "$STATE" = "SCHEDULED" ] || [ "$STATE" = "RUNNING" ]; then
    echo "⏳ Job is still running. State: $STATE"
    echo ""
    echo "💡 Run this command again to check status:"
    echo "   $0 $JOB_RUN_ID"
fi

