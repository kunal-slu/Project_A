#!/bin/bash
# Wait for EMR Serverless Job to Complete
# Usage: ./wait_for_job_completion.sh <JOB_RUN_ID> [max_wait_minutes]

set -e

JOB_RUN_ID="${1}"
MAX_WAIT_MINUTES="${2:-30}"
EMR_APP_ID="${EMR_APP_ID:-00g0tm6kccmdcf09}"
AWS_PROFILE="${AWS_PROFILE:-kunal21}"
AWS_REGION="${AWS_REGION:-us-east-1}"

if [ -z "$JOB_RUN_ID" ]; then
    echo "❌ Error: JOB_RUN_ID required"
    echo "Usage: $0 <JOB_RUN_ID> [max_wait_minutes]"
    exit 1
fi

echo "⏳ Waiting for job $JOB_RUN_ID to complete..."
echo "   Max wait time: ${MAX_WAIT_MINUTES} minutes"
echo ""

START_TIME=$(date +%s)
MAX_WAIT_SECONDS=$((MAX_WAIT_MINUTES * 60))
CHECK_INTERVAL=10

while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    
    if [ $ELAPSED -gt $MAX_WAIT_SECONDS ]; then
        echo "⏰ Timeout: Job did not complete within ${MAX_WAIT_MINUTES} minutes"
        exit 1
    fi
    
    STATE=$(aws emr-serverless get-job-run \
      --application-id "${EMR_APP_ID}" \
      --job-run-id "${JOB_RUN_ID}" \
      --region $AWS_REGION \
      --profile $AWS_PROFILE \
      --query 'jobRun.state' \
      --output text 2>/dev/null || echo "UNKNOWN")
    
    ELAPSED_MIN=$((ELAPSED / 60))
    ELAPSED_SEC=$((ELAPSED % 60))
    
    case "$STATE" in
        "SUCCESS")
            echo ""
            echo "✅ Job completed successfully!"
            echo "   Elapsed time: ${ELAPSED_MIN}m ${ELAPSED_SEC}s"
            exit 0
            ;;
        "FAILED"|"CANCELLED")
            echo ""
            echo "❌ Job failed with state: $STATE"
            echo "   Elapsed time: ${ELAPSED_MIN}m ${ELAPSED_SEC}s"
            echo ""
            echo "📋 Getting error details..."
            aws emr-serverless get-job-run \
              --application-id "${EMR_APP_ID}" \
              --job-run-id "${JOB_RUN_ID}" \
              --region $AWS_REGION \
              --profile $AWS_PROFILE \
              --query 'jobRun.[state,stateDetails,exitMessage]' \
              --output table
            exit 1
            ;;
        "PENDING"|"SCHEDULED"|"RUNNING")
            echo -ne "\r⏳ Status: $STATE | Elapsed: ${ELAPSED_MIN}m ${ELAPSED_SEC}s"
            sleep $CHECK_INTERVAL
            ;;
        *)
            echo ""
            echo "⚠️  Unknown state: $STATE"
            sleep $CHECK_INTERVAL
            ;;
    esac
done

