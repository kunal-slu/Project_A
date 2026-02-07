#!/bin/bash
# Backfill Bronze layer for a specific date and source

set -e

SOURCE="${1:-all}"
DATE="${2:-$(date +%Y-%m-%d)}"

echo "Starting backfill for source: $SOURCE, date: $DATE"

# Configuration
CONFIG_FILE="aws/config/config-prod.yaml"
EMR_APPLICATION_ID="${EMR_APPLICATION_ID}"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET}"

# Submit EMR job
aws emr-serverless start-job-run \
    --application-id "$EMR_APPLICATION_ID" \
    --execution-role-arn "arn:aws:iam::ACCOUNT_ID:role/emr-serverless-execution-role" \
    --name "backfill-bronze-${SOURCE}-${DATE}" \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://'${ARTIFACTS_BUCKET}'/jobs/backfill_bronze.py",
            "entryPointArguments": ["--source", "'${SOURCE}'", "--date", "'${DATE}'"],
            "sparkSubmitParameters": "--conf spark.executor.memory=8g --conf spark.driver.memory=4g"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://company-logs-ACCOUNT_ID/emr-serverless/"
            }
        }
    }'

echo "Backfill job submitted successfully"
