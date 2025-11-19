#!/usr/bin/env bash
# Upload missing data files to AWS S3 bronze layer
# Based on user's data comparison report

set -euo pipefail

AWS_PROFILE="${AWS_PROFILE:-kunal21}"
AWS_REGION="${AWS_REGION:-us-east-1}"
LAKE_BUCKET="my-etl-lake-demo-424570854632"
ARTIFACTS_BUCKET="my-etl-artifacts-demo-424570854632"

PROJECT_ROOT="/Users/kunal/IdeaProjects/Project_A"

echo "📤 Uploading missing data files to AWS S3..."

# Check if files exist locally
if [ ! -f "${PROJECT_ROOT}/aws/data/samples/fx/fx_rates_historical_730_days.csv" ]; then
    echo "⚠️  fx_rates_historical_730_days.csv not found locally"
    echo "   Expected location: ${PROJECT_ROOT}/aws/data/samples/fx/fx_rates_historical_730_days.csv"
else
    echo "✅ Found fx_rates_historical_730_days.csv locally"
    aws s3 cp \
        "${PROJECT_ROOT}/aws/data/samples/fx/fx_rates_historical_730_days.csv" \
        "s3://${LAKE_BUCKET}/bronze/fx/fx_rates_historical_730_days.csv" \
        --profile "${AWS_PROFILE}" \
        --region "${AWS_REGION}"
    echo "✅ Uploaded fx_rates_historical_730_days.csv to S3"
fi

# Check for financial_metrics_24_months.csv
if [ ! -f "${PROJECT_ROOT}/aws/data/samples/financial_metrics_24_months.csv" ]; then
    echo "⚠️  financial_metrics_24_months.csv not found locally"
    echo "   Searching for it..."
    find "${PROJECT_ROOT}" -name "*financial_metrics*" -type f 2>/dev/null | head -5
else
    echo "✅ Found financial_metrics_24_months.csv locally"
    aws s3 cp \
        "${PROJECT_ROOT}/aws/data/samples/financial_metrics_24_months.csv" \
        "s3://${LAKE_BUCKET}/bronze/metrics/financial_metrics_24_months.csv" \
        --profile "${AWS_PROFILE}" \
        --region "${AWS_REGION}"
    echo "✅ Uploaded financial_metrics_24_months.csv to S3"
fi

echo ""
echo "✅ Upload complete!"
echo ""
echo "📋 Verify uploads:"
echo "   aws s3 ls s3://${LAKE_BUCKET}/bronze/fx/ --profile ${AWS_PROFILE} --region ${AWS_REGION}"
echo "   aws s3 ls s3://${LAKE_BUCKET}/bronze/metrics/ --profile ${AWS_PROFILE} --region ${AWS_REGION}"

