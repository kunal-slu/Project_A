#!/usr/bin/env bash
# Upload missing bronze files to S3 for data parity between local and AWS ETL

set -euo pipefail

AWS_PROFILE="${AWS_PROFILE:-kunal21}"
AWS_REGION="${AWS_REGION:-us-east-1}"
BUCKET="my-etl-lake-demo-424570854632"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=========================================="
echo "Uploading Missing Bronze Files to S3"
echo "=========================================="
echo "Bucket: s3://${BUCKET}/bronze/"
echo "Profile: ${AWS_PROFILE}"
echo "Region: ${AWS_REGION}"
echo ""

# Check AWS credentials
if ! aws sts get-caller-identity --profile "${AWS_PROFILE}" --region "${AWS_REGION}" >/dev/null 2>&1; then
    echo "❌ Error: AWS credentials not configured for profile '${AWS_PROFILE}'"
    exit 1
fi

# Upload CRM files
echo "📤 Uploading CRM files..."
aws s3 cp "${PROJECT_ROOT}/aws/data/samples/crm/accounts.csv" \
  "s3://${BUCKET}/bronze/crm/accounts.csv" \
  --profile "${AWS_PROFILE}" \
  --region "${AWS_REGION}"

aws s3 cp "${PROJECT_ROOT}/aws/data/samples/crm/contacts.csv" \
  "s3://${BUCKET}/bronze/crm/contacts.csv" \
  --profile "${AWS_PROFILE}" \
  --region "${AWS_REGION}"

aws s3 cp "${PROJECT_ROOT}/aws/data/samples/crm/opportunities.csv" \
  "s3://${BUCKET}/bronze/crm/opportunities.csv" \
  --profile "${AWS_PROFILE}" \
  --region "${AWS_REGION}"

echo "✅ CRM files uploaded"
echo ""

# Upload Redshift behavior file
echo "📤 Uploading Redshift behavior file..."
aws s3 cp "${PROJECT_ROOT}/aws/data/samples/redshift/redshift_customer_behavior_50000.csv" \
  "s3://${BUCKET}/bronze/redshift/redshift_customer_behavior_50000.csv" \
  --profile "${AWS_PROFILE}" \
  --region "${AWS_REGION}"

echo "✅ Redshift behavior file uploaded"
echo ""

# Upload Kafka events file
echo "📤 Uploading Kafka events file..."
aws s3 cp "${PROJECT_ROOT}/aws/data/samples/kafka/stream_kafka_events_100000.csv" \
  "s3://${BUCKET}/bronze/kafka/stream_kafka_events_100000.csv" \
  --profile "${AWS_PROFILE}" \
  --region "${AWS_REGION}"

echo "✅ Kafka events file uploaded"
echo ""

# Verify uploads
echo "=========================================="
echo "Verifying Uploads"
echo "=========================================="
echo ""

files=(
  "bronze/crm/accounts.csv"
  "bronze/crm/contacts.csv"
  "bronze/crm/opportunities.csv"
  "bronze/redshift/redshift_customer_behavior_50000.csv"
  "bronze/kafka/stream_kafka_events_100000.csv"
)

all_ok=true
for file in "${files[@]}"; do
  if aws s3 ls "s3://${BUCKET}/${file}" --profile "${AWS_PROFILE}" --region "${AWS_REGION}" >/dev/null 2>&1; then
    size=$(aws s3 ls "s3://${BUCKET}/${file}" --profile "${AWS_PROFILE}" --region "${AWS_REGION}" | awk '{print $3}')
    echo "✅ ${file} (${size} bytes)"
  else
    echo "❌ ${file} - NOT FOUND"
    all_ok=false
  fi
done

echo ""
if [ "$all_ok" = true ]; then
  echo "=========================================="
  echo "✅ All files uploaded successfully!"
  echo "=========================================="
  echo ""
  echo "Next steps:"
  echo "1. Re-run AWS EMR Bronze→Silver job"
  echo "2. Re-run AWS EMR Silver→Gold job"
  echo "3. Compare row counts with local ETL results"
  exit 0
else
  echo "=========================================="
  echo "❌ Some files failed to upload"
  echo "=========================================="
  exit 1
fi

