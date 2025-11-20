#!/usr/bin/env bash
# Upload local sample data to S3 bronze layer for unified data source

set -euo pipefail

AWS_PROFILE="${AWS_PROFILE:-kunal21}"
AWS_REGION="${AWS_REGION:-us-east-1}"
S3_BRONZE_ROOT="s3://my-etl-lake-demo-424570854632/bronze"
LOCAL_SAMPLES_ROOT="./aws/data/samples"

echo "=========================================="
echo "Uploading Sample Data to S3 Bronze Layer"
echo "=========================================="
echo "Local source: ${LOCAL_SAMPLES_ROOT}"
echo "S3 target:    ${S3_BRONZE_ROOT}"
echo ""

# Upload CRM data
if [ -d "${LOCAL_SAMPLES_ROOT}/crm" ]; then
    echo "📤 Uploading CRM data..."
    aws s3 sync "${LOCAL_SAMPLES_ROOT}/crm/" \
        "${S3_BRONZE_ROOT}/crm/" \
        --profile "${AWS_PROFILE}" \
        --region "${AWS_REGION}"
    echo "✅ CRM data uploaded"
fi

# Upload Snowflake data
if [ -d "${LOCAL_SAMPLES_ROOT}/snowflake" ]; then
    echo "📤 Uploading Snowflake data..."
    aws s3 sync "${LOCAL_SAMPLES_ROOT}/snowflake/" \
        "${S3_BRONZE_ROOT}/snowflakes/" \
        --profile "${AWS_PROFILE}" \
        --region "${AWS_REGION}"
    echo "✅ Snowflake data uploaded"
fi

# Upload Redshift data
if [ -d "${LOCAL_SAMPLES_ROOT}/redshift" ]; then
    echo "📤 Uploading Redshift data..."
    aws s3 sync "${LOCAL_SAMPLES_ROOT}/redshift/" \
        "${S3_BRONZE_ROOT}/redshift/" \
        --profile "${AWS_PROFILE}" \
        --region "${AWS_REGION}"
    echo "✅ Redshift data uploaded"
fi

# Upload Kafka data
if [ -d "${LOCAL_SAMPLES_ROOT}/kafka" ]; then
    echo "📤 Uploading Kafka data..."
    aws s3 sync "${LOCAL_SAMPLES_ROOT}/kafka/" \
        "${S3_BRONZE_ROOT}/kafka/" \
        --profile "${AWS_PROFILE}" \
        --region "${AWS_REGION}"
    echo "✅ Kafka data uploaded"
fi

# Upload FX data
if [ -d "${LOCAL_SAMPLES_ROOT}/fx" ]; then
    echo "📤 Uploading FX data..."
    aws s3 sync "${LOCAL_SAMPLES_ROOT}/fx/" \
        "${S3_BRONZE_ROOT}/fx/" \
        --profile "${AWS_PROFILE}" \
        --region "${AWS_REGION}"
    echo "✅ FX data uploaded"
fi

echo ""
echo "=========================================="
echo "✅ Upload Complete!"
echo "=========================================="
echo ""
echo "Verifying uploads..."
aws s3 ls "${S3_BRONZE_ROOT}/" --recursive \
    --profile "${AWS_PROFILE}" \
    --region "${AWS_REGION}" \
    | head -20
