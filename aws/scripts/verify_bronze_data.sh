#!/bin/bash
# Verify Bronze Data in Lake Bucket
# Usage: ./verify_bronze_data.sh [source] [table]
# Example: ./verify_bronze_data.sh snowflake orders

set -e

LAKE_BUCKET="${LAKE_BUCKET:-my-etl-lake-demo-424570854632}"
AWS_PROFILE="${AWS_PROFILE:-kunal21}"
AWS_REGION="${AWS_REGION:-us-east-1}"
SOURCE="${1:-snowflake}"
TABLE="${2:-orders}"

echo "🔍 Verifying Bronze Data"
echo "=========================================="
echo "Lake Bucket: $LAKE_BUCKET"
echo "Source: $SOURCE"
echo "Table: $TABLE"
echo ""

# Check if bronze directory exists
echo "📁 Checking bronze/ directory structure..."
if aws s3 ls "s3://${LAKE_BUCKET}/bronze/" --profile $AWS_PROFILE --region $AWS_REGION 2>/dev/null | grep -q .; then
    echo "✅ Bronze directory exists"
    aws s3 ls "s3://${LAKE_BUCKET}/bronze/" --profile $AWS_PROFILE --region $AWS_REGION
else
    echo "⚠️  Bronze directory not found or empty"
fi
echo ""

# Check specific source/table path
BRONZE_PATH="s3://${LAKE_BUCKET}/bronze/${SOURCE}/${TABLE}"
echo "📊 Checking specific path: ${BRONZE_PATH}"
if aws s3 ls "${BRONZE_PATH}/" --recursive --profile $AWS_PROFILE --region $AWS_REGION 2>/dev/null | head -20; then
    echo ""
    echo "✅ Data found at ${BRONZE_PATH}"
    
    # Count files
    FILE_COUNT=$(aws s3 ls "${BRONZE_PATH}/" --recursive --profile $AWS_PROFILE --region $AWS_REGION 2>/dev/null | wc -l | tr -d ' ')
    echo "📈 Total files: $FILE_COUNT"
    
    # Show size
    echo ""
    echo "💾 Data size:"
    aws s3 ls "${BRONZE_PATH}/" --recursive --summarize --profile $AWS_PROFILE --region $AWS_REGION 2>/dev/null | tail -1
else
    echo "⚠️  No data found at ${BRONZE_PATH}"
    echo ""
    echo "💡 Checking alternative paths..."
    
    # Try different path structures
    for alt_path in \
        "s3://${LAKE_BUCKET}/bronze/${SOURCE}/" \
        "s3://${LAKE_BUCKET}/bronze/" \
        "s3://${LAKE_BUCKET}/"; do
        echo "  Checking: $alt_path"
        if aws s3 ls "${alt_path}" --profile $AWS_PROFILE --region $AWS_REGION 2>/dev/null | head -5; then
            echo "    ✅ Found content"
            break
        fi
    done
fi

echo ""
echo "=========================================="
echo "💡 To check all bronze data recursively:"
echo "   aws s3 ls s3://${LAKE_BUCKET}/bronze/ --recursive --profile $AWS_PROFILE --region $AWS_REGION | head -20"
echo ""
echo "💡 To check Delta table structure:"
echo "   aws s3 ls s3://${LAKE_BUCKET}/bronze/${SOURCE}/${TABLE}/ --recursive --profile $AWS_PROFILE --region $AWS_REGION"

