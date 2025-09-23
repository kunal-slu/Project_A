#!/bin/bash
# EMR Serverless job submission script with Delta Lake configuration

set -euo pipefail

# Default values
APP_ID=""
ROLE_ARN=""
CODE_BUCKET=""
LAKE_BUCKET=""
ENTRY_POINT=""
WHEEL=""
EXTRA_ARGS=""
REGION="us-east-1"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --app-id)
            APP_ID="$2"
            shift 2
            ;;
        --role-arn)
            ROLE_ARN="$2"
            shift 2
            ;;
        --code-bucket)
            CODE_BUCKET="$2"
            shift 2
            ;;
        --lake-bucket)
            LAKE_BUCKET="$2"
            shift 2
            ;;
        --entry-point)
            ENTRY_POINT="$2"
            shift 2
            ;;
        --wheel)
            WHEEL="$2"
            shift 2
            ;;
        --region)
            REGION="$2"
            shift 2
            ;;
        --extra-args)
            EXTRA_ARGS="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 --app-id APP_ID --role-arn ROLE_ARN --code-bucket CODE_BUCKET --lake-bucket LAKE_BUCKET --entry-point ENTRY_POINT [OPTIONS]"
            echo ""
            echo "Required:"
            echo "  --app-id          EMR Serverless application ID"
            echo "  --role-arn        IAM role ARN for job execution"
            echo "  --code-bucket     S3 bucket containing job code"
            echo "  --lake-bucket     S3 bucket for data lake (for monitoring logs)"
            echo "  --entry-point     Main Python module to run"
            echo ""
            echo "Optional:"
            echo "  --wheel           Python wheel file name (default: auto-detect)"
            echo "  --region          AWS region (default: us-east-1)"
            echo "  --extra-args      Additional arguments to pass to the job"
            echo ""
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate required parameters
if [[ -z "$APP_ID" || -z "$ROLE_ARN" || -z "$CODE_BUCKET" || -z "$LAKE_BUCKET" || -z "$ENTRY_POINT" ]]; then
    echo "Error: Missing required parameters"
    echo "Use --help for usage information"
    exit 1
fi

# Auto-detect wheel if not provided
if [[ -z "$WHEEL" ]]; then
    WHEEL=$(aws s3 ls "s3://$CODE_BUCKET/dist/" --region "$REGION" | grep "\.whl$" | tail -1 | awk '{print $4}')
    if [[ -z "$WHEEL" ]]; then
        echo "Error: No wheel file found in s3://$CODE_BUCKET/dist/"
        exit 1
    fi
    echo "Auto-detected wheel: $WHEEL"
fi

# Generate job name with timestamp
JOB_NAME="$(basename "$ENTRY_POINT")-$(date +%Y%m%d-%H%M%S)"

# Build Spark submit parameters with Delta Lake configuration
SPARK_SUBMIT_PARAMS="--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
SPARK_SUBMIT_PARAMS="$SPARK_SUBMIT_PARAMS --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
SPARK_SUBMIT_PARAMS="$SPARK_SUBMIT_PARAMS --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
SPARK_SUBMIT_PARAMS="$SPARK_SUBMIT_PARAMS --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4"

# Add S3 monitoring logs
SPARK_SUBMIT_PARAMS="$SPARK_SUBMIT_PARAMS --conf spark.sql.adaptive.enabled=true"
SPARK_SUBMIT_PARAMS="$SPARK_SUBMIT_PARAMS --conf spark.sql.adaptive.coalescePartitions.enabled=true"
SPARK_SUBMIT_PARAMS="$SPARK_SUBMIT_PARAMS --conf spark.eventLog.enabled=true"
SPARK_SUBMIT_PARAMS="$SPARK_SUBMIT_PARAMS --conf spark.eventLog.dir=s3://$LAKE_BUCKET/logs/spark-events/"

# Add extra arguments if provided
if [[ -n "$EXTRA_ARGS" ]]; then
    SPARK_SUBMIT_PARAMS="$SPARK_SUBMIT_PARAMS $EXTRA_ARGS"
fi

# Build entry point arguments
ENTRY_POINT_ARGS="--wheel s3://$CODE_BUCKET/dist/$WHEEL"
if [[ -n "$EXTRA_ARGS" ]]; then
    ENTRY_POINT_ARGS="$ENTRY_POINT_ARGS $EXTRA_ARGS"
fi

echo "Submitting EMR Serverless job..."
echo "Application ID: $APP_ID"
echo "Job Name: $JOB_NAME"
echo "Entry Point: $ENTRY_POINT"
echo "Wheel: $WHEEL"
echo "Spark Parameters: $SPARK_SUBMIT_PARAMS"

# Submit the job
JOB_RUN_ID=$(aws emr-serverless start-job-run \
    --application-id "$APP_ID" \
    --execution-role-arn "$ROLE_ARN" \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "'"$ENTRY_POINT"'",
            "entryPointArguments": ["'"$ENTRY_POINT_ARGS"'"],
            "sparkSubmitParameters": "'"$SPARK_SUBMIT_PARAMS"'"
        }
    }' \
    --name "$JOB_NAME" \
    --region "$REGION" \
    --output json | jq -r '.jobRunId')

echo "Job submitted successfully!"
echo "Job Run ID: $JOB_RUN_ID"
echo "Monitor job status with:"
echo "  aws emr-serverless get-job-run --application-id $APP_ID --job-run-id $JOB_RUN_ID --region $REGION"
echo ""
echo "View logs at: s3://$LAKE_BUCKET/logs/spark-events/"