#!/bin/bash
# EMR Serverless Job Submission Template
# Just change the entryPoint to submit different jobs
# The wheel file path stays the same for all jobs

export EMR_APP_ID="00g0tm6kccmdcf09"
export EMR_ROLE_ARN="arn:aws:iam::424570854632:role/project-a-dev-emr-exec"
export ARTIFACTS_BUCKET="my-etl-artifacts-demo-424570854632"
export AWS_PROFILE="${AWS_PROFILE:-kunal21}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

# CHANGE THIS: Update entryPoint for different jobs
ENTRY_POINT="s3://${ARTIFACTS_BUCKET}/jobs/ingest/snowflake_to_bronze.py"
# Examples:
# ENTRY_POINT="s3://${ARTIFACTS_BUCKET}/jobs/redshift_to_bronze.py"
# ENTRY_POINT="s3://${ARTIFACTS_BUCKET}/jobs/salesforce_to_bronze.py"
# ENTRY_POINT="s3://${ARTIFACTS_BUCKET}/jobs/vendor_to_bronze.py"

# WHEEL PATH STAYS THE SAME - DO NOT CHANGE
WHEEL_S3_URI="s3://my-etl-artifacts-demo-424570854632/packages/project_a-0.1.0-py3-none-any.whl"

# Submit job
aws emr-serverless start-job-run \
  --application-id "${EMR_APP_ID}" \
  --execution-role-arn "${EMR_ROLE_ARN}" \
  --region $AWS_REGION \
  --profile $AWS_PROFILE \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"${ENTRY_POINT}\",
      \"entryPointArguments\": [
        \"--env\", \"dev\",
        \"--config\", \"s3://${ARTIFACTS_BUCKET}/config/dev.yaml\"
      ],
      \"sparkSubmitParameters\": \"--py-files s3://my-etl-artifacts-demo-424570854632/packages/project_a-0.1.0-py3-none-any.whl --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog\"
    }
  }"

# After submission, you'll get a jobRunId
# Use it to check logs:
# ./aws/scripts/check_job_logs.sh <JOB_RUN_ID>

