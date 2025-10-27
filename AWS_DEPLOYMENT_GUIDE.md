# 🚀 AWS Deployment Guide - Complete

## Quick Start for AWS Deployment

### Step 1: Set Environment Variables
```bash
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AWS_REGION="us-east-1"
export PROJECT="pyspark-etl-project"
export ENVIRONMENT="dev"
```

### Step 2: Deploy Infrastructure
```bash
cd aws/infra/terraform
terraform init
terraform apply -var="project=$PROJECT" -var="environment=$ENVIRONMENT"

# Save outputs
export EMR_APP_ID=$(terraform output -raw emr_serverless_application_id)
export EMR_ROLE_ARN=$(terraform output -raw emr_serverless_job_role_arn)
export LAKE_BUCKET=$(terraform output -raw data_lake_bucket)
export CODE_BUCKET=$(terraform output -raw artifacts_bucket)
```

### Step 3: Build and Upload Code
```bash
python -m build
aws s3 cp dist/*.whl s3://$CODE_BUCKET/dist/
aws s3 sync src/ s3://$CODE_BUCKET/src/
aws s3 sync jobs/ s3://$CODE_BUCKET/jobs/
aws s3 sync config/ s3://$CODE_BUCKET/config/
```

### Step 4: Submit Your First Job
```bash
aws emr-serverless start-job-run \
  --application-id $EMR_APP_ID \
  --execution-role-arn $EMR_ROLE_ARN \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'$CODE_BUCKET'/jobs/hubspot_to_bronze.py"
    }
  }'
```

### Step 5: Monitor Job
```bash
# Get job run ID
export JOB_RUN_ID="your-job-run-id"

# Check status
aws emr-serverless get-job-run \
  --application-id $EMR_APP_ID \
  --job-run-id $JOB_RUN_ID

# View logs
aws logs tail /aws/emr-serverless/spark --follow
```

## Project Structure

```
✅ src/pyspark_interview_project/     # Main package
✅ jobs/                              # EMR job wrappers
✅ config/                            # Configuration files
✅ aws/infra/terraform/              # Infrastructure
✅ aws/scripts/                       # Deployment scripts
✅ aws/emr_configs/                  # EMR configuration
```

## Key Files

- `config/dq.yaml` - Data quality configuration
- `config/local.yaml` - Local development
- `config/config-dev.yaml` - Development environment
- `jobs/hubspot_to_bronze.py` - Example job
- `aws/emr_configs/spark-defaults.conf` - Spark tuning

## Success Criteria

After deployment, you should have:
✅ Data in S3 Bronze layer
✅ CloudWatch logs showing successful execution
✅ No errors in EMR job status
✅ Delta tables visible in S3

## Troubleshooting

### Common Issues

1. **Import errors**: Check `src/pyspark_interview_project/utils/__init__.py`
2. **Permissions**: Verify IAM role has S3 access
3. **Delta Lake**: Ensure Delta packages are included
4. **Config**: Check `config/dev.yaml` paths

## Next Steps

1. Test locally first
2. Deploy to dev environment
3. Validate with test data
4. Promote to production

