#!/bin/bash
# Source Terraform outputs for deployment scripts
# This script loads Terraform outputs and makes them available as environment variables

set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Check if terraform directory exists
if [ ! -d "infra/terraform" ]; then
    echo "Error: infra/terraform directory not found"
    exit 1
fi

print_status "Sourcing Terraform outputs..."

# Change to terraform directory
cd infra/terraform

# Initialize terraform if needed
if [ ! -d ".terraform" ]; then
    print_status "Initializing Terraform..."
    terraform init
fi

# Get outputs and export as environment variables
export EMR_APP_ID=$(terraform output -raw emr_application_id 2>/dev/null || echo "")
export EMR_JOB_ROLE_ARN=$(terraform output -raw iam_roles 2>/dev/null | jq -r '.emr_job_role' 2>/dev/null || echo "")
export GLUE_DB_SILVER=$(terraform output -raw glue_databases 2>/dev/null | jq -r '.silver' 2>/dev/null || echo "silver_db")
export GLUE_DB_GOLD=$(terraform output -raw glue_databases 2>/dev/null | jq -r '.gold' 2>/dev/null || echo "gold_db")
export S3_LAKE_BUCKET=$(terraform output -raw data_lake_bucket 2>/dev/null || echo "")
export S3_CHECKPOINT_PREFIX="checkpoints"

# Export additional useful variables
export BRONZE_PATH=$(terraform output -raw runtime_config 2>/dev/null | jq -r '.bronze_path' 2>/dev/null || echo "")
export SILVER_PATH=$(terraform output -raw runtime_config 2>/dev/null | jq -r '.silver_path' 2>/dev/null || echo "")
export GOLD_PATH=$(terraform output -raw runtime_config 2>/dev/null | jq -r '.gold_path' 2>/dev/null || echo "")
export MWAA_ENVIRONMENT_NAME=$(terraform output -raw mwaa_environment_name 2>/dev/null || echo "")

# Return to original directory
cd - > /dev/null

# Validate critical variables
if [ -z "$EMR_APP_ID" ]; then
    echo "Warning: EMR_APP_ID not found in Terraform outputs"
fi

if [ -z "$S3_LAKE_BUCKET" ]; then
    echo "Warning: S3_LAKE_BUCKET not found in Terraform outputs"
fi

print_success "Terraform outputs sourced successfully"
print_status "Key variables:"
echo "  EMR_APP_ID: $EMR_APP_ID"
echo "  S3_LAKE_BUCKET: $S3_LAKE_BUCKET"
echo "  GLUE_DB_SILVER: $GLUE_DB_SILVER"
echo "  GLUE_DB_GOLD: $GLUE_DB_GOLD"
