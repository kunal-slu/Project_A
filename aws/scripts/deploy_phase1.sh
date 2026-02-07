#!/bin/bash
# Phase 1: Terraform Infrastructure Deployment Script
# This script guides you through deploying all AWS infrastructure

set -e

PROFILE="${AWS_PROFILE:-kunal21}"
REGION="${AWS_REGION:-us-east-1}"
TERRAFORM_DIR="aws/terraform"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}🏗️  Phase 1: Terraform Infrastructure Deployment${NC}"
echo "=========================================="
echo ""

# Step 1: Navigate to Terraform directory
echo -e "${YELLOW}Step 1: Checking Terraform directory...${NC}"
cd "$(dirname "$0")/../.."
cd "$TERRAFORM_DIR"
echo "✅ Current directory: $(pwd)"
echo ""

# Step 2: Verify dev.tfvars exists
echo -e "${YELLOW}Step 2: Checking env/dev.tfvars...${NC}"
if [ ! -f "env/dev.tfvars" ]; then
    echo -e "${RED}❌ env/dev.tfvars not found${NC}"
    echo "Creating it now..."
    mkdir -p env
    cat > env/dev.tfvars <<EOF
project_name = "project-a"
environment  = "dev"
aws_region   = "us-east-1"
aws_profile  = "kunal21"
account_id   = "424570854632"

buckets = {
  lake      = "my-etl-lake-demo-424570854632"
  code      = "my-etl-code-demo-424570854632"
  logs      = "my-etl-logs-demo-424570854632"
  artifacts = "my-etl-artifacts-demo-424570854632"
}

alarm_email = "kunal.ks5064@gmail.com"
EOF
    echo -e "${GREEN}✅ Created env/dev.tfvars${NC}"
else
    echo -e "${GREEN}✅ env/dev.tfvars exists${NC}"
fi
echo ""

# Step 3: Check backend configuration
echo -e "${YELLOW}Step 3: Checking Terraform backend...${NC}"
if grep -q 'backend "s3"' main.tf 2>/dev/null; then
    echo -e "${YELLOW}⚠️  Backend S3 configured. Make sure state bucket exists.${NC}"
    echo "   If not, you can comment out the backend block for local state."
else
    echo -e "${GREEN}✅ Using local state (terraform.tfstate)${NC}"
fi
echo ""

# Step 4: Initialize Terraform
echo -e "${YELLOW}Step 4: Initializing Terraform...${NC}"
if [ ! -d ".terraform" ]; then
    echo "Running: terraform init"
    terraform init
    echo -e "${GREEN}✅ Terraform initialized${NC}"
else
    echo -e "${GREEN}✅ Terraform already initialized${NC}"
fi
echo ""

# Step 5: Validate configuration
echo -e "${YELLOW}Step 5: Validating Terraform configuration...${NC}"
terraform validate
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Configuration is valid${NC}"
else
    echo -e "${RED}❌ Validation failed. Please fix errors above.${NC}"
    exit 1
fi
echo ""

# Step 6: Format Terraform files
echo -e "${YELLOW}Step 6: Formatting Terraform files...${NC}"
terraform fmt
echo -e "${GREEN}✅ Files formatted${NC}"
echo ""

# Step 7: Plan
echo -e "${YELLOW}Step 7: Running Terraform plan...${NC}"
echo "This shows what will be created/modified (no changes made yet)"
echo ""
read -p "Press Enter to continue with plan, or Ctrl+C to cancel..."
terraform plan -var-file=env/dev.tfvars
echo ""

# Step 8: Apply
echo -e "${YELLOW}Step 8: Applying Terraform configuration...${NC}"
echo "This will CREATE/UPDATE AWS resources!"
echo ""
read -p "Type 'yes' to proceed with terraform apply: " confirm
if [ "$confirm" != "yes" ]; then
    echo -e "${YELLOW}⚠️  Apply cancelled${NC}"
    exit 0
fi

terraform apply -var-file=env/dev.tfvars -auto-approve
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Terraform apply completed successfully!${NC}"
else
    echo -e "${RED}❌ Terraform apply failed. Check errors above.${NC}"
    exit 1
fi
echo ""

# Step 9: Export outputs
echo -e "${YELLOW}Step 9: Exporting Terraform outputs...${NC}"
terraform output -json > terraform-outputs.dev.json
if [ -f "terraform-outputs.dev.json" ]; then
    echo -e "${GREEN}✅ Outputs saved to terraform-outputs.dev.json${NC}"
    echo ""
    echo "Outputs:"
    if command -v jq &> /dev/null; then
        cat terraform-outputs.dev.json | jq '.'
    else
        cat terraform-outputs.dev.json
    fi
else
    echo -e "${RED}❌ Failed to create outputs file${NC}"
fi
echo ""

# Step 10: Verification
echo -e "${YELLOW}Step 10: Running verification checks...${NC}"
echo ""
cd ../..
if [ -f "aws/scripts/verify_phase1.sh" ]; then
    echo "Running verification script..."
    ./aws/scripts/verify_phase1.sh
else
    echo -e "${YELLOW}⚠️  Verification script not found. Running manual checks...${NC}"
    echo ""
    echo "S3 Buckets:"
    aws s3 ls --profile $PROFILE --region $REGION | grep "my-etl-" || echo "No buckets found"
    echo ""
    echo "EMR Serverless:"
    aws emr-serverless list-applications --profile $PROFILE --region $REGION --query 'applications[*].[name,state]' --output table || echo "Error checking EMR"
    echo ""
    echo "Glue Databases:"
    aws glue get-databases --profile $PROFILE --region $REGION --query 'DatabaseList[*].Name' --output table || echo "Error checking Glue"
fi

echo ""
echo "=========================================="
echo -e "${GREEN}✅ Phase 1 Complete!${NC}"
echo ""
echo "📋 Next Steps:"
echo "1. Review terraform-outputs.dev.json"
echo "2. Update config/dev.yaml with values from outputs"
echo "3. Proceed to Phase 2: Create Secrets Manager entries"
echo ""
echo "📚 Files created:"
echo "  - aws/terraform/terraform-outputs.dev.json"
echo "  - aws/terraform/terraform.tfstate (or in S3 backend)"

