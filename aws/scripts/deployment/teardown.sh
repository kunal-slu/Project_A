#!/bin/bash
# AWS Infrastructure Teardown Script
# This script destroys all AWS resources created by terraform

set -e

echo "🗑️  AWS INFRASTRUCTURE TEARDOWN"
echo "================================="

# Check if we're in the right directory
if [ ! -f "aws/terraform/main.tf" ]; then
    echo "❌ Error: Run this script from the project root directory"
    exit 1
fi

# Confirm destruction
echo "⚠️  WARNING: This will destroy ALL AWS resources!"
echo "This includes:"
echo "  • S3 buckets and data"
echo "  • EMR Serverless applications"
echo "  • IAM roles and policies"
echo "  • Glue databases and tables"
echo "  • CloudWatch logs"
echo ""
read -p "Are you sure you want to continue? (type 'yes' to confirm): " confirm

if [ "$confirm" != "yes" ]; then
    echo "❌ Teardown cancelled"
    exit 1
fi

# Change to terraform directory
cd aws/terraform

echo "🔍 Checking terraform state..."
if [ ! -f "terraform.tfstate" ]; then
    echo "❌ No terraform state found. Nothing to destroy."
    exit 1
fi

echo "🗑️  Destroying infrastructure..."
terraform destroy -auto-approve

echo "🧹 Cleaning up local files..."
rm -f terraform.tfstate*
rm -f .terraform.lock.hcl
rm -rf .terraform/

echo "✅ Teardown complete!"
echo "All AWS resources have been destroyed."
