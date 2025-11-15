#!/bin/bash
# Phase 6 Setup Script - Observe & Govern
# Sets up CloudWatch, SNS, Lineage, and Lake Formation

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=========================================="
echo "Phase 6: Observe & Govern Setup"
echo "=========================================="

# Check AWS credentials
if ! aws sts get-caller-identity --profile kunal21 --region us-east-1 > /dev/null 2>&1; then
    echo "❌ AWS credentials not configured"
    echo "   Run: aws configure --profile kunal21"
    exit 1
fi

export AWS_PROFILE=kunal21
export AWS_REGION=us-east-1
export ARTIFACTS_BUCKET=my-etl-artifacts-demo-424570854632

# 1. Update Terraform variables
echo ""
echo "1️⃣ Updating Terraform variables..."
cd aws/terraform

# Check if alarm_email is set
if ! grep -q "alarm_email" env/dev.tfvars 2>/dev/null; then
    echo "alarm_email = \"kunal.ks5064@gmail.com\"" >> env/dev.tfvars
    echo "✅ Added alarm_email to dev.tfvars"
fi

if ! grep -q "enable_cloudwatch_dashboards" env/dev.tfvars 2>/dev/null; then
    echo "enable_cloudwatch_dashboards = true" >> env/dev.tfvars
    echo "✅ Added enable_cloudwatch_dashboards"
fi

if ! grep -q "enable_emr_alarms" env/dev.tfvars 2>/dev/null; then
    echo "enable_emr_alarms = true" >> env/dev.tfvars
    echo "✅ Added enable_emr_alarms"
fi

if ! grep -q "enable_lake_formation" env/dev.tfvars 2>/dev/null; then
    echo "enable_lake_formation = true" >> env/dev.tfvars
    echo "✅ Added enable_lake_formation"
fi

# 2. Upload lineage config to S3
echo ""
echo "2️⃣ Uploading lineage config to S3..."
cd "$PROJECT_ROOT"

if [ -f "config/lineage.yaml" ]; then
    aws s3 cp config/lineage.yaml \
        "s3://${ARTIFACTS_BUCKET}/config/lineage.yaml" \
        --profile "$AWS_PROFILE" \
        --region "$AWS_REGION"
    echo "✅ Lineage config uploaded"
else
    echo "⚠️  config/lineage.yaml not found"
fi

# 3. Apply Terraform (optional - user can do this manually)
echo ""
echo "3️⃣ Terraform changes ready"
echo "   To apply:"
echo "   cd aws/terraform"
echo "   terraform plan -var-file=env/dev.tfvars"
echo "   terraform apply -var-file=env/dev.tfvars"

# 4. Verify SNS topic (after Terraform apply)
echo ""
echo "4️⃣ After Terraform apply, verify SNS topic:"
echo "   aws sns list-topics --profile kunal21 --region us-east-1"
echo ""
echo "   Look for: arn:aws:sns:us-east-1:424570854632:project-a-dev-alerts"
echo ""
echo "   Check email subscription:"
echo "   - Check your inbox (kunal.ks5064@gmail.com)"
echo "   - Look for 'AWS Notification – Subscription Confirmation'"
echo "   - Click 'Confirm subscription'"

# 5. Verify CloudWatch dashboard
echo ""
echo "5️⃣ After Terraform apply, verify CloudWatch dashboard:"
echo "   - Go to CloudWatch Console → Dashboards"
echo "   - Look for: project-a-dev-emr-monitoring"

# 6. Verify Lake Formation
echo ""
echo "6️⃣ After Terraform apply, verify Lake Formation:"
echo "   - Go to Lake Formation Console"
echo "   - Check Data lake locations are registered"
echo "   - Verify admins are configured"

echo ""
echo "=========================================="
echo "✅ Phase 6 setup complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Apply Terraform changes"
echo "2. Confirm SNS email subscription"
echo "3. Test metrics emission by running a job"
echo "4. Verify lineage events (if Marquez/OpenLineage is running)"

