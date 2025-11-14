#!/bin/bash
# Quick Phase 1 Verification Script
# Run this to verify all Terraform resources are deployed correctly

PROFILE="${AWS_PROFILE:-kunal21}"
REGION="${AWS_REGION:-us-east-1}"

echo "🔍 Phase 1 Verification Checklist"
echo "=================================="
echo "Profile: $PROFILE"
echo "Region: $REGION"
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check command success
check_status() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ PASS${NC}"
    else
        echo -e "${RED}❌ FAIL${NC}"
    fi
}

# 1. AWS Identity
echo "1. AWS Identity:"
aws sts get-caller-identity --profile $PROFILE --region $REGION 2>&1
check_status
echo ""

# 2. S3 Buckets
echo "2. S3 Buckets:"
BUCKETS=$(aws s3 ls --profile $PROFILE --region $REGION 2>&1 | grep "my-etl-" || echo "")
if [ -z "$BUCKETS" ]; then
    echo -e "${RED}❌ No buckets found${NC}"
else
    echo "$BUCKETS" | while IFS= read -r line; do
        if [ ! -z "$line" ]; then
            echo "  - $(echo "$line" | awk '{print $3}')"
        fi
    done
    echo -e "${GREEN}✅ Found buckets${NC}"
fi
echo ""

# 3. EMR Serverless
echo "3. EMR Serverless Applications:"
EMR_APPS=$(aws emr-serverless list-applications --profile $PROFILE --region $REGION 2>&1)
if echo "$EMR_APPS" | grep -q "project-a-dev-spark" || echo "$EMR_APPS" | grep -q "applications"; then
    if command -v jq &> /dev/null; then
        echo "$EMR_APPS" | jq -r '.applications[]? | "  - \(.name): \(.state)"' 2>/dev/null || echo "$EMR_APPS"
    else
        echo "$EMR_APPS" | grep -E "(name|state|id)" || echo "$EMR_APPS"
    fi
    echo -e "${GREEN}✅ EMR app check completed${NC}"
else
    echo -e "${RED}❌ EMR app not found or error occurred${NC}"
    echo "$EMR_APPS"
fi
echo ""

# 4. Glue Databases
echo "4. Glue Databases:"
GLUE_DBS=$(aws glue get-databases --profile $PROFILE --region $REGION 2>&1)
if echo "$GLUE_DBS" | grep -q "project-a" || echo "$GLUE_DBS" | grep -q "DatabaseList"; then
    if command -v jq &> /dev/null; then
        echo "$GLUE_DBS" | jq -r '.DatabaseList[]? | select(.Name | contains("project-a")) | "  - \(.Name)"' 2>/dev/null || echo "$GLUE_DBS"
    else
        echo "$GLUE_DBS" | grep -E "(project-a|Name)" || echo "$GLUE_DBS"
    fi
    echo -e "${GREEN}✅ Glue databases check completed${NC}"
else
    echo -e "${RED}❌ Glue databases not found or error occurred${NC}"
    echo "$GLUE_DBS"
fi
echo ""

# 5. IAM Roles
echo "5. IAM Roles:"
IAM_ROLES=$(aws iam list-roles --profile $PROFILE 2>&1)
if echo "$IAM_ROLES" | grep -q "project-a-dev" || echo "$IAM_ROLES" | grep -q "Roles"; then
    if command -v jq &> /dev/null; then
        echo "$IAM_ROLES" | jq -r '.Roles[]? | select(.RoleName | contains("project-a-dev")) | "  - \(.RoleName)"' 2>/dev/null || echo "$IAM_ROLES"
    else
        echo "$IAM_ROLES" | grep "project-a-dev" || echo "$IAM_ROLES"
    fi
    echo -e "${GREEN}✅ IAM roles check completed${NC}"
else
    echo -e "${RED}❌ IAM roles not found or error occurred${NC}"
    echo "$IAM_ROLES"
fi
echo ""

# 6. CloudWatch Log Groups
echo "6. CloudWatch Log Groups:"
LOG_GROUPS=$(aws logs describe-log-groups --log-group-name-prefix /aws/emr-serverless/ --profile $PROFILE --region $REGION 2>&1)
if echo "$LOG_GROUPS" | grep -q "project-a-dev" || echo "$LOG_GROUPS" | grep -q "logGroups"; then
    if command -v jq &> /dev/null; then
        echo "$LOG_GROUPS" | jq -r '.logGroups[]? | "  - \(.logGroupName)"' 2>/dev/null || echo "$LOG_GROUPS"
    else
        echo "$LOG_GROUPS" | grep "logGroupName" || echo "$LOG_GROUPS"
    fi
    echo -e "${GREEN}✅ Log groups found${NC}"
else
    echo -e "${YELLOW}⚠️  Log groups may not exist until first EMR job runs${NC}"
    echo "$LOG_GROUPS" | head -5
fi
echo ""

# 7. SNS Topics
echo "7. SNS Topics:"
SNS_TOPICS=$(aws sns list-topics --profile $PROFILE --region $REGION 2>&1)
if echo "$SNS_TOPICS" | grep -q "project-a-dev" || echo "$SNS_TOPICS" | grep -q "Topics"; then
    if command -v jq &> /dev/null; then
        echo "$SNS_TOPICS" | jq -r '.Topics[]? | select(.TopicArn | contains("project-a-dev")) | "  - \(.TopicArn)"' 2>/dev/null || echo "$SNS_TOPICS"
    else
        echo "$SNS_TOPICS" | grep "project-a-dev" || echo "$SNS_TOPICS"
    fi
    echo -e "${GREEN}✅ SNS topics found${NC}"
else
    echo -e "${YELLOW}⚠️  SNS topics not found${NC}"
    echo "$SNS_TOPICS" | head -3
fi
echo ""

# 8. KMS Keys
echo "8. KMS Keys:"
KMS_KEYS=$(aws kms list-aliases --profile $PROFILE --region $REGION 2>&1)
if echo "$KMS_KEYS" | grep -q "project-a-dev-cmk" || echo "$KMS_KEYS" | grep -q "Aliases"; then
    if command -v jq &> /dev/null; then
        echo "$KMS_KEYS" | jq -r '.Aliases[]? | select(.AliasName | contains("project-a-dev")) | "  - \(.AliasName)"' 2>/dev/null || echo "$KMS_KEYS"
    else
        echo "$KMS_KEYS" | grep "project-a-dev" || echo "$KMS_KEYS"
    fi
    echo -e "${GREEN}✅ KMS keys found${NC}"
else
    echo -e "${RED}❌ KMS keys not found${NC}"
    echo "$KMS_KEYS" | head -3
fi
echo ""

# 9. Terraform Outputs
echo "9. Terraform Outputs:"
if [ -f "aws/terraform/terraform-outputs.dev.json" ]; then
    echo -e "${GREEN}✅ terraform-outputs.dev.json exists${NC}"
    if command -v jq &> /dev/null; then
        cat aws/terraform/terraform-outputs.dev.json | jq '.' 2>/dev/null || cat aws/terraform/terraform-outputs.dev.json
    else
        cat aws/terraform/terraform-outputs.dev.json
    fi
else
    echo -e "${YELLOW}⚠️  terraform-outputs.dev.json not found${NC}"
    echo "   Run: cd aws/terraform && terraform output -json > terraform-outputs.dev.json"
fi
echo ""

echo "=================================="
echo "✅ Verification complete!"
echo ""
echo "📋 Next Steps:"
echo "1. Review any ❌ FAIL items above"
echo "2. Check AWS Console for visual verification"
echo "3. Share results if you need help troubleshooting"

