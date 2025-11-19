#!/bin/bash
# EMR on EC2 Step Submission Script
# Usage: ./run_emr_steps.sh [bronze|silver|both]

set -e

export AWS_PROFILE="${AWS_PROFILE:-kunal21}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export EMR_CLUSTER_ID="${EMR_CLUSTER_ID:-j-3N2JXYADSENNU}"
export ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-my-etl-artifacts-demo-424570854632}"

STEP="${1:-both}"

echo "=== EMR on EC2 Step Submission ==="
echo "Cluster ID: $EMR_CLUSTER_ID"
echo "Profile: $AWS_PROFILE"
echo "Region: $AWS_REGION"
echo ""

if [ "$STEP" = "bronze" ] || [ "$STEP" = "both" ]; then
  echo "📦 Submitting Bronze→Silver step..."
  aws emr add-steps \
    --cluster-id "$EMR_CLUSTER_ID" \
    --steps file://steps_bronze_to_silver.json \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --output json | jq -r '.StepIds[0]' | while read step_id; do
      echo "✅ Bronze→Silver step submitted: $step_id"
    done
  echo ""
fi

if [ "$STEP" = "silver" ] || [ "$STEP" = "both" ]; then
  echo "📦 Submitting Silver→Gold step..."
  aws emr add-steps \
    --cluster-id "$EMR_CLUSTER_ID" \
    --steps file://steps_silver_to_gold.json \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --output json | jq -r '.StepIds[0]' | while read step_id; do
      echo "✅ Silver→Gold step submitted: $step_id"
    done
  echo ""
fi

echo "=== Monitoring Steps ==="
echo "To check step status, run:"
echo "  aws emr list-steps --cluster-id $EMR_CLUSTER_ID --region $AWS_REGION --profile $AWS_PROFILE --output table"
echo ""

