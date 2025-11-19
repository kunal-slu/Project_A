#!/usr/bin/env bash
set -euo pipefail

AWS_PROFILE=kunal21
AWS_REGION=us-east-1
EMR_LOG_BUCKET=aws-logs-424570854632-us-east-1
EMR_CLUSTER_ID=j-3N2JXYADSENNU

echo "=== Listing EMR container logs for cluster: $EMR_CLUSTER_ID ==="
echo ""

# List all container logs, sorted by time (most recent last)
aws s3 ls "s3://${EMR_LOG_BUCKET}/j-${EMR_CLUSTER_ID}/containers/" \
  --recursive \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  | grep "stderr.gz" \
  | sort -k1,1 -k2,2 \
  | tail -40

echo ""
echo "=== Finding most recent stderr.gz ==="
LATEST_STDERR=$(aws s3 ls "s3://${EMR_LOG_BUCKET}/j-${EMR_CLUSTER_ID}/containers/" \
  --recursive \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  2>/dev/null | grep "stderr.gz" | sort -k1,1 -k2,2 | tail -1 | awk '{print $4}')

if [ -z "$LATEST_STDERR" ]; then
  echo "❌ No stderr.gz found"
  exit 1
fi

echo "Found: $LATEST_STDERR"
echo ""

echo "=== Downloading latest stderr ==="
aws s3 cp \
  "s3://${EMR_LOG_BUCKET}/${LATEST_STDERR}" \
  ./silver_to_gold_stderr_latest.gz \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION"

echo ""
echo "=== First 200 lines (looking for Python errors) ==="
gunzip -c silver_to_gold_stderr_latest.gz | sed -n '1,200p'

echo ""
echo "=== Searching for Python errors/tracebacks ==="
gunzip -c silver_to_gold_stderr_latest.gz | grep -A 50 -i -E "(error|exception|traceback|failed|import|keyerror|attributeerror|nameerror|typeerror)" | head -150

echo ""
echo "=== Last 100 lines (end of execution) ==="
gunzip -c silver_to_gold_stderr_latest.gz | tail -100

