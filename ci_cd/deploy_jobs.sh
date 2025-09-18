#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_URL=${WORKSPACE_URL:-""}
TOKEN=${TOKEN:-""}
JOB_JSON=${JOB_JSON:-"jobs/databricks_job.json"}

if [[ -z "$WORKSPACE_URL" || -z "$TOKEN" ]]; then
  echo "Set WORKSPACE_URL and TOKEN env vars" >&2
  exit 1
fi

echo "[deploy_jobs] Deploying job spec: $JOB_JSON"
curl -sS -X POST "$WORKSPACE_URL/api/2.1/jobs/create" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d @"$JOB_JSON" | cat


