#!/usr/bin/env bash
set -euo pipefail

JOB_NAME=${1:?"Usage: scripts/run_job.sh <job_name> [config] [env]"}
CONFIG_PATH=${2:-local/config/local.yaml}
ENV_NAME=${3:-local}

python3 -m project_a.pipeline.run_pipeline --job "$JOB_NAME" --env "$ENV_NAME" --config "$CONFIG_PATH"
