#!/usr/bin/env bash
set -euo pipefail

LAYER=${1:-all}
CONFIG_PATH=${2:-local/config/local.yaml}
ENV_NAME=${3:-local}
OUTPUT=${4:-artifacts/dq/reports/comprehensive_dq.txt}

python3 jobs/dq/run_comprehensive_dq.py --layer "$LAYER" --env "$ENV_NAME" --config "$CONFIG_PATH" --output "$OUTPUT"
