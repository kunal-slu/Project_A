#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH=${1:-local/config/local.yaml}
ENV_NAME=${2:-local}

python3 jobs/streaming/kafka_orders_stream.py --config "$CONFIG_PATH" --env "$ENV_NAME"
