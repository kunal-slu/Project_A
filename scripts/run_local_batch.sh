#!/usr/bin/env bash
set -euo pipefail
export PYTHONPATH="$(pwd)/src:${PYTHONPATH:-}"
export ENV=${ENV:-local}
mkdir -p /tmp/lakehouse /tmp/lakehouse/_chk
python -m pyspark_interview_project






