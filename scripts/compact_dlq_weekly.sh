#!/usr/bin/env bash
set -euo pipefail

# Weekly DLQ compaction job
# Merges DLQ records back into main pipeline if business wants eventual completeness

export PYTHONPATH="$(pwd)/src:${PYTHONPATH:-}"
export ENV=${ENV:-local}

echo "Starting DLQ compaction for week $(date +%Y-%W)"

python -c "
from pyspark_interview_project.dlq_compaction import compact_dlq_weekly
compact_dlq_weekly()
"

echo "DLQ compaction completed"






