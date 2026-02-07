#!/bin/bash
# Backfill Bronze layer for a specific date range.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

usage() {
    echo "Usage: $0 --start-date YYYY-MM-DD [--end-date YYYY-MM-DD] [--source SOURCE] [--config CONFIG]"
    echo ""
    echo "Backfill Bronze layer for a date range."
    echo ""
    echo "Options:"
    echo "  --start-date    Start date (YYYY-MM-DD)"
    echo "  --end-date      End date (YYYY-MM-DD, defaults to start-date)"
    echo "  --source        Data source (snowflake, redshift, crm, all)"
    echo "  --config        Config file path (default: config/local.yaml)"
    echo "  --dry-run       Dry run mode (don't execute)"
    exit 1
}

START_DATE=""
END_DATE=""
SOURCE="all"
CONFIG="config/local.yaml"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --start-date)
            START_DATE="$2"
            shift 2
            ;;
        --end-date)
            END_DATE="$2"
            shift 2
            ;;
        --source)
            SOURCE="$2"
            shift 2
            ;;
        --config)
            CONFIG="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        *)
            usage
            ;;
    esac
done

if [ -z "$START_DATE" ]; then
    echo "Error: --start-date is required"
    usage
fi

END_DATE="${END_DATE:-$START_DATE}"

echo "========================================="
echo "Bronze Backfill"
echo "========================================="
echo "Start Date: $START_DATE"
echo "End Date:   $END_DATE"
echo "Source:     $SOURCE"
echo "Config:     $CONFIG"
echo "Dry Run:    $DRY_RUN"
echo "========================================="

if [ "$DRY_RUN" = true ]; then
    echo "DRY RUN MODE - No changes will be made"
    exit 0
fi

# Python script to handle backfill
cd "$PROJECT_ROOT"

python3 << PYEOF
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.extract.snowflake_orders import extract_snowflake_orders
from pyspark_interview_project.extract.redshift_behavior import extract_redshift_behavior

config = load_conf("$CONFIG")
spark = build_spark(app_name="backfill_bronze", config=config)

start_date = datetime.strptime("$START_DATE", "%Y-%m-%d")
end_date = datetime.strptime("$END_DATE", "%Y-%m-%d")
source = "$SOURCE"

current_date = start_date
while current_date <= end_date:
    date_str = current_date.strftime("%Y-%m-%d")
    print(f"Processing date: {date_str}")
    
    # Set watermark to date
    since_ts = datetime.combine(current_date, datetime.min.time())
    
    if source in ["all", "snowflake"]:
        print(f"  Extracting Snowflake orders for {date_str}...")
        df = extract_snowflake_orders(spark, config, since_ts=since_ts)
        print(f"  ✅ Extracted {df.count():,} records")
    
    if source in ["all", "redshift"]:
        print(f"  Extracting Redshift behavior for {date_str}...")
        df = extract_redshift_behavior(spark, config, since_ts=since_ts)
        print(f"  ✅ Extracted {df.count():,} records")
    
    current_date += timedelta(days=1)

spark.stop()
print("✅ Backfill completed")
PYEOF

echo "✅ Backfill script completed"

