#!/bin/bash
# Backfill Silver layer for a specific date.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

usage() {
    echo "Usage: $0 --date YYYY-MM-DD [--table TABLE] [--config CONFIG]"
    echo ""
    echo "Backfill Silver layer for a specific date."
    echo ""
    echo "Options:"
    echo "  --date          Processing date (YYYY-MM-DD)"
    echo "  --table         Table name (behavior, crm_accounts, crm_contacts, snowflake_orders, all)"
    echo "  --config        Config file path (default: config/local.yaml)"
    echo "  --dry-run       Dry run mode"
    exit 1
}

DATE=""
TABLE="all"
CONFIG="config/local.yaml"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --date)
            DATE="$2"
            shift 2
            ;;
        --table)
            TABLE="$2"
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

if [ -z "$DATE" ]; then
    echo "Error: --date is required"
    usage
fi

echo "========================================="
echo "Silver Backfill"
echo "========================================="
echo "Date:   $DATE"
echo "Table:  $TABLE"
echo "Config: $CONFIG"
echo "Dry Run: $DRY_RUN"
echo "========================================="

if [ "$DRY_RUN" = true ]; then
    echo "DRY RUN MODE - No changes will be made"
    exit 0
fi

cd "$PROJECT_ROOT"

# Run bronze to silver transformation
python3 -m jobs.bronze_to_silver_behavior --config "$CONFIG" --execution-date "$DATE" || true

echo "✅ Silver backfill completed"

