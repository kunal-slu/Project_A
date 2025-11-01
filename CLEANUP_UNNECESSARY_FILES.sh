#!/bin/bash
# Safe cleanup script for unnecessary files and folders

set -e  # Exit on error

echo "🧹 Starting cleanup of unnecessary files and folders..."
echo ""

# Function to delete item and update counters
DELETED_COUNT=0
DELETED_SIZE=0

delete_item() {
    local item="$1"
    local reason="$2"
    
    if [ -e "$item" ]; then
        if [ -d "$item" ]; then
            size=$(du -sk "$item" 2>/dev/null | cut -f1 || echo 0)
            rm -rf "$item"
            echo "  ✅ Deleted directory: $item (${reason})"
        elif [ -f "$item" ]; then
            size=$(stat -f%z "$item" 2>/dev/null || echo 0)
            rm -f "$item"
            echo "  ✅ Deleted file: $item (${reason})"
        else
            size=0
        fi
        
        DELETED_SIZE=$((DELETED_SIZE + size))
        DELETED_COUNT=$((DELETED_COUNT + 1))
    fi
}

# Delete Python cache files
echo "1. Deleting Python cache files..."
while IFS= read -r dir; do
    delete_item "$dir" "Python cache"
done < <(find . -type d -name "__pycache__" -not -path "./.venv/*")

while IFS= read -r file; do
    delete_item "$file" "Python cache"
done < <(find . -type f -name "*.pyc" -not -path "./.venv/*")

while IFS= read -r file; do
    delete_item "$file" "Python backup"
done < <(find . -type f -name "*.py~")

# Delete OS-specific files
echo ""
echo "2. Deleting OS-specific files..."
while IFS= read -r file; do
    delete_item "$file" "macOS system file"
done < <(find . -type f -name ".DS_Store")

# Delete unnecessary directories (build artifacts)
echo ""
echo "3. Deleting unnecessary directories..."
delete_item "_local_pipeline_output" "Local test output"
delete_item "_salesforce_pipeline_output" "Test output"
delete_item "_state" "State directory"
delete_item "artifacts" "Build artifacts"
delete_item "databricks" "Databricks-specific files"
delete_item "azure" "Azure-specific files"
delete_item "dq" "Old DQ directory"
delete_item "env" "Old env directory"
delete_item "ge" "Old Great Expectations directory"
delete_item "htmlcov" "Coverage report HTML"
delete_item "k8s" "Kubernetes configs (not used)"
delete_item "venv" "Old virtualenv"
delete_item "venv_old" "Old virtualenv"
delete_item "ci_cd" "CI/CD files (not used)"
delete_item "infra" "Infrastructure directory (terraform in aws/)"

# Delete log files
echo ""
echo "4. Deleting log files..."
delete_item "etl_run.log" "ETL run log"
while IFS= read -r file; do
    delete_item "$file" "Log file"
done < <(find . -maxdepth 3 -type f -name "*.log" ! -path "./aws/*" ! -path "./logs/*" 2>/dev/null)

# Delete temporary files
echo ""
echo "5. Deleting temporary files..."
while IFS= read -r file; do
    delete_item "$file" "Temporary file"
done < <(find . -maxdepth 3 -type f \( -name "*.tmp" -o -name "*.temp" \) 2>/dev/null)

# Delete old test output directories
echo ""
echo "6. Deleting old test outputs..."
for dir in output_data test_outputs test_results; do
    delete_item "$dir" "Test output"
done

# Summary
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Cleanup completed!"
echo "   • Items deleted: $DELETED_COUNT"
echo "   • Space freed: ~$((DELETED_SIZE / 1024))MB"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
