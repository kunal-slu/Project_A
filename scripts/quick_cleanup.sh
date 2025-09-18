#!/bin/bash

# Quick Cleanup Script for Week-Old Output Data
# This script provides simple commands to delete old output data

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
DAYS_OLD=${1:-7}  # Default to 7 days, can be overridden
CURRENT_DATE=$(date +"%Y-%m-%d")
CUTOFF_DATE=$(date -v-${DAYS_OLD}d +"%Y-%m-%d" 2>/dev/null || date -d "$DAYS_OLD days ago" +"%Y-%m-%d" 2>/dev/null || echo "unknown")

echo -e "${BLUE}=== Quick Cleanup Script ===${NC}"
echo -e "${YELLOW}Deleting files older than $DAYS_OLD days (before $CUTOFF_DATE)${NC}"
echo ""

# Function to show directory size
show_directory_size() {
    local dir=$1
    if [ -d "$dir" ]; then
        local size=$(du -sh "$dir" 2>/dev/null | cut -f1)
        local files=$(find "$dir" -type f 2>/dev/null | wc -l)
        echo -e "${GREEN}📁 $dir: $files files, $size${NC}"
    else
        echo -e "${YELLOW}📁 $dir: Does not exist${NC}"
    fi
}

# Function to cleanup directory
cleanup_directory() {
    local dir=$1
    local days=$2
    
    if [ ! -d "$dir" ]; then
        echo -e "${YELLOW}Directory $dir does not exist, skipping...${NC}"
        return
    fi
    
    echo -e "${BLUE}Cleaning up $dir...${NC}"
    
    # Find and delete old files
    local deleted_count=0
    local deleted_size=0
    
            while IFS= read -r -d '' file; do
            if [ -f "$file" ]; then
                local file_age=$(stat -f "%Sm" -t "%Y-%m-%d" "$file" 2>/dev/null || echo "unknown")
                local file_size=$(stat -f%z "$file" 2>/dev/null || echo "0")
            
            echo -e "${RED}🗑️  Deleting: $file (age: $file_age, size: $file_size bytes)${NC}"
            rm -f "$file"
            deleted_count=$((deleted_count + 1))
            deleted_size=$((deleted_size + file_size))
        fi
    done < <(find "$dir" -type f -mtime +$days -print0 2>/dev/null)
    
    # Remove empty directories
    find "$dir" -type d -empty -delete 2>/dev/null
    
    if [ $deleted_count -gt 0 ]; then
        echo -e "${GREEN}✅ Deleted $deleted_count files ($deleted_size bytes) from $dir${NC}"
    else
        echo -e "${YELLOW}ℹ️  No old files found in $dir${NC}"
    fi
    
    echo ""
}

# Show initial statistics
echo -e "${BLUE}📊 Initial Directory Statistics:${NC}"
directories=(
    "data/output_data"
    "data/lakehouse/bronze"
    "data/lakehouse/silver"
    "data/lakehouse/gold"
    "logs"
)

total_initial_files=0
total_initial_size=0

for dir in "${directories[@]}"; do
    show_directory_size "$dir"
    if [ -d "$dir" ]; then
        files=$(find "$dir" -type f 2>/dev/null | wc -l)
        size=$(du -sb "$dir" 2>/dev/null | cut -f1 || echo "0")
        total_initial_files=$((total_initial_files + files))
        total_initial_size=$((total_initial_size + size))
    fi
done

echo -e "${GREEN}📊 Total initial: $total_initial_files files, $total_initial_size bytes${NC}"
echo ""

# Cleanup operations
echo -e "${BLUE}🧹 Starting cleanup operations...${NC}"
echo ""

for dir in "${directories[@]}"; do
    cleanup_directory "$dir" "$DAYS_OLD"
done

# Show final statistics
echo -e "${BLUE}📊 Final Directory Statistics:${NC}"
total_final_files=0
total_final_size=0

for dir in "${directories[@]}"; do
    show_directory_size "$dir"
    if [ -d "$dir" ]; then
        files=$(find "$dir" -type f 2>/dev/null | wc -l)
        size=$(du -sb "$dir" 2>/dev/null | cut -f1 || echo "0")
        total_final_files=$((total_final_files + files))
        total_final_size=$((total_final_size + size))
    fi
done

echo -e "${GREEN}📊 Total final: $total_final_files files, $total_final_size bytes${NC}"
echo ""

# Summary
space_saved=$((total_initial_size - total_final_size))
files_deleted=$((total_initial_files - total_final_files))

echo -e "${BLUE}📈 Cleanup Summary:${NC}"
echo -e "${GREEN}🗑️  Files deleted: $files_deleted${NC}"
echo -e "${GREEN}💾 Space saved: $space_saved bytes${NC}"
echo -e "${GREEN}📅 Cutoff date: $CUTOFF_DATE${NC}"
echo ""

echo -e "${GREEN}✅ Week-old data cleanup completed successfully!${NC}"

# Optional: Run Python script for Delta table cleanup
echo ""
echo -e "${YELLOW}💡 To also clean up Delta table data, run:${NC}"
echo -e "${BLUE}source .venv/bin/activate && python scripts/delete_week_old_data.py${NC}"
