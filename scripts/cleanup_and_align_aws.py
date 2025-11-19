#!/usr/bin/env python3
"""
Cleanup and align AWS code with local.
- Delete empty directories
- Remove duplicate/unused files
- Align AWS configs with local
"""
import os
import shutil
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

# Empty directories to delete
EMPTY_DIRS = [
    "artifacts/spark-66616a6a-9ea4-4d72-bbb5-8ddcefc0b20e",
    "local/jobs/transform",
    "jobs/gold",
    "aws/dags/development/archive",
]

# Duplicate/unused files to delete
DUPLICATE_FILES = [
    # Duplicate job files (use jobs/transform/*.py as canonical)
    "src/jobs/bronze_to_silver.py",
    "src/jobs/silver_to_gold.py",
    "src/project_a/jobs/bronze_to_silver.py",
    "src/project_a/jobs/silver_to_gold.py",
    "src/project_a/pyspark_interview_project/transform/bronze_to_silver.py",
    "src/project_a/pyspark_interview_project/pipeline/bronze_to_silver.py",
    "src/project_a/pyspark_interview_project/transform/silver_to_gold.py",
    "src/project_a/pyspark_interview_project/pipeline/silver_to_gold.py",
]

# Files that don't affect the project
UNUSED_FILES = [
    # Old cleanup scripts (already executed)
    "scripts/cleanup_all_unnecessary_files.py",
    "scripts/comprehensive_cleanup.py",
    "scripts/safe_cleanup.py",
    "scripts/quick_cleanup.sh",
    "scripts/cleanup_unwanted_files.sh",
    "scripts/cleanup_data_files.sh",
    "scripts/cleanup_old_data.py",
    "scripts/cleanup_project.py",
    # Old validation scripts (replaced by working code)
    "scripts/aws_fix_immediate.py",
    "scripts/aws_remediation_comprehensive.py",
    "scripts/aws_validation_comprehensive.py",
    "scripts/validate_aws_code.py",
    "scripts/validate_production_readiness.py",
    "scripts/validate_project_e2e.py",
    "scripts/verify_production_setup.py",
    # Old analysis scripts
    "scripts/analyze_and_improve.py",
    "scripts/review_codebase.py",
    "scripts/comprehensive_project_check.py",
    "scripts/end_to_end_check.py",
    # Old data generation scripts (data is already in place)
    "scripts/generate_input_data.py",
    "scripts/generate_data_catalog.py",
    "scripts/fix_all_source_data.py",
    "scripts/enhance_data_sources.py",
    # Old implementation scripts
    "scripts/implement_improvements.py",
    "scripts/fix_critical_errors.py",
    "scripts/fix_data_quality_issues.py",
    # Old ETL runners (use scripts/run_local_etl_fixed.sh)
    "scripts/run_full_etl.py",
    "scripts/run_enterprise_etl.sh",
    "scripts/run_etl_with_java11.sh",
    # Old maintenance scripts
    "scripts/maintenance.sql",
    "scripts/delta_maintenance.py",
    "scripts/delta_optimize_vacuum.py",
    "scripts/delete_week_old_data.py",
    # Old deployment scripts (use aws/scripts/*)
    "scripts/setup_phase6.sh",
    "scripts/fix_all_etl_code.sh",
    # Old docs
    "CLEANUP_SUMMARY.md",
    "RUN_LOCAL_ETL.md",
]

def delete_empty_dirs():
    """Delete empty directories."""
    deleted = []
    for dir_path in EMPTY_DIRS:
        full_path = PROJECT_ROOT / dir_path
        if full_path.exists() and full_path.is_dir():
            try:
                # Check if truly empty
                if not any(full_path.iterdir()):
                    shutil.rmtree(full_path)
                    deleted.append(dir_path)
                    print(f"✅ Deleted empty directory: {dir_path}")
            except Exception as e:
                print(f"⚠️  Could not delete {dir_path}: {e}")
    return deleted

def delete_duplicate_files():
    """Delete duplicate files."""
    deleted = []
    for file_path in DUPLICATE_FILES:
        full_path = PROJECT_ROOT / file_path
        if full_path.exists() and full_path.is_file():
            try:
                full_path.unlink()
                deleted.append(file_path)
                print(f"✅ Deleted duplicate file: {file_path}")
            except Exception as e:
                print(f"⚠️  Could not delete {file_path}: {e}")
    return deleted

def delete_unused_files():
    """Delete unused files."""
    deleted = []
    for file_path in UNUSED_FILES:
        full_path = PROJECT_ROOT / file_path
        if full_path.exists():
            try:
                if full_path.is_file():
                    full_path.unlink()
                elif full_path.is_dir():
                    shutil.rmtree(full_path)
                deleted.append(file_path)
                print(f"✅ Deleted unused file: {file_path}")
            except Exception as e:
                print(f"⚠️  Could not delete {file_path}: {e}")
    return deleted

def find_all_empty_dirs():
    """Find all empty directories in project."""
    empty_dirs = []
    for root, dirs, files in os.walk(PROJECT_ROOT):
        # Skip .git, .venv, __pycache__, dist, build
        if any(skip in root for skip in ['.git', '.venv', '__pycache__', 'dist', 'build', 'node_modules']):
            continue
        root_path = Path(root)
        if not any(root_path.iterdir()):
            rel_path = root_path.relative_to(PROJECT_ROOT)
            empty_dirs.append(str(rel_path))
    return empty_dirs

if __name__ == "__main__":
    print("🧹 Starting cleanup and alignment...")
    
    # Find all empty directories
    all_empty = find_all_empty_dirs()
    print(f"\n📁 Found {len(all_empty)} empty directories")
    for ed in sorted(all_empty)[:20]:
        print(f"  - {ed}")
    
    # Delete known empty directories
    print("\n🗑️  Deleting known empty directories...")
    deleted_dirs = delete_empty_dirs()
    
    # Delete duplicate files
    print("\n🗑️  Deleting duplicate files...")
    deleted_dupes = delete_duplicate_files()
    
    # Delete unused files
    print("\n🗑️  Deleting unused files...")
    deleted_unused = delete_unused_files()
    
    print(f"\n✅ Cleanup complete:")
    print(f"  - Deleted {len(deleted_dirs)} empty directories")
    print(f"  - Deleted {len(deleted_dupes)} duplicate files")
    print(f"  - Deleted {len(deleted_unused)} unused files")

