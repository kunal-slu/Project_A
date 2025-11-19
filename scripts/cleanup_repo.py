#!/usr/bin/env python3
"""
Comprehensive repository cleanup script.

This script:
1. Creates legacy/ folders
2. Moves unused files to legacy/
3. Removes junk files (.DS_Store, __pycache__, etc.)
4. Updates .gitignore
"""

import os
import shutil
from pathlib import Path
from typing import List, Tuple

# Files/directories to definitely keep (core pipeline)
CORE_FILES = {
    # Main ETL jobs
    "jobs/transform/bronze_to_silver.py",
    "jobs/transform/silver_to_gold.py",
    
    # Core library modules
    "src/project_a/pyspark_interview_project/transform/bronze_loaders.py",
    "src/project_a/pyspark_interview_project/transform/silver_builders.py",
    "src/project_a/pyspark_interview_project/transform/gold_builders.py",
    "src/project_a/pyspark_interview_project/io/delta_writer.py",
    "src/project_a/utils/spark_session.py",
    "src/project_a/utils/config_loader.py",
    "src/project_a/utils/path_resolver.py",
    
    # Main DAG (keep one)
    "aws/dags/daily_batch_pipeline_dag.py",
}

# Files to move to legacy (based on analysis)
LEGACY_CANDIDATES = [
    # Old DAGs (keep only daily_batch_pipeline_dag.py)
    "aws/dags/daily_pipeline_dag_complete.py",
    "aws/dags/dq_watchdog_dag.py",
    "aws/dags/maintenance_dag.py",
    "aws/dags/project_a_daily_pipeline.py",
    "aws/dags/salesforce_ingestion_dag.py",
    
    # Old job files (keep only the refactored ones)
    "jobs/transform/bronze_to_silver_old.py",
    "jobs/transform/silver_to_gold_old.py",
]

def create_legacy_folders():
    """Create legacy folder structure."""
    legacy_dirs = [
        "src/project_a/legacy",
        "aws/jobs_legacy",
        "aws/scripts_legacy",
        "aws/dags_legacy",
        "jobs_legacy",
        "legacy",
    ]
    
    for dir_path in legacy_dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        print(f"✅ Created {dir_path}/")

def move_to_legacy(source: str, dest: str):
    """Move a file or directory to legacy location."""
    source_path = Path(source)
    dest_path = Path(dest)
    
    if not source_path.exists():
        print(f"⚠️  Skipping {source} (does not exist)")
        return False
    
    # Create destination directory
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        if source_path.is_file():
            shutil.move(str(source_path), str(dest_path))
            print(f"  ✅ Moved {source} -> {dest}")
        elif source_path.is_dir():
            shutil.move(str(source_path), str(dest_path))
            print(f"  ✅ Moved directory {source} -> {dest}")
        return True
    except Exception as e:
        print(f"  ❌ Failed to move {source}: {e}")
        return False

def remove_junk_files():
    """Remove junk files (.DS_Store, __pycache__, *.pyc, etc.)."""
    junk_patterns = [
        ".DS_Store",
        "*.pyc",
        "__MACOSX",
        "__pycache__",
    ]
    
    removed_count = 0
    
    # Remove .DS_Store files
    for ds_file in Path('.').rglob('.DS_Store'):
        try:
            ds_file.unlink()
            removed_count += 1
        except Exception:
            pass
    
    # Remove __pycache__ directories
    for pycache_dir in Path('.').rglob('__pycache__'):
        if '.venv' not in str(pycache_dir) and 'venv' not in str(pycache_dir):
            try:
                shutil.rmtree(pycache_dir)
                removed_count += 1
            except Exception:
                pass
    
    # Remove .pyc files
    for pyc_file in Path('.').rglob('*.pyc'):
        if '.venv' not in str(pyc_file) and 'venv' not in str(pyc_file):
            try:
                pyc_file.unlink()
                removed_count += 1
            except Exception:
                pass
    
    print(f"✅ Removed {removed_count} junk files/directories")
    return removed_count

def update_gitignore():
    """Update .gitignore to exclude junk files."""
    gitignore_path = Path('.gitignore')
    
    junk_entries = [
        "# Junk files",
        ".DS_Store",
        "*.pyc",
        "__pycache__/",
        "__MACOSX/",
        "*.pyc",
        "",
        "# Build artifacts",
        "dist/",
        "build/",
        "*.egg-info/",
        "",
        "# Runtime logs",
        "logs/",
        "*.log",
    ]
    
    if gitignore_path.exists():
        content = gitignore_path.read_text()
        for entry in junk_entries:
            if entry and entry not in content:
                content += f"\n{entry}"
        gitignore_path.write_text(content)
        print("✅ Updated .gitignore")
    else:
        gitignore_path.write_text('\n'.join(junk_entries))
        print("✅ Created .gitignore")

def main():
    """Main cleanup function."""
    print("🧹 Starting repository cleanup...")
    print("=" * 80)
    
    # 1. Create legacy folders
    print("\n📁 Creating legacy folders...")
    create_legacy_folders()
    
    # 2. Move unused files to legacy
    print("\n📦 Moving unused files to legacy...")
    moved_count = 0
    for source in LEGACY_CANDIDATES:
        if Path(source).exists():
            # Determine legacy location
            if 'aws/dags' in source:
                dest = source.replace('aws/dags', 'aws/dags_legacy')
            elif 'jobs/transform' in source and '_old.py' in source:
                dest = source.replace('jobs/transform', 'jobs_legacy/transform')
            else:
                dest = f"legacy/{source}"
            
            if move_to_legacy(source, dest):
                moved_count += 1
    
    print(f"\n✅ Moved {moved_count} files to legacy/")
    
    # 3. Remove junk files
    print("\n🗑️  Removing junk files...")
    remove_junk_files()
    
    # 4. Update .gitignore
    print("\n📝 Updating .gitignore...")
    update_gitignore()
    
    print("\n" + "=" * 80)
    print("✅ Cleanup complete!")
    print("=" * 80)

if __name__ == '__main__':
    main()

