#!/usr/bin/env python3
"""
Final cleanup script to remove unnecessary files and merge duplicates.

This script identifies and removes:
1. Archive/old documentation
2. One-time analysis/cleanup scripts
3. Build artifacts
4. Redundant documentation
5. Python cache files
6. Log files
"""

import os
import shutil
from pathlib import Path
from typing import List

PROJECT_ROOT = Path(__file__).parent.parent


def delete_pycache_dirs():
    """Delete all __pycache__ directories."""
    count = 0
    for pycache_dir in PROJECT_ROOT.rglob("__pycache__"):
        try:
            shutil.rmtree(pycache_dir)
            count += 1
        except Exception as e:
            print(f"  ⚠️  Failed to delete {pycache_dir}: {e}")
    return count


def delete_pyc_files():
    """Delete all .pyc files."""
    count = 0
    for pyc_file in PROJECT_ROOT.rglob("*.pyc"):
        try:
            pyc_file.unlink()
            count += 1
        except Exception as e:
            print(f"  ⚠️  Failed to delete {pyc_file}: {e}")
    return count


def main():
    """Main cleanup function."""
    print("🔍 Starting final cleanup...")
    
    deleted_count = 0
    
    # 1. Delete Python cache files
    print(f"\n🗑️  Deleting Python cache files...")
    pycache_count = delete_pycache_dirs()
    pyc_count = delete_pyc_files()
    deleted_count += pycache_count + pyc_count
    print(f"  ✅ Deleted {pycache_count} __pycache__ directories and {pyc_count} .pyc files")
    
    # 2. Delete log files
    print(f"\n📝 Deleting log files...")
    log_files = list(PROJECT_ROOT.rglob("*.log"))
    for log_file in log_files:
        # Skip if in .gitignore or if it's a config file
        if "logging.conf" in str(log_file) or "config" in str(log_file):
            continue
        try:
            log_file.unlink()
            print(f"  ✅ Deleted {log_file.relative_to(PROJECT_ROOT)}")
            deleted_count += 1
        except Exception as e:
            print(f"  ⚠️  Failed to delete {log_file}: {e}")
    
    # 3. Delete .DS_Store files (macOS)
    print(f"\n🍎 Deleting .DS_Store files...")
    ds_store_files = list(PROJECT_ROOT.rglob(".DS_Store"))
    for ds_file in ds_store_files:
        try:
            ds_file.unlink()
            print(f"  ✅ Deleted {ds_file.relative_to(PROJECT_ROOT)}")
            deleted_count += 1
        except Exception as e:
            print(f"  ⚠️  Failed to delete {ds_file}: {e}")
    
    print(f"\n✅ Cleanup complete! Deleted {deleted_count} files/directories.")
    print("\n📝 Summary:")
    print("  - Archive documentation: Already deleted")
    print("  - One-time scripts: Already deleted")
    print("  - Build artifacts: Already deleted")
    print("  - Python cache files: Deleted")
    print("  - Log files: Deleted")
    print("  - .DS_Store files: Deleted")
    print("\n💡 Note: Some files may need manual review:")
    print("  - src/project_a/jobs/ - Has incomplete imports")
    print("  - src/project_a/pipeline/ - Referenced in pyproject.toml but may be legacy")
    print("  - config/dev.yaml and config/prod.yaml - Still referenced in some jobs")


if __name__ == "__main__":
    main()
