#!/usr/bin/env python3
"""
Move unused modules to legacy/ folders.

This script identifies modules that are not imported anywhere and moves them safely.
"""

import os
import shutil
import re
from pathlib import Path
from typing import Set, Dict, List

# Modules that are DEFINITELY used (core pipeline)
USED_MODULES = {
    # Transform modules (used by core jobs)
    'project_a.pyspark_interview_project.transform.bronze_loaders',
    'project_a.pyspark_interview_project.transform.silver_builders',
    'project_a.pyspark_interview_project.transform.gold_builders',
    'project_a.pyspark_interview_project.transform.base_transformer',
    
    # IO modules
    'project_a.pyspark_interview_project.io.delta_writer',
    
    # Monitoring
    'project_a.pyspark_interview_project.monitoring.lineage_decorator',
    'project_a.pyspark_interview_project.monitoring.metrics_collector',
    
    # Utils
    'project_a.pyspark_interview_project.utils.config_loader',
    
    # Extract
    'project_a.extract.fx_json_reader',
    'project_a.pyspark_interview_project.extract.fx_json_reader',
}

# Files that are DEFINITELY used (entry points, core modules)
USED_FILES = {
    # Core jobs
    'jobs/transform/bronze_to_silver.py',
    'jobs/transform/silver_to_gold.py',
    
    # Core library
    'src/project_a/pyspark_interview_project/transform/bronze_loaders.py',
    'src/project_a/pyspark_interview_project/transform/silver_builders.py',
    'src/project_a/pyspark_interview_project/transform/gold_builders.py',
    'src/project_a/pyspark_interview_project/transform/base_transformer.py',
    'src/project_a/pyspark_interview_project/transform/__init__.py',
    'src/project_a/pyspark_interview_project/io/delta_writer.py',
    'src/project_a/pyspark_interview_project/io/__init__.py',
    'src/project_a/pyspark_interview_project/monitoring/lineage_decorator.py',
    'src/project_a/pyspark_interview_project/monitoring/metrics_collector.py',
    'src/project_a/pyspark_interview_project/monitoring/__init__.py',
    'src/project_a/pyspark_interview_project/utils/config_loader.py',
    'src/project_a/pyspark_interview_project/__init__.py',
    
    # Utils (used by jobs)
    'src/project_a/utils/spark_session.py',
    'src/project_a/utils/path_resolver.py',
    'src/project_a/utils/logging.py',
    'src/project_a/utils/run_audit.py',
}

def find_all_python_files(root: str = ".") -> List[Path]:
    """Find all Python files in pyspark_interview_project."""
    root_path = Path(root)
    pyspark_dir = root_path / "src/project_a/pyspark_interview_project"
    
    if not pyspark_dir.exists():
        return []
    
    python_files = []
    for py_file in pyspark_dir.rglob("*.py"):
        if "__pycache__" not in str(py_file):
            python_files.append(py_file)
    
    return sorted(python_files)

def check_if_imported(file_path: Path, root: str = ".") -> bool:
    """Check if a module is imported anywhere in the codebase."""
    root_path = Path(root)
    
    # Get module path
    try:
        rel_path = file_path.relative_to(root_path / "src")
        module_parts = list(rel_path.parts)
        if module_parts[-1].endswith('.py'):
            module_parts[-1] = module_parts[-1][:-3]
        if module_parts[-1] == '__init__':
            module_parts = module_parts[:-1]
        
        module_path = '.'.join(module_parts)
        module_name = module_parts[-1] if module_parts else None
    except Exception:
        return True  # If we can't determine, assume used
    
    # Search for imports in all Python files
    search_dirs = [
        root_path / "jobs",
        root_path / "aws/dags",
        root_path / "aws/jobs",
        root_path / "aws/scripts",
        root_path / "tests",
        root_path / "src",
    ]
    
    for search_dir in search_dirs:
        if not search_dir.exists():
            continue
        
        for py_file in search_dir.rglob("*.py"):
            if "__pycache__" in str(py_file):
                continue
            
            try:
                content = py_file.read_text(encoding='utf-8')
                
                # Check for module path import
                if module_path in content or (module_name and module_name in content):
                    # More precise check
                    patterns = [
                        f'from {module_path}',
                        f'import {module_path}',
                        f'from .*{module_name}',
                    ]
                    for pattern in patterns:
                        if re.search(pattern, content):
                            return True
            except Exception:
                continue
    
    return False

def move_to_legacy(source: Path, root: str = ".") -> bool:
    """Move a file to legacy location."""
    root_path = Path(root)
    
    try:
        rel_path = source.relative_to(root_path)
        
        # Determine legacy location
        if 'src/project_a/pyspark_interview_project' in str(rel_path):
            legacy_path = root_path / "src/project_a/legacy" / rel_path.relative_to("src/project_a/pyspark_interview_project")
        elif 'aws/jobs' in str(rel_path):
            legacy_path = root_path / "aws/jobs_legacy" / rel_path.relative_to("aws/jobs")
        elif 'aws/scripts' in str(rel_path):
            legacy_path = root_path / "aws/scripts_legacy" / rel_path.relative_to("aws/scripts")
        else:
            legacy_path = root_path / "legacy" / rel_path
        
        # Create parent directory
        legacy_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Move file
        shutil.move(str(source), str(legacy_path))
        print(f"  ✅ Moved {rel_path} -> {legacy_path.relative_to(root_path)}")
        return True
    except Exception as e:
        print(f"  ❌ Failed to move {source}: {e}")
        return False

def main():
    """Main function."""
    print("🔍 Analyzing unused modules in pyspark_interview_project...")
    print("=" * 80)
    
    # Find all Python files
    python_files = find_all_python_files()
    print(f"\n📁 Found {len(python_files)} Python files in pyspark_interview_project/")
    
    # Identify unused files
    unused_files = []
    used_files = []
    
    for py_file in python_files:
        rel_path = py_file.relative_to(Path('.'))
        rel_str = str(rel_path)
        
        # Skip if definitely used
        if rel_str in USED_FILES:
            used_files.append(py_file)
            continue
        
        # Check if imported
        if check_if_imported(py_file):
            used_files.append(py_file)
        else:
            unused_files.append(py_file)
    
    print(f"\n✅ Used files: {len(used_files)}")
    print(f"❌ Unused files: {len(unused_files)}")
    
    if unused_files:
        print("\n📦 Moving unused files to legacy...")
        moved = 0
        for file in unused_files[:20]:  # Limit to first 20 for safety
            if move_to_legacy(file):
                moved += 1
        print(f"\n✅ Moved {moved} files to legacy/")
    else:
        print("\n✅ No unused files found to move")

if __name__ == '__main__':
    main()

