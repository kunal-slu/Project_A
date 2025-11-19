#!/usr/bin/env python3
"""
Comprehensive Repository Structure Analysis

Categorizes all files in Project_A to determine what to keep, archive, or delete.
"""

import os
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set

PROJECT_ROOT = Path(__file__).parent.parent

# Categories
CORE_PIPELINE = "CORE_PIPELINE"
SUPPORTING = "SUPPORTING"
EXPERIMENT = "EXPERIMENT"
LEGACY = "LEGACY"
UNUSED = "UNUSED"
DOCS = "DOCS"
CONFIG = "CONFIG"
TEST = "TEST"
ARCHIVE = "ARCHIVE"

# Core pipeline patterns
CORE_PATTERNS = {
    "jobs/transform/bronze_to_silver.py": CORE_PIPELINE,
    "jobs/transform/silver_to_gold.py": CORE_PIPELINE,
    "jobs/ingest/": CORE_PIPELINE,
    "src/project_a/utils/spark_session.py": CORE_PIPELINE,
    "src/project_a/utils/config": CORE_PIPELINE,
    "src/project_a/utils/path_resolver.py": CORE_PIPELINE,
    "src/project_a/transform/": CORE_PIPELINE,
    "src/project_a/extract/": CORE_PIPELINE,
}

# Supporting patterns
SUPPORTING_PATTERNS = {
    "config/": CONFIG,
    "aws/terraform/": SUPPORTING,
    "aws/dags/": SUPPORTING,
    "aws/scripts/": SUPPORTING,
    "tests/": TEST,
    "docs/": DOCS,
}

# Archive/delete patterns
ARCHIVE_PATTERNS = {
    "notebooks/": EXPERIMENT,
    "experiments/": EXPERIMENT,
    "scratch/": EXPERIMENT,
    "old/": LEGACY,
    "deprecated/": LEGACY,
    "_old": LEGACY,
    ".bak": LEGACY,
    ".tmp": UNUSED,
}


def categorize_file(file_path: Path) -> str:
    """Categorize a file based on its path and patterns."""
    rel_path = str(file_path.relative_to(PROJECT_ROOT))
    
    # Check core patterns
    for pattern, category in CORE_PATTERNS.items():
        if pattern in rel_path:
            return category
    
    # Check supporting patterns
    for pattern, category in SUPPORTING_PATTERNS.items():
        if pattern in rel_path:
            return category
    
    # Check archive patterns
    for pattern, category in ARCHIVE_PATTERNS.items():
        if pattern in rel_path or rel_path.endswith(pattern):
            return category
    
    # Default categorization by directory
    parts = rel_path.split("/")
    if parts[0] == "src":
        if "pyspark_interview_project" in rel_path:
            # Check if it's actually used
            if "utils" in rel_path or "transform" in rel_path or "extract" in rel_path:
                return CORE_PIPELINE
            return EXPERIMENT  # Likely legacy compatibility shim
        return CORE_PIPELINE
    elif parts[0] == "jobs":
        return CORE_PIPELINE
    elif parts[0] == "aws":
        return SUPPORTING
    elif parts[0] == "config":
        return CONFIG
    elif parts[0] == "tests":
        return TEST
    elif parts[0] == "docs":
        return DOCS
    elif parts[0] == "notebooks":
        return EXPERIMENT
    
    return UNUSED


def analyze_repository():
    """Analyze the entire repository structure."""
    print("=" * 80)
    print("PROJECT_A REPOSITORY STRUCTURE ANALYSIS")
    print("=" * 80)
    print()
    
    # Find all Python files
    python_files = []
    for root, dirs, files in os.walk(PROJECT_ROOT):
        # Skip common ignore directories
        dirs[:] = [d for d in dirs if d not in {'.git', '.venv', '__pycache__', 'node_modules', 'dist', 'build', '.pytest_cache', '.idea'}]
        for file in files:
            if file.endswith('.py'):
                python_files.append(Path(root) / file)
    
    # Categorize files
    categories: Dict[str, List[Path]] = defaultdict(list)
    for file_path in python_files:
        category = categorize_file(file_path)
        categories[category].append(file_path)
    
    # Print summary
    print("FILE CATEGORIZATION SUMMARY")
    print("=" * 80)
    for category in sorted(categories.keys()):
        files = categories[category]
        print(f"\n{category}: {len(files)} files")
        for file_path in sorted(files)[:10]:  # Show first 10
            rel_path = file_path.relative_to(PROJECT_ROOT)
            print(f"  - {rel_path}")
        if len(files) > 10:
            print(f"  ... and {len(files) - 10} more")
    
    # Generate report
    report_path = PROJECT_ROOT / "CLEANUP_ANALYSIS.md"
    with open(report_path, 'w') as f:
        f.write("# Repository Cleanup Analysis\n\n")
        f.write(f"Generated: {Path(__file__).name}\n\n")
        f.write("## Summary\n\n")
        f.write(f"Total Python files analyzed: {len(python_files)}\n\n")
        
        for category in sorted(categories.keys()):
            files = categories[category]
            f.write(f"### {category} ({len(files)} files)\n\n")
            for file_path in sorted(files):
                rel_path = file_path.relative_to(PROJECT_ROOT)
                f.write(f"- `{rel_path}`\n")
            f.write("\n")
    
    print(f"\n✅ Analysis report written to: {report_path.relative_to(PROJECT_ROOT)}")
    
    # Recommendations
    print("\n" + "=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    print(f"\nCORE_PIPELINE: {len(categories[CORE_PIPELINE])} files - KEEP and polish")
    print(f"SUPPORTING: {len(categories[SUPPORTING])} files - KEEP")
    print(f"CONFIG: {len(categories[CONFIG])} files - KEEP")
    print(f"TEST: {len(categories[TEST])} files - Review and keep working tests")
    print(f"DOCS: {len(categories[DOCS])} files - Keep essential docs")
    print(f"EXPERIMENT: {len(categories[EXPERIMENT])} files - MOVE to archive/")
    print(f"LEGACY: {len(categories[LEGACY])} files - DELETE or archive")
    print(f"UNUSED: {len(categories[UNUSED])} files - DELETE")


if __name__ == "__main__":
    analyze_repository()

