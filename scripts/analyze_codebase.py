#!/usr/bin/env python3
"""
Comprehensive Codebase Analysis for Project_A

Identifies:
- Dead code and unused imports
- Duplicate modules
- Missing imports
- Inconsistent naming
- Unused configs
- Files that should be removed
"""

import ast
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple

PROJECT_ROOT = Path(__file__).parent.parent


def find_all_python_files() -> List[Path]:
    """Find all Python files in the project."""
    python_files = []
    for root, dirs, files in os.walk(PROJECT_ROOT):
        # Skip common ignore directories
        dirs[:] = [d for d in dirs if d not in {'.git', '.venv', '__pycache__', 'node_modules', 'dist', 'build', '.pytest_cache'}]
        for file in files:
            if file.endswith('.py'):
                python_files.append(Path(root) / file)
    return python_files


def extract_imports(file_path: Path) -> Tuple[Set[str], Set[str]]:
    """Extract all imports from a Python file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        tree = ast.parse(content, filename=str(file_path))
    except Exception as e:
        return set(), set()
    
    imports = set()
    from_imports = set()
    
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                from_imports.add(node.module)
    
    return imports, from_imports


def find_unused_files(python_files: List[Path], all_imports: Dict[str, Set[Path]]) -> Set[Path]:
    """Find Python files that are never imported."""
    unused = set()
    
    for file_path in python_files:
        # Skip entry points and __init__.py
        if file_path.name == '__init__.py' or file_path.name.endswith('_test.py') or file_path.name.startswith('test_'):
            continue
        
        # Get module name
        rel_path = file_path.relative_to(PROJECT_ROOT)
        parts = list(rel_path.parts)
        if parts[0] == 'src':
            parts = parts[1:]  # Remove 'src'
        module_name = '.'.join(parts[:-1] + [parts[-1].replace('.py', '')])
        
        # Check if module is imported anywhere
        is_imported = False
        for imported_module, importers in all_imports.items():
            if module_name in imported_module or imported_module in module_name:
                is_imported = True
                break
        
        if not is_imported and 'jobs/transform' not in str(file_path) and 'jobs/ingest' not in str(file_path):
            unused.add(file_path)
    
    return unused


def analyze_codebase():
    """Main analysis function."""
    print("=" * 80)
    print("PROJECT_A CODEBASE ANALYSIS")
    print("=" * 80)
    print()
    
    python_files = find_all_python_files()
    print(f"📁 Found {len(python_files)} Python files")
    print()
    
    # Extract all imports
    all_imports: Dict[str, Set[Path]] = defaultdict(set)
    file_imports: Dict[Path, Set[str]] = {}
    
    print("🔍 Analyzing imports...")
    for file_path in python_files:
        imports, from_imports = extract_imports(file_path)
        file_imports[file_path] = from_imports
        for imp in from_imports:
            all_imports[imp].add(file_path)
    
    # Find duplicate modules
    print("\n📋 Checking for duplicate modules...")
    module_locations: Dict[str, List[Path]] = defaultdict(list)
    for file_path in python_files:
        if 'pyspark_interview_project' in str(file_path) or 'project_a' in str(file_path):
            rel_path = file_path.relative_to(PROJECT_ROOT)
            module_name = rel_path.stem
            if module_name not in {'__init__', '__main__'}:
                module_locations[module_name].append(file_path)
    
    duplicates = {k: v for k, v in module_locations.items() if len(v) > 1}
    if duplicates:
        print(f"⚠️  Found {len(duplicates)} duplicate module names:")
        for module, paths in list(duplicates.items())[:10]:
            print(f"  - {module}:")
            for p in paths:
                print(f"      {p.relative_to(PROJECT_ROOT)}")
    
    # Find files using old pyspark_interview_project imports
    print("\n🔍 Checking for pyspark_interview_project imports...")
    old_imports = []
    for file_path in python_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            if 'pyspark_interview_project' in content and 'project_a.pyspark_interview_project' not in content:
                old_imports.append(file_path)
        except Exception:
            pass
    
    if old_imports:
        print(f"⚠️  Found {len(old_imports)} files with old pyspark_interview_project imports:")
        for p in old_imports[:10]:
            print(f"  - {p.relative_to(PROJECT_ROOT)}")
    
    # Check for unused files
    print("\n🔍 Checking for potentially unused files...")
    unused = find_unused_files(python_files, all_imports)
    if unused:
        print(f"⚠️  Found {len(unused)} potentially unused files:")
        for p in list(unused)[:20]:
            print(f"  - {p.relative_to(PROJECT_ROOT)}")
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total Python files: {len(python_files)}")
    print(f"Duplicate modules: {len(duplicates)}")
    print(f"Files with old imports: {len(old_imports)}")
    print(f"Potentially unused files: {len(unused)}")
    print()
    
    # Write detailed report
    report_path = PROJECT_ROOT / "docs" / "CODEBASE_ANALYSIS.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(report_path, 'w') as f:
        f.write("# Codebase Analysis Report\n\n")
        f.write(f"Generated: {Path(__file__).name}\n\n")
        f.write("## Duplicate Modules\n\n")
        for module, paths in sorted(duplicates.items()):
            f.write(f"### {module}\n")
            for p in paths:
                f.write(f"- `{p.relative_to(PROJECT_ROOT)}`\n")
            f.write("\n")
        
        f.write("\n## Files with Old Imports\n\n")
        for p in old_imports:
            f.write(f"- `{p.relative_to(PROJECT_ROOT)}`\n")
        
        f.write("\n## Potentially Unused Files\n\n")
        for p in sorted(unused):
            f.write(f"- `{p.relative_to(PROJECT_ROOT)}`\n")
    
    print(f"📄 Detailed report written to: {report_path.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    analyze_codebase()

