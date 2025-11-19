#!/usr/bin/env python3
"""
Analyze Python modules to find unused files.

This script:
1. Finds all Python files in the repo
2. Builds a reference graph (imports, DAG references, etc.)
3. Identifies unused modules
4. Generates a report for moving to legacy/
"""

import ast
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Set, List, Tuple

# Directories to analyze
SOURCE_DIRS = [
    "src/project_a",
    "jobs",
    "aws/dags",
    "aws/jobs",
    "aws/scripts",
    "aws/tests",
    "tests",
]

# Directories to ignore
IGNORE_DIRS = {
    "__pycache__",
    ".git",
    ".venv",
    "venv",
    "node_modules",
    "dist",
    "build",
    "legacy",
    ".pytest_cache",
}

def find_python_files(root_dir: str = ".") -> List[Path]:
    """Find all Python files in source directories."""
    python_files = []
    root = Path(root_dir)
    
    for source_dir in SOURCE_DIRS:
        source_path = root / source_dir
        if source_path.exists():
            for py_file in source_path.rglob("*.py"):
                # Skip ignored directories
                if any(ignore in py_file.parts for ignore in IGNORE_DIRS):
                    continue
                python_files.append(py_file)
    
    return sorted(python_files)

def extract_module_path(file_path: Path, root_dir: str = ".") -> List[str]:
    """Extract possible module paths from a file path."""
    root = Path(root_dir)
    try:
        rel_path = file_path.relative_to(root)
    except ValueError:
        return []
    
    parts = list(rel_path.parts)
    
    # Remove .py extension
    if parts[-1].endswith('.py'):
        parts[-1] = parts[-1][:-3]
    
    # Remove __init__ from path
    if parts[-1] == '__init__':
        parts = parts[:-1]
    
    # Generate possible module paths
    module_paths = []
    
    # Full path from root
    if parts:
        module_paths.append('.'.join(parts))
    
    # Path from src/ (if in src/)
    if 'src' in parts:
        src_idx = parts.index('src')
        if src_idx + 1 < len(parts):
            module_paths.append('.'.join(parts[src_idx + 1:]))
    
    # Path from project_a/ (if in project_a/)
    if 'project_a' in parts:
        proj_idx = parts.index('project_a')
        if proj_idx + 1 < len(parts):
            module_paths.append('.'.join(parts[proj_idx + 1:]))
    
    # Path from pyspark_interview_project/ (legacy compatibility)
    if 'pyspark_interview_project' in parts:
        pspark_idx = parts.index('pyspark_interview_project')
        if pspark_idx + 1 < len(parts):
            module_paths.append('.'.join(parts[pspark_idx + 1:]))
    
    return module_paths

def extract_imports(file_path: Path) -> Set[str]:
    """Extract all import statements from a Python file."""
    imports = set()
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception:
        return imports
    
    try:
        tree = ast.parse(content, filename=str(file_path))
    except SyntaxError:
        # If file has syntax errors, try regex fallback
        return extract_imports_regex(content)
    
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.add(node.module.split('.')[0])
    
    return imports

def extract_imports_regex(content: str) -> Set[str]:
    """Fallback regex-based import extraction."""
    imports = set()
    
    # Match import statements
    import_pattern = r'^\s*(?:from\s+)?import\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)'
    for line in content.split('\n'):
        match = re.match(import_pattern, line)
        if match:
            module = match.group(1).split('.')[0]
            imports.add(module)
    
    # Match from ... import statements
    from_pattern = r'^\s*from\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\s+import'
    for line in content.split('\n'):
        match = re.match(from_pattern, line)
        if match:
            module = match.group(1).split('.')[0]
            imports.add(module)
    
    return imports

def find_file_references(root_dir: str = ".") -> Dict[str, Set[str]]:
    """Find all file references (imports, DAG references, etc.) across the repo."""
    root = Path(root_dir)
    references = defaultdict(set)
    
    # Find all Python files
    all_files = []
    for source_dir in SOURCE_DIRS + ["aws/dags", "aws/scripts"]:
        source_path = root / source_dir
        if source_path.exists():
            for py_file in source_path.rglob("*.py"):
                if any(ignore in py_file.parts for ignore in IGNORE_DIRS):
                    continue
                all_files.append(py_file)
    
    # Also check JSON files (EMR step configs, etc.)
    for json_file in root.rglob("*.json"):
        if any(ignore in json_file.parts for ignore in IGNORE_DIRS):
            continue
        all_files.append(json_file)
    
    # Also check YAML files (configs, DAGs might reference files)
    for yaml_file in root.rglob("*.yaml"):
        if any(ignore in yaml_file.parts for ignore in IGNORE_DIRS):
            continue
        all_files.append(yaml_file)
    
    for yaml_file in root.rglob("*.yml"):
        if any(ignore in yaml_file.parts for ignore in IGNORE_DIRS):
            continue
        all_files.append(yaml_file)
    
    # Also check shell scripts
    for sh_file in root.rglob("*.sh"):
        if any(ignore in sh_file.parts for ignore in IGNORE_DIRS):
            continue
        all_files.append(sh_file)
    
    # Extract references from each file
    for file_path in all_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception:
            continue
        
        # Extract Python imports
        if file_path.suffix == '.py':
            imports = extract_imports(file_path)
            for imp in imports:
                references[imp].add(str(file_path))
        
        # Extract file path references (for DAGs, scripts, etc.)
        # Look for patterns like: s3://.../jobs/xxx.py, python jobs/xxx.py, etc.
        patterns = [
            r'jobs/([a-zA-Z_][a-zA-Z0-9_/]*\.py)',
            r's3://[^/]+/jobs/([a-zA-Z_][a-zA-Z0-9_/]*\.py)',
            r'python\s+[^\s]+/([a-zA-Z_][a-zA-Z0-9_/]*\.py)',
            r'entryPoint["\']?\s*[:=]\s*["\']?s3://[^/]+/jobs/([a-zA-Z_][a-zA-Z0-9_/]*\.py)',
            r'entryPoint["\']?\s*[:=]\s*["\']?([a-zA-Z_][a-zA-Z0-9_/]*\.py)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            for match in matches:
                references[match].add(str(file_path))
        
        # Extract module references in strings
        module_patterns = [
            r'["\']([a-zA-Z_][a-zA-Z0-9_.]+)["\']',  # Quoted module names
            r'from\s+([a-zA-Z_][a-zA-Z0-9_.]+)\s+import',
            r'import\s+([a-zA-Z_][a-zA-Z0-9_.]+)',
        ]
        
        for pattern in module_patterns:
            matches = re.findall(pattern, content)
            for match in matches:
                # Only consider if it looks like a module path
                if '.' in match or match in ['project_a', 'pyspark_interview_project']:
                    references[match].add(str(file_path))
    
    return dict(references)

def analyze_unused_modules():
    """Main analysis function."""
    print("🔍 Analyzing Python modules...")
    print("=" * 80)
    
    # Find all Python files
    python_files = find_python_files()
    print(f"\n📁 Found {len(python_files)} Python files")
    
    # Build module path map
    module_to_file = {}
    file_to_modules = {}
    
    for py_file in python_files:
        module_paths = extract_module_path(py_file)
        file_to_modules[py_file] = module_paths
        for module_path in module_paths:
            if module_path not in module_to_file:
                module_to_file[module_path] = []
            module_to_file[module_path].append(py_file)
    
    print(f"📦 Found {len(module_to_file)} unique module paths")
    
    # Find all references
    print("\n🔗 Building reference graph...")
    references = find_file_references()
    print(f"📊 Found {len(references)} referenced modules/patterns")
    
    # Determine which files are used
    used_files = set()
    unused_files = []
    
    # Files that are definitely used (entry points)
    entry_point_patterns = [
        r'if\s+__name__\s*==\s*["\']__main__["\']',
        r'def\s+main\s*\(',
    ]
    
    for py_file in python_files:
        is_used = False
        
        # Check if it's an entry point
        try:
            with open(py_file, 'r', encoding='utf-8') as f:
                content = f.read()
                for pattern in entry_point_patterns:
                    if re.search(pattern, content):
                        is_used = True
                        break
        except Exception:
            pass
        
        # Check if any of its module paths are referenced
        module_paths = file_to_modules.get(py_file, [])
        for module_path in module_paths:
            # Check if module is imported
            module_base = module_path.split('.')[0]
            if module_base in references:
                is_used = True
                break
            
            # Check if full module path is referenced
            if module_path in references:
                is_used = True
                break
            
            # Check if filename is referenced
            filename = py_file.name
            if filename in references:
                is_used = True
                break
        
        # Check if file path is referenced in scripts/DAGs
        rel_path = str(py_file.relative_to(Path('.')))
        if rel_path in references:
            is_used = True
        
        if is_used:
            used_files.add(py_file)
        else:
            unused_files.append(py_file)
    
    # Print results
    print("\n" + "=" * 80)
    print("📊 ANALYSIS RESULTS")
    print("=" * 80)
    print(f"\n✅ Used files: {len(used_files)}")
    print(f"❌ Potentially unused files: {len(unused_files)}")
    
    # Group unused files by directory
    unused_by_dir = defaultdict(list)
    for file in unused_files:
        dir_path = file.parent
        unused_by_dir[str(dir_path)].append(file)
    
    print("\n📁 Unused files by directory:")
    for dir_path in sorted(unused_by_dir.keys()):
        files = unused_by_dir[dir_path]
        print(f"\n  {dir_path}/ ({len(files)} files)")
        for file in sorted(files):
            print(f"    - {file.name}")
    
    # Generate legacy move plan
    print("\n" + "=" * 80)
    print("📋 LEGACY MOVE PLAN")
    print("=" * 80)
    
    legacy_plan = []
    for file in unused_files:
        rel_path = file.relative_to(Path('.'))
        
        # Determine legacy location
        if 'src/project_a' in str(rel_path):
            legacy_path = Path('src/project_a/legacy') / rel_path.relative_to('src/project_a')
        elif 'aws/jobs' in str(rel_path):
            legacy_path = Path('aws/jobs_legacy') / rel_path.relative_to('aws/jobs')
        elif 'aws/scripts' in str(rel_path):
            legacy_path = Path('aws/scripts_legacy') / rel_path.relative_to('aws/scripts')
        elif 'jobs' in str(rel_path):
            legacy_path = Path('jobs_legacy') / rel_path.relative_to('jobs')
        else:
            legacy_path = Path('legacy') / rel_path
        
        legacy_plan.append((file, legacy_path))
        print(f"  mv {rel_path} -> {legacy_path}")
    
    return {
        'used_files': used_files,
        'unused_files': unused_files,
        'legacy_plan': legacy_plan,
    }

if __name__ == '__main__':
    results = analyze_unused_modules()
    
    # Write report
    report_file = Path('CLEANUP_ANALYSIS_REPORT.md')
    with open(report_file, 'w') as f:
        f.write("# Cleanup Analysis Report\n\n")
        f.write(f"## Summary\n\n")
        f.write(f"- Used files: {len(results['used_files'])}\n")
        f.write(f"- Unused files: {len(results['unused_files'])}\n\n")
        f.write("## Unused Files\n\n")
        for file in sorted(results['unused_files']):
            f.write(f"- {file}\n")
    
    print(f"\n✅ Report written to {report_file}")

