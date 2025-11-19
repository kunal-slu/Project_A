#!/usr/bin/env python3
"""
Deep review and fix script for Project A.
Fixes common issues:
- Duplicate imports
- Bare except clauses
- Unused imports
- Code quality issues
"""
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

def fix_duplicate_imports(file_path: Path) -> bool:
    """Remove duplicate imports."""
    content = file_path.read_text(encoding="utf-8")
    original = content
    
    # Track imports
    imports_seen = set()
    lines = content.split("\n")
    new_lines = []
    modified = False
    
    for line in lines:
        # Check for duplicate imports
        if line.strip().startswith("import ") or line.strip().startswith("from "):
            import_key = line.strip()
            if import_key in imports_seen:
                # Skip duplicate
                modified = True
                continue
            imports_seen.add(import_key)
        new_lines.append(line)
    
    if modified:
        file_path.write_text("\n".join(new_lines), encoding="utf-8")
        return True
    return False

def fix_bare_except(file_path: Path) -> bool:
    """Fix bare except clauses to be more specific."""
    content = file_path.read_text(encoding="utf-8")
    original = content
    
    # Replace bare except: with except Exception:
    # But be careful - some bare except might be intentional
    # Pattern: except:\n or except: (with space)
    content = re.sub(r'except:\s*\n', 'except Exception:\n', content)
    
    if content != original:
        file_path.write_text(content, encoding="utf-8")
        return True
    return False

def check_unused_imports(file_path: Path) -> list:
    """Check for potentially unused imports (basic check)."""
    content = file_path.read_text(encoding="utf-8")
    lines = content.split("\n")
    
    imports = []
    for i, line in enumerate(lines):
        if line.strip().startswith("import ") or line.strip().startswith("from "):
            imports.append((i, line.strip()))
    
    # Basic check: if import is never used
    unused = []
    for line_num, import_line in imports:
        # Extract module/name
        if "import" in import_line:
            parts = import_line.split("import")
            if len(parts) == 2:
                module = parts[0].replace("from", "").strip()
                names = [n.strip() for n in parts[1].split(",")]
                
                # Check if used in rest of file
                rest_of_file = "\n".join(lines[line_num+1:])
                for name in names:
                    # Remove "as alias" if present
                    actual_name = name.split(" as ")[0].strip()
                    # Check if it's used (simple check)
                    if actual_name not in rest_of_file and actual_name != "*":
                        # But exclude common ones that might be used indirectly
                        if actual_name not in ["os", "sys", "logging", "Path", "datetime"]:
                            unused.append((line_num+1, import_line, actual_name))
    
    return unused

if __name__ == "__main__":
    print("🔍 Starting deep review...")
    
    # Files to review
    files_to_review = [
        PROJECT_ROOT / "jobs/transform/bronze_to_silver.py",
        PROJECT_ROOT / "jobs/transform/silver_to_gold.py",
    ]
    
    for file_path in files_to_review:
        if not file_path.exists():
            continue
        
        print(f"\n📄 Reviewing {file_path.name}...")
        
        # Fix duplicate imports
        if fix_duplicate_imports(file_path):
            print(f"  ✅ Fixed duplicate imports")
        
        # Fix bare except
        if fix_bare_except(file_path):
            print(f"  ✅ Fixed bare except clauses")
        
        # Check unused imports
        unused = check_unused_imports(file_path)
        if unused:
            print(f"  ⚠️  Potentially unused imports:")
            for line_num, import_line, name in unused[:5]:  # Show first 5
                print(f"     Line {line_num}: {name}")
    
    print("\n✅ Deep review complete!")

