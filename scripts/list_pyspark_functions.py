#!/usr/bin/env python3
"""
List all PySpark functions used across the codebase (best-effort).
Outputs a markdown report to artifacts/reports/pyspark_functions_used.md.
"""

from __future__ import annotations

import re
from pathlib import Path


ROOTS = ["src", "jobs"]


def main() -> None:
    funcs = set()
    imports = set()

    for root in ROOTS:
        for path in Path(root).rglob("*.py"):
            text = path.read_text(encoding="utf-8", errors="ignore")

            # from pyspark.sql.functions import foo, bar
            for m in re.finditer(r"from\s+pyspark\.sql\.functions\s+import\s+([^\n]+)", text):
                items = m.group(1)
                # strip parentheses
                items = items.replace("(", "").replace(")", "")
                for part in items.split(","):
                    name = part.strip()
                    if name and name != "as":
                        imports.add(name)

            # F.foo usages
            for m in re.finditer(r"\bF\.([A-Za-z_][A-Za-z0-9_]*)", text):
                funcs.add(m.group(1))

    all_funcs = sorted(funcs | imports)
    out_path = Path("artifacts/reports/pyspark_functions_used.md")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines = []
    lines.append("# PySpark Functions Used (Project A)")
    lines.append("")
    lines.append(f"Total unique functions: {len(all_funcs)}")
    lines.append("")
    for name in all_funcs:
        lines.append(f"- `{name}`")

    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
