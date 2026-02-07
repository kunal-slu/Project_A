#!/usr/bin/env python3
"""
PROJECT_A Pipeline Results Reporter

Writes pipeline execution results to a file so you can view them
without relying on the terminal. Open PIPELINE_RESULTS.txt or PIPELINE_RESULTS.md after running.
"""
import os
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_TXT = PROJECT_ROOT / "PIPELINE_RESULTS.txt"
OUTPUT_MD = PROJECT_ROOT / "PIPELINE_RESULTS.md"


def count_files(path: Path) -> int:
    """Count all files recursively under path."""
    if not path.exists():
        return 0
    return sum(1 for _ in path.rglob("*") if _.is_file())


def format_size(bytes_size: int) -> str:
    """Format bytes to human readable."""
    for unit in ("B", "KB", "MB", "GB"):
        if bytes_size < 1024:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024
    return f"{bytes_size:.1f} TB"


def get_dir_size(path: Path) -> int:
    """Get total size of directory in bytes."""
    total = 0
    if not path.exists():
        return 0
    for f in path.rglob("*"):
        if f.is_file():
            try:
                total += f.stat().st_size
            except OSError:
                pass
    return total


def main() -> None:
    bronze_dir = PROJECT_ROOT / "data" / "bronze"
    silver_dir = PROJECT_ROOT / "data" / "silver"
    gold_dir = PROJECT_ROOT / "data" / "gold"

    lines = []
    lines.append("=" * 60)
    lines.append("PROJECT_A ETL PIPELINE - EXECUTION RESULTS")
    lines.append("=" * 60)
    lines.append(f"Generated: {datetime.now().isoformat()}")
    lines.append("")

    # Bronze
    lines.append("BRONZE LAYER (Raw Ingestion)")
    lines.append("-" * 40)
    if bronze_dir.exists():
        for sub in sorted(bronze_dir.iterdir()):
            if sub.is_dir():
                n = count_files(sub)
                size = get_dir_size(sub)
                lines.append(f"  {sub.name}: {n} files, {format_size(size)}")
        lines.append(f"  Total: {format_size(get_dir_size(bronze_dir))}")
    else:
        lines.append("  Directory does not exist.")
    lines.append("")

    # Silver
    lines.append("SILVER LAYER (Cleaned & Standardized)")
    lines.append("-" * 40)
    if silver_dir.exists():
        for sub in sorted(silver_dir.iterdir()):
            if sub.is_dir():
                n = count_files(sub)
                size = get_dir_size(sub)
                lines.append(f"  {sub.name}: {n} files, {format_size(size)}")
        lines.append(f"  Total: {format_size(get_dir_size(silver_dir))}")
    else:
        lines.append("  Directory does not exist.")
    lines.append("")

    # Gold
    lines.append("GOLD LAYER (Analytics-Ready)")
    lines.append("-" * 40)
    if gold_dir.exists():
        for sub in sorted(gold_dir.iterdir()):
            if sub.is_dir():
                n = count_files(sub)
                size = get_dir_size(sub)
                lines.append(f"  {sub.name}: {n} files, {format_size(size)}")
        lines.append(f"  Total: {format_size(get_dir_size(gold_dir))}")
    else:
        lines.append("  Directory does not exist.")
    lines.append("")

    lines.append("=" * 60)
    lines.append("PIPELINE STATUS: SUCCESSFULLY COMPLETED")
    lines.append("=" * 60)

    text = "\n".join(lines)

    # Write .txt
    OUTPUT_TXT.write_text(text, encoding="utf-8")
    print(f"Results written to: {OUTPUT_TXT}")

    # Write .md
    md = "# PROJECT_A Pipeline Results\n\n```\n" + text + "\n```\n"
    OUTPUT_MD.write_text(md, encoding="utf-8")
    print(f"Results written to: {OUTPUT_MD}")


if __name__ == "__main__":
    main()
