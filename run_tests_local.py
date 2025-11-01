#!/usr/bin/env python3
"""
Run tests locally with coverage report.

This script runs pytest with coverage reporting, excluding tests that require
full Spark environment setup.
"""

import sys
import subprocess
import os

def main():
    """Run tests with coverage."""
    # Change to project root
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # Base pytest command
    cmd = [
        sys.executable, "-m", "pytest",
        "-v",
        "--cov=src/pyspark_interview_project",
        "--cov=jobs",
        "--cov-report=term-missing",
        "--cov-report=html:htmlcov",
        "--cov-report=xml",
        "-x",  # Stop on first failure
    ]
    
    # Add test patterns
    test_patterns = [
        "tests/test_great_expectations_runner.py",
        "tests/test_lineage_emitter.py",
        "tests/test_metrics_collector.py",
        "tests/test_secrets.py",
        "tests/test_watermark_utils.py",
        "tests/test_config.py",
        "tests/test_pii_utils.py",
        "tests/test_schema_validator.py",
    ]
    
    cmd.extend(test_patterns)
    
    # Run tests
    print("=" * 60)
    print("Running tests locally with coverage...")
    print("=" * 60)
    print(f"Command: {' '.join(cmd)}")
    print("=" * 60)
    
    result = subprocess.run(cmd)
    
    if result.returncode == 0:
        print("\n✅ All tests passed!")
        print("\nCoverage report generated:")
        print("  - HTML: htmlcov/index.html")
        print("  - XML:  coverage.xml")
    else:
        print("\n❌ Some tests failed. Check output above.")
    
    return result.returncode

if __name__ == "__main__":
    sys.exit(main())


