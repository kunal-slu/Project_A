#!/usr/bin/env python3
"""
Simple test runner that runs tests that work without full Spark setup.
"""
import sys
import subprocess
import os

def main():
    """Run working tests."""
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # Run tests that don't require Spark
    test_files = [
        "tests/test_lineage_emitter.py",
        "tests/test_secrets.py",
        "tests/test_config.py",
        "tests/test_pii_utils.py",
        "tests/test_great_expectations_runner.py::test_ge_runner_init",
        "tests/test_great_expectations_runner.py::test_run_dq_checkpoint_convenience",
    ]
    
    cmd = [
        sys.executable, "-m", "pytest",
        "-v",
        "--tb=short",
        "--cov=src/pyspark_interview_project.monitoring.lineage_emitter",
        "--cov=src/pyspark_interview_project.utils.secrets",
        "--cov=src/pyspark_interview_project.utils.config",
        "--cov=src/pyspark_interview_project.utils.pii_utils",
        "--cov-report=term-missing",
    ] + test_files
    
    print("Running tests locally...")
    print(f"Command: {' '.join(cmd)}")
    print("=" * 60)
    
    result = subprocess.run(cmd)
    return result.returncode

if __name__ == "__main__":
    sys.exit(main())


