#!/usr/bin/env python3
"""
One-shot verification script for production setup.
Run this to verify all enterprise features are working.
"""
import os
import sys
import subprocess
import time
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command(cmd, description, timeout=300):
    """Run a command with timeout and logging."""
    logger.info(f"Running: {description}")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        if result.returncode == 0:
            logger.info(f"✅ {description} - SUCCESS")
            return True
        else:
            logger.error(f"❌ {description} - FAILED")
            logger.error(f"Error: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        logger.error(f"❌ {description} - TIMEOUT")
        return False
    except Exception as e:
        logger.error(f"❌ {description} - ERROR: {e}")
        return False

def check_file_exists(file_path, description):
    """Check if a file exists."""
    if Path(file_path).exists():
        logger.info(f"✅ {description} - EXISTS")
        return True
    else:
        logger.error(f"❌ {description} - MISSING")
        return False

def main():
    """Run all verification checks."""
    logger.info("🚀 Starting Production Setup Verification")
    logger.info("=" * 50)
    
    checks_passed = 0
    total_checks = 0
    
    # Check 1: Package structure
    logger.info("\n📁 Checking package structure...")
    package_checks = [
        ("src/project_a/cli.py", "CLI module"),
        ("src/project_a/delta_utils.py", "Delta utilities"),
        ("src/project_a/jobs/contacts_silver.py", "Contacts silver transform"),
        ("src/project_a/dq/run_ge.py", "Great Expectations runner"),
        ("dq/contracts/contacts_silver.yaml", "Data quality contract"),
        ("airflow/dags/production/bronze_hubspot_ingest.py", "Bronze ingest DAG"),
        ("airflow/dags/production/delta_maintenance.py", "Delta maintenance DAG"),
        ("pyproject.toml", "Project configuration"),
        ("requirements.txt", "Production requirements"),
        ("requirements-dev.txt", "Development requirements"),
        ("Dockerfile", "Docker configuration"),
        (".github/workflows/ci.yml", "CI/CD pipeline"),
        (".pre-commit-config.yaml", "Pre-commit hooks"),
        ("infra/terraform/main.tf", "Terraform configuration"),
        ("scripts/maintenance.sql", "Maintenance SQL"),
        ("tests/test_dag_imports.py", "DAG import tests"),
    ]
    
    for file_path, description in package_checks:
        total_checks += 1
        if check_file_exists(file_path, description):
            checks_passed += 1
    
    # Check 2: Install dependencies
    logger.info("\n📦 Checking dependency installation...")
    total_checks += 1
    if run_command("pip install -r requirements.txt", "Install production dependencies"):
        checks_passed += 1
    
    total_checks += 1
    if run_command("pip install -r requirements-dev.txt", "Install development dependencies"):
        checks_passed += 1
    
    # Check 3: Code quality tools
    logger.info("\n🔍 Checking code quality tools...")
    total_checks += 1
    if run_command("ruff check .", "Ruff linting check"):
        checks_passed += 1
    
    total_checks += 1
    if run_command("ruff format . --check", "Ruff formatting check"):
        checks_passed += 1
    
    total_checks += 1
    if run_command("yamllint .", "YAML linting check"):
        checks_passed += 1
    
    # Check 4: Airflow DAG imports
    logger.info("\n🌪️ Checking Airflow DAG imports...")
    total_checks += 1
    if run_command("airflow db init", "Initialize Airflow database"):
        checks_passed += 1
    
    total_checks += 1
    if run_command("airflow dags list", "List Airflow DAGs"):
        checks_passed += 1
    
    # Check 5: Delta Lake smoke test
    logger.info("\n🏔️ Checking Delta Lake functionality...")
    delta_test = '''
from pyspark.sql import SparkSession
import tempfile
import os

spark = (SparkSession.builder
        .appName("delta-smoke-test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate())

with tempfile.TemporaryDirectory() as tmp_dir:
    delta_path = os.path.join(tmp_dir, "test_delta")
    df = spark.range(10)
    df.write.format("delta").mode("overwrite").save(delta_path)
    count = spark.read.format("delta").load(delta_path).count()
    print(f"Delta test: {count} records")
    spark.stop()
'''
    
    total_checks += 1
    if run_command(f'python -c "{delta_test}"', "Delta Lake smoke test"):
        checks_passed += 1
    
    # Check 6: Unit tests
    logger.info("\n🧪 Checking unit tests...")
    total_checks += 1
    if run_command("pytest tests/ -v", "Run unit tests"):
        checks_passed += 1
    
    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("📊 VERIFICATION SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Checks passed: {checks_passed}/{total_checks}")
    logger.info(f"Success rate: {(checks_passed/total_checks)*100:.1f}%")
    
    if checks_passed == total_checks:
        logger.info("🎉 ALL CHECKS PASSED - PRODUCTION READY!")
        return 0
    else:
        logger.error(f"❌ {total_checks - checks_passed} CHECKS FAILED")
        return 1

if __name__ == "__main__":
    sys.exit(main())
