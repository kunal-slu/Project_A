#!/usr/bin/env python3
"""
Production readiness validation script.
Validates that all required components are in place for AWS production deployment.
"""

import os
import sys
import logging
from pathlib import Path
from typing import List, Dict, Any

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

logger = logging.getLogger(__name__)


def check_required_files() -> List[str]:
    """Check that all required files exist."""
    required_files = [
        "aws/scripts/emr_submit.sh",
        "aws/scripts/register_glue_tables.py",
        "aws/scripts/run_ge_checks.py",
        "aws/scripts/lf_tags_seed.py",
        "aws/scripts/delta_optimize_vacuum.py",
        "src/pyspark_interview_project/jobs/fx_to_bronze.py",
        "src/pyspark_interview_project/jobs/fx_bronze_to_silver.py",
        "src/pyspark_interview_project/jobs/salesforce_to_bronze.py",
        "src/pyspark_interview_project/jobs/kafka_orders_stream.py",
        "src/pyspark_interview_project/jobs/snowflake_to_bronze.py",
        "src/pyspark_interview_project/jobs/snowflake_bronze_to_silver_merge.py",
        "dags/returns_pipeline_dag.py",
        "dags/catalog_and_dq_dag.py",
        "dq/suites/silver_fx_rates.yml",
        "dq/suites/silver_orders.yml",
        "dq/suites/silver_salesforce.yml",
        "kafka/schemas/orders_events.json",
        "aws/RUNBOOK_AWS_2025.md",
        "aws/infra/terraform/main.tf",
        "aws/infra/terraform/variables.tf",
        "aws/infra/terraform/outputs.tf",
        ".github/workflows/ci.yml",
        "Makefile",
        "pyproject.toml"
    ]
    
    missing_files = []
    for file_path in required_files:
        if not Path(file_path).exists():
            missing_files.append(file_path)
    
    return missing_files


def check_environment_variables() -> List[str]:
    """Check that all required environment variables are documented."""
    required_env_vars = [
        "LAKE_BUCKET",
        "CODE_BUCKET", 
        "EMR_APP_ID",
        "EMR_ROLE_ARN",
        "SF_SECRET_NAME",
        "KAFKA_BOOTSTRAP",
        "KAFKA_API_KEY",
        "KAFKA_API_SECRET",
        "SNOWFLAKE_URL",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_WAREHOUSE",
        "LOG_LEVEL"
    ]
    
    # Check if these are documented in the runbook
    runbook_path = "aws/RUNBOOK_AWS_2025.md"
    if not Path(runbook_path).exists():
        return ["AWS runbook not found"]
    
    with open(runbook_path, 'r') as f:
        runbook_content = f.read()
    
    missing_docs = []
    for var in required_env_vars:
        if var not in runbook_content:
            missing_docs.append(var)
    
    return missing_docs


def check_delta_configs() -> List[str]:
    """Check that Delta Lake configurations are properly set."""
    issues = []
    
    # Check spark.py for Delta configs
    spark_utils_path = "src/pyspark_interview_project/utils/spark.py"
    if Path(spark_utils_path).exists():
        with open(spark_utils_path, 'r') as f:
            content = f.read()
            if "spark.sql.extensions" not in content:
                issues.append("Missing Delta extensions in spark.py")
            if "spark.sql.catalog.spark_catalog" not in content:
                issues.append("Missing Delta catalog in spark.py")
    else:
        issues.append("spark.py not found")
    
    return issues


def check_secrets_management() -> List[str]:
    """Check that no hardcoded credentials exist."""
    issues = []
    
    # Check for hardcoded passwords
    for py_file in Path("src").rglob("*.py"):
        with open(py_file, 'r') as f:
            content = f.read()
            if 'password="' in content and 'os.getenv' not in content:
                issues.append(f"Potential hardcoded password in {py_file}")
            if 'api_key="' in content and 'os.getenv' not in content:
                issues.append(f"Potential hardcoded API key in {py_file}")
    
    return issues


def check_makefile_targets() -> List[str]:
    """Check that Makefile has all required targets."""
    required_targets = [
        "run-bronze",
        "run-silver", 
        "run-gold",
        "test",
        "dist",
        "optimize-vacuum",
        "lf-setup",
        "kafka-produce"
    ]
    
    issues = []
    if Path("Makefile").exists():
        with open("Makefile", 'r') as f:
            content = f.read()
            for target in required_targets:
                if target not in content:
                    issues.append(f"Missing Makefile target: {target}")
    else:
        issues.append("Makefile not found")
    
    return issues


def main():
    """Main validation function."""
    print("🔍 Validating Production Readiness for AWS...")
    print("=" * 50)
    
    all_issues = []
    
    # Check required files
    print("\n📁 Checking required files...")
    missing_files = check_required_files()
    if missing_files:
        print(f"❌ Missing files: {missing_files}")
        all_issues.extend(missing_files)
    else:
        print("✅ All required files present")
    
    # Check environment variables documentation
    print("\n🌍 Checking environment variables documentation...")
    missing_env_docs = check_environment_variables()
    if missing_env_docs:
        print(f"❌ Missing env var documentation: {missing_env_docs}")
        all_issues.extend(missing_env_docs)
    else:
        print("✅ All environment variables documented")
    
    # Check Delta configurations
    print("\n🔧 Checking Delta Lake configurations...")
    delta_issues = check_delta_configs()
    if delta_issues:
        print(f"❌ Delta config issues: {delta_issues}")
        all_issues.extend(delta_issues)
    else:
        print("✅ Delta configurations correct")
    
    # Check secrets management
    print("\n🔐 Checking secrets management...")
    secret_issues = check_secrets_management()
    if secret_issues:
        print(f"❌ Secret management issues: {secret_issues}")
        all_issues.extend(secret_issues)
    else:
        print("✅ No hardcoded credentials found")
    
    # Check Makefile targets
    print("\n⚙️ Checking Makefile targets...")
    makefile_issues = check_makefile_targets()
    if makefile_issues:
        print(f"❌ Makefile issues: {makefile_issues}")
        all_issues.extend(makefile_issues)
    else:
        print("✅ All Makefile targets present")
    
    # Summary
    print("\n" + "=" * 50)
    if all_issues:
        print(f"❌ Found {len(all_issues)} issues that need to be resolved:")
        for issue in all_issues:
            print(f"  - {issue}")
        return 1
    else:
        print("🎉 All validation checks passed! Project is production-ready for AWS.")
        print("\n📋 Next steps:")
        print("1. Set up AWS credentials and environment variables")
        print("2. Follow the AWS runbook: aws/RUNBOOK_AWS_2025.md")
        print("3. Deploy infrastructure with Terraform")
        print("4. Run the golden path pipeline")
        return 0


if __name__ == "__main__":
    sys.exit(main())
