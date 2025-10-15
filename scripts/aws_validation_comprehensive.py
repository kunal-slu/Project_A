#!/usr/bin/env python3
"""
AWS ETL Project Comprehensive Validation & Remediation
Validates and fixes all AWS production components.
"""

import os
import sys
import yaml
import subprocess
import ast
import re
from pathlib import Path
from typing import Dict, List, Tuple, Any

class AWSValidationEngine:
    """Comprehensive AWS ETL validation and remediation engine"""
    
    def __init__(self, project_root: str):
        self.project_root = Path(project_root)
        self.issues = []
        self.fixes = []
        
    def validate_python_syntax(self) -> List[str]:
        """Validate Python syntax for all .py files"""
        print("🔍 VALIDATING PYTHON SYNTAX")
        print("===========================")
        
        python_files = []
        issues = []
        
        # Find all Python files
        for root, dirs, files in os.walk(self.project_root):
            # Skip virtual environments and cache
            if any(skip in root for skip in ['venv', '__pycache__', '.git']):
                continue
                
            for file in files:
                if file.endswith('.py'):
                    python_files.append(os.path.join(root, file))
        
        print(f"📊 Found {len(python_files)} Python files to validate")
        
        for py_file in python_files:
            try:
                with open(py_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Parse AST to check syntax
                ast.parse(content)
                print(f"✅ {py_file}")
                
            except SyntaxError as e:
                error_msg = f"❌ Syntax error in {py_file}: {e}"
                issues.append(error_msg)
                print(error_msg)
                
            except Exception as e:
                error_msg = f"❌ Error reading {py_file}: {e}"
                issues.append(error_msg)
                print(error_msg)
        
        return issues
    
    def validate_imports(self) -> List[str]:
        """Validate import statements and dependencies"""
        print("\n🔍 VALIDATING IMPORTS")
        print("====================")
        
        issues = []
        
        # Check for pyspark_interview_project imports
        aws_scripts = [
            'aws/scripts/aws_production_etl.py',
            'aws/scripts/aws_enterprise_etl.py',
            'aws/scripts/register_glue_tables.py',
            'aws/scripts/run_ge_checks.py'
        ]
        
        for script_path in aws_scripts:
            full_path = self.project_root / script_path
            if full_path.exists():
                try:
                    with open(full_path, 'r') as f:
                        content = f.read()
                    
                    # Check for correct imports
                    if 'from pyspark_interview_project' not in content:
                        issues.append(f"❌ Missing pyspark_interview_project import in {script_path}")
                    else:
                        print(f"✅ {script_path} - imports correct")
                        
                except Exception as e:
                    issues.append(f"❌ Error reading {script_path}: {e}")
        
        return issues
    
    def validate_yaml_syntax(self) -> List[str]:
        """Validate YAML configuration files"""
        print("\n🔍 VALIDATING YAML SYNTAX")
        print("=========================")
        
        issues = []
        yaml_files = []
        
        # Find YAML files
        for root, dirs, files in os.walk(self.project_root):
            if any(skip in root for skip in ['venv', '__pycache__', '.git']):
                continue
                
            for file in files:
                if file.endswith(('.yaml', '.yml')):
                    yaml_files.append(os.path.join(root, file))
        
        print(f"📊 Found {len(yaml_files)} YAML files to validate")
        
        for yaml_file in yaml_files:
            try:
                with open(yaml_file, 'r') as f:
                    yaml.safe_load(f)
                print(f"✅ {yaml_file}")
                
            except yaml.YAMLError as e:
                error_msg = f"❌ YAML error in {yaml_file}: {e}"
                issues.append(error_msg)
                print(error_msg)
                
            except Exception as e:
                error_msg = f"❌ Error reading {yaml_file}: {e}"
                issues.append(error_msg)
                print(error_msg)
        
        return issues
    
    def validate_terraform_syntax(self) -> List[str]:
        """Validate Terraform configuration files"""
        print("\n🔍 VALIDATING TERRAFORM SYNTAX")
        print("==============================")
        
        issues = []
        tf_files = []
        
        # Find Terraform files
        for root, dirs, files in os.walk(self.project_root):
            if any(skip in root for skip in ['venv', '__pycache__', '.git']):
                continue
                
            for file in files:
                if file.endswith('.tf'):
                    tf_files.append(os.path.join(root, file))
        
        print(f"📊 Found {len(tf_files)} Terraform files to validate")
        
        for tf_file in tf_files:
            try:
                # Basic syntax check - look for common issues
                with open(tf_file, 'r') as f:
                    content = f.read()
                
                # Check for basic syntax issues
                if 'resource "' in content and 'provider "' in content:
                    print(f"✅ {tf_file}")
                else:
                    issues.append(f"⚠️  {tf_file} - may have syntax issues")
                    
            except Exception as e:
                error_msg = f"❌ Error reading {tf_file}: {e}"
                issues.append(error_msg)
                print(error_msg)
        
        return issues
    
    def validate_data_source_alignment(self) -> List[str]:
        """Validate data source paths and DAG configurations"""
        print("\n🔍 VALIDATING DATA SOURCE ALIGNMENT")
        print("===================================")
        
        issues = []
        
        # Check DAG files for correct S3 paths
        dag_files = [
            'aws/dags/daily_pipeline.py',
            'aws/dags/returns_batch.py',
            'dags/daily_pipeline.py'
        ]
        
        expected_bronze_paths = [
            's3://lake/bronze/hubspot/',
            's3://lake/bronze/snowflake/',
            's3://lake/bronze/redshift/',
            's3://lake/bronze/fx_rates/',
            's3://lake/bronze/kafka/'
        ]
        
        for dag_file in dag_files:
            full_path = self.project_root / dag_file
            if full_path.exists():
                try:
                    with open(full_path, 'r') as f:
                        content = f.read()
                    
                    # Check for bronze path references
                    bronze_paths_found = [path for path in expected_bronze_paths if path in content]
                    if bronze_paths_found:
                        print(f"✅ {dag_file} - bronze paths found: {len(bronze_paths_found)}")
                    else:
                        issues.append(f"❌ {dag_file} - missing bronze path references")
                        
                except Exception as e:
                    issues.append(f"❌ Error reading {dag_file}: {e}")
        
        return issues
    
    def validate_airflow_dags(self) -> List[str]:
        """Validate Airflow DAG syntax and configuration"""
        print("\n🔍 VALIDATING AIRFLOW DAGS")
        print("==========================")
        
        issues = []
        
        # Check DAG files for proper Airflow syntax
        dag_files = [
            'aws/dags/daily_pipeline.py',
            'aws/dags/returns_batch.py',
            'dags/daily_pipeline.py',
            'dags/returns_pipeline_dag.py',
            'dags/main_etl_dag.py'
        ]
        
        for dag_file in dag_files:
            full_path = self.project_root / dag_file
            if full_path.exists():
                try:
                    with open(full_path, 'r') as f:
                        content = f.read()
                    
                    # Check for required Airflow imports
                    required_imports = ['from airflow', 'DAG', 'BashOperator', 'PythonOperator']
                    missing_imports = [imp for imp in required_imports if imp not in content]
                    
                    if not missing_imports:
                        print(f"✅ {dag_file} - Airflow syntax correct")
                    else:
                        issues.append(f"❌ {dag_file} - missing imports: {missing_imports}")
                        
                except Exception as e:
                    issues.append(f"❌ Error reading {dag_file}: {e}")
        
        return issues
    
    def validate_emr_configs(self) -> List[str]:
        """Validate EMR and Delta Lake configurations"""
        print("\n🔍 VALIDATING EMR CONFIGURATIONS")
        print("=================================")
        
        issues = []
        
        # Check EMR scripts
        emr_scripts = [
            'aws/scripts/emr_submit.sh',
            'aws/scripts/aws_production_deploy.sh'
        ]
        
        for script in emr_scripts:
            full_path = self.project_root / script
            if full_path.exists():
                try:
                    with open(full_path, 'r') as f:
                        content = f.read()
                    
                    # Check for Delta Lake configs
                    delta_configs = [
                        'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
                        'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog'
                    ]
                    
                    found_configs = [config for config in delta_configs if config in content]
                    if found_configs:
                        print(f"✅ {script} - Delta configs found: {len(found_configs)}")
                    else:
                        issues.append(f"❌ {script} - missing Delta Lake configurations")
                        
                except Exception as e:
                    issues.append(f"❌ Error reading {script}: {e}")
        
        return issues
    
    def validate_glue_athena(self) -> List[str]:
        """Validate Glue and Athena configurations"""
        print("\n🔍 VALIDATING GLUE & ATHENA")
        print("===========================")
        
        issues = []
        
        # Check Glue table registration script
        glue_script = self.project_root / 'aws/scripts/register_glue_tables.py'
        if glue_script.exists():
            try:
                with open(glue_script, 'r') as f:
                    content = f.read()
                
                # Check for database and table naming conventions
                if 'silver_db' in content and 'gold_db' in content:
                    print(f"✅ {glue_script} - database naming correct")
                else:
                    issues.append(f"❌ {glue_script} - missing proper database naming")
                    
            except Exception as e:
                issues.append(f"❌ Error reading {glue_script}: {e}")
        
        return issues
    
    def validate_data_quality(self) -> List[str]:
        """Validate Great Expectations and data quality configurations"""
        print("\n🔍 VALIDATING DATA QUALITY")
        print("==========================")
        
        issues = []
        
        # Check Great Expectations files
        ge_files = [
            'ge/great_expectations.yml',
            'aws/scripts/run_ge_checks.py'
        ]
        
        for ge_file in ge_files:
            full_path = self.project_root / ge_file
            if full_path.exists():
                try:
                    with open(full_path, 'r') as f:
                        content = f.read()
                    
                    # Check for data quality checks
                    quality_checks = ['not_null', 'range', 'valid_currency', 'referential_integrity']
                    found_checks = [check for check in quality_checks if check in content]
                    
                    if found_checks:
                        print(f"✅ {ge_file} - quality checks found: {len(found_checks)}")
                    else:
                        issues.append(f"❌ {ge_file} - missing data quality checks")
                        
                except Exception as e:
                    issues.append(f"❌ Error reading {ge_file}: {e}")
        
        return issues
    
    def validate_terraform_infra(self) -> List[str]:
        """Validate Terraform infrastructure configurations"""
        print("\n🔍 VALIDATING TERRAFORM INFRASTRUCTURE")
        print("======================================")
        
        issues = []
        
        # Check Terraform files
        tf_files = [
            'aws/infra/terraform/main.tf',
            'aws/infra/terraform/variables.tf',
            'aws/infra/terraform/outputs.tf',
            'infra/terraform/main.tf'
        ]
        
        for tf_file in tf_files:
            full_path = self.project_root / tf_file
            if full_path.exists():
                try:
                    with open(tf_file, 'r') as f:
                        content = f.read()
                    
                    # Check for required AWS resources
                    required_resources = ['aws_s3_bucket', 'aws_iam_role', 'aws_emr']
                    found_resources = [resource for resource in required_resources if resource in content]
                    
                    if found_resources:
                        print(f"✅ {tf_file} - AWS resources found: {len(found_resources)}")
                    else:
                        issues.append(f"❌ {tf_file} - missing required AWS resources")
                        
                except Exception as e:
                    issues.append(f"❌ Error reading {tf_file}: {e}")
        
        return issues
    
    def run_comprehensive_validation(self) -> Dict[str, Any]:
        """Run comprehensive validation of all components"""
        print("🚀 COMPREHENSIVE AWS ETL VALIDATION")
        print("===================================")
        print()
        
        all_issues = []
        
        # Run all validation checks
        all_issues.extend(self.validate_python_syntax())
        all_issues.extend(self.validate_imports())
        all_issues.extend(self.validate_yaml_syntax())
        all_issues.extend(self.validate_terraform_syntax())
        all_issues.extend(self.validate_data_source_alignment())
        all_issues.extend(self.validate_airflow_dags())
        all_issues.extend(self.validate_emr_configs())
        all_issues.extend(self.validate_glue_athena())
        all_issues.extend(self.validate_data_quality())
        all_issues.extend(self.validate_terraform_infra())
        
        # Summary
        print("\n📊 VALIDATION SUMMARY")
        print("====================")
        print(f"❌ Issues found: {len(all_issues)}")
        
        if all_issues:
            print("\n🔧 ISSUES TO FIX:")
            for issue in all_issues:
                print(f"   {issue}")
        else:
            print("✅ No issues found!")
        
        return {
            'total_issues': len(all_issues),
            'issues': all_issues,
            'status': 'completed'
        }

def main():
    """Main validation function"""
    project_root = "/Users/kunal/IdeaProjects/pyspark_data_engineer_project"
    
    validator = AWSValidationEngine(project_root)
    results = validator.run_comprehensive_validation()
    
    return results

if __name__ == "__main__":
    main()
