#!/usr/bin/env python3
"""
End-to-End Project Check
Comprehensive check of all codes, data, and local execution.
"""

import os
import pandas as pd
import yaml
import subprocess
import sys
from pathlib import Path

def check_project_structure():
    """Check overall project structure"""
    print("🔍 CHECKING PROJECT STRUCTURE")
    print("=============================")
    print()
    
    errors = []
    warnings = []
    
    # Essential directories
    essential_dirs = [
        'aws/data_fixed/01_hubspot_crm',
        'aws/data_fixed/02_snowflake_warehouse',
        'aws/data_fixed/03_redshift_analytics',
        'aws/data_fixed/04_stream_data',
        'aws/data_fixed/05_fx_rates',
        'config',
        'src/pyspark_interview_project',
        'airflow/dags',
        'docs'
    ]
    
    for dir_path in essential_dirs:
        if os.path.exists(dir_path):
            print(f"✅ {dir_path}")
        else:
            error = f"❌ Missing directory: {dir_path}"
            errors.append(error)
            print(error)
    
    return errors, warnings

def check_data_files():
    """Check all data files"""
    print("\n🔍 CHECKING DATA FILES")
    print("======================")
    print()
    
    errors = []
    warnings = []
    
    # Expected data files
    expected_files = {
        '01_hubspot_crm': ['hubspot_contacts_25000.csv', 'hubspot_deals_30000.csv'],
        '02_snowflake_warehouse': ['snowflake_customers_50000.csv', 'snowflake_orders_100000.csv', 'snowflake_products_10000.csv'],
        '03_redshift_analytics': ['redshift_customer_behavior_50000.csv'],
        '04_stream_data': ['stream_kafka_events_100000.csv'],
        '05_fx_rates': ['fx_rates_historical_730_days.csv']
    }
    
    total_records = 0
    
    for source_dir, expected_file_list in expected_files.items():
        dir_path = f"aws/data_fixed/{source_dir}"
        if os.path.exists(dir_path):
            print(f"📁 {source_dir.upper()}:")
            for expected_file in expected_file_list:
                file_path = f"{dir_path}/{expected_file}"
                if os.path.exists(file_path):
                    try:
                        df = pd.read_csv(file_path)
                        records = len(df)
                        total_records += records
                        print(f"   ✅ {expected_file}: {records:,} records")
                    except Exception as e:
                        error = f"❌ Error reading {file_path}: {e}"
                        errors.append(error)
                        print(f"   {error}")
                else:
                    error = f"❌ Missing file: {file_path}"
                    errors.append(error)
                    print(f"   {error}")
        else:
            error = f"❌ Missing directory: {dir_path}"
            errors.append(error)
            print(error)
    
    print(f"\n📊 Total records across all sources: {total_records:,}")
    
    return errors, warnings

def check_python_syntax():
    """Check Python syntax for all files"""
    print("\n🔍 CHECKING PYTHON SYNTAX")
    print("==========================")
    print()
    
    errors = []
    warnings = []
    
    # Check main Python files
    python_files = [
        'src/pyspark_interview_project/pipeline.py',
        'src/pyspark_interview_project/pipeline_core.py',
        'src/pyspark_interview_project/schema_validator.py',
        'src/pyspark_interview_project/utils/spark_session.py',
        'src/pyspark_interview_project/dq/runner.py',
        'src/pyspark_interview_project/io/path_resolver.py',
        'src/pyspark_interview_project/lineage/openlineage_emitter.py'
    ]
    
    for py_file in python_files:
        if os.path.exists(py_file):
            try:
                with open(py_file, 'r') as f:
                    code = f.read()
                compile(code, py_file, 'exec')
                print(f"✅ {py_file}")
            except SyntaxError as e:
                error = f"❌ Syntax error in {py_file}: {e}"
                errors.append(error)
                print(error)
            except Exception as e:
                error = f"❌ Error checking {py_file}: {e}"
                errors.append(error)
                print(error)
        else:
            warning = f"⚠️  Missing file: {py_file}"
            warnings.append(warning)
            print(warning)
    
    # Check Airflow DAGs
    dag_files = [
        'airflow/dags/pyspark_etl_dag.py',
        'airflow/dags/external_data_ingestion_dag.py'
    ]
    
    for dag_file in dag_files:
        if os.path.exists(dag_file):
            try:
                with open(dag_file, 'r') as f:
                    code = f.read()
                compile(code, dag_file, 'exec')
                print(f"✅ {dag_file}")
            except SyntaxError as e:
                error = f"❌ Syntax error in {dag_file}: {e}"
                errors.append(error)
                print(error)
            except Exception as e:
                error = f"❌ Error checking {dag_file}: {e}"
                errors.append(error)
                print(error)
        else:
            warning = f"⚠️  Missing file: {dag_file}"
            warnings.append(warning)
            print(warning)
    
    return errors, warnings

def check_config_files():
    """Check configuration files"""
    print("\n🔍 CHECKING CONFIGURATION FILES")
    print("================================")
    print()
    
    errors = []
    warnings = []
    
    # Check YAML files
    yaml_files = [
        'config/default.yaml',
        'config/aws.yaml',
        'config/azure.yaml',
        'config/local.yaml'
    ]
    
    for yaml_file in yaml_files:
        if os.path.exists(yaml_file):
            try:
                with open(yaml_file, 'r') as f:
                    yaml.safe_load(f)
                print(f"✅ {yaml_file}")
            except Exception as e:
                error = f"❌ Invalid YAML in {yaml_file}: {e}"
                errors.append(error)
                print(error)
        else:
            warning = f"⚠️  Missing config file: {yaml_file}"
            warnings.append(warning)
            print(warning)
    
    # Check requirements.txt
    if os.path.exists('requirements.txt'):
        try:
            with open('requirements.txt', 'r') as f:
                requirements = f.read()
            print("✅ requirements.txt")
            
            # Check for essential dependencies
            essential_deps = ['pyspark', 'delta-spark', 'pandas', 'numpy']
            for dep in essential_deps:
                if dep in requirements.lower():
                    print(f"   ✅ {dep} found")
                else:
                    warning = f"⚠️  Missing dependency: {dep}"
                    warnings.append(warning)
                    print(f"   {warning}")
        except Exception as e:
            error = f"❌ Error reading requirements.txt: {e}"
            errors.append(error)
            print(error)
    else:
        error = "❌ Missing requirements.txt"
        errors.append(error)
        print(error)
    
    return errors, warnings

def check_data_quality():
    """Check data quality"""
    print("\n🔍 CHECKING DATA QUALITY")
    print("========================")
    print()
    
    errors = []
    warnings = []
    
    # Check each data file for quality issues
    data_files = [
        'aws/data_fixed/01_hubspot_crm/hubspot_contacts_25000.csv',
        'aws/data_fixed/01_hubspot_crm/hubspot_deals_30000.csv',
        'aws/data_fixed/02_snowflake_warehouse/snowflake_customers_50000.csv',
        'aws/data_fixed/02_snowflake_warehouse/snowflake_orders_100000.csv',
        'aws/data_fixed/02_snowflake_warehouse/snowflake_products_10000.csv',
        'aws/data_fixed/03_redshift_analytics/redshift_customer_behavior_50000.csv',
        'aws/data_fixed/04_stream_data/stream_kafka_events_100000.csv',
        'aws/data_fixed/05_fx_rates/fx_rates_historical_730_days.csv'
    ]
    
    for file_path in data_files:
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
                filename = os.path.basename(file_path)
                
                print(f"📊 {filename}:")
                print(f"   Records: {len(df):,}")
                print(f"   Columns: {len(df.columns)}")
                
                # Check for duplicates in key columns
                if 'customer_id' in df.columns:
                    duplicates = df.duplicated(subset=['customer_id']).sum()
                    if duplicates > 0:
                        warning = f"⚠️  {duplicates} duplicate customer_ids"
                        warnings.append(warning)
                        print(f"   {warning}")
                    else:
                        print("   ✅ No duplicate customer_ids")
                
                if 'order_id' in df.columns:
                    duplicates = df.duplicated(subset=['order_id']).sum()
                    if duplicates > 0:
                        error = f"❌ {duplicates} duplicate order_ids"
                        errors.append(error)
                        print(f"   {error}")
                    else:
                        print("   ✅ No duplicate order_ids")
                
                # Check for high null values
                null_counts = df.isnull().sum()
                high_null_cols = null_counts[null_counts > len(df) * 0.5]
                if len(high_null_cols) > 0:
                    warning = f"⚠️  High null values: {high_null_cols.to_dict()}"
                    warnings.append(warning)
                    print(f"   {warning}")
                else:
                    print("   ✅ No high null values")
                
                print()
                
            except Exception as e:
                error = f"❌ Error reading {file_path}: {e}"
                errors.append(error)
                print(error)
        else:
            error = f"❌ File not found: {file_path}"
            errors.append(error)
            print(error)
    
    return errors, warnings

def check_dependencies():
    """Check if required dependencies are available"""
    print("\n🔍 CHECKING DEPENDENCIES")
    print("========================")
    print()
    
    errors = []
    warnings = []
    
    # Check Python version
    python_version = sys.version_info
    print(f"Python version: {python_version.major}.{python_version.minor}.{python_version.micro}")
    
    if python_version.major < 3 or (python_version.major == 3 and python_version.minor < 8):
        error = "❌ Python 3.8+ required"
        errors.append(error)
        print(error)
    else:
        print("✅ Python version OK")
    
    # Check if pip is available
    try:
        subprocess.run(['pip', '--version'], check=True, capture_output=True)
        print("✅ pip available")
    except (subprocess.CalledProcessError, FileNotFoundError):
        error = "❌ pip not available"
        errors.append(error)
        print(error)
    
    # Check if required packages can be imported
    required_packages = ['pandas', 'numpy', 'yaml']
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package} available")
        except ImportError:
            warning = f"⚠️  {package} not available (will be installed from requirements.txt)"
            warnings.append(warning)
            print(warning)
    
    return errors, warnings

def test_local_execution():
    """Test local execution of the pipeline"""
    print("\n🔍 TESTING LOCAL EXECUTION")
    print("==========================")
    print()
    
    errors = []
    warnings = []
    
    # Check if main pipeline file exists
    pipeline_file = 'src/pyspark_interview_project/pipeline.py'
    if not os.path.exists(pipeline_file):
        error = f"❌ Main pipeline file not found: {pipeline_file}"
        errors.append(error)
        print(error)
        return errors, warnings
    
    # Check if config file exists
    config_file = 'config/local.yaml'
    if not os.path.exists(config_file):
        warning = f"⚠️  Local config file not found: {config_file}"
        warnings.append(warning)
        print(warning)
        print("   Using default.yaml instead...")
        config_file = 'config/default.yaml'
    
    if not os.path.exists(config_file):
        error = f"❌ No config file found"
        errors.append(error)
        print(error)
        return errors, warnings
    
    print(f"✅ Using config file: {config_file}")
    
    # Test Python syntax of main pipeline
    try:
        with open(pipeline_file, 'r') as f:
            code = f.read()
        compile(code, pipeline_file, 'exec')
        print("✅ Main pipeline syntax OK")
    except SyntaxError as e:
        error = f"❌ Syntax error in main pipeline: {e}"
        errors.append(error)
        print(error)
    except Exception as e:
        error = f"❌ Error checking main pipeline: {e}"
        errors.append(error)
        print(error)
    
    # Test if we can import the main module
    try:
        sys.path.insert(0, 'src')
        import pyspark_interview_project.pipeline
        print("✅ Main pipeline module can be imported")
    except ImportError as e:
        warning = f"⚠️  Cannot import main pipeline module: {e}"
        warnings.append(warning)
        print(warning)
    except Exception as e:
        warning = f"⚠️  Error importing main pipeline module: {e}"
        warnings.append(warning)
        print(warning)
    
    return errors, warnings

def run_final_verification():
    """Run final verification"""
    print("\n🔍 FINAL VERIFICATION")
    print("====================")
    print()
    
    # Check if all essential files exist
    essential_files = [
        'README.md',
        'requirements.txt',
        'config/default.yaml',
        'config/aws.yaml',
        'src/pyspark_interview_project/pipeline.py',
        'airflow/dags/pyspark_etl_dag.py',
        'docs/SIMPLIFIED_DATA_SOURCES.md'
    ]
    
    all_files_exist = True
    for file_path in essential_files:
        if os.path.exists(file_path):
            print(f"✅ {file_path}")
        else:
            print(f"❌ Missing: {file_path}")
            all_files_exist = False
    
    # Check data file counts
    data_source_dirs = [
        'aws/data_fixed/01_hubspot_crm',
        'aws/data_fixed/02_snowflake_warehouse',
        'aws/data_fixed/03_redshift_analytics',
        'aws/data_fixed/04_stream_data',
        'aws/data_fixed/05_fx_rates'
    ]
    
    total_data_files = 0
    for dir_path in data_source_dirs:
        if os.path.exists(dir_path):
            csv_files = [f for f in os.listdir(dir_path) if f.endswith('.csv')]
            total_data_files += len(csv_files)
            print(f"✅ {dir_path}: {len(csv_files)} files")
        else:
            print(f"❌ Missing: {dir_path}")
            all_files_exist = False
    
    print(f"\n📊 Total data files: {total_data_files}")
    
    return all_files_exist

def main():
    """Main end-to-end check function"""
    print("🚀 COMPREHENSIVE END-TO-END PROJECT CHECK")
    print("=========================================")
    print()
    
    all_errors = []
    all_warnings = []
    
    try:
        # Run all checks
        errors, warnings = check_project_structure()
        all_errors.extend(errors)
        all_warnings.extend(warnings)
        
        errors, warnings = check_data_files()
        all_errors.extend(errors)
        all_warnings.extend(warnings)
        
        errors, warnings = check_python_syntax()
        all_errors.extend(errors)
        all_warnings.extend(warnings)
        
        errors, warnings = check_config_files()
        all_errors.extend(errors)
        all_warnings.extend(warnings)
        
        errors, warnings = check_data_quality()
        all_errors.extend(errors)
        all_warnings.extend(warnings)
        
        errors, warnings = check_dependencies()
        all_errors.extend(errors)
        all_warnings.extend(warnings)
        
        errors, warnings = test_local_execution()
        all_errors.extend(errors)
        all_warnings.extend(warnings)
        
        # Final verification
        all_good = run_final_verification()
        
        # Summary
        print("\n📊 END-TO-END CHECK SUMMARY")
        print("===========================")
        print(f"❌ Errors found: {len(all_errors)}")
        print(f"⚠️  Warnings found: {len(all_warnings)}")
        print()
        
        if len(all_errors) > 0:
            print("❌ ERRORS:")
            for error in all_errors:
                print(f"   {error}")
            print()
        
        if len(all_warnings) > 0:
            print("⚠️  WARNINGS:")
            for warning in all_warnings:
                print(f"   {warning}")
            print()
        
        if len(all_errors) == 0:
            print("✅ NO CRITICAL ERRORS FOUND!")
            print("🎯 Project is ready for local execution")
            print("🚀 Ready to run end-to-end!")
        else:
            print("❌ CRITICAL ERRORS FOUND!")
            print("🔧 Please fix the errors above before running")
        
        return len(all_errors) == 0
        
    except Exception as e:
        print(f"\n❌ END-TO-END CHECK FAILED: {e}")
        return False

if __name__ == "__main__":
    main()
