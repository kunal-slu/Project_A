#!/usr/bin/env python3
"""
Comprehensive Project Error Check
Analyzes the entire project for errors, inconsistencies, and issues.
"""

import os
import pandas as pd
import yaml
import json
from pathlib import Path

def check_project_structure():
    """Check overall project structure"""
    print("🔍 CHECKING PROJECT STRUCTURE")
    print("=============================")
    print()
    
    errors = []
    warnings = []
    
    # Check if essential directories exist
    essential_dirs = [
        'aws/data_fixed',
        'config',
        'src/pyspark_interview_project',
        'scripts',
        'docs',
        'tests'
    ]
    
    for dir_path in essential_dirs:
        if os.path.exists(dir_path):
            print(f"✅ Directory exists: {dir_path}")
        else:
            error = f"❌ Missing directory: {dir_path}"
            errors.append(error)
            print(error)
    
    # Check data source directories
    data_source_dirs = [
        'aws/data_fixed/01_hubspot_crm',
        'aws/data_fixed/02_snowflake_warehouse',
        'aws/data_fixed/03_redshift_analytics',
        'aws/data_fixed/04_stream_data',
        'aws/data_fixed/05_fx_rates'
    ]
    
    for dir_path in data_source_dirs:
        if os.path.exists(dir_path):
            files = [f for f in os.listdir(dir_path) if f.endswith('.csv')]
            print(f"✅ {dir_path}: {len(files)} files")
        else:
            error = f"❌ Missing data source directory: {dir_path}"
            errors.append(error)
            print(error)
    
    return errors, warnings

def check_data_files():
    """Check data files for errors"""
    print("\n🔍 CHECKING DATA FILES")
    print("=======================")
    print()
    
    errors = []
    warnings = []
    
    # Expected files
    expected_files = {
        '01_hubspot_crm': ['hubspot_contacts_25000.csv', 'hubspot_deals_30000.csv'],
        '02_snowflake_warehouse': ['snowflake_customers_50000.csv', 'snowflake_orders_100000.csv', 'snowflake_products_10000.csv'],
        '03_redshift_analytics': ['redshift_customer_behavior_50000.csv'],
        '04_stream_data': ['stream_kafka_events_100000.csv'],
        '05_fx_rates': ['fx_rates_historical_730_days.csv']
    }
    
    for source_dir, expected_file_list in expected_files.items():
        dir_path = f"aws/data_fixed/{source_dir}"
        if os.path.exists(dir_path):
            actual_files = [f for f in os.listdir(dir_path) if f.endswith('.csv')]
            
            # Check for missing files
            for expected_file in expected_file_list:
                if expected_file not in actual_files:
                    error = f"❌ Missing file: {dir_path}/{expected_file}"
                    errors.append(error)
                    print(error)
                else:
                    print(f"✅ Found: {expected_file}")
            
            # Check for unexpected files
            for actual_file in actual_files:
                if actual_file not in expected_file_list:
                    warning = f"⚠️  Unexpected file: {dir_path}/{actual_file}"
                    warnings.append(warning)
                    print(warning)
        else:
            error = f"❌ Missing directory: {dir_path}"
            errors.append(error)
            print(error)
    
    return errors, warnings

def check_data_quality():
    """Check data quality issues"""
    print("\n🔍 CHECKING DATA QUALITY")
    print("========================")
    print()
    
    errors = []
    warnings = []
    
    # Check each data file
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
                
                # Check for null values
                null_counts = df.isnull().sum()
                high_null_cols = null_counts[null_counts > len(df) * 0.5]
                if len(high_null_cols) > 0:
                    warning = f"⚠️  High null values in {filename}: {high_null_cols.to_dict()}"
                    warnings.append(warning)
                    print(f"   {warning}")
                
                # Check for duplicates
                if 'customer_id' in df.columns:
                    duplicates = df.duplicated(subset=['customer_id']).sum()
                    if duplicates > 0:
                        error = f"❌ Duplicate customer_ids in {filename}: {duplicates}"
                        errors.append(error)
                        print(f"   {error}")
                
                if 'order_id' in df.columns:
                    duplicates = df.duplicated(subset=['order_id']).sum()
                    if duplicates > 0:
                        error = f"❌ Duplicate order_ids in {filename}: {duplicates}"
                        errors.append(error)
                        print(f"   {error}")
                
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
                print(f"✅ Valid YAML: {yaml_file}")
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
            print("✅ requirements.txt exists")
            
            # Check for common issues
            if 'pyspark' in requirements and 'delta-spark' in requirements:
                print("✅ PySpark and Delta Lake dependencies found")
            else:
                warning = "⚠️  Missing PySpark or Delta Lake dependencies"
                warnings.append(warning)
                print(warning)
        except Exception as e:
            error = f"❌ Error reading requirements.txt: {e}"
            errors.append(error)
            print(error)
    else:
        error = "❌ Missing requirements.txt"
        errors.append(error)
        print(error)
    
    return errors, warnings

def check_python_code():
    """Check Python code for syntax errors"""
    print("\n🔍 CHECKING PYTHON CODE")
    print("========================")
    print()
    
    errors = []
    warnings = []
    
    # Check main Python files
    python_files = [
        'src/pyspark_interview_project/pipeline.py',
        'src/pyspark_interview_project/pipeline_core.py',
        'src/pyspark_interview_project/schema_validator.py'
    ]
    
    for py_file in python_files:
        if os.path.exists(py_file):
            try:
                with open(py_file, 'r') as f:
                    code = f.read()
                compile(code, py_file, 'exec')
                print(f"✅ Valid Python syntax: {py_file}")
            except SyntaxError as e:
                error = f"❌ Syntax error in {py_file}: {e}"
                errors.append(error)
                print(error)
            except Exception as e:
                error = f"❌ Error checking {py_file}: {e}"
                errors.append(error)
                print(error)
        else:
            warning = f"⚠️  Missing Python file: {py_file}"
            warnings.append(warning)
            print(warning)
    
    # Check script files
    script_files = [
        'scripts/generate_enhanced_sample_data.py',
        'scripts/generate_hubspot_data.py',
        'scripts/organize_final_data_sources.py',
        'scripts/simplify_data_sources.py',
        'scripts/comprehensive_data_quality_check.py',
        'scripts/analyze_schema_pattern.py',
        'scripts/analyze_data_sources_quantity.py',
        'scripts/cleanup_unnecessary_data.py'
    ]
    
    for script_file in script_files:
        if os.path.exists(script_file):
            try:
                with open(script_file, 'r') as f:
                    code = f.read()
                compile(code, script_file, 'exec')
                print(f"✅ Valid Python syntax: {script_file}")
            except SyntaxError as e:
                error = f"❌ Syntax error in {script_file}: {e}"
                errors.append(error)
                print(error)
            except Exception as e:
                error = f"❌ Error checking {script_file}: {e}"
                errors.append(error)
                print(error)
        else:
            warning = f"⚠️  Missing script file: {script_file}"
            warnings.append(warning)
            print(warning)
    
    return errors, warnings

def check_airflow_dags():
    """Check Airflow DAG files"""
    print("\n🔍 CHECKING AIRFLOW DAGS")
    print("========================")
    print()
    
    errors = []
    warnings = []
    
    # Check DAG files
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
                print(f"✅ Valid Python syntax: {dag_file}")
            except SyntaxError as e:
                error = f"❌ Syntax error in {dag_file}: {e}"
                errors.append(error)
                print(error)
            except Exception as e:
                error = f"❌ Error checking {dag_file}: {e}"
                errors.append(error)
                print(error)
        else:
            warning = f"⚠️  Missing DAG file: {dag_file}"
            warnings.append(warning)
            print(warning)
    
    return errors, warnings

def check_documentation():
    """Check documentation files"""
    print("\n🔍 CHECKING DOCUMENTATION")
    print("=========================")
    print()
    
    errors = []
    warnings = []
    
    # Check documentation files
    doc_files = [
        'docs/FINAL_DATA_SOURCES.md',
        'docs/SIMPLIFIED_DATA_SOURCES.md',
        'docs/DATA_QUALITY_REPORT.md',
        'docs/FINAL_DATA_SOURCES_ORGANIZATION.md'
    ]
    
    for doc_file in doc_files:
        if os.path.exists(doc_file):
            try:
                with open(doc_file, 'r') as f:
                    content = f.read()
                if len(content) > 100:  # Basic content check
                    print(f"✅ Documentation exists: {doc_file}")
                else:
                    warning = f"⚠️  Short documentation: {doc_file}"
                    warnings.append(warning)
                    print(warning)
            except Exception as e:
                error = f"❌ Error reading {doc_file}: {e}"
                errors.append(error)
                print(error)
        else:
            warning = f"⚠️  Missing documentation: {doc_file}"
            warnings.append(warning)
            print(warning)
    
    return errors, warnings

def check_data_relationships():
    """Check data relationships and consistency"""
    print("\n🔍 CHECKING DATA RELATIONSHIPS")
    print("===============================")
    print()
    
    errors = []
    warnings = []
    
    try:
        # Load key data files
        customers = pd.read_csv('aws/data_fixed/02_snowflake_warehouse/snowflake_customers_50000.csv')
        orders = pd.read_csv('aws/data_fixed/02_snowflake_warehouse/snowflake_orders_100000.csv')
        products = pd.read_csv('aws/data_fixed/02_snowflake_warehouse/snowflake_products_10000.csv')
        hubspot_contacts = pd.read_csv('aws/data_fixed/01_hubspot_crm/hubspot_contacts_25000.csv')
        
        # Check customer-order relationships
        customer_ids_in_orders = set(orders['customer_id'].unique())
        customer_ids_in_customers = set(customers['customer_id'].unique())
        orphaned_orders = customer_ids_in_orders - customer_ids_in_customers
        
        if len(orphaned_orders) > 0:
            error = f"❌ Orphaned orders (no customer): {len(orphaned_orders)}"
            errors.append(error)
            print(error)
        else:
            print("✅ All orders have valid customers")
        
        # Check product-order relationships
        product_ids_in_orders = set(orders['product_id'].unique())
        product_ids_in_products = set(products['product_id'].unique())
        orphaned_products = product_ids_in_orders - product_ids_in_products
        
        if len(orphaned_products) > 0:
            error = f"❌ Orphaned orders (no product): {len(orphaned_products)}"
            errors.append(error)
            print(error)
        else:
            print("✅ All orders have valid products")
        
        # Check HubSpot customer relationships
        hubspot_customer_ids = set(hubspot_contacts['customer_id'].dropna().unique())
        matching_customers = hubspot_customer_ids & customer_ids_in_customers
        match_rate = len(matching_customers) / len(hubspot_customer_ids) if len(hubspot_customer_ids) > 0 else 0
        
        if match_rate < 0.5:
            warning = f"⚠️  Low HubSpot customer match rate: {match_rate:.1%}"
            warnings.append(warning)
            print(warning)
        else:
            print(f"✅ HubSpot customer match rate: {match_rate:.1%}")
        
    except Exception as e:
        error = f"❌ Error checking data relationships: {e}"
        errors.append(error)
        print(error)
    
    return errors, warnings

def main():
    """Main project check function"""
    print("🚀 COMPREHENSIVE PROJECT ERROR CHECK")
    print("====================================")
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
        
        errors, warnings = check_data_quality()
        all_errors.extend(errors)
        all_warnings.extend(warnings)
        
        errors, warnings = check_config_files()
        all_errors.extend(errors)
        all_warnings.extend(warnings)
        
        errors, warnings = check_python_code()
        all_errors.extend(errors)
        all_warnings.extend(warnings)
        
        errors, warnings = check_airflow_dags()
        all_errors.extend(errors)
        all_warnings.extend(warnings)
        
        errors, warnings = check_documentation()
        all_errors.extend(errors)
        all_warnings.extend(warnings)
        
        errors, warnings = check_data_relationships()
        all_errors.extend(errors)
        all_warnings.extend(warnings)
        
        # Summary
        print("\n📊 PROJECT CHECK SUMMARY")
        print("========================")
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
            print("🎯 Project is in good condition")
        else:
            print("❌ CRITICAL ERRORS FOUND!")
            print("🔧 Please fix the errors above")
        
        return len(all_errors) == 0
        
    except Exception as e:
        print(f"\n❌ PROJECT CHECK FAILED: {e}")
        return False

if __name__ == "__main__":
    main()
