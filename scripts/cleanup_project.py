#!/usr/bin/env python3
"""
Project Cleanup Script
Removes unnecessary files and makes the project clean and readable.
"""

import os
import shutil

def cleanup_unnecessary_files():
    """Remove unnecessary files from the project"""
    print("🧹 CLEANING UP PROJECT")
    print("======================")
    print()
    
    # Files to keep (essential only)
    essential_files = {
        'aws/data_fixed/01_hubspot_crm': [
            'hubspot_contacts_25000.csv',
            'hubspot_deals_30000.csv'
        ],
        'aws/data_fixed/02_snowflake_warehouse': [
            'snowflake_customers_50000.csv',
            'snowflake_orders_100000.csv',
            'snowflake_products_10000.csv'
        ],
        'aws/data_fixed/03_redshift_analytics': [
            'redshift_customer_behavior_50000.csv'
        ],
        'aws/data_fixed/04_stream_data': [
            'stream_kafka_events_100000.csv'
        ],
        'aws/data_fixed/05_fx_rates': [
            'fx_rates_historical_730_days.csv'
        ]
    }
    
    # Essential Python files to keep
    essential_python_files = [
        'src/pyspark_interview_project/pipeline.py',
        'src/pyspark_interview_project/pipeline_core.py',
        'src/pyspark_interview_project/schema_validator.py',
        'src/pyspark_interview_project/utils/spark_session.py',
        'src/pyspark_interview_project/dq/runner.py',
        'src/pyspark_interview_project/io/path_resolver.py',
        'src/pyspark_interview_project/lineage/openlineage_emitter.py'
    ]
    
    # Essential config files to keep
    essential_config_files = [
        'config/default.yaml',
        'config/aws.yaml',
        'config/azure.yaml',
        'config/local.yaml',
        'requirements.txt',
        'pytest.ini'
    ]
    
    # Essential Airflow files to keep
    essential_airflow_files = [
        'airflow/dags/pyspark_etl_dag.py',
        'airflow/dags/external_data_ingestion_dag.py'
    ]
    
    # Essential documentation files to keep
    essential_doc_files = [
        'docs/SIMPLIFIED_DATA_SOURCES.md',
        'docs/DATA_QUALITY_REPORT.md'
    ]
    
    removed_count = 0
    
    print("🗑️  REMOVING UNNECESSARY DATA FILES:")
    print("====================================")
    
    # Clean up data files
    for source_dir, essential_list in essential_files.items():
        if os.path.exists(source_dir):
            all_files = [f for f in os.listdir(source_dir) if f.endswith('.csv')]
            for file in all_files:
                if file not in essential_list:
                    file_path = f"{source_dir}/{file}"
                    try:
                        os.remove(file_path)
                        print(f"✅ Removed: {file}")
                        removed_count += 1
                    except Exception as e:
                        print(f"❌ Failed to remove {file}: {e}")
    
    print("\n🗑️  REMOVING UNNECESSARY SCRIPT FILES:")
    print("=====================================")
    
    # Remove unnecessary script files
    script_files_to_remove = [
        'scripts/generate_enhanced_sample_data.py',
        'scripts/generate_hubspot_data.py',
        'scripts/organize_final_data_sources.py',
        'scripts/simplify_data_sources.py',
        'scripts/comprehensive_data_quality_check.py',
        'scripts/analyze_schema_pattern.py',
        'scripts/analyze_data_sources_quantity.py',
        'scripts/cleanup_unnecessary_data.py',
        'scripts/validate_enhanced_data.py',
        'scripts/reorganize_data_sources.py',
        'scripts/fix_all_errors.py',
        'scripts/cleanup_project.py'
    ]
    
    for script_file in script_files_to_remove:
        if os.path.exists(script_file):
            try:
                os.remove(script_file)
                print(f"✅ Removed: {script_file}")
                removed_count += 1
            except Exception as e:
                print(f"❌ Failed to remove {script_file}: {e}")
    
    print("\n🗑️  REMOVING UNNECESSARY DOCUMENTATION:")
    print("======================================")
    
    # Remove unnecessary documentation files
    doc_files_to_remove = [
        'docs/FINAL_DATA_SOURCES.md',
        'docs/FINAL_DATA_SOURCES_ORGANIZATION.md',
        'docs/ENHANCED_DATA_ANALYSIS.md'
    ]
    
    for doc_file in doc_files_to_remove:
        if os.path.exists(doc_file):
            try:
                os.remove(doc_file)
                print(f"✅ Removed: {doc_file}")
                removed_count += 1
            except Exception as e:
                print(f"❌ Failed to remove {doc_file}: {e}")
    
    print("\n🗑️  REMOVING UNNECESSARY CONFIG FILES:")
    print("=====================================")
    
    # Remove unnecessary config files
    config_files_to_remove = [
        'config/config-template.yaml',
        'config/config-dev.yaml',
        'config/config-prod.yaml',
        'config/aws/config-simple.yaml',
        'config/aws/connections.yaml',
        'config/aws/config-aws-enterprise.yaml',
        'config/aws/config-aws-enterprise-internal.yaml'
    ]
    
    for config_file in config_files_to_remove:
        if os.path.exists(config_file):
            try:
                os.remove(config_file)
                print(f"✅ Removed: {config_file}")
                removed_count += 1
            except Exception as e:
                print(f"❌ Failed to remove {config_file}: {e}")
    
    print("\n🗑️  REMOVING UNNECESSARY PYTHON MODULES:")
    print("=======================================")
    
    # Remove unnecessary Python modules
    python_modules_to_remove = [
        'src/pyspark_interview_project/jobs/redshift_to_bronze.py',
        'src/pyspark_interview_project/jobs/hubspot_to_bronze.py',
        'src/pyspark_interview_project/dq/external_data_quality.py',
        'src/pyspark_interview_project/extract.py',
        'src/pyspark_interview_project/streaming.py'
    ]
    
    for module_file in python_modules_to_remove:
        if os.path.exists(module_file):
            try:
                os.remove(module_file)
                print(f"✅ Removed: {module_file}")
                removed_count += 1
            except Exception as e:
                print(f"❌ Failed to remove {module_file}: {e}")
    
    print("\n🗑️  REMOVING UNNECESSARY SHELL SCRIPTS:")
    print("======================================")
    
    # Remove unnecessary shell scripts
    shell_scripts_to_remove = [
        'scripts/compact_dlq_weekly.sh',
        'scripts/run_local_batch.sh'
    ]
    
    for script_file in shell_scripts_to_remove:
        if os.path.exists(script_file):
            try:
                os.remove(script_file)
                print(f"✅ Removed: {script_file}")
                removed_count += 1
            except Exception as e:
                print(f"❌ Failed to remove {script_file}: {e}")
    
    print("\n🗑️  REMOVING UNNECESSARY MARKDOWN FILES:")
    print("======================================")
    
    # Remove unnecessary markdown files
    markdown_files_to_remove = [
        'PACKAGING_OBSERVABILITY_IMPROVEMENTS.md',
        'README.md'
    ]
    
    for md_file in markdown_files_to_remove:
        if os.path.exists(md_file):
            try:
                os.remove(md_file)
                print(f"✅ Removed: {md_file}")
                removed_count += 1
            except Exception as e:
                print(f"❌ Failed to remove {md_file}: {e}")
    
    return removed_count

def create_clean_readme():
    """Create a clean, simple README"""
    print("\n📝 CREATING CLEAN README:")
    print("=========================")
    
    readme_content = """# PySpark Data Engineering Project

## 🎯 Project Overview

This project provides a comprehensive PySpark data engineering pipeline with 5 essential data sources for learning and practice.

## 📊 Data Sources

### 1️⃣ HubSpot CRM
- **Contacts**: Customer contact information (25K records)
- **Deals**: Sales opportunities and pipeline (30K records)

### 2️⃣ Snowflake Warehouse
- **Customers**: Customer master data (50K records)
- **Orders**: Order transactions (100K records)
- **Products**: Product catalog (10K records)

### 3️⃣ Redshift Analytics
- **Customer Behavior**: User behavior analytics (50K records)

### 4️⃣ Stream Data
- **Kafka Events**: Real-time event streaming (100K records)

### 5️⃣ FX Rates
- **Historical Rates**: Exchange rates (20K records)

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- PySpark 3.5+
- Delta Lake

### Installation
```bash
pip install -r requirements.txt
```

### Running the Pipeline
```bash
python src/pyspark_interview_project/pipeline.py config/config-dev.yaml
```

## 📁 Project Structure

```
├── aws/data_fixed/           # Data sources
│   ├── 01_hubspot_crm/       # HubSpot CRM data
│   ├── 02_snowflake_warehouse/ # Snowflake warehouse data
│   ├── 03_redshift_analytics/ # Redshift analytics data
│   ├── 04_stream_data/       # Streaming data
│   └── 05_fx_rates/          # FX rates data
├── config/                   # Configuration files
├── src/pyspark_interview_project/ # Main pipeline code
├── airflow/dags/             # Airflow DAGs
└── docs/                     # Documentation
```

## 🎯 Learning Objectives

- **Data Engineering**: ETL pipelines, data quality, transformations
- **Analytics**: Aggregations, window functions, statistical analysis
- **Performance**: Optimization, partitioning, caching strategies
- **Integration**: Multi-source data integration
- **Real-time Processing**: Streaming data and event processing

## 📚 Documentation

- [Simplified Data Sources](docs/SIMPLIFIED_DATA_SOURCES.md)
- [Data Quality Report](docs/DATA_QUALITY_REPORT.md)

## 🔧 Configuration

All configurations are managed in the `config/` directory:
- `default.yaml` - Base configuration
- `aws.yaml` - AWS-specific settings
- `azure.yaml` - Azure-specific settings
- `local.yaml` - Local development settings

## 🚀 Ready for PySpark Practice!

This project provides realistic, high-quality data for comprehensive PySpark learning and practice.
"""
    
    with open('README.md', 'w') as f:
        f.write(readme_content)
    
    print("✅ Created clean README.md")

def verify_clean_structure():
    """Verify the clean project structure"""
    print("\n🔍 VERIFYING CLEAN PROJECT STRUCTURE:")
    print("====================================")
    print()
    
    # Check essential directories
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
            files = [f for f in os.listdir(dir_path) if not f.startswith('.')]
            print(f"✅ {dir_path}: {len(files)} files")
        else:
            print(f"❌ Missing: {dir_path}")
    
    # Check essential files
    essential_files = [
        'README.md',
        'requirements.txt',
        'config/default.yaml',
        'config/aws.yaml',
        'src/pyspark_interview_project/pipeline.py',
        'airflow/dags/pyspark_etl_dag.py',
        'docs/SIMPLIFIED_DATA_SOURCES.md'
    ]
    
    print("\n📁 ESSENTIAL FILES:")
    for file_path in essential_files:
        if os.path.exists(file_path):
            print(f"✅ {file_path}")
        else:
            print(f"❌ Missing: {file_path}")

def main():
    """Main cleanup function"""
    print("🚀 CLEANING UP PROJECT")
    print("======================")
    print()
    
    try:
        # Remove unnecessary files
        removed_count = cleanup_unnecessary_files()
        
        # Create clean README
        create_clean_readme()
        
        # Verify clean structure
        verify_clean_structure()
        
        print(f"\n✅ PROJECT CLEANUP COMPLETE!")
        print("============================")
        print(f"📊 Removed {removed_count} unnecessary files")
        print("🎯 Project is now clean and readable")
        print("🎯 Only essential files remain")
        print("🎯 Perfect for PySpark practice")
        
    except Exception as e:
        print(f"\n❌ CLEANUP FAILED: {e}")
        return False
    
    return True

if __name__ == "__main__":
    main()
