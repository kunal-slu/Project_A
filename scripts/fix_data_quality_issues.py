#!/usr/bin/env python3
"""
Fix critical data quality issues in the data sources.
This script addresses the major problems identified in the data analysis.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import uuid

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fix_duplicate_customer_ids():
    """Fix duplicate customer_ids in Snowflake Orders."""
    logger.info("Fixing duplicate customer_ids in Snowflake Orders...")
    
    # Read the orders file
    orders_file = Path("aws/data_fixed/snowflake_orders_100000.csv")
    if not orders_file.exists():
        logger.error(f"Orders file not found: {orders_file}")
        return
    
    # Load data
    df = pd.read_csv(orders_file)
    logger.info(f"Original orders count: {len(df)}")
    
    # Check for duplicates
    duplicate_count = df['customer_id'].duplicated().sum()
    logger.info(f"Found {duplicate_count} duplicate customer_ids")
    
    # Remove duplicates, keeping the first occurrence
    df_cleaned = df.drop_duplicates(subset=['customer_id'], keep='first')
    logger.info(f"After removing duplicates: {len(df_cleaned)} records")
    
    # Save cleaned data
    df_cleaned.to_csv(orders_file, index=False)
    logger.info("✅ Fixed duplicate customer_ids in Snowflake Orders")

def generate_consistent_customer_ids():
    """Generate consistent customer_id across all sources."""
    logger.info("Generating consistent customer_id across all sources...")
    
    # Read HubSpot contacts
    contacts_file = Path("aws/data_fixed/hubspot_contacts_25000.csv")
    if contacts_file.exists():
        df_contacts = pd.read_csv(contacts_file)
        
        # Generate customer_id for HubSpot contacts
        df_contacts['customer_id'] = df_contacts['hubspot_contact_id'].apply(
            lambda x: f"CUST-{x.split('-')[-1].zfill(6)}"
        )
        
        # Save updated contacts
        df_contacts.to_csv(contacts_file, index=False)
        logger.info("✅ Generated customer_id for HubSpot contacts")
    
    # Read HubSpot deals
    deals_file = Path("aws/data_fixed/hubspot_deals_30000.csv")
    if deals_file.exists():
        df_deals = pd.read_csv(deals_file)
        
        # Generate customer_id for HubSpot deals
        df_deals['customer_id'] = df_deals['hubspot_deal_id'].apply(
            lambda x: f"CUST-{x.split('-')[-1].zfill(6)}"
        )
        
        # Save updated deals
        df_deals.to_csv(deals_file, index=False)
        logger.info("✅ Generated customer_id for HubSpot deals")

def standardize_date_formats():
    """Standardize date formats across all sources."""
    logger.info("Standardizing date formats...")
    
    # List of files to process
    files_to_process = [
        "aws/data_fixed/hubspot_contacts_25000.csv",
        "aws/data_fixed/hubspot_deals_30000.csv",
        "aws/data_fixed/snowflake_orders_100000.csv",
        "aws/data_fixed/redshift_customer_behavior_50000.csv"
    ]
    
    for file_path in files_to_process:
        if Path(file_path).exists():
            df = pd.read_csv(file_path)
            
            # Find date columns
            date_columns = [col for col in df.columns if 'date' in col.lower() or 'timestamp' in col.lower()]
            
            for col in date_columns:
                if col in df.columns:
                    try:
                        # Convert to datetime and then to ISO format
                        df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
                        logger.info(f"✅ Standardized {col} in {file_path}")
                    except Exception as e:
                        logger.warning(f"Could not standardize {col} in {file_path}: {e}")
            
            # Save updated file
            df.to_csv(file_path, index=False)

def add_data_validation_rules():
    """Add basic data validation rules."""
    logger.info("Adding data validation rules...")
    
    # Create validation rules file
    validation_rules = {
        "customers": {
            "required_fields": ["customer_id", "email", "first_name", "last_name"],
            "unique_fields": ["customer_id", "email"],
            "numeric_ranges": {
                "lead_score": {"min": 0, "max": 100}
            }
        },
        "orders": {
            "required_fields": ["order_id", "customer_id", "product_id", "order_date"],
            "unique_fields": ["order_id"],
            "numeric_ranges": {
                "quantity": {"min": 1, "max": 1000},
                "unit_price": {"min": 0.01, "max": 10000}
            }
        },
        "products": {
            "required_fields": ["product_id", "product_name", "category"],
            "unique_fields": ["product_id"],
            "numeric_ranges": {
                "price": {"min": 0.01, "max": 10000}
            }
        }
    }
    
    # Save validation rules
    import json
    with open("data_validation_rules.json", "w") as f:
        json.dump(validation_rules, f, indent=2)
    
    logger.info("✅ Created data validation rules")

def create_data_quality_report():
    """Create a comprehensive data quality report."""
    logger.info("Creating data quality report...")
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "data_sources": {},
        "summary": {
            "total_records": 0,
            "duplicate_records": 0,
            "missing_values": 0,
            "data_quality_score": 0
        }
    }
    
    # Analyze each data source
    data_files = [
        ("hubspot_contacts", "aws/data_fixed/hubspot_contacts_25000.csv"),
        ("hubspot_deals", "aws/data_fixed/hubspot_deals_30000.csv"),
        ("snowflake_customers", "aws/data_fixed/snowflake_customers_50000.csv"),
        ("snowflake_orders", "aws/data_fixed/snowflake_orders_100000.csv"),
        ("snowflake_products", "aws/data_fixed/snowflake_products_10000.csv"),
        ("redshift_behavior", "aws/data_fixed/redshift_customer_behavior_50000.csv"),
        ("stream_events", "aws/data_fixed/stream_kafka_events_100000.csv"),
        ("fx_rates", "aws/data_fixed/fx_rates_historical_730_days.csv")
    ]
    
    for source_name, file_path in data_files:
        if Path(file_path).exists():
            df = pd.read_csv(file_path)
            
            source_report = {
                "record_count": int(len(df)),
                "columns": list(df.columns),
                "duplicates": int(df.duplicated().sum()),
                "missing_values": {k: int(v) for k, v in df.isnull().sum().to_dict().items()},
                "data_types": {k: str(v) for k, v in df.dtypes.to_dict().items()}
            }
            
            report["data_sources"][source_name] = source_report
            report["summary"]["total_records"] += int(len(df))
            report["summary"]["duplicate_records"] += int(df.duplicated().sum())
            report["summary"]["missing_values"] += int(df.isnull().sum().sum())
    
    # Calculate data quality score
    total_records = report["summary"]["total_records"]
    total_issues = report["summary"]["duplicate_records"] + report["summary"]["missing_values"]
    quality_score = max(0, 100 - (total_issues / total_records * 100)) if total_records > 0 else 0
    report["summary"]["data_quality_score"] = round(quality_score, 2)
    
    # Save report
    import json
    with open("data_quality_report.json", "w") as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"✅ Data quality score: {quality_score:.2f}%")
    logger.info("✅ Created comprehensive data quality report")

def main():
    """Main function to fix all data quality issues."""
    logger.info("🚀 Starting data quality fixes...")
    
    try:
        # Fix critical issues
        fix_duplicate_customer_ids()
        generate_consistent_customer_ids()
        standardize_date_formats()
        add_data_validation_rules()
        create_data_quality_report()
        
        logger.info("✅ All data quality fixes completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error during data quality fixes: {e}")
        raise

if __name__ == "__main__":
    main()
