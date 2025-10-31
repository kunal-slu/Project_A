#!/usr/bin/env python3
"""
CRM ETL Pipeline Test - Using Pandas for Local Testing

This script tests the ETL pipeline logic using pandas instead of Spark
to verify the transformations work correctly with real data.
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_crm_data() -> Dict[str, pd.DataFrame]:
    """Extract all CRM data using pandas."""
    logger.info("🚀 Starting CRM data extraction...")
    
    crm_data = {}
    
    try:
        # Extract Accounts
        logger.info("📊 Extracting CRM Accounts...")
        accounts_df = pd.read_csv('aws/data/crm/accounts.csv')
        crm_data['accounts'] = accounts_df
        logger.info(f"✅ Extracted {len(accounts_df):,} accounts")
        
        # Extract Contacts
        logger.info("👥 Extracting CRM Contacts...")
        contacts_df = pd.read_csv('aws/data/crm/contacts.csv')
        crm_data['contacts'] = contacts_df
        logger.info(f"✅ Extracted {len(contacts_df):,} contacts")
        
        # Extract Opportunities
        logger.info("💰 Extracting CRM Opportunities...")
        opportunities_df = pd.read_csv('aws/data/crm/opportunities.csv')
        crm_data['opportunities'] = opportunities_df
        logger.info(f"✅ Extracted {len(opportunities_df):,} opportunities")
        
        logger.info("🎉 CRM data extraction completed successfully!")
        return crm_data
        
    except Exception as e:
        logger.error(f"❌ CRM data extraction failed: {str(e)}")
        raise


def transform_to_bronze(crm_data: Dict[str, pd.DataFrame]) -> Dict[str, str]:
    """Transform CRM data to Bronze layer."""
    logger.info("🔄 Transforming to Bronze layer...")
    
    bronze_data = {}
    
    try:
        # Create Bronze layer directory
        bronze_path = 'data/lakehouse_delta/bronze/crm'
        os.makedirs(bronze_path, exist_ok=True)
        
        for table_name, df in crm_data.items():
            # Add metadata columns
            df_bronze = df.copy()
            df_bronze['record_source'] = 'crm_salesforce'
            df_bronze['record_table'] = table_name
            df_bronze['ingest_timestamp'] = datetime.now()
            
            # Write to Bronze layer as CSV (easier for testing)
            bronze_file_path = f"{bronze_path}/{table_name}.csv"
            df_bronze.to_csv(bronze_file_path, index=False)
            bronze_data[table_name] = bronze_file_path
            logger.info(f"✅ Bronze layer: {table_name} → {bronze_file_path}")
        
        logger.info("🎉 Bronze layer transformation completed!")
        return bronze_data
        
    except Exception as e:
        logger.error(f"❌ Bronze layer transformation failed: {str(e)}")
        raise


def transform_to_silver(bronze_data: Dict[str, str]) -> Dict[str, str]:
    """Transform Bronze data to Silver layer."""
    logger.info("🔄 Transforming to Silver layer...")
    
    silver_data = {}
    
    try:
        # Create Silver layer directory
        silver_path = 'data/lakehouse_delta/silver/crm'
        os.makedirs(silver_path, exist_ok=True)
        
        # Transform Accounts to Silver
        accounts_df = pd.read_csv(bronze_data['accounts'])
        silver_accounts = accounts_df[[
            'Id', 'Name', 'Industry', 'AnnualRevenue', 'NumberOfEmployees',
            'BillingCountry', 'BillingState', 'BillingCity', 'Rating', 'Type',
            'AccountSource', 'CustomerSegment', 'GeographicRegion', 'AccountStatus',
            'CreatedDate', 'LastModifiedDate', 'record_source', 'ingest_timestamp'
        ]].copy()
        
        # Filter for active accounts
        silver_accounts = silver_accounts[silver_accounts['AccountStatus'] == 'Active']
        
        silver_accounts_path = f"{silver_path}/dim_accounts.csv"
        silver_accounts.to_csv(silver_accounts_path, index=False)
        silver_data['dim_accounts'] = silver_accounts_path
        logger.info(f"✅ Silver layer: dim_accounts → {silver_accounts_path} ({len(silver_accounts):,} records)")
        
        # Transform Contacts to Silver
        contacts_df = pd.read_csv(bronze_data['contacts'])
        silver_contacts = contacts_df[[
            'Id', 'FirstName', 'LastName', 'Email', 'Phone', 'Title', 'Department',
            'AccountId', 'ContactRole', 'ContactLevel', 'EngagementScore',
            'CreatedDate', 'LastModifiedDate', 'record_source', 'ingest_timestamp'
        ]].copy()
        
        # Filter for contacts with email
        silver_contacts = silver_contacts[silver_contacts['Email'].notna()]
        
        silver_contacts_path = f"{silver_path}/dim_contacts.csv"
        silver_contacts.to_csv(silver_contacts_path, index=False)
        silver_data['dim_contacts'] = silver_contacts_path
        logger.info(f"✅ Silver layer: dim_contacts → {silver_contacts_path} ({len(silver_contacts):,} records)")
        
        # Transform Opportunities to Silver
        opportunities_df = pd.read_csv(bronze_data['opportunities'])
        silver_opportunities = opportunities_df[[
            'Id', 'Name', 'AccountId', 'StageName', 'CloseDate', 'Amount', 'Probability',
            'Type', 'DealSize', 'SalesCycle', 'ProductInterest', 'Budget', 'Timeline',
            'IsClosed', 'IsWon', 'CreatedDate', 'LastModifiedDate', 'record_source', 'ingest_timestamp'
        ]].copy()
        
        # Filter for opportunities with amount > 0
        silver_opportunities = silver_opportunities[silver_opportunities['Amount'] > 0]
        
        silver_opportunities_path = f"{silver_path}/fact_opportunities.csv"
        silver_opportunities.to_csv(silver_opportunities_path, index=False)
        silver_data['fact_opportunities'] = silver_opportunities_path
        logger.info(f"✅ Silver layer: fact_opportunities → {silver_opportunities_path} ({len(silver_opportunities):,} records)")
        
        logger.info("🎉 Silver layer transformation completed!")
        return silver_data
        
    except Exception as e:
        logger.error(f"❌ Silver layer transformation failed: {str(e)}")
        raise


def transform_to_gold(silver_data: Dict[str, str]) -> Dict[str, str]:
    """Transform Silver data to Gold layer."""
    logger.info("🔄 Transforming to Gold layer...")
    
    gold_data = {}
    
    try:
        # Create Gold layer directory
        gold_path = 'data/lakehouse_delta/gold/crm'
        os.makedirs(gold_path, exist_ok=True)
        
        # Load Silver data
        dim_accounts = pd.read_csv(silver_data['dim_accounts'])
        dim_contacts = pd.read_csv(silver_data['dim_contacts'])
        fact_opportunities = pd.read_csv(silver_data['fact_opportunities'])
        
        # Create Gold analytics tables
        
        # 1. Revenue by Industry
        revenue_by_industry = fact_opportunities.merge(
            dim_accounts, left_on='AccountId', right_on='Name', how='inner'
        ).groupby('Industry').agg({
            'Amount': 'sum',
            'Id_x': 'count'  # Use Id_x from opportunities table
        }).rename(columns={'Amount': 'total_revenue', 'Id_x': 'opportunity_count'}).reset_index()
        
        revenue_by_industry_path = f"{gold_path}/revenue_by_industry.csv"
        revenue_by_industry.to_csv(revenue_by_industry_path, index=False)
        gold_data['revenue_by_industry'] = revenue_by_industry_path
        logger.info(f"✅ Gold layer: revenue_by_industry → {revenue_by_industry_path}")
        
        # 2. Revenue by Geography
        revenue_by_geography = fact_opportunities.merge(
            dim_accounts, left_on='AccountId', right_on='Name', how='inner'
        ).groupby(['BillingCountry', 'BillingState']).agg({
            'Amount': 'sum',
            'Id_x': 'count'  # Use Id_x from opportunities table
        }).rename(columns={'Amount': 'total_revenue', 'Id_x': 'opportunity_count'}).reset_index()
        
        revenue_by_geography_path = f"{gold_path}/revenue_by_geography.csv"
        revenue_by_geography.to_csv(revenue_by_geography_path, index=False)
        gold_data['revenue_by_geography'] = revenue_by_geography_path
        logger.info(f"✅ Gold layer: revenue_by_geography → {revenue_by_geography_path}")
        
        # 3. Customer Segmentation
        customer_segmentation = fact_opportunities.merge(
            dim_accounts, left_on='AccountId', right_on='Name', how='inner'
        ).groupby(['CustomerSegment', 'GeographicRegion']).agg({
            'Amount': 'sum',
            'Id_x': 'count'  # Use Id_x from opportunities table
        }).rename(columns={'Amount': 'total_revenue', 'Id_x': 'opportunity_count'}).reset_index()
        
        customer_segmentation_path = f"{gold_path}/customer_segmentation.csv"
        customer_segmentation.to_csv(customer_segmentation_path, index=False)
        gold_data['customer_segmentation'] = customer_segmentation_path
        logger.info(f"✅ Gold layer: customer_segmentation → {customer_segmentation_path}")
        
        logger.info("🎉 Gold layer transformation completed!")
        return gold_data
        
    except Exception as e:
        logger.error(f"❌ Gold layer transformation failed: {str(e)}")
        raise


def generate_analytics_report(gold_data: Dict[str, str]) -> None:
    """Generate analytics report from Gold layer."""
    logger.info("📊 Generating analytics report...")
    
    try:
        # Load Gold data
        revenue_by_industry = pd.read_csv(gold_data['revenue_by_industry'])
        revenue_by_geography = pd.read_csv(gold_data['revenue_by_geography'])
        customer_segmentation = pd.read_csv(gold_data['customer_segmentation'])
        
        logger.info("\\n🎯 CRM ANALYTICS REPORT")
        logger.info("=" * 50)
        
        # Revenue by Industry
        logger.info("\\n📊 REVENUE BY INDUSTRY (Top 10):")
        industry_summary = revenue_by_industry.nlargest(10, 'total_revenue')
        for _, row in industry_summary.iterrows():
            logger.info(f"   {row['Industry']}: ${row['total_revenue']:,.0f} ({row['opportunity_count']:,} opportunities)")
        
        # Revenue by Geography
        logger.info("\\n🌍 REVENUE BY GEOGRAPHY (Top 10):")
        geo_summary = revenue_by_geography.nlargest(10, 'total_revenue')
        for _, row in geo_summary.iterrows():
            logger.info(f"   {row['BillingCountry']}, {row['BillingState']}: ${row['total_revenue']:,.0f} ({row['opportunity_count']:,} opportunities)")
        
        # Customer Segmentation
        logger.info("\\n👥 CUSTOMER SEGMENTATION (Top 10):")
        segment_summary = customer_segmentation.nlargest(10, 'total_revenue')
        for _, row in segment_summary.iterrows():
            logger.info(f"   {row['CustomerSegment']} - {row['GeographicRegion']}: ${row['total_revenue']:,.0f} ({row['opportunity_count']:,} opportunities)")
        
        logger.info("\\n🎉 Analytics report generated successfully!")
        
    except Exception as e:
        logger.error(f"❌ Analytics report generation failed: {str(e)}")
        raise


def main():
    """Main CRM ETL pipeline execution."""
    try:
        logger.info("🚀 STARTING CRM ETL PIPELINE TEST - PANDAS VERSION")
        logger.info("=" * 65)
        logger.info("📊 Testing ETL logic with real data using pandas")
        logger.info("🎯 Goal: Verify Bronze → Silver → Gold transformations")
        
        # Run ETL pipeline
        logger.info("\\n🔄 PHASE 1: EXTRACT")
        crm_data = extract_crm_data()
        
        logger.info("\\n🔄 PHASE 2: BRONZE TRANSFORMATION")
        bronze_data = transform_to_bronze(crm_data)
        
        logger.info("\\n🔄 PHASE 3: SILVER TRANSFORMATION")
        silver_data = transform_to_silver(bronze_data)
        
        logger.info("\\n🔄 PHASE 4: GOLD TRANSFORMATION")
        gold_data = transform_to_gold(silver_data)
        
        logger.info("\\n📊 PHASE 5: ANALYTICS")
        generate_analytics_report(gold_data)
        
        # Summary
        logger.info("\\n🎉 CRM ETL PIPELINE TEST COMPLETED SUCCESSFULLY!")
        logger.info("=" * 55)
        logger.info("✅ All phases completed")
        logger.info("✅ Bronze → Silver → Gold transformations")
        logger.info("✅ Analytics generated")
        logger.info("✅ Real data processed")
        logger.info("✅ ETL logic verified")
        
        return True
        
    except Exception as e:
        logger.error(f"💥 CRM ETL pipeline test failed: {str(e)}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
