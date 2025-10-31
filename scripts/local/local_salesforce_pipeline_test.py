#!/usr/bin/env python3
"""
Local Salesforce Pipeline Test

This script runs the complete Salesforce ETL pipeline locally using sample data,
simulating Bronze → Silver → Gold transformations.
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import yaml

# Add project root to path
sys.path.append(str(Path(__file__).parent / "src"))

from pyspark_interview_project.utils.spark_session import build_spark

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LocalSalesforcePipelineTest:
    """Local Salesforce ETL pipeline test."""
    
    def __init__(self, config_path: str):
        self.config = self.load_config(config_path)
        self.spark = build_spark("LocalSalesforceTest", self.config)
        self.data_dir = Path("data/salesforce")
        self.output_dir = Path("_salesforce_pipeline_output")
        
        # Create output directories
        self.output_dir.mkdir(exist_ok=True)
        (self.output_dir / "bronze").mkdir(exist_ok=True)
        (self.output_dir / "silver").mkdir(exist_ok=True)
        (self.output_dir / "gold").mkdir(exist_ok=True)
        
    def load_config(self, config_path: str):
        """Load configuration from YAML file."""
        with open(config_path) as f:
            return yaml.safe_load(f)
    
    def validate_schema(self, df, schema_path: str) -> bool:
        """Validate DataFrame against schema contract."""
        logger.info(f"Validating schema: {schema_path}")
        
        try:
            with open(schema_path) as f:
                schema = json.load(f)
            
            # Check required columns
            required_cols = schema.get("required_non_null", [])
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                logger.error(f"Missing required columns: {missing_cols}")
                return False
            
            # Check for null values in required columns
            for col in required_cols:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    logger.warning(f"Column {col} has {null_count} null values")
            
            logger.info("✅ Schema validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Schema validation failed: {str(e)}")
            return False
    
    def bronze_ingestion(self):
        """Simulate Bronze layer ingestion for Salesforce data."""
        logger.info("🔄 Starting Bronze layer ingestion for Salesforce...")
        
        # Read and validate accounts
        accounts_df = pd.read_csv(self.data_dir / "salesforce_accounts.csv")
        if not self.validate_schema(accounts_df, "config/schema_definitions/salesforce_accounts_bronze.json"):
            return False
        
        accounts_df.to_parquet(self.output_dir / "bronze" / "salesforce_accounts.parquet")
        logger.info(f"✅ Accounts ingested: {len(accounts_df)} records")
        
        # Read and validate contacts
        contacts_df = pd.read_csv(self.data_dir / "salesforce_contacts.csv")
        if not self.validate_schema(contacts_df, "config/schema_definitions/salesforce_contacts_bronze.json"):
            return False
        
        contacts_df.to_parquet(self.output_dir / "bronze" / "salesforce_contacts.parquet")
        logger.info(f"✅ Contacts ingested: {len(contacts_df)} records")
        
        # Read and validate opportunities
        opportunities_df = pd.read_csv(self.data_dir / "salesforce_opportunities.csv")
        if not self.validate_schema(opportunities_df, "config/schema_definitions/salesforce_opportunities_bronze.json"):
            return False
        
        opportunities_df.to_parquet(self.output_dir / "bronze" / "salesforce_opportunities.parquet")
        logger.info(f"✅ Opportunities ingested: {len(opportunities_df)} records")
        
        logger.info("🎉 Bronze ingestion completed successfully!")
        return True
    
    def silver_transformation(self):
        """Simulate Silver layer transformation for Salesforce data."""
        logger.info("🔄 Starting Silver layer transformation for Salesforce...")
        
        # Load Bronze data
        accounts_df = pd.read_parquet(self.output_dir / "bronze" / "salesforce_accounts.parquet")
        contacts_df = pd.read_parquet(self.output_dir / "bronze" / "salesforce_contacts.parquet")
        opportunities_df = pd.read_parquet(self.output_dir / "bronze" / "salesforce_opportunities.parquet")
        
        # Clean accounts data
        accounts_clean = accounts_df.copy()
        accounts_clean['account_name'] = accounts_clean['account_name'].str.strip()
        accounts_clean['industry'] = accounts_clean['industry'].str.title()
        accounts_clean['created_date'] = pd.to_datetime(accounts_clean['created_date'])
        accounts_clean['last_modified_date'] = pd.to_datetime(accounts_clean['last_modified_date'])
        
        # Remove duplicates
        accounts_clean = accounts_clean.drop_duplicates(subset=['account_id'])
        
        accounts_clean.to_parquet(self.output_dir / "silver" / "accounts_clean.parquet")
        logger.info(f"✅ Accounts cleaned: {len(accounts_clean)} records")
        
        # Clean contacts data
        contacts_clean = contacts_df.copy()
        contacts_clean['email'] = contacts_clean['email'].str.lower().str.strip()
        contacts_clean['first_name'] = contacts_clean['first_name'].str.title()
        contacts_clean['last_name'] = contacts_clean['last_name'].str.title()
        contacts_clean['created_date'] = pd.to_datetime(contacts_clean['created_date'])
        contacts_clean['last_modified_date'] = pd.to_datetime(contacts_clean['last_modified_date'])
        
        # Remove duplicates
        contacts_clean = contacts_clean.drop_duplicates(subset=['contact_id'])
        
        contacts_clean.to_parquet(self.output_dir / "silver" / "contacts_clean.parquet")
        logger.info(f"✅ Contacts cleaned: {len(contacts_clean)} records")
        
        # Clean opportunities data
        opportunities_clean = opportunities_df.copy()
        opportunities_clean['opportunity_name'] = opportunities_clean['opportunity_name'].str.strip()
        opportunities_clean['stage_name'] = opportunities_clean['stage_name'].str.strip()
        opportunities_clean['close_date'] = pd.to_datetime(opportunities_clean['close_date'])
        opportunities_clean['created_date'] = pd.to_datetime(opportunities_clean['created_date'])
        opportunities_clean['last_modified_date'] = pd.to_datetime(opportunities_clean['last_modified_date'])
        
        # Remove duplicates
        opportunities_clean = opportunities_clean.drop_duplicates(subset=['opportunity_id'])
        
        opportunities_clean.to_parquet(self.output_dir / "silver" / "opportunities_clean.parquet")
        logger.info(f"✅ Opportunities cleaned: {len(opportunities_clean)} records")
        
        logger.info("🎉 Silver transformation completed successfully!")
        return True
    
    def gold_business_logic(self):
        """Simulate Gold layer business logic for Salesforce data."""
        logger.info("🔄 Starting Gold layer business logic for Salesforce...")
        
        # Load Silver data
        accounts_df = pd.read_parquet(self.output_dir / "silver" / "accounts_clean.parquet")
        contacts_df = pd.read_parquet(self.output_dir / "silver" / "contacts_clean.parquet")
        opportunities_df = pd.read_parquet(self.output_dir / "silver" / "opportunities_clean.parquet")
        
        # Create account dimension
        account_dim = accounts_df[['account_id', 'account_name', 'account_type', 'industry', 
                                 'billing_city', 'billing_state', 'billing_country', 
                                 'annual_revenue', 'number_of_employees', 'rating']].copy()
        
        # Add account segments based on revenue
        account_dim['revenue_segment'] = pd.cut(account_dim['annual_revenue'], 
                                              bins=[0, 1000000, 10000000, 100000000, float('inf')], 
                                              labels=['Small', 'Medium', 'Large', 'Enterprise'])
        
        # Add employee segments
        account_dim['employee_segment'] = pd.cut(account_dim['number_of_employees'], 
                                               bins=[0, 50, 500, 1000, float('inf')], 
                                               labels=['Startup', 'Small', 'Medium', 'Large'])
        
        account_dim.to_parquet(self.output_dir / "gold" / "dim_accounts.parquet")
        logger.info(f"✅ Account dimension created: {len(account_dim)} records")
        
        # Create contact dimension
        contact_dim = contacts_df[['contact_id', 'account_id', 'first_name', 'last_name', 
                                 'email', 'title', 'department', 'lead_source', 'status']].copy()
        contact_dim['contact_name'] = contact_dim['first_name'] + ' ' + contact_dim['last_name']
        
        # Add contact segments based on title
        contact_dim['contact_level'] = contact_dim['title'].apply(
            lambda x: 'Executive' if any(word in x.lower() for word in ['ceo', 'cto', 'cfo', 'president', 'vp', 'director']) 
            else 'Manager' if any(word in x.lower() for word in ['manager', 'lead', 'head']) 
            else 'Individual'
        )
        
        contact_dim.to_parquet(self.output_dir / "gold" / "dim_contacts.parquet")
        logger.info(f"✅ Contact dimension created: {len(contact_dim)} records")
        
        # Create opportunities fact table
        opportunities_fact = opportunities_df[['opportunity_id', 'account_id', 'contact_id', 
                                              'opportunity_name', 'stage_name', 'amount', 
                                              'probability', 'close_date', 'type', 'lead_source']].copy()
        
        # Add time dimensions
        opportunities_fact['close_year'] = opportunities_fact['close_date'].dt.year
        opportunities_fact['close_month'] = opportunities_fact['close_date'].dt.month
        opportunities_fact['close_quarter'] = opportunities_fact['close_date'].dt.quarter
        
        # Add opportunity segments
        opportunities_fact['opportunity_size'] = pd.cut(opportunities_fact['amount'], 
                                                      bins=[0, 25000, 100000, 500000, float('inf')], 
                                                      labels=['Small', 'Medium', 'Large', 'Enterprise'])
        
        opportunities_fact.to_parquet(self.output_dir / "gold" / "fact_opportunities.parquet")
        logger.info(f"✅ Opportunities fact table created: {len(opportunities_fact)} records")
        
        # Create sales analytics
        sales_analytics = opportunities_fact.groupby('account_id').agg({
            'opportunity_id': 'count',
            'amount': ['sum', 'mean'],
            'probability': 'mean'
        }).round(2)
        
        sales_analytics.columns = ['total_opportunities', 'total_pipeline', 'avg_deal_size', 'avg_probability']
        sales_analytics = sales_analytics.reset_index()
        
        # Add pipeline segments
        sales_analytics['pipeline_segment'] = pd.cut(sales_analytics['total_pipeline'], 
                                                   bins=[0, 100000, 500000, 1000000, float('inf')], 
                                                   labels=['Low', 'Medium', 'High', 'Very High'])
        
        sales_analytics.to_parquet(self.output_dir / "gold" / "sales_analytics.parquet")
        logger.info(f"✅ Sales analytics created: {len(sales_analytics)} records")
        
        logger.info("🎉 Gold business logic completed successfully!")
        return True
    
    def data_quality_check(self):
        """Run data quality checks."""
        logger.info("🔄 Running data quality checks...")
        
        # Check Bronze layer
        bronze_files = list((self.output_dir / "bronze").glob("*.parquet"))
        logger.info(f"Bronze files: {len(bronze_files)}")
        
        # Check Silver layer
        silver_files = list((self.output_dir / "silver").glob("*.parquet"))
        logger.info(f"Silver files: {len(silver_files)}")
        
        # Check Gold layer
        gold_files = list((self.output_dir / "gold").glob("*.parquet"))
        logger.info(f"Gold files: {len(gold_files)}")
        
        # Validate record counts
        accounts_df = pd.read_parquet(self.output_dir / "bronze" / "salesforce_accounts.parquet")
        contacts_df = pd.read_parquet(self.output_dir / "bronze" / "salesforce_contacts.parquet")
        opportunities_df = pd.read_parquet(self.output_dir / "bronze" / "salesforce_opportunities.parquet")
        
        if len(accounts_df) == 0:
            logger.error("❌ No accounts in Bronze layer")
            return False
        
        if len(contacts_df) == 0:
            logger.error("❌ No contacts in Bronze layer")
            return False
        
        if len(opportunities_df) == 0:
            logger.error("❌ No opportunities in Bronze layer")
            return False
        
        logger.info(f"✅ Data quality check passed: {len(accounts_df)} accounts, {len(contacts_df)} contacts, {len(opportunities_df)} opportunities")
        return True
    
    def run_pipeline(self):
        """Run the complete Salesforce pipeline."""
        logger.info("🚀 Starting Local Salesforce Pipeline Test")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        try:
            # Step 1: Bronze ingestion
            if not self.bronze_ingestion():
                logger.error("❌ Bronze ingestion failed")
                return False
            
            # Step 2: Silver transformation
            if not self.silver_transformation():
                logger.error("❌ Silver transformation failed")
                return False
            
            # Step 3: Gold business logic
            if not self.gold_business_logic():
                logger.error("❌ Gold business logic failed")
                return False
            
            # Step 4: Data quality check
            if not self.data_quality_check():
                logger.error("❌ Data quality check failed")
                return False
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("=" * 60)
            logger.info("🎉 SALESFORCE PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info(f"⏱️  Total duration: {duration:.2f} seconds")
            logger.info(f"📁 Output directory: {self.output_dir}")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed with error: {str(e)}")
            return False
        
        finally:
            if hasattr(self.spark, 'stop'):
                self.spark.stop()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Local Salesforce Pipeline Test")
    parser.add_argument("--config", default="config/dev.yaml", help="Configuration file path")
    
    args = parser.parse_args()
    
    if not Path(args.config).exists():
        logger.error(f"Configuration file not found: {args.config}")
        sys.exit(1)
    
    pipeline = LocalSalesforcePipelineTest(args.config)
    success = pipeline.run_pipeline()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
