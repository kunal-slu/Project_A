#!/usr/bin/env python3
"""
Local Pipeline Smoke Test

This script runs the complete ETL pipeline locally using sample data,
simulating Bronze → Silver → Gold transformations without AWS dependencies.

Usage:
    python local_pipeline_smoke.py --config config/dev.yaml
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
import yaml

def load_config(config_path: str):
    """Load configuration from YAML file."""
    with open(config_path) as f:
        return yaml.safe_load(f)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LocalPipelineSmokeTest:
    """Local ETL pipeline smoke test."""
    
    def __init__(self, config_path: str):
        self.config = load_config(config_path)
        self.spark = build_spark("LocalSmokeTest", self.config)
        self.data_dir = Path("data/input_data")
        self.output_dir = Path("_local_pipeline_output")
        
        # Create output directories
        self.output_dir.mkdir(exist_ok=True)
        (self.output_dir / "bronze").mkdir(exist_ok=True)
        (self.output_dir / "silver").mkdir(exist_ok=True)
        (self.output_dir / "gold").mkdir(exist_ok=True)
        
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
        """Simulate Bronze layer ingestion."""
        logger.info("🔄 Starting Bronze layer ingestion...")
        
        # Read and validate customers
        customers_df = pd.read_csv(self.data_dir / "customers.csv")
        if not self.validate_schema(customers_df, "config/schema_definitions/customers_bronze.json"):
            return False
        
        customers_df.to_parquet(self.output_dir / "bronze" / "customers.parquet")
        logger.info(f"✅ Customers ingested: {len(customers_df)} records")
        
        # Read and validate orders
        with open(self.data_dir / "orders.json") as f:
            orders_data = json.load(f)
        orders_df = pd.DataFrame(orders_data)
        
        if not self.validate_schema(orders_df, "config/schema_definitions/orders_bronze.json"):
            return False
        
        orders_df.to_parquet(self.output_dir / "bronze" / "orders.parquet")
        logger.info(f"✅ Orders ingested: {len(orders_df)} records")
        
        # Read and validate products
        products_df = pd.read_csv(self.data_dir / "products.csv")
        if not self.validate_schema(products_df, "config/schema_definitions/products_bronze.json"):
            return False
        
        products_df.to_parquet(self.output_dir / "bronze" / "products.parquet")
        logger.info(f"✅ Products ingested: {len(products_df)} records")
        
        # Read and validate returns
        with open(self.data_dir / "returns.json") as f:
            returns_data = json.load(f)
        returns_df = pd.DataFrame(returns_data)
        
        if not self.validate_schema(returns_df, "config/schema_definitions/returns_bronze.json"):
            return False
        
        returns_df.to_parquet(self.output_dir / "bronze" / "returns.parquet")
        logger.info(f"✅ Returns ingested: {len(returns_df)} records")
        
        logger.info("🎉 Bronze ingestion completed successfully!")
        return True
    
    def silver_transformation(self):
        """Simulate Silver layer transformation."""
        logger.info("🔄 Starting Silver layer transformation...")
        
        # Load Bronze data
        customers_df = pd.read_parquet(self.output_dir / "bronze" / "customers.parquet")
        orders_df = pd.read_parquet(self.output_dir / "bronze" / "orders.parquet")
        products_df = pd.read_parquet(self.output_dir / "bronze" / "products.parquet")
        returns_df = pd.read_parquet(self.output_dir / "bronze" / "returns.parquet")
        
        # Clean customers data
        customers_clean = customers_df.copy()
        customers_clean['email'] = customers_clean['email'].str.lower().str.strip()
        customers_clean['first_name'] = customers_clean['first_name'].str.title()
        customers_clean['last_name'] = customers_clean['last_name'].str.title()
        customers_clean['registration_date'] = pd.to_datetime(customers_clean['registration_date'])
        
        # Remove duplicates
        customers_clean = customers_clean.drop_duplicates(subset=['customer_id'])
        
        customers_clean.to_parquet(self.output_dir / "silver" / "customers_clean.parquet")
        logger.info(f"✅ Customers cleaned: {len(customers_clean)} records")
        
        # Clean orders data
        orders_clean = orders_df.copy()
        orders_clean['order_date'] = pd.to_datetime(orders_clean['order_date'])
        
        # Parse payment information
        orders_clean['payment_method'] = orders_clean['payment'].apply(
            lambda x: json.loads(x)['method'] if isinstance(x, str) else x['method']
        )
        orders_clean['payment_status'] = orders_clean['payment'].apply(
            lambda x: json.loads(x)['status'] if isinstance(x, str) else x['status']
        )
        
        # Remove duplicates
        orders_clean = orders_clean.drop_duplicates(subset=['order_id'])
        
        orders_clean.to_parquet(self.output_dir / "silver" / "orders_clean.parquet")
        logger.info(f"✅ Orders cleaned: {len(orders_clean)} records")
        
        # Clean products data
        products_clean = products_df.copy()
        products_clean['launch_date'] = pd.to_datetime(products_clean['launch_date'])
        products_clean['category'] = products_clean['category'].str.title()
        
        # Remove duplicates
        products_clean = products_clean.drop_duplicates(subset=['product_id'])
        
        products_clean.to_parquet(self.output_dir / "silver" / "products_clean.parquet")
        logger.info(f"✅ Products cleaned: {len(products_clean)} records")
        
        logger.info("🎉 Silver transformation completed successfully!")
        return True
    
    def gold_business_logic(self):
        """Simulate Gold layer business logic."""
        logger.info("🔄 Starting Gold layer business logic...")
        
        # Load Silver data
        customers_df = pd.read_parquet(self.output_dir / "silver" / "customers_clean.parquet")
        orders_df = pd.read_parquet(self.output_dir / "silver" / "orders_clean.parquet")
        products_df = pd.read_parquet(self.output_dir / "silver" / "products_clean.parquet")
        
        # Create customer dimension
        customer_dim = customers_df[['customer_id', 'first_name', 'last_name', 'email', 
                                   'city', 'state', 'country', 'age', 'gender']].copy()
        customer_dim['customer_name'] = customer_dim['first_name'] + ' ' + customer_dim['last_name']
        customer_dim['age_group'] = pd.cut(customer_dim['age'], 
                                         bins=[0, 25, 35, 50, 65, 100], 
                                         labels=['18-25', '26-35', '36-50', '51-65', '65+'])
        
        customer_dim.to_parquet(self.output_dir / "gold" / "dim_customers.parquet")
        logger.info(f"✅ Customer dimension created: {len(customer_dim)} records")
        
        # Create product dimension
        product_dim = products_df[['product_id', 'product_name', 'category', 'brand', 
                                  'price', 'rating']].copy()
        product_dim['price_tier'] = pd.cut(product_dim['price'], 
                                         bins=[0, 100, 500, 1000, float('inf')], 
                                         labels=['Budget', 'Mid-range', 'Premium', 'Luxury'])
        
        product_dim.to_parquet(self.output_dir / "gold" / "dim_products.parquet")
        logger.info(f"✅ Product dimension created: {len(product_dim)} records")
        
        # Create sales fact table
        sales_fact = orders_df[['order_id', 'customer_id', 'product_id', 'order_date', 
                               'quantity', 'total_amount', 'payment_method', 'payment_status']].copy()
        sales_fact['order_year'] = sales_fact['order_date'].dt.year
        sales_fact['order_month'] = sales_fact['order_date'].dt.month
        sales_fact['order_quarter'] = sales_fact['order_date'].dt.quarter
        
        sales_fact.to_parquet(self.output_dir / "gold" / "fact_sales.parquet")
        logger.info(f"✅ Sales fact table created: {len(sales_fact)} records")
        
        # Create customer analytics
        customer_analytics = sales_fact.groupby('customer_id').agg({
            'order_id': 'count',
            'total_amount': ['sum', 'mean'],
            'quantity': 'sum'
        }).round(2)
        
        customer_analytics.columns = ['total_orders', 'total_spent', 'avg_order_value', 'total_items']
        customer_analytics = customer_analytics.reset_index()
        
        # Add customer segmentation
        customer_analytics['segment'] = pd.cut(customer_analytics['total_spent'], 
                                             bins=[0, 1000, 5000, 10000, float('inf')], 
                                             labels=['Bronze', 'Silver', 'Gold', 'Platinum'])
        
        customer_analytics.to_parquet(self.output_dir / "gold" / "customer_analytics.parquet")
        logger.info(f"✅ Customer analytics created: {len(customer_analytics)} records")
        
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
        customers_df = pd.read_parquet(self.output_dir / "bronze" / "customers.parquet")
        orders_df = pd.read_parquet(self.output_dir / "bronze" / "orders.parquet")
        
        if len(customers_df) == 0:
            logger.error("❌ No customers in Bronze layer")
            return False
        
        if len(orders_df) == 0:
            logger.error("❌ No orders in Bronze layer")
            return False
        
        logger.info(f"✅ Data quality check passed: {len(customers_df)} customers, {len(orders_df)} orders")
        return True
    
    def run_pipeline(self):
        """Run the complete pipeline."""
        logger.info("🚀 Starting Local Pipeline Smoke Test")
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
            logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
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
    parser = argparse.ArgumentParser(description="Local Pipeline Smoke Test")
    parser.add_argument("--config", default="config/dev.yaml", help="Configuration file path")
    
    args = parser.parse_args()
    
    if not Path(args.config).exists():
        logger.error(f"Configuration file not found: {args.config}")
        sys.exit(1)
    
    pipeline = LocalPipelineSmokeTest(args.config)
    success = pipeline.run_pipeline()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
