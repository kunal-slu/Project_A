#!/usr/bin/env python3
"""
Enhance data sources with additional improvements for better analytics.
This script adds business logic, relationships, and advanced data features.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime, timedelta
import random
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def add_customer_segmentation():
    """Add customer segmentation based on behavior and value."""
    logger.info("Adding customer segmentation...")
    
    # Read customers data
    customers_file = Path("aws/data_fixed/snowflake_customers_50000.csv")
    if not customers_file.exists():
        logger.error("Customers file not found")
        return
    
    df_customers = pd.read_csv(customers_file)
    
    # Add customer segmentation based on lifetime_value and other factors
    def assign_segment(row):
        if row['lifetime_value'] >= 5000:
            return 'VIP'
        elif row['lifetime_value'] >= 2000:
            return 'High Value'
        elif row['lifetime_value'] >= 500:
            return 'Medium Value'
        elif row['lifetime_value'] >= 100:
            return 'Low Value'
        else:
            return 'Prospect'
    
    df_customers['customer_segment'] = df_customers.apply(assign_segment, axis=1)
    
    # Add customer lifetime value estimation (enhance existing)
    df_customers['estimated_clv'] = df_customers['lifetime_value'] + np.random.normal(0, 200, len(df_customers))
    df_customers['estimated_clv'] = df_customers['estimated_clv'].clip(lower=0)
    
    # Save enhanced customers
    df_customers.to_csv(customers_file, index=False)
    logger.info("✅ Added customer segmentation and CLV estimation")

def add_product_categories():
    """Add detailed product categories and subcategories."""
    logger.info("Adding detailed product categories...")
    
    # Read products data
    products_file = Path("aws/data_fixed/snowflake_products_10000.csv")
    if not products_file.exists():
        logger.error("Products file not found")
        return
    
    df_products = pd.read_csv(products_file)
    
    # Define product categories and subcategories
    categories = {
        'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Accessories', 'Gaming'],
        'Clothing': ['Men\'s', 'Women\'s', 'Kids', 'Shoes', 'Accessories'],
        'Home & Garden': ['Furniture', 'Appliances', 'Decor', 'Tools', 'Outdoor'],
        'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Water Sports', 'Winter Sports'],
        'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Children\'s', 'Reference'],
        'Beauty': ['Skincare', 'Makeup', 'Hair Care', 'Fragrance', 'Tools'],
        'Automotive': ['Parts', 'Accessories', 'Tools', 'Maintenance', 'Electronics'],
        'Toys': ['Action Figures', 'Dolls', 'Educational', 'Outdoor', 'Games']
    }
    
    # Add subcategory
    def assign_subcategory(category):
        if category in categories:
            return random.choice(categories[category])
        return 'Other'
    
    df_products['subcategory'] = df_products['category'].apply(assign_subcategory)
    
    # Add product rating and review count
    df_products['rating'] = np.round(np.random.normal(4.2, 0.8, len(df_products)), 1)
    df_products['rating'] = df_products['rating'].clip(1.0, 5.0)
    
    df_products['review_count'] = np.random.poisson(25, len(df_products))
    
    # Add product availability
    df_products['in_stock'] = np.random.choice([True, False], len(df_products), p=[0.85, 0.15])
    
    # Save enhanced products
    df_products.to_csv(products_file, index=False)
    logger.info("✅ Added detailed product categories and ratings")

def add_order_analytics():
    """Add order analytics and business metrics."""
    logger.info("Adding order analytics...")
    
    # Read orders data
    orders_file = Path("aws/data_fixed/snowflake_orders_100000.csv")
    if not orders_file.exists():
        logger.error("Orders file not found")
        return
    
    df_orders = pd.read_csv(orders_file)
    
    # Add order analytics
    df_orders['order_month'] = pd.to_datetime(df_orders['order_date']).dt.month
    df_orders['order_quarter'] = pd.to_datetime(df_orders['order_date']).dt.quarter
    df_orders['order_year'] = pd.to_datetime(df_orders['order_date']).dt.year
    
    # Add order value segments
    def assign_order_segment(total_amount):
        if total_amount >= 1000:
            return 'High Value'
        elif total_amount >= 500:
            return 'Medium Value'
        elif total_amount >= 100:
            return 'Standard'
        else:
            return 'Low Value'
    
    df_orders['order_segment'] = df_orders['total_amount'].apply(assign_order_segment)
    
    # Add customer order count (simplified)
    customer_order_counts = df_orders['customer_id'].value_counts()
    df_orders['customer_order_count'] = df_orders['customer_id'].map(customer_order_counts)
    
    # Add order fulfillment metrics
    df_orders['fulfillment_days'] = np.random.poisson(3, len(df_orders))
    df_orders['shipping_cost'] = df_orders['total_amount'] * 0.05 + np.random.normal(0, 5, len(df_orders))
    df_orders['shipping_cost'] = df_orders['shipping_cost'].clip(lower=0)
    
    # Save enhanced orders
    df_orders.to_csv(orders_file, index=False)
    logger.info("✅ Added order analytics and business metrics")

def add_behavioral_insights():
    """Add behavioral insights to customer behavior data."""
    logger.info("Adding behavioral insights...")
    
    # Read behavior data
    behavior_file = Path("aws/data_fixed/redshift_customer_behavior_50000.csv")
    if not behavior_file.exists():
        logger.error("Behavior file not found")
        return
    
    df_behavior = pd.read_csv(behavior_file)
    
    # Add behavioral segments based on duration_seconds
    def assign_behavior_segment(row):
        if row['duration_seconds'] >= 1800:
            return 'Highly Engaged'
        elif row['duration_seconds'] >= 600:
            return 'Engaged'
        elif row['duration_seconds'] >= 300:
            return 'Moderate'
        else:
            return 'Low Engagement'
    
    df_behavior['behavior_segment'] = df_behavior.apply(assign_behavior_segment, axis=1)
    
    # Add conversion probability
    df_behavior['conversion_probability'] = np.random.beta(2, 5, len(df_behavior))
    
    # Add customer journey stage
    journey_stages = ['Awareness', 'Consideration', 'Purchase', 'Retention', 'Advocacy']
    df_behavior['journey_stage'] = np.random.choice(journey_stages, len(df_behavior))
    
    # Add additional behavioral metrics
    df_behavior['engagement_score'] = (df_behavior['duration_seconds'] / 3600) * 100  # Convert to hours and scale
    df_behavior['engagement_score'] = df_behavior['engagement_score'].clip(0, 100)
    
    # Add session quality rating
    def assign_session_quality(row):
        if row['duration_seconds'] >= 1800 and row['conversion_value'] > 0:
            return 'Excellent'
        elif row['duration_seconds'] >= 600:
            return 'Good'
        elif row['duration_seconds'] >= 300:
            return 'Average'
        else:
            return 'Poor'
    
    df_behavior['session_quality'] = df_behavior.apply(assign_session_quality, axis=1)
    
    # Save enhanced behavior
    df_behavior.to_csv(behavior_file, index=False)
    logger.info("✅ Added behavioral insights and customer journey data")

def add_financial_metrics():
    """Add financial metrics and KPIs."""
    logger.info("Adding financial metrics...")
    
    # Create financial metrics file
    metrics_data = []
    
    # Generate monthly financial metrics
    start_date = datetime(2023, 1, 1)
    for month in range(24):  # 2 years of data
        current_date = start_date + timedelta(days=30 * month)
        
        metrics = {
            'month': current_date.strftime('%Y-%m'),
            'revenue': np.random.normal(1000000, 100000),
            'orders_count': np.random.poisson(5000),
            'avg_order_value': np.random.normal(200, 50),
            'customer_acquisition_cost': np.random.normal(50, 10),
            'customer_lifetime_value': np.random.normal(500, 100),
            'churn_rate': np.random.beta(2, 8),
            'retention_rate': np.random.beta(8, 2),
            'conversion_rate': np.random.beta(3, 7)
        }
        
        metrics_data.append(metrics)
    
    df_metrics = pd.DataFrame(metrics_data)
    
    # Save financial metrics
    metrics_file = Path("aws/data_fixed/financial_metrics_24_months.csv")
    df_metrics.to_csv(metrics_file, index=False)
    logger.info("✅ Added financial metrics and KPIs")

def create_data_relationships():
    """Create explicit data relationships and foreign keys."""
    logger.info("Creating data relationships...")
    
    # Create relationships mapping
    relationships = {
        "primary_keys": {
            "customers": "customer_id",
            "products": "product_id", 
            "orders": "order_id",
            "contacts": "hubspot_contact_id",
            "deals": "hubspot_deal_id"
        },
        "foreign_keys": {
            "orders": {
                "customer_id": "customers.customer_id",
                "product_id": "products.product_id"
            },
            "deals": {
                "customer_id": "customers.customer_id"
            },
            "behavior": {
                "customer_id": "customers.customer_id"
            }
        },
        "business_rules": {
            "customer_lifecycle": [
                "Prospect -> Lead -> Opportunity -> Customer -> Advocate"
            ],
            "order_states": [
                "PLACED -> CONFIRMED -> SHIPPED -> DELIVERED -> COMPLETED"
            ],
            "deal_stages": [
                "New -> Qualified -> Proposal -> Negotiation -> Closed Won/Lost"
            ]
        }
    }
    
    # Save relationships
    with open("data_relationships.json", "w") as f:
        json.dump(relationships, f, indent=2)
    
    logger.info("✅ Created data relationships and business rules")

def main():
    """Main function to enhance all data sources."""
    logger.info("🚀 Starting data source enhancements...")
    
    try:
        add_customer_segmentation()
        add_product_categories()
        add_order_analytics()
        add_behavioral_insights()
        add_financial_metrics()
        create_data_relationships()
        
        logger.info("✅ All data source enhancements completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error during data enhancements: {e}")
        raise

if __name__ == "__main__":
    main()
