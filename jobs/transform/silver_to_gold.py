"""
Silver to Gold Transformation Job

Transforms data from Silver layer to Gold layer with dimensional modeling and analytics.
"""
import logging
from typing import Dict, Any
from project_a.core.base_job import BaseJob
from project_a.core.config import ProjectConfig


logger = logging.getLogger(__name__)


class SilverToGoldJob(BaseJob):
    """Job to transform data from Silver to Gold layer."""
    
    def __init__(self, config: ProjectConfig):
        super().__init__(config)
        self.job_name = "silver_to_gold"
    
    def run(self, ctx) -> Dict[str, Any]:
        """Execute the Silver to Gold transformation."""
        logger.info("Starting Silver to Gold transformation...")
        
        try:
            # Get Spark session from context
            spark = ctx.spark
            
            # Get silver and gold paths from config
            silver_path = self.config.get('paths', {}).get('silver_root', 'data/silver')
            gold_path = self.config.get('paths', {}).get('gold_root', 'data/gold')
            
            # Build Customer Dimension
            logger.info("Building Customer Dimension...")
            self.build_customer_dimension(spark, silver_path, gold_path)
            
            # Build Product Dimension
            logger.info("Building Product Dimension...")
            self.build_product_dimension(spark, silver_path, gold_path)
            
            # Build Fact Orders
            logger.info("Building Fact Orders...")
            self.build_fact_orders(spark, silver_path, gold_path)
            
            # Build Customer 360 View
            logger.info("Building Customer 360 View...")
            self.build_customer_360(spark, silver_path, gold_path)
            
            # Build Customer Behavior Analytics
            logger.info("Building Customer Behavior Analytics...")
            self.build_behavior_analytics(spark, silver_path, gold_path)
            
            # Apply data quality checks
            logger.info("Applying data quality checks...")
            self.apply_dq_rules(None, "gold.layer")
            
            # Log lineage
            self.log_lineage(
                source="silver",
                target="gold",
                records_processed={}
            )
            
            result = {
                "status": "success",
                "output_path": gold_path,
                "models_created": ["customer_dim", "product_dim", "fact_orders", "customer_360", "behavior_analytics"]
            }
            
            logger.info(f"Silver to Gold transformation completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Silver to Gold transformation failed: {e}")
            raise
    
    def build_customer_dimension(self, spark, silver_path: str, gold_path: str):
        """Build Customer Dimension from Silver data."""
        # Read customer data from silver
        customers_df = spark.read.parquet(f"{silver_path}/customers_silver")
        
        # Create customer dimension
        customer_dim = customers_df.select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "country",
            "registration_date"
        ).distinct()
        
        # Write to gold
        customer_dim.write.mode("overwrite").parquet(f"{gold_path}/dim_customer")
    
    def build_product_dimension(self, spark, silver_path: str, gold_path: str):
        """Build Product Dimension from Silver data."""
        # Read product data from silver
        products_df = spark.read.parquet(f"{silver_path}/products_silver")
        
        # Create product dimension
        product_dim = products_df.select(
            "product_id",
            "product_name",
            "category",
            "price_usd",
            "cost_usd",
            "supplier_id"
        ).distinct()
        
        # Write to gold
        product_dim.write.mode("overwrite").parquet(f"{gold_path}/dim_product")
    
    def build_fact_orders(self, spark, silver_path: str, gold_path: str):
        """Build Fact Orders from Silver data."""
        # Read order and customer data from silver
        orders_df = spark.read.parquet(f"{silver_path}/orders_silver")
        customers_df = spark.read.parquet(f"{silver_path}/customers_silver")
        
        # Join orders with customers to create fact table
        fact_orders = orders_df.join(
            customers_df.select("customer_id", "country"),
            "customer_id",
            "inner"
        ).select(
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "total_amount",  # Now matches Silver schema
            "quantity",
            "status",
            "country"
        )
        
        # Write to gold
        fact_orders.write.mode("overwrite").parquet(f"{gold_path}/fact_orders")
    
    def build_customer_360(self, spark, silver_path: str, gold_path: str):
        """Build Customer 360 view combining multiple data sources."""
        # Read customer, order, and behavior data from silver
        customers_df = spark.read.parquet(f"{silver_path}/customers_silver")
        orders_df = spark.read.parquet(f"{silver_path}/orders_silver")
        behavior_df = spark.read.parquet(f"{silver_path}/customer_behavior_silver")
        
        # Aggregate order metrics
        order_metrics = orders_df.groupBy("customer_id").agg(
            {"total_amount": "sum", "order_id": "count"}  # Now matches Silver schema
        ).withColumnRenamed("sum(total_amount)", "total_spent") \
         .withColumnRenamed("count(order_id)", "order_count")
        
        # Aggregate behavior metrics
        behavior_metrics = behavior_df.groupBy("customer_id").agg(
            {"time_spent_seconds": "sum", "session_id": "count"}
        ).withColumnRenamed("sum(time_spent_seconds)", "total_time_spent") \
         .withColumnRenamed("count(session_id)", "session_count")
        
        # Combine all data for customer 360 view
        customer_360 = customers_df.join(order_metrics, "customer_id", "left") \
                                  .join(behavior_metrics, "customer_id", "left")
        
        # Write to gold
        customer_360.write.mode("overwrite").parquet(f"{gold_path}/customer_360")
    
    def build_behavior_analytics(self, spark, silver_path: str, gold_path: str):
        """Build Customer Behavior Analytics from Silver data."""
        # Read behavior data from silver
        behavior_df = spark.read.parquet(f"{silver_path}/customer_behavior_silver")
        
        # Create behavior analytics
        behavior_analytics = behavior_df.groupBy("customer_id", "event_type", "device_type", "browser") \
                                       .agg({"time_spent_seconds": "avg", "session_id": "count"}) \
                                       .withColumnRenamed("avg(time_spent_seconds)", "avg_time_spent") \
                                       .withColumnRenamed("count(session_id)", "session_count")
        
        # Write to gold
        behavior_analytics.write.mode("overwrite").parquet(f"{gold_path}/behavior_analytics")