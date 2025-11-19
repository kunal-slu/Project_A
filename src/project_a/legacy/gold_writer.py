"""
Gold Layer Writer with MERGE Operations
"""
import logging
import os
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)

class GoldWriter:
    """Gold layer writer with idempotent MERGE operations"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def publish_gold(self, source_df: DataFrame, gold_path: str, key: str = "customer_id") -> bool:
        """
        Publish data to Gold layer using MERGE operation for idempotency
        
        Args:
            source_df: DataFrame to publish
            gold_path: Path to Gold layer table
            key: Primary key for MERGE operation
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(gold_path), exist_ok=True)
            
            # Check if table exists
            table_exists = os.path.exists(gold_path)
            
            if table_exists:
                # Table exists - use MERGE operation
                logger.info(f"📊 Merging data into existing Gold table: {gold_path}")
                
                target = DeltaTable.forPath(self.spark, gold_path)
                
                # Perform MERGE operation
                (target.alias("t")
                 .merge(source_df.alias("s"), f"t.{key} = s.{key}")
                 .whenMatchedUpdateAll()
                 .whenNotMatchedInsertAll()
                 .execute())
                
                logger.info(f"✅ MERGE completed for {gold_path}")
                
            else:
                # Table doesn't exist - create new table
                logger.info(f"📊 Creating new Gold table: {gold_path}")
                
                source_df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .save(gold_path)
                
                logger.info(f"✅ New table created at {gold_path}")
            
            # Log the write path and count
            final_df = self.spark.read.format("delta").load(gold_path)
            row_count = final_df.count()
            
            logger.info(f"✅ Gold written to: {gold_path}")
            logger.info(f"📊 Gold row count: {row_count}")
            
            # Show version history
            try:
                history_df = self.spark.sql(f"DESCRIBE HISTORY delta.`{gold_path}`")
                logger.info(f"📝 Version history for {os.path.basename(gold_path)}:")
                history_df.show(5, truncate=False)
            except Exception as e:
                logger.warning(f"⚠️ Could not get version history: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error publishing to Gold layer {gold_path}: {e}")
            return False
    
    def publish_customer_analytics(self, customer_df: DataFrame, gold_base_path: str) -> bool:
        """Publish customer analytics to Gold layer"""
        gold_path = os.path.join(gold_base_path, "customer_analytics")
        
        # Generate customer analytics
        analytics_df = self._generate_customer_analytics(customer_df)
        
        return self.publish_gold(analytics_df, gold_path, "segment")
    
    def publish_order_analytics(self, order_df: DataFrame, gold_base_path: str) -> bool:
        """Publish order analytics to Gold layer"""
        gold_path = os.path.join(gold_base_path, "order_analytics")
        
        # Generate order analytics
        analytics_df = self._generate_order_analytics(order_df)
        
        return self.publish_gold(analytics_df, gold_path, "amount_category")
    
    def publish_monthly_revenue(self, order_df: DataFrame, gold_base_path: str) -> bool:
        """Publish monthly revenue to Gold layer"""
        gold_path = os.path.join(gold_base_path, "monthly_revenue")
        
        # Generate monthly revenue
        revenue_df = self._generate_monthly_revenue(order_df)
        
        return self.publish_gold(revenue_df, gold_path, "month")
    
    def _generate_customer_analytics(self, customer_df: DataFrame) -> DataFrame:
        """Generate customer analytics from customer data"""
        from pyspark.sql import functions as F
        
        return customer_df.groupBy("segment") \
            .agg(
                F.count("customer_id").alias("customer_count"),
                F.avg(F.datediff(F.current_date(), F.col("created_date"))).alias("avg_lifetime_days")
            )
    
    def _generate_order_analytics(self, order_df: DataFrame) -> DataFrame:
        """Generate order analytics from order data"""
        from pyspark.sql import functions as F
        
        # Create amount categories
        order_with_category = order_df.withColumn(
            "amount_category",
            F.when(F.col("amount") < 100, "Small")
            .when(F.col("amount") < 500, "Medium")
            .otherwise("Large")
        )
        
        return order_with_category.groupBy("amount_category") \
            .agg(
                F.count("order_id").alias("order_count"),
                F.sum("amount").alias("total_amount"),
                F.avg("amount").alias("avg_amount")
            )
    
    def _generate_monthly_revenue(self, order_df: DataFrame) -> DataFrame:
        """Generate monthly revenue from order data"""
        from pyspark.sql import functions as F
        
        return order_df.withColumn("month", F.month("order_date")) \
            .groupBy("month") \
            .agg(F.sum("amount").alias("total_revenue"))
