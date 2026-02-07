"""
Snowflake to Bronze Ingestion Job

Ingests data from Snowflake into the Bronze layer.
"""
import logging
from typing import Dict, Any
from project_a.core.base_job import BaseJob
from project_a.core.config import ProjectConfig


logger = logging.getLogger(__name__)


class SnowflakeToBronzeJob(BaseJob):
    """Job to ingest data from Snowflake to Bronze layer."""
    
    def __init__(self, config: ProjectConfig):
        super().__init__(config)
        self.job_name = "snowflake_to_bronze"
    
    def run(self, ctx) -> Dict[str, Any]:
        """Execute the Snowflake to Bronze ingestion."""
        logger.info("Starting Snowflake to Bronze ingestion...")
        
        try:
            # Get Spark session from context
            spark = ctx.spark
            
            # Get source and target paths from config
            source_path = self.config.get('sources', {}).get('snowflake', {}).get('base_path', 'data/samples/snowflake')
            bronze_path = f"{self.config.get('paths', {}).get('bronze_root', 'data/bronze')}/snowflake"
            
            # Ingest customers data
            logger.info("Ingesting customers data from Snowflake...")
            customers_df = spark.read.option("header", "true").csv(f"{source_path}/snowflake_customers_50000.csv")
            customers_df.write.mode("overwrite").parquet(f"{bronze_path}/customers")
            
            # Ingest orders data
            logger.info("Ingesting orders data from Snowflake...")
            orders_df = spark.read.option("header", "true").csv(f"{source_path}/snowflake_orders_100000.csv")
            orders_df.write.mode("overwrite").parquet(f"{bronze_path}/orders")
            
            # Ingest products data
            logger.info("Ingesting products data from Snowflake...")
            products_df = spark.read.option("header", "true").csv(f"{source_path}/snowflake_products_10000.csv")
            products_df.write.mode("overwrite").parquet(f"{bronze_path}/products")
            
            # Apply data quality checks
            logger.info("Applying data quality checks...")
            self.apply_dq_rules(customers_df, "bronze.snowflake.customers")
            self.apply_dq_rules(orders_df, "bronze.snowflake.orders")
            self.apply_dq_rules(products_df, "bronze.snowflake.products")
            
            # Cache counts to avoid multiple computations
            customers_count = customers_df.count()
            orders_count = orders_df.count()
            products_count = products_df.count()
            
            # Log lineage
            self.log_lineage(
                source="snowflake",
                target="bronze.snowflake",
                records_processed={
                    "customers": customers_count,
                    "orders": orders_count,
                    "products": products_count
                }
            )
            
            result = {
                "status": "success",
                "records_processed": {
                    "customers": customers_count,
                    "orders": orders_count,
                    "products": products_count
                },
                "output_path": bronze_path
            }
            
            logger.info(f"Snowflake to Bronze ingestion completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Snowflake to Bronze ingestion failed: {e}")
            raise