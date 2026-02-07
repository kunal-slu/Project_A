"""
Redshift to Bronze Ingestion Job

Ingests data from Redshift into the Bronze layer.
"""
import logging
from typing import Dict, Any
from project_a.core.base_job import BaseJob
from project_a.core.config import ProjectConfig


logger = logging.getLogger(__name__)


class RedshiftToBronzeJob(BaseJob):
    """Job to ingest data from Redshift to Bronze layer."""
    
    def __init__(self, config: ProjectConfig):
        super().__init__(config)
        self.job_name = "redshift_to_bronze"
    
    def run(self, ctx) -> Dict[str, Any]:
        """Execute the Redshift to Bronze ingestion."""
        logger.info("Starting Redshift to Bronze ingestion...")
        
        try:
            # Get Spark session from context
            spark = ctx.spark
            
            # Get source and target paths from config
            source_path = self.config.get('sources', {}).get('redshift', {}).get('base_path', 'data/samples/redshift')
            bronze_path = f"{self.config.get('paths', {}).get('bronze_root', 'data/bronze')}/redshift"
            
            # Ingest customer behavior data
            logger.info("Ingesting customer behavior data from Redshift...")
            behavior_df = spark.read.option("header", "true").csv(f"{source_path}/redshift_customer_behavior_50000.csv")
            behavior_df.write.mode("overwrite").parquet(f"{bronze_path}/customer_behavior")
            
            # Apply data quality checks
            logger.info("Applying data quality checks...")
            self.apply_dq_rules(behavior_df, "bronze.redshift.customer_behavior")
            
            # Log lineage
            self.log_lineage(
                source="redshift",
                target="bronze.redshift",
                records_processed={
                    "customer_behavior": behavior_df.count()
                }
            )
            
            result = {
                "status": "success",
                "records_processed": {
                    "customer_behavior": behavior_df.count()
                },
                "output_path": bronze_path
            }
            
            logger.info(f"Redshift to Bronze ingestion completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Redshift to Bronze ingestion failed: {e}")
            raise