"""
CRM to Bronze Ingestion Job

Ingests data from CRM systems into the Bronze layer.
"""
import logging
from typing import Dict, Any
from project_a.core.base_job import BaseJob
from project_a.core.config import ProjectConfig


logger = logging.getLogger(__name__)


class CrmToBronzeJob(BaseJob):
    """Job to ingest data from CRM to Bronze layer."""
    
    def __init__(self, config: ProjectConfig):
        super().__init__(config)
        self.job_name = "crm_to_bronze"
    
    def run(self, ctx) -> Dict[str, Any]:
        """Execute the CRM to Bronze ingestion."""
        logger.info("Starting CRM to Bronze ingestion...")
        
        try:
            # Get Spark session from context
            spark = ctx.spark
            
            # Get source and target paths from config
            source_path = self.config.get('sources', {}).get('crm', {}).get('base_path', 'data/samples/crm')
            bronze_path = f"{self.config.get('paths', {}).get('bronze_root', 'data/bronze')}/crm"
            
            # Ingest accounts data
            logger.info("Ingesting accounts data from CRM...")
            accounts_df = spark.read.option("header", "true").csv(f"{source_path}/accounts.csv")
            accounts_df.write.mode("overwrite").parquet(f"{bronze_path}/accounts")
            
            # Ingest contacts data
            logger.info("Ingesting contacts data from CRM...")
            contacts_df = spark.read.option("header", "true").csv(f"{source_path}/contacts.csv")
            contacts_df.write.mode("overwrite").parquet(f"{bronze_path}/contacts")
            
            # Ingest opportunities data
            logger.info("Ingesting opportunities data from CRM...")
            opportunities_df = spark.read.option("header", "true").csv(f"{source_path}/opportunities.csv")
            opportunities_df.write.mode("overwrite").parquet(f"{bronze_path}/opportunities")
            
            # Apply data quality checks
            logger.info("Applying data quality checks...")
            self.apply_dq_rules(accounts_df, "bronze.crm.accounts")
            self.apply_dq_rules(contacts_df, "bronze.crm.contacts")
            self.apply_dq_rules(opportunities_df, "bronze.crm.opportunities")
            
            # Cache counts to avoid multiple computations
            accounts_count = accounts_df.count()
            contacts_count = contacts_df.count()
            opportunities_count = opportunities_df.count()
            
            # Log lineage
            self.log_lineage(
                source="crm",
                target="bronze.crm",
                records_processed={
                    "accounts": accounts_count,
                    "contacts": contacts_count,
                    "opportunities": opportunities_count
                }
            )
            
            result = {
                "status": "success",
                "records_processed": {
                    "accounts": accounts_count,
                    "contacts": contacts_count,
                    "opportunities": opportunities_count
                },
                "output_path": bronze_path
            }
            
            logger.info(f"CRM to Bronze ingestion completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"CRM to Bronze ingestion failed: {e}")
            raise