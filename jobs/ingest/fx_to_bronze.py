"""
FX Rates to Bronze Ingestion Job

Ingests FX rates data into the Bronze layer.
"""
import logging
from typing import Dict, Any
from project_a.core.base_job import BaseJob
from project_a.core.config import ProjectConfig


logger = logging.getLogger(__name__)


class FxToBronzeJob(BaseJob):
    """Job to ingest FX rates data to Bronze layer."""
    
    def __init__(self, config: ProjectConfig):
        super().__init__(config)
        self.job_name = "fx_to_bronze"
    
    def run(self, ctx) -> Dict[str, Any]:
        """Execute the FX to Bronze ingestion."""
        logger.info("Starting FX to Bronze ingestion...")
        
        try:
            # Get Spark session from context
            spark = ctx.spark
            
            # Get source and target paths from config
            source_path = self.config.get('sources', {}).get('fx', {}).get('raw_path', 'data/samples/fx')
            bronze_path = f"{self.config.get('paths', {}).get('bronze_root', 'data/bronze')}/fx"
            
            # Ingest FX rates data
            logger.info("Ingesting FX rates data...")
            fx_rates_df = spark.read.json(f"{source_path}/fx_rates_historical.json")
            fx_rates_df.write.mode("overwrite").parquet(f"{bronze_path}/fx_rates")
            
            # Apply data quality checks
            logger.info("Applying data quality checks...")
            self.apply_dq_rules(fx_rates_df, "bronze.fx.rates")
            
            # Log lineage
            self.log_lineage(
                source="fx_rates",
                target="bronze.fx",
                records_processed={
                    "fx_rates": fx_rates_df.count()
                }
            )
            
            result = {
                "status": "success",
                "records_processed": {
                    "fx_rates": fx_rates_df.count()
                },
                "output_path": bronze_path
            }
            
            logger.info(f"FX to Bronze ingestion completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"FX to Bronze ingestion failed: {e}")
            raise