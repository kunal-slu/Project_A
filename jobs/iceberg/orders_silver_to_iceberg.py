"""
Silver Orders to Iceberg Migration Job

Converts orders_silver from Parquet to Iceberg format with:
- ACID transactions
- Schema evolution support
- Time travel capability
- Merge-based incremental updates

Why Iceberg for orders_silver?
- High update frequency (refunds, corrections)
- Need for ACID guarantees
- Late-arriving data handling
- Audit trail requirements
"""

import logging
from typing import Dict, Any
from datetime import datetime, timedelta
from pyspark.sql import functions as F

from project_a.core.base_job import BaseJob
from project_a.core.config import ProjectConfig
from project_a.iceberg_utils import IcebergWriter, IcebergReader, initialize_iceberg_spark

logger = logging.getLogger(__name__)


class OrdersSilverToIcebergJob(BaseJob):
    """
    Migrate and maintain orders_silver as Iceberg table.
    
    Features:
    - Initial migration from Parquet
    - Incremental updates with MERGE
    - Rolling 3-day window for late data
    - Snapshot management
    """
    
    def __init__(self, config: ProjectConfig):
        super().__init__(config)
        self.job_name = "orders_silver_to_iceberg"
        self.table_name = "silver.orders_iceberg"
    
    def run(self, ctx) -> Dict[str, Any]:
        """Execute the Iceberg migration/update."""
        logger.info(f"Starting {self.job_name}...")
        
        try:
            spark = ctx.spark
            
            # Initialize Iceberg utilities
            iceberg_writer = IcebergWriter(spark, catalog_name="local")
            iceberg_reader = IcebergReader(spark, catalog_name="local")
            
            # Get configuration
            silver_path = self.config.get('paths', {}).get('silver_root', 'data/silver')
            parquet_path = f"{silver_path}/orders_silver"
            
            # Check if this is initial migration or incremental update
            table_exists = self._table_exists(spark, self.table_name)
            
            if not table_exists:
                logger.info("Initial migration: Creating Iceberg table...")
                self._initial_migration(spark, parquet_path, iceberg_writer)
            else:
                logger.info("Incremental update: Merging new/updated records...")
                self._incremental_update(spark, parquet_path, iceberg_writer, iceberg_reader)
            
            # Get final statistics
            current_snapshot = iceberg_reader.read_current(self.table_name)
            total_records = current_snapshot.count()
            
            # Get snapshot history
            snapshots = iceberg_reader.get_snapshots(self.table_name)
            snapshot_count = snapshots.count()
            
            result = {
                "status": "success",
                "table_name": self.table_name,
                "total_records": total_records,
                "snapshots": snapshot_count,
                "migration_complete": not table_exists,
                "format": "iceberg"
            }
            
            logger.info(f"Iceberg job completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Iceberg job failed: {e}", exc_info=True)
            raise
    
    def _table_exists(self, spark, table_name: str) -> bool:
        """Check if Iceberg table exists."""
        try:
            spark.sql(f"DESCRIBE TABLE local.{table_name}")
            return True
        except Exception:
            return False
    
    def _initial_migration(self, spark, parquet_path: str, writer: IcebergWriter):
        """
        Initial migration from Parquet to Iceberg.
        
        Creates Iceberg table with proper partitioning and properties.
        """
        logger.info(f"Reading from Parquet: {parquet_path}")
        
        # Read all historical data
        orders_df = spark.read.parquet(parquet_path)
        
        # Add metadata columns for tracking
        orders_enriched = orders_df \
            .withColumn("_iceberg_migrated_at", F.current_timestamp()) \
            .withColumn("_version", F.lit(1))
        
        logger.info(f"Migrating {orders_enriched.count()} orders to Iceberg...")
        
        # Create Iceberg table with partitioning
        writer.create_table(
            df=orders_enriched,
            table_name=self.table_name,
            partition_by=["order_date"],  # Partition by date for query performance
            properties={
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "snappy"
            }
        )
        
        logger.info("Initial migration complete")
    
    def _incremental_update(
        self,
        spark,
        parquet_path: str,
        writer: IcebergWriter,
        reader: IcebergReader
    ):
        """
        Incremental update with rolling window for late data.
        
        Strategy:
        - Reprocess last 3 days to handle late arrivals
        - MERGE into existing Iceberg table
        - Update existing records, insert new ones
        """
        # Calculate lookback window (3 days for late data)
        lookback_days = 3
        cutoff_date = datetime.now().date() - timedelta(days=lookback_days)
        
        logger.info(f"Processing orders from {cutoff_date} onwards (rolling {lookback_days}-day window)")
        
        # Read recent orders from Parquet source
        recent_orders = spark.read.parquet(parquet_path) \
            .filter(F.col("order_date") >= F.lit(cutoff_date))
        
        # Add/update metadata
        recent_orders = recent_orders \
            .withColumn("_updated_at", F.current_timestamp()) \
            .withColumn("_version", F.lit(2))  # Increment version for updates
        
        records_to_process = recent_orders.count()
        logger.info(f"Found {records_to_process} records in rolling window")
        
        if records_to_process > 0:
            # MERGE into Iceberg table
            # This handles both updates (existing orders) and inserts (new orders)
            writer.write_merge(
                df=recent_orders,
                table_name=self.table_name,
                merge_key="order_id",  # Match on order_id
                update_cols=["total_amount", "product_id", "_updated_at", "_version"]
            )
            
            logger.info(f"Successfully merged {records_to_process} records")
        else:
            logger.info("No new records to process")


def main():
    """Standalone execution for testing."""
    from project_a.core.config import ProjectConfig
    from project_a.core.context import JobContext
    
    # Load configuration
    config = ProjectConfig("config/dev.yaml", env="dev")
    
    # Execute job
    job = OrdersSilverToIcebergJob(config)
    
    with JobContext(config, app_name="orders_silver_to_iceberg") as ctx:
        result = job.run(ctx)
        print(f"Job Result: {result}")


if __name__ == "__main__":
    main()
