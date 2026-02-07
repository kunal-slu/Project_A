"""
Delta Lake utilities for reading and writing Delta tables.
"""

import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """
    Read data from a Delta table.
    
    Args:
        spark: SparkSession object
        path: Path to the Delta table
        
    Returns:
        DataFrame with data from the Delta table
    """
    try:
        df = spark.read.format("delta").load(path)
        logger.info(f"Successfully read Delta table from {path}")
        return df
    except Exception as e:
        logger.error(f"Failed to read Delta table from {path}: {e}")
        raise


def write_delta(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """
    Write DataFrame to a Delta table.
    
    Args:
        df: DataFrame to write
        path: Path where to write the Delta table
        mode: Write mode (overwrite, append, etc.)
    """
    try:
        df.write.format("delta").mode(mode).save(path)
        logger.info(f"Successfully wrote Delta table to {path} in {mode} mode")
    except Exception as e:
        logger.error(f"Failed to write Delta table to {path}: {e}")
        raise


class DeltaUtils:
    """
    Utility class for Delta Lake operations.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def optimize_table(self, table_path: str) -> dict:
        """
        Optimize a Delta table.
        
        Args:
            table_path: Path to the Delta table
            
        Returns:
            Dictionary with optimization results
        """
        try:
            from delta.tables import DeltaTable
            delta_table = DeltaTable.forPath(self.spark, table_path)
            delta_table.optimize().execute()
            return {"success": True, "message": f"Table {table_path} optimized"}
        except Exception as e:
            logger.error(f"Failed to optimize table {table_path}: {e}")
            return {"success": False, "error": str(e)}
    
    def set_retention_policy(self, table_path: str, retention_days: int = 7) -> dict:
        """
        Set retention policy for a Delta table.
        
        Args:
            table_path: Path to the Delta table
            retention_days: Number of days to retain history
            
        Returns:
            Dictionary with retention policy results
        """
        try:
            from delta.tables import DeltaTable
            delta_table = DeltaTable.forPath(self.spark, table_path)
            delta_table.vacuum(retention_hours=retention_days * 24)
            return {"success": True, "message": f"Retention policy set to {retention_days} days"}
        except Exception as e:
            logger.error(f"Failed to set retention policy for {table_path}: {e}")
            return {"success": False, "error": str(e)}
    
    def cleanup_old_partitions(self, table_path: str, partition_column: str, days_to_keep: int = 30) -> dict:
        """
        Clean up old partitions from a Delta table.
        
        Args:
            table_path: Path to the Delta table
            partition_column: Name of the partition column
            days_to_keep: Number of days of data to keep
            
        Returns:
            Dictionary with cleanup results
        """
        try:
            from delta.tables import DeltaTable
            from pyspark.sql.functions import current_date, date_sub
            
            delta_table = DeltaTable.forPath(self.spark, table_path)
            cutoff_date = current_date() - date_sub(current_date(), days_to_keep)
            
            # Delete old partitions
            delta_table.delete(col(partition_column) < cutoff_date)
            
            return {"success": True, "message": f"Cleaned up partitions older than {days_to_keep} days"}
        except Exception as e:
            logger.error(f"Failed to cleanup old partitions for {table_path}: {e}")
            return {"success": False, "error": str(e)}
    
    def get_table_stats(self, table_path: str) -> dict:
        """
        Get statistics for a Delta table.
        
        Args:
            table_path: Path to the Delta table
            
        Returns:
            Dictionary with table statistics
        """
        try:
            df = read_delta(self.spark, table_path)
            count = df.count()
            columns = len(df.columns)
            
            return {
                "success": True,
                "row_count": count,
                "column_count": columns,
                "path": table_path
            }
        except Exception as e:
            logger.error(f"Failed to get stats for {table_path}: {e}")
            return {"success": False, "error": str(e)}
    
    def run_maintenance_routine(self, table_path: str) -> dict:
        """
        Run a complete maintenance routine on a Delta table.
        
        Args:
            table_path: Path to the Delta table
            
        Returns:
            Dictionary with maintenance results
        """
        try:
            results = []
            
            # Optimize table
            opt_result = self.optimize_table(table_path)
            results.append(opt_result)
            
            # Set retention policy
            retention_result = self.set_retention_policy(table_path)
            results.append(retention_result)
            
            # Get stats
            stats_result = self.get_table_stats(table_path)
            results.append(stats_result)
            
            return {
                "success": all(r.get("success", False) for r in results),
                "results": results
            }
        except Exception as e:
            logger.error(f"Failed to run maintenance routine for {table_path}: {e}")
            return {"success": False, "error": str(e)}
