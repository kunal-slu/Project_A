#!/usr/bin/env python3
"""
Delta table optimization and vacuum script.
Compacts small files and runs VACUUM with safe retention.
"""

import os
import sys
import logging
from typing import List, Dict, Any
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession
from delta.tables import DeltaTable

from pyspark_interview_project.utils.spark import get_spark_session
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.logging import setup_json_logging

logger = logging.getLogger(__name__)


def optimize_table(spark: SparkSession, table_path: str, table_name: str) -> None:
    """
    Optimize a Delta table by compacting small files.
    
    Args:
        spark: Spark session
        table_path: Path to the Delta table
        table_name: Name of the table
    """
    logger.info(f"Optimizing table {table_name} at {table_path}")
    
    try:
        # Read Delta table
        delta_table = DeltaTable.forPath(spark, table_path)
        
        # Get current file statistics
        file_stats = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
        num_files = file_stats['numFiles']
        total_size = file_stats['sizeInBytes']
        
        logger.info(f"Table {table_name}: {num_files} files, {total_size} bytes")
        
        # Optimize if there are many small files
        if num_files > 100:  # Threshold for optimization
            logger.info(f"Optimizing {table_name} - compacting {num_files} files")
            delta_table.optimize().executeCompaction()
            logger.info(f"Optimization completed for {table_name}")
        else:
            logger.info(f"Skipping optimization for {table_name} - only {num_files} files")
            
    except Exception as e:
        logger.error(f"Failed to optimize table {table_name}: {e}")
        raise


def vacuum_table(spark: SparkSession, table_path: str, table_name: str, retention_hours: int = 168) -> None:
    """
    Vacuum a Delta table to remove old files.
    
    Args:
        spark: Spark session
        table_path: Path to the Delta table
        table_name: Name of the table
        retention_hours: Retention period in hours (default: 7 days)
    """
    logger.info(f"Vacuuming table {table_name} at {table_path}")
    
    try:
        # Read Delta table
        delta_table = DeltaTable.forPath(spark, table_path)
        
        # Get current file statistics before vacuum
        file_stats_before = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
        files_before = file_stats_before['numFiles']
        size_before = file_stats_before['sizeInBytes']
        
        # Run vacuum
        delta_table.vacuum(retentionHours=retention_hours)
        
        # Get file statistics after vacuum
        file_stats_after = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
        files_after = file_stats_after['numFiles']
        size_after = file_stats_after['sizeInBytes']
        
        files_removed = files_before - files_after
        size_removed = size_before - size_after
        
        logger.info(f"Vacuum completed for {table_name}:")
        logger.info(f"  Files removed: {files_removed}")
        logger.info(f"  Size removed: {size_removed} bytes")
        
    except Exception as e:
        logger.error(f"Failed to vacuum table {table_name}: {e}")
        raise


def get_table_list(spark: SparkSession, lake_root: str, layers: List[str]) -> List[Dict[str, str]]:
    """
    Get list of Delta tables in the specified layers.
    
    Args:
        spark: Spark session
        lake_root: Root S3 path of the data lake
        layers: List of data layers to process
        
    Returns:
        List of table dictionaries
    """
    tables = []
    
    for layer in layers:
        layer_path = f"{lake_root}/{layer}"
        
        try:
            # List directories in the layer
            df = spark.sql(f"SHOW FILES FROM '{layer_path}'")
            directories = [row[0] for row in df.collect() if row[0].endswith('/')]
            
            for directory in directories:
                table_name = directory.rstrip('/').split('/')[-1]
                table_path = f"{layer_path}/{table_name}"
                
                # Verify it's a Delta table
                try:
                    spark.read.format("delta").load(table_path).limit(1).collect()
                    tables.append({
                        "name": f"{layer}_{table_name}",
                        "path": table_path,
                        "layer": layer
                    })
                except Exception:
                    logger.warning(f"Skipping {table_path} - not a valid Delta table")
                    continue
                    
        except Exception as e:
            logger.warning(f"Failed to scan layer {layer}: {e}")
            continue
    
    return tables


def process_tables(spark: SparkSession, lake_root: str, layers: List[str], retention_hours: int = 168) -> None:
    """
    Process all tables in the specified layers.
    
    Args:
        spark: Spark session
        lake_root: Root S3 path of the data lake
        layers: List of data layers to process
        retention_hours: Retention period for vacuum
    """
    logger.info("Starting Delta table optimization and vacuum process")
    
    # Get list of tables
    tables = get_table_list(spark, lake_root, layers)
    
    if not tables:
        logger.warning("No Delta tables found to process")
        return
    
    logger.info(f"Found {len(tables)} tables to process")
    
    # Process each table
    for table in tables:
        try:
            # Optimize table
            optimize_table(spark, table["path"], table["name"])
            
            # Vacuum table
            vacuum_table(spark, table["path"], table["name"], retention_hours)
            
        except Exception as e:
            logger.error(f"Failed to process table {table['name']}: {e}")
            continue
    
    logger.info("Delta table optimization and vacuum process completed")


def main():
    """Main function to run Delta optimization and vacuum."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Optimize and vacuum Delta tables")
    parser.add_argument("--lake-root", required=True, help="S3 root path of the data lake")
    parser.add_argument("--layers", nargs="+", default=["silver", "gold"], help="Data layers to process")
    parser.add_argument("--retention-hours", type=int, default=168, help="Retention period in hours (default: 7 days)")
    parser.add_argument("--config", help="Configuration file path")
    args = parser.parse_args()
    
    # Setup logging
    setup_json_logging()
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logging.getLogger().setLevel(getattr(logging, log_level.upper()))
    
    try:
        # Load configuration if provided
        if args.config:
            config = load_conf(args.config)
            lake_root = config.get("lake", {}).get("root", args.lake_root)
        else:
            lake_root = args.lake_root
        
        # Create Spark session with Delta support
        spark = get_spark_session(
            "DeltaOptimizeVacuum",
            extra_conf={
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
            }
        )
        
        # Process tables
        process_tables(spark, lake_root, args.layers, args.retention_hours)
        
        print("✅ Delta table optimization and vacuum completed successfully!")
        
    except Exception as e:
        logger.error(f"Delta optimization failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
