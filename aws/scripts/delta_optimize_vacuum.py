#!/usr/bin/env python3
"""
Delta Lake optimization and vacuum script.
Compacts small files and runs VACUUM on Delta tables.
"""

import os
import sys
import logging
import argparse
from typing import List, Dict, Any
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark_interview_project.utils.spark import get_spark_session, get_delta_config
from pyspark_interview_project.utils.config import load_conf

logger = logging.getLogger(__name__)


def optimize_delta_table(spark, table_path: str, table_name: str) -> Dict[str, Any]:
    """
    Optimize Delta table by compacting small files.
    
    Args:
        spark: Spark session
        table_path: Path to Delta table
        table_name: Name of the table
        
    Returns:
        Optimization results
    """
    logger.info(f"Optimizing Delta table: {table_name}")
    
    try:
        # Read Delta table
        df = spark.read.format("delta").load(table_path)
        
        # Get current file statistics
        file_stats = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
        num_files_before = file_stats['numFiles']
        size_bytes_before = file_stats['sizeInBytes']
        
        logger.info(f"Before optimization - Files: {num_files_before}, Size: {size_bytes_before} bytes")
        
        # Optimize if there are many small files
        if num_files_before > 10:  # Threshold for optimization
            logger.info(f"Running OPTIMIZE on {table_name}")
            spark.sql(f"OPTIMIZE delta.`{table_path}`")
            
            # Get post-optimization stats
            file_stats_after = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
            num_files_after = file_stats_after['numFiles']
            size_bytes_after = file_stats_after['sizeInBytes']
            
            logger.info(f"After optimization - Files: {num_files_after}, Size: {size_bytes_after} bytes")
            
            return {
                "table_name": table_name,
                "files_before": num_files_before,
                "files_after": num_files_after,
                "size_before": size_bytes_before,
                "size_after": size_bytes_after,
                "files_reduced": num_files_before - num_files_after,
                "optimized": True
            }
        else:
            logger.info(f"Skipping optimization for {table_name} - only {num_files_before} files")
            return {
                "table_name": table_name,
                "files_before": num_files_before,
                "files_after": num_files_before,
                "size_before": size_bytes_before,
                "size_after": size_bytes_before,
                "files_reduced": 0,
                "optimized": False
            }
            
    except Exception as e:
        logger.error(f"Failed to optimize table {table_name}: {e}")
        return {
            "table_name": table_name,
            "error": str(e),
            "optimized": False
        }


def vacuum_delta_table(spark, table_path: str, table_name: str, retention_hours: int = 168) -> Dict[str, Any]:
    """
    Vacuum Delta table to remove old files.
    
    Args:
        spark: Spark session
        table_path: Path to Delta table
        table_name: Name of the table
        retention_hours: Hours to retain files (default 7 days)
        
    Returns:
        Vacuum results
    """
    logger.info(f"Vacuuming Delta table: {table_name}")
    
    try:
        # Get table history before vacuum
        history_before = spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`").count()
        
        # Run VACUUM
        logger.info(f"Running VACUUM on {table_name} with {retention_hours}h retention")
        spark.sql(f"VACUUM delta.`{table_path}` RETAIN {retention_hours} HOURS")
        
        # Get table history after vacuum
        history_after = spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`").count()
        
        logger.info(f"Vacuum completed for {table_name}")
        
        return {
            "table_name": table_name,
            "history_before": history_before,
            "history_after": history_after,
            "retention_hours": retention_hours,
            "vacuumed": True
        }
        
    except Exception as e:
        logger.error(f"Failed to vacuum table {table_name}: {e}")
        return {
            "table_name": table_name,
            "error": str(e),
            "vacuumed": False
        }


def optimize_and_vacuum_tables(spark, lake_root: str, layers: List[str]) -> Dict[str, Any]:
    """
    Optimize and vacuum all Delta tables in specified layers.
    
    Args:
        spark: Spark session
        lake_root: Root path of the data lake
        layers: List of layers to process (e.g., ['silver', 'gold'])
        
    Returns:
        Summary of optimization results
    """
    logger.info(f"Starting Delta optimization for layers: {layers}")
    
    results = {
        "optimization": [],
        "vacuum": [],
        "summary": {}
    }
    
    # Define tables to process
    tables = {
        "silver": ["fx_rates", "orders", "customers", "products"],
        "gold": ["dim_fx", "dim_customers", "dim_products", "fact_orders"]
    }
    
    total_tables = 0
    optimized_tables = 0
    vacuumed_tables = 0
    
    for layer in layers:
        if layer not in tables:
            logger.warning(f"Unknown layer: {layer}")
            continue
            
        logger.info(f"Processing {layer} layer")
        
        for table_name in tables[layer]:
            table_path = f"{lake_root}/{layer}/{table_name}"
            total_tables += 1
            
            # Optimize table
            opt_result = optimize_delta_table(spark, table_path, f"{layer}.{table_name}")
            results["optimization"].append(opt_result)
            
            if opt_result.get("optimized", False):
                optimized_tables += 1
            
            # Vacuum table
            vac_result = vacuum_delta_table(spark, table_path, f"{layer}.{table_name}")
            results["vacuum"].append(vac_result)
            
            if vac_result.get("vacuumed", False):
                vacuumed_tables += 1
    
    # Summary
    results["summary"] = {
        "total_tables": total_tables,
        "optimized_tables": optimized_tables,
        "vacuumed_tables": vacuumed_tables,
        "optimization_rate": f"{(optimized_tables/total_tables)*100:.1f}%" if total_tables > 0 else "0%",
        "vacuum_rate": f"{(vacuumed_tables/total_tables)*100:.1f}%" if total_tables > 0 else "0%"
    }
    
    logger.info(f"Optimization complete: {optimized_tables}/{total_tables} tables optimized, {vacuumed_tables}/{total_tables} tables vacuumed")
    
    return results


def main():
    """Main entry point for Delta optimization script."""
    parser = argparse.ArgumentParser(description="Delta Lake optimization and vacuum")
    parser.add_argument("--lake-root", required=True, help="Data lake root path")
    parser.add_argument("--layers", nargs="+", default=["silver", "gold"], help="Layers to process")
    parser.add_argument("--retention-hours", type=int, default=168, help="Vacuum retention hours")
    parser.add_argument("--config", help="Configuration file path")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info("Starting Delta Lake optimization and vacuum")
    
    # Load configuration if provided
    config = None
    if args.config:
        config = load_conf(args.config)
    
    # Create Spark session with Delta config
    spark = get_spark_session(
        app_name="delta-optimize-vacuum",
        config=get_delta_config()
    )
    
    try:
        # Run optimization and vacuum
        results = optimize_and_vacuum_tables(spark, args.lake_root, args.layers)
        
        # Print summary
        print("\n" + "="*50)
        print("DELTA OPTIMIZATION SUMMARY")
        print("="*50)
        print(f"Total tables processed: {results['summary']['total_tables']}")
        print(f"Tables optimized: {results['summary']['optimized_tables']}")
        print(f"Tables vacuumed: {results['summary']['vacuumed_tables']}")
        print(f"Optimization rate: {results['summary']['optimization_rate']}")
        print(f"Vacuum rate: {results['summary']['vacuum_rate']}")
        print("="*50)
        
        # Print detailed results
        print("\nOPTIMIZATION DETAILS:")
        for result in results["optimization"]:
            if result.get("optimized", False):
                print(f"✓ {result['table_name']}: {result['files_before']} → {result['files_after']} files "
                      f"({result['files_reduced']} reduced)")
            else:
                print(f"- {result['table_name']}: Skipped ({result['files_before']} files)")
        
        print("\nVACUUM DETAILS:")
        for result in results["vacuum"]:
            if result.get("vacuumed", False):
                print(f"✓ {result['table_name']}: Vacuumed with {result['retention_hours']}h retention")
            else:
                print(f"✗ {result['table_name']}: Failed - {result.get('error', 'Unknown error')}")
        
        logger.info("Delta optimization and vacuum completed successfully")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
