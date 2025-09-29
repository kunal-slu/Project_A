#!/usr/bin/env python3
"""
Glue + Athena table registration script.
Registers Delta tables in Glue Data Catalog for querying with Athena.
"""

import os
import sys
import logging
import boto3
from typing import List, Dict, Any
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark_interview_project.utils.spark import get_spark_session
from pyspark_interview_project.utils.config import load_conf

logger = logging.getLogger(__name__)


def register_delta_table(
    spark,
    database_name: str,
    table_name: str,
    s3_location: str,
    table_type: str = "DELTA"
) -> None:
    """
    Register a Delta table in Glue Data Catalog.
    
    Args:
        spark: Spark session
        database_name: Glue database name
        table_name: Table name
        s3_location: S3 location of the Delta table
        table_type: Table type (default: DELTA)
    """
    logger.info(f"Registering table {database_name}.{table_name} at {s3_location}")
    
    # Create external table SQL
    create_table_sql = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{table_name}
    USING DELTA
    LOCATION '{s3_location}'
    TBLPROPERTIES ('table_type'='{table_type}')
    """
    
    try:
        spark.sql(create_table_sql)
        logger.info(f"Successfully registered table {database_name}.{table_name}")
    except Exception as e:
        logger.error(f"Failed to register table {database_name}.{table_name}: {e}")
        raise


def scan_and_register_tables(
    spark,
    lake_root: str,
    database_name: str,
    layer: str
) -> List[str]:
    """
    Scan S3 location and register all Delta tables found.
    
    Args:
        spark: Spark session
        lake_root: Root S3 path of the data lake
        database_name: Glue database name
        layer: Data layer (bronze, silver, gold)
        
    Returns:
        List of registered table names
    """
    registered_tables = []
    s3_prefix = f"{lake_root}/{layer}/"
    
    logger.info(f"Scanning for Delta tables in {s3_prefix}")
    
    # List all directories in the layer using S3 client
    try:
        s3_client = boto3.client('s3')
        bucket_name = s3_prefix.replace('s3://', '').split('/')[0]
        prefix = '/'.join(s3_prefix.replace('s3://', '').split('/')[1:])
        
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
        
        for obj in response.get('CommonPrefixes', []):
            table_name = obj['Prefix'].replace(prefix, '').rstrip('/')
            table_location = f"{s3_prefix}{table_name}"
            
            # Check if it's a Delta table by looking for _delta_log
            try:
                # Try to read as Delta table to validate
                test_df = spark.read.format("delta").load(table_location)
                test_df.limit(1).collect()  # Force evaluation
                
                # Register the table
                register_delta_table(spark, database_name, table_name, table_location)
                registered_tables.append(table_name)
                
            except Exception as e:
                logger.warning(f"Skipping {table_location} - not a valid Delta table: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Failed to scan directory {s3_prefix}: {e}")
        raise
    
    return registered_tables


def create_database_if_not_exists(spark, database_name: str) -> None:
    """
    Create Glue database if it doesn't exist.
    
    Args:
        spark: Spark session
        database_name: Database name
    """
    logger.info(f"Creating database {database_name} if it doesn't exist")
    
    create_db_sql = f"""
    CREATE DATABASE IF NOT EXISTS {database_name}
    COMMENT 'Data lake {database_name} layer'
    """
    
    try:
        spark.sql(create_db_sql)
        logger.info(f"Database {database_name} is ready")
    except Exception as e:
        logger.error(f"Failed to create database {database_name}: {e}")
        raise


def main():
    """Main function to register Glue tables."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Register Delta tables in Glue Data Catalog")
    parser.add_argument("--lake-root", required=True, help="S3 root path of the data lake")
    parser.add_argument("--database", required=True, help="Glue database name")
    parser.add_argument("--layer", required=True, help="Data layer (bronze, silver, gold)")
    parser.add_argument("--table", help="Specific table name to register (optional)")
    parser.add_argument("--config", help="Configuration file path")
    args = parser.parse_args()
    
    # Setup logging
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Load configuration if provided
        if args.config:
            config = load_conf(args.config)
            lake_root = config.get("lake", {}).get("root", args.lake_root)
        else:
            lake_root = args.lake_root
        
        # Create Spark session
        spark = get_spark_session("GlueTableRegistration")
        
        # Create database if it doesn't exist
        create_database_if_not_exists(spark, args.database)
        
        if args.table:
            # Register specific table
            table_location = f"{lake_root}/{args.layer}/{args.table}"
            register_delta_table(spark, args.database, args.table, table_location)
            registered_tables = [args.table]
        else:
            # Scan and register all tables in the layer
            registered_tables = scan_and_register_tables(
                spark, lake_root, args.database, args.layer
            )
        
        logger.info(f"Successfully registered {len(registered_tables)} tables:")
        for table in registered_tables:
            logger.info(f"  - {args.database}.{table}")
        
        print(f"✅ Registered {len(registered_tables)} tables in {args.database}.{args.layer}")
        
    except Exception as e:
        logger.error(f"Table registration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()