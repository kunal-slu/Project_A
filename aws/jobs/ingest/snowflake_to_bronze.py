#!/usr/bin/env python3
"""
Snowflake sample data extraction job.
Extracts sample data from Snowflake and writes to S3 bronze layer.
"""

import os
import sys
import logging
from typing import Dict, Any
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date

from pyspark_interview_project.utils.spark import get_spark_session
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.logging import setup_json_logging

logger = logging.getLogger(__name__)


def get_snowflake_connection_string() -> str:
    """
    Get Snowflake JDBC connection string from environment variables.
    
    Returns:
        JDBC connection string
    """
    sf_url = os.getenv("SF_URL")
    sf_user = os.getenv("SF_USER")
    sf_pass = os.getenv("SF_PASS")
    sf_db = os.getenv("SF_DB", "SNOWFLAKE_SAMPLE_DATA")
    sf_schema = os.getenv("SF_SCHEMA", "TPCH_SF1")
    sf_wh = os.getenv("SF_WH", "COMPUTE_WH")
    
    return (
        f"jdbc:snowflake://{sf_url}/"
        f"?db={sf_db}&schema={sf_schema}&warehouse={sf_wh}&user={sf_user}&password={sf_pass}"
    )


def get_sample_queries() -> Dict[str, str]:
    """
    Get sample queries for Snowflake sample data.
    
    Returns:
        Dictionary with table names and queries
    """
    return {
        "orders": """
            SELECT 
                o_orderkey as order_id,
                o_custkey as customer_id,
                o_orderstatus as status,
                o_totalprice as total_price,
                o_orderdate as order_date,
                o_orderpriority as priority,
                o_clerk as clerk,
                o_shippriority as ship_priority,
                o_comment as comment
            FROM orders 
            LIMIT 10000
        """,
        "customers": """
            SELECT 
                c_custkey as customer_id,
                c_name as name,
                c_address as address,
                c_nationkey as nation_key,
                c_phone as phone,
                c_acctbal as account_balance,
                c_mktsegment as market_segment,
                c_comment as comment
            FROM customer 
            LIMIT 1000
        """,
        "lineitems": """
            SELECT 
                l_orderkey as order_id,
                l_partkey as part_id,
                l_suppkey as supplier_id,
                l_linenumber as line_number,
                l_quantity as quantity,
                l_extendedprice as extended_price,
                l_discount as discount,
                l_tax as tax,
                l_returnflag as return_flag,
                l_linestatus as line_status,
                l_shipdate as ship_date,
                l_commitdate as commit_date,
                l_receiptdate as receipt_date,
                l_shipinstruct as ship_instruction,
                l_shipmode as ship_mode,
                l_comment as comment
            FROM lineitem 
            LIMIT 50000
        """
    }


def extract_snowflake_data(
    spark: SparkSession,
    table_name: str,
    query: str,
    connection_string: str
) -> Any:
    """
    Extract data from Snowflake using JDBC.
    
    Args:
        spark: Spark session
        table_name: Table name
        query: SQL query to execute
        connection_string: JDBC connection string
        
    Returns:
        Spark DataFrame with Snowflake data
    """
    logger.info(f"Extracting data from Snowflake {table_name}")
    
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", connection_string) \
            .option("query", query) \
            .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
            .load()
        
        logger.info(f"Successfully extracted {df.count()} records from {table_name}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to extract data from Snowflake {table_name}: {e}")
        raise


def add_metadata_columns(df: Any, source: str, table_name: str) -> Any:
    """
    Add metadata columns to the DataFrame.
    
    Args:
        df: Input DataFrame
        source: Source system name
        table_name: Table name
        
    Returns:
        DataFrame with metadata columns
    """
    return df.withColumn("_source", lit(source)) \
             .withColumn("_table", lit(table_name)) \
             .withColumn("_extracted_at", current_timestamp()) \
             .withColumn("_proc_date", to_date(current_timestamp()))


def write_to_bronze(df: Any, table_name: str, lake_root: str) -> None:
    """
    Write DataFrame to bronze layer with partitioning by date.
    
    Args:
        df: Input DataFrame
        table_name: Table name
        lake_root: S3 root path of the data lake
    """
    output_path = f"{lake_root}/bronze/snowflake/{table_name}"
    
    logger.info(f"Writing {table_name} to bronze layer: {output_path}")
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy("_proc_date") \
        .save(output_path)
    
    logger.info(f"Successfully wrote {table_name} to bronze layer")


def process_snowflake_backfill(spark: SparkSession, config: Dict[str, Any]) -> None:
    """
    Process Snowflake sample data backfill.
    
    Args:
        spark: Spark session
        config: Configuration dictionary
    """
    logger.info("Starting Snowflake sample data backfill")
    
    # Get connection string
    connection_string = get_snowflake_connection_string()
    
    # Get data lake configuration
    lake_root = config["lake"]["root"]
    
    # Get sample queries
    queries = get_sample_queries()
    
    # Process each table
    for table_name, query in queries.items():
        try:
            df = extract_snowflake_data(spark, table_name, query, connection_string)
            df_with_meta = add_metadata_columns(df, "snowflake", table_name)
            write_to_bronze(df_with_meta, table_name, lake_root)
        except Exception as e:
            logger.error(f"Failed to process {table_name}: {e}")
            raise
    
    logger.info("Snowflake sample data backfill completed successfully")


def main():
    """Main function to run the Snowflake backfill job."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract sample data from Snowflake")
    parser.add_argument("--config", required=True, help="Configuration file path")
    args = parser.parse_args()
    
    # Setup logging
    setup_json_logging()
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logging.getLogger().setLevel(getattr(logging, log_level.upper()))
    
    try:
        # Load configuration
        config = load_conf(args.config)
        
        # Create Spark session with Delta support
        spark = get_spark_session(
            "SnowflakeToBronze",
            extra_conf={
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
            }
        )
        
        # Process Snowflake data
        process_snowflake_backfill(spark, config)
        
    except Exception as e:
        logger.error(f"Snowflake extraction failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()