"""
Abstracted table write operations supporting Parquet, Delta, and Iceberg formats.

This module provides a unified interface for writing DataFrames to different storage formats
based on configuration, enabling seamless switching between formats.
"""

import logging
from typing import Dict, Any, Optional
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def write_table(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    cfg: Optional[Dict[str, Any]] = None,
    partition_cols: Optional[list] = None,
    spark: Optional[SparkSession] = None
) -> None:
    """
    Write DataFrame to table using configured storage format.
    
    Supports:
    - Iceberg: Writes to Glue catalog using Iceberg format
    - Delta: Writes to Delta Lake format
    - Parquet: Writes plain Parquet files to S3/local path
    
    Args:
        df: DataFrame to write
        table_name: Target table name (e.g., "silver.customer_behavior" for Iceberg,
                    or path for Parquet/Delta)
        mode: Write mode ("overwrite", "append", "ignore")
        cfg: Configuration dictionary (defaults to empty dict)
        partition_cols: Optional list of partition column names
        spark: SparkSession (optional, will infer from df)
    
    Examples:
        # Iceberg write (registered in Glue)
        write_table(df, "silver.customer_behavior", cfg={"storage": {"format": "iceberg", "catalog": "glue_catalog"}})
        
        # Delta write (to S3 path)
        write_table(df, "s3://bucket/silver/customer_behavior", cfg={"storage": {"format": "delta"}})
        
        # Parquet write (to S3 path)
        write_table(df, "s3://bucket/silver/customer_behavior", cfg={"storage": {"format": "parquet"}})
    """
    if cfg is None:
        cfg = {}
    
    storage = cfg.get("storage", {})
    fmt = storage.get("format", "parquet")
    catalog = storage.get("catalog", "glue_catalog")
    # Use config-based warehouse path
    warehouse = (
        storage.get("warehouse")
        or cfg.get('paths', {}).get('silver_root', 's3://my-etl-lake-demo')
        or storage.get("warehouse", "s3://my-etl-lake-demo")
    )
    data_lake = cfg.get("data_lake", {})
    
    logger.info(f"Writing table '{table_name}' using format '{fmt}'")
    
    if fmt == "iceberg":
        # Iceberg write: table_name expected as "layer.table_name" (e.g., "silver.customer_behavior")
        full_table_name = f"{catalog}.{table_name}"
        
        logger.info(f"Writing to Iceberg table: {full_table_name}")
        
        try:
            writer = df.writeTo(full_table_name).using("iceberg")
            
            # Set overwrite mode
            if mode == "overwrite":
                # Iceberg supports dynamic partition overwrite
                writer = writer.option("overwrite-mode", "dynamic").replace()
            elif mode == "append":
                writer = writer.append()
            else:
                writer = writer.createOrReplace()
            
            # Apply partitioning if specified
            if partition_cols:
                writer = writer.partitionedBy(*partition_cols)
            
            writer.execute()
            logger.info(f"Successfully wrote to Iceberg table: {full_table_name}")
        except Exception as e:
            logger.error(f"Failed to write Iceberg table {full_table_name}: {e}")
            # Fallback: try createOrReplace if replace fails
            try:
                df.writeTo(full_table_name).using("iceberg").createOrReplace()
                logger.info(f"Successfully wrote to Iceberg table using createOrReplace: {full_table_name}")
            except Exception as e2:
                logger.error(f"Iceberg write failed completely: {e2}")
                raise
        
    elif fmt == "delta":
        # Delta Lake write: table_name is S3 path
        if not table_name.startswith("s3://") and not table_name.startswith("s3a://"):
            # Construct path from data_lake config if relative
            if "silver" in table_name:
                base_path = data_lake.get("silver_path", "data/lakehouse_delta/silver")
            elif "gold" in table_name:
                base_path = data_lake.get("gold_path", "data/lakehouse_delta/gold")
            else:
                base_path = data_lake.get("bronze_path", "data/lakehouse_delta/bronze")
            
            # Extract table name from "layer.table" format
            if "." in table_name:
                table_name = table_name.split(".", 1)[1]
            
            delta_path = f"{base_path}/{table_name}"
        else:
            delta_path = table_name
        
        logger.info(f"Writing to Delta table: {delta_path}")
        
        writer = df.write.format("delta").mode(mode)
        
        if mode == "overwrite":
            writer = writer.option("mergeSchema", "true")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.save(delta_path)
        logger.info(f"Successfully wrote to Delta table: {delta_path}")
        
    else:
        # Plain Parquet write: table_name is S3/local path
        if not table_name.startswith("s3://") and not table_name.startswith("s3a://") and not table_name.startswith("/"):
            # Construct path from data_lake config if relative
            if "silver" in table_name:
                base_path = data_lake.get("silver_path", "data/lakehouse_delta/silver")
            elif "gold" in table_name:
                base_path = data_lake.get("gold_path", "data/lakehouse_delta/gold")
            else:
                base_path = data_lake.get("bronze_path", "data/lakehouse_delta/bronze")
            
            # Extract table name from "layer.table" format
            if "." in table_name:
                table_name = table_name.split(".", 1)[1]
            
            parquet_path = f"{base_path}/{table_name}"
        else:
            parquet_path = table_name
        
        logger.info(f"Writing to Parquet: {parquet_path}")
        
        writer = df.write.mode(mode).format("parquet").option("compression", "snappy")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.save(parquet_path)
        logger.info(f"Successfully wrote to Parquet: {parquet_path}")


def read_table(
    table_name: str,
    cfg: Optional[Dict[str, Any]] = None,
    spark: Optional[SparkSession] = None
) -> DataFrame:
    """
    Read table using configured storage format.
    
    Args:
        table_name: Table name (e.g., "silver.customer_behavior" for Iceberg,
                    or path for Parquet/Delta)
        cfg: Configuration dictionary
        spark: SparkSession (required for Iceberg, optional for others)
    
    Returns:
        DataFrame from the table
    """
    if cfg is None:
        cfg = {}
    
    if spark is None:
        raise ValueError("SparkSession is required for reading tables")
    
    storage = cfg.get("storage", {})
    fmt = storage.get("format", "parquet")
    catalog = storage.get("catalog", "glue_catalog")
    data_lake = cfg.get("data_lake", {})
    
    logger.info(f"Reading table '{table_name}' using format '{fmt}'")
    
    if fmt == "iceberg":
        # Iceberg read
        full_table_name = f"{catalog}.{table_name}"
        logger.info(f"Reading from Iceberg table: {full_table_name}")
        return spark.table(full_table_name)
        
    elif fmt == "delta":
        # Delta read
        if not table_name.startswith("s3://") and not table_name.startswith("s3a://"):
            if "silver" in table_name:
                base_path = data_lake.get("silver_path", "data/lakehouse_delta/silver")
            elif "gold" in table_name:
                base_path = data_lake.get("gold_path", "data/lakehouse_delta/gold")
            else:
                base_path = data_lake.get("bronze_path", "data/lakehouse_delta/bronze")
            
            if "." in table_name:
                table_name = table_name.split(".", 1)[1]
            
            delta_path = f"{base_path}/{table_name}"
        else:
            delta_path = table_name
        
        logger.info(f"Reading from Delta table: {delta_path}")
        return spark.read.format("delta").load(delta_path)
        
    else:
        # Parquet read
        if not table_name.startswith("s3://") and not table_name.startswith("s3a://") and not table_name.startswith("/"):
            if "silver" in table_name:
                base_path = data_lake.get("silver_path", "data/lakehouse_delta/silver")
            elif "gold" in table_name:
                base_path = data_lake.get("gold_path", "data/lakehouse_delta/gold")
            else:
                base_path = data_lake.get("bronze_path", "data/lakehouse_delta/bronze")
            
            if "." in table_name:
                table_name = table_name.split(".", 1)[1]
            
            parquet_path = f"{base_path}/{table_name}"
        else:
            parquet_path = table_name
        
        logger.info(f"Reading from Parquet: {parquet_path}")
        return spark.read.format("parquet").load(parquet_path)

