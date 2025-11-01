"""
Publish Gold tables to data warehouses (Snowflake, Redshift).

Provides idempotent publishing with MERGE/upsert support.
"""

import logging
from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp

from pyspark_interview_project.utils.secrets import get_snowflake_credentials, get_redshift_credentials

logger = logging.getLogger(__name__)


def publish_to_snowflake(
    df: DataFrame,
    table_name: str,
    mode: str = "merge",
    keys: Optional[List[str]] = None,
    config: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Publish DataFrame to Snowflake table.
    
    Args:
        df: DataFrame to publish
        table_name: Target table name (e.g., "ANALYTICS.CUSTOMER_360")
        mode: Write mode ("merge", "overwrite", "append")
        keys: Primary key columns for MERGE operation
        config: Configuration dictionary
        
    Returns:
        True if successful
    """
    logger.info(f"Publishing to Snowflake: {table_name} (mode={mode})")
    
    creds = get_snowflake_credentials(config or {})
    
    # Build Snowflake options
    account = creds.get('account', '').replace('.snowflakecomputing.com', '')
    sf_options = {
        "sfURL": f"{account}.snowflakecomputing.com",
        "sfUser": creds.get("user"),
        "sfPassword": creds.get("password"),
        "sfDatabase": creds.get("database", "ANALYTICS"),
        "sfSchema": creds.get("schema", "PUBLIC"),
        "sfWarehouse": creds.get("warehouse"),
    }
    
    df = df.withColumn("_load_ts", current_timestamp())
    
    if mode == "merge" and keys:
        # Use staging table + MERGE pattern
        staging_table = f"{table_name}_STAGING"
        
        # Write to staging
        df.write \
            .format("snowflake") \
            .options(**sf_options) \
            .option("dbtable", staging_table.split('.')[-1]) \
            .mode("overwrite") \
            .save()
        
        # Build MERGE SQL
        columns = df.columns
        update_cols = ", ".join([f"t.{c} = s.{c}" for c in columns if c not in keys])
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f"s.{c}" for c in columns])
        on_clause = " AND ".join([f"t.{k} = s.{k}" for k in keys])
        
        merge_sql = f"""
            MERGE INTO {table_name} t
            USING {staging_table} s
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {update_cols}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """.strip()
        
        # Execute MERGE via JDBC
        jdbc_url = (
            f"jdbc:snowflake://{sf_options['sfURL']}/?"
            f"db={sf_options['sfDatabase']}&"
            f"schema={sf_options['sfSchema']}&"
            f"warehouse={sf_options['sfWarehouse']}"
        )
        
        spark = df.sparkSession
        spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", sf_options["sfUser"]) \
            .option("password", sf_options["sfPassword"]) \
            .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
            .option("query", merge_sql) \
            .load() \
            .collect()
        
        # Cleanup staging
        spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", sf_options["sfUser"]) \
            .option("password", sf_options["sfPassword"]) \
            .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
            .option("query", f"DROP TABLE IF EXISTS {staging_table}") \
            .load() \
            .collect()
        
        logger.info(f"✅ MERGE completed for {table_name}")
    else:
        # Simple overwrite or append
        df.write \
            .format("snowflake") \
            .options(**sf_options) \
            .option("dbtable", table_name.split('.')[-1]) \
            .mode(mode) \
            .save()
        
        logger.info(f"✅ {mode} completed for {table_name}")
    
    return True


def publish_to_redshift(
    df: DataFrame,
    table_name: str,
    mode: str = "append",
    config: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Publish DataFrame to Redshift table.
    
    Args:
        df: DataFrame to publish
        table_name: Target table name
        mode: Write mode ("append", "overwrite")
        config: Configuration dictionary
        
    Returns:
        True if successful
    """
    logger.info(f"Publishing to Redshift: {table_name} (mode={mode})")
    
    creds = get_redshift_credentials(config or {})
    
    # Build JDBC URL
    jdbc_url = (
        f"jdbc:redshift://{creds.get('host')}:{creds.get('port', 5439)}/"
        f"{creds.get('database')}"
    )
    
    df = df.withColumn("_load_ts", current_timestamp())
    
    # Write to Redshift
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("user", creds.get("user")) \
        .option("password", creds.get("password")) \
        .option("driver", "com.amazon.redshift.jdbc.Driver") \
        .option("dbtable", table_name) \
        .mode(mode) \
        .save()
    
    logger.info(f"✅ Published {df.count():,} rows to Redshift {table_name}")
    
    return True

