"""
Snowflake writer module for loading data to Snowflake.

Supports both append and MERGE modes for idempotent loading.
"""

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession

# Optional import - secrets module may not exist
try:
    from project_a.utils.secrets import get_snowflake_credentials
except ImportError:

    def get_snowflake_credentials(*args, **kwargs):
        raise NotImplementedError("Secrets module not available")


logger = logging.getLogger(__name__)


def write_df_to_snowflake(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    config: dict[str, Any],
    mode: str = "append",
    pk: list[str] | None = None,
) -> bool:
    """
    Write DataFrame to Snowflake with optional MERGE support.

    Args:
        spark: SparkSession
        df: DataFrame to write
        table_name: Target Snowflake table (e.g., "ANALYTICS.CUSTOMER_360")
        config: Configuration dictionary
        mode: Write mode ("append", "overwrite", "merge")
        pk: List of primary key columns for MERGE operation

    Returns:
        True if successful

    Example:
        write_df_to_snowflake(
            spark, gold_df, "ANALYTICS.CUSTOMER_360",
            config, mode="merge", pk=["customer_id", "event_ts"]
        )
    """
    logger.info(f"Writing DataFrame to Snowflake table: {table_name} (mode={mode})")

    try:
        # Get Snowflake credentials from config
        sf_creds = get_snowflake_credentials(config)

        # Build Snowflake connection options
        account = sf_creds.get("account", "").replace(".snowflakecomputing.com", "")
        sf_options = {
            "sfURL": f"{account}.snowflakecomputing.com",
            "sfUser": sf_creds.get("user"),
            "sfPassword": sf_creds.get("password"),
            "sfDatabase": sf_creds.get("database", "ANALYTICS"),
            "sfSchema": sf_creds.get("schema", "PUBLIC"),
            "sfWarehouse": sf_creds.get("warehouse"),
            "dbtable": table_name.split(".")[-1],  # Table name only
        }

        record_count = df.count()
        logger.info(f"Writing {record_count:,} records to {table_name}")

        # Handle different write modes
        if mode == "merge" and pk:
            logger.info(f"Using MERGE mode with primary keys: {pk}")
            _write_with_merge(spark, df, table_name, sf_options, pk)
        else:
            # Simple append or overwrite
            df.write.format("snowflake").options(**sf_options).mode(mode).save()

            logger.info(
                f"✅ Successfully wrote {record_count:,} records to {table_name} (mode={mode})"
            )

        return True

    except Exception as e:
        logger.error(f"❌ Failed to write to Snowflake {table_name}: {e}")
        raise


def _write_with_merge(
    spark: SparkSession, df: DataFrame, table_name: str, sf_options: dict[str, Any], pk: list[str]
) -> None:
    """
    Write DataFrame to Snowflake using MERGE for idempotent upserts.

    Args:
        spark: SparkSession
        df: DataFrame to write
        table_name: Target table name
        sf_options: Snowflake connection options
        pk: Primary key columns
    """
    # Create staging table
    staging_table = f"{table_name}_staging"

    logger.info(f"Writing to staging table: {staging_table}")

    # Write to staging
    df.write.format("snowflake").options(**sf_options).option(
        "dbtable", staging_table.split(".")[-1]
    ).mode("overwrite").save()

    # Build MERGE SQL
    merge_sql = _build_snowflake_merge_sql(
        target_table=table_name, staging_table=staging_table, pk=pk, columns=df.columns
    )

    logger.info(f"Executing MERGE SQL on {table_name}")

    # Execute MERGE via JDBC
    sf_url = f"jdbc:snowflake://{sf_options['sfURL']}"

    spark.read.format("jdbc").option("url", sf_url).option("user", sf_options["sfUser"]).option(
        "password", sf_options["sfPassword"]
    ).option("query", merge_sql).load().collect()  # Execute query

    logger.info(f"✅ MERGE completed for {table_name}")


def _build_snowflake_merge_sql(
    target_table: str, staging_table: str, pk: list[str], columns: list[str]
) -> str:
    """
    Build Snowflake MERGE SQL statement.

    Args:
        target_table: Target table name
        staging_table: Staging table name
        pk: Primary key columns
        columns: All columns to merge

    Returns:
        MERGE SQL statement
    """
    target_alias = "target"
    source_alias = "source"

    # ON clause
    on_clause = " AND ".join([f"{target_alias}.{key} = {source_alias}.{key}" for key in pk])

    # UPDATE clause (all columns except PK)
    update_cols = [col for col in columns if col not in pk]
    update_clause = ", ".join([f"{col} = {source_alias}.{col}" for col in update_cols])

    # INSERT clause
    insert_cols = ", ".join(columns)
    insert_values = ", ".join([f"{source_alias}.{col}" for col in columns])

    merge_sql = f"""
        MERGE INTO {target_table} AS {target_alias}
        USING {staging_table} AS {source_alias}
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_values})
    """

    return merge_sql.strip()
