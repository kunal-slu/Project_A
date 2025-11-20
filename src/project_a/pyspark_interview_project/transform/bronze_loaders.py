"""
Bronze Layer Data Loaders

Reusable functions for loading data from Bronze layer (CSV, JSON, Delta, Parquet).
Handles graceful fallbacks and schema validation.
"""

import logging
from typing import Dict, Any, Tuple, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


def load_with_fallback(
    spark: SparkSession,
    path: str,
    schema: Optional[StructType] = None,
    format_hint: Optional[str] = None
) -> DataFrame:
    """
    Load data with fallback: Delta → Parquet → CSV.
    
    Args:
        spark: SparkSession
        path: Data path (may be Delta table path or file path)
        schema: Optional schema for CSV reads
        format_hint: Hint for format (e.g., "csv", "json")
        
    Returns:
        DataFrame with data, or empty DataFrame with schema if no data found
    """
    # Try Delta first
    try:
        delta_path = path.replace(".csv", "").replace(".json", "")
        df = spark.read.format("delta").load(delta_path)
        # Verify it's not empty
        try:
            _ = df.first()
            logger.debug(f"Loaded from Delta: {delta_path}")
            return df
        except Exception:
            raise Exception("Delta table is empty")
    except Exception as e:
        logger.debug(f"Delta load failed: {e}, trying Parquet...")
    
    # Try Parquet
    try:
        parquet_path = path.replace(".csv", "").replace(".json", "")
        df = spark.read.format("parquet").load(parquet_path)
        try:
            _ = df.first()
            logger.debug(f"Loaded from Parquet: {parquet_path}")
            return df
        except Exception:
            raise Exception("Parquet table is empty")
    except Exception as e:
        logger.debug(f"Parquet load failed: {e}, trying CSV/JSON...")
    
    # Try CSV or JSON
    try:
        if format_hint == "json" or path.endswith(".json"):
            if schema:
                df = spark.read.schema(schema).json(path)
            else:
                df = spark.read.json(path)
        else:
            # CSV - read without schema to avoid column misalignment
            # Schema will be applied later via explicit column selection
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        
        logger.info(f"Loaded from {format_hint or 'CSV'}: {path}")
        return df
    except Exception as e:
        logger.warning(f"All load attempts failed for {path}: {e}")
        # Return empty DataFrame with schema if provided
        if schema:
            return spark.createDataFrame([], schema)
        else:
            raise ValueError(f"Could not load data from {path} and no schema provided for empty DataFrame")


def load_crm_bronze_data(
    spark: SparkSession,
    config: Dict[str, Any]
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Load CRM bronze data (accounts, contacts, opportunities).
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        
    Returns:
        Tuple of (accounts_df, contacts_df, opportunities_df)
    """
    from project_a.utils.path_resolver import resolve_source_file_path
    from project_a.schemas.bronze_schemas import (
        CRM_ACCOUNTS_SCHEMA,
        CRM_CONTACTS_SCHEMA,
        CRM_OPPORTUNITIES_SCHEMA
    )
    
    logger.info("📥 Loading CRM bronze data...")
    
    try:
        accounts_path = resolve_source_file_path(config, "crm", "accounts")
        contacts_path = resolve_source_file_path(config, "crm", "contacts")
        opps_path = resolve_source_file_path(config, "crm", "opportunities")
    except (ValueError, KeyError) as e:
        logger.warning(f"Path resolution failed: {e}, using fallback paths")
        crm_cfg = config.get("sources", {}).get("crm", {})
        base_path = crm_cfg.get("base_path", config.get("paths", {}).get("bronze_root", "") + "/crm")
        files = crm_cfg.get("files", {})
        accounts_path = f"{base_path}/{files.get('accounts', 'accounts.csv')}"
        contacts_path = f"{base_path}/{files.get('contacts', 'contacts.csv')}"
        opps_path = f"{base_path}/{files.get('opportunities', 'opportunities.csv')}"
    
    df_accounts = load_with_fallback(spark, accounts_path, CRM_ACCOUNTS_SCHEMA)
    df_contacts = load_with_fallback(spark, contacts_path, CRM_CONTACTS_SCHEMA)
    df_opps = load_with_fallback(spark, opps_path, CRM_OPPORTUNITIES_SCHEMA)
    
    # Log row counts safely
    try:
        logger.info(f"  Accounts: {df_accounts.count():,} rows")
        logger.info(f"  Contacts: {df_contacts.count():,} rows")
        logger.info(f"  Opportunities: {df_opps.count():,} rows")
    except Exception as e:
        logger.warning(f"Could not count rows: {e}")
    
    return df_accounts, df_contacts, df_opps


def load_snowflake_bronze_data(
    spark: SparkSession,
    config: Dict[str, Any]
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Load Snowflake bronze data (customers, orders, products).
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        
    Returns:
        Tuple of (customers_df, orders_df, products_df)
    """
    from project_a.utils.path_resolver import resolve_source_file_path
    from project_a.schemas.bronze_schemas import (
        SNOWFLAKE_CUSTOMERS_SCHEMA,
        SNOWFLAKE_ORDERS_SCHEMA,
        SNOWFLAKE_PRODUCTS_SCHEMA
    )
    
    logger.info("📥 Loading Snowflake bronze data...")
    
    try:
        customers_path = resolve_source_file_path(config, "snowflake", "customers")
        orders_path = resolve_source_file_path(config, "snowflake", "orders")
        products_path = resolve_source_file_path(config, "snowflake", "products")
    except (ValueError, KeyError) as e:
        logger.warning(f"Path resolution failed: {e}, using fallback paths")
        snowflake_cfg = config.get("sources", {}).get("snowflake", {})
        base_path = snowflake_cfg.get("base_path", config.get("paths", {}).get("bronze_root", "") + "/snowflake")
        files = snowflake_cfg.get("files", {})
        customers_path = f"{base_path}/{files.get('customers', 'snowflake_customers_50000.csv')}"
        orders_path = f"{base_path}/{files.get('orders', 'snowflake_orders_100000.csv')}"
        products_path = f"{base_path}/{files.get('products', 'snowflake_products_10000.csv')}"
    
    df_customers = load_with_fallback(spark, customers_path, SNOWFLAKE_CUSTOMERS_SCHEMA)
    df_orders = load_with_fallback(spark, orders_path, SNOWFLAKE_ORDERS_SCHEMA)
    df_products = load_with_fallback(spark, products_path, SNOWFLAKE_PRODUCTS_SCHEMA)
    
    try:
        logger.info(f"  Customers: {df_customers.count():,} rows")
        logger.info(f"  Orders: {df_orders.count():,} rows")
        logger.info(f"  Products: {df_products.count():,} rows")
    except Exception as e:
        logger.warning(f"Could not count rows: {e}")
    
    return df_customers, df_orders, df_products


def load_redshift_behavior_bronze_data(
    spark: SparkSession,
    config: Dict[str, Any]
) -> DataFrame:
    """
    Load Redshift behavior bronze data.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        
    Returns:
        Behavior DataFrame
    """
    from project_a.utils.path_resolver import resolve_source_file_path
    from project_a.schemas.bronze_schemas import REDSHIFT_BEHAVIOR_SCHEMA
    
    logger.info("📥 Loading Redshift behavior bronze data...")
    
    try:
        behavior_path = resolve_source_file_path(config, "redshift", "behavior")
    except (ValueError, KeyError) as e:
        logger.warning(f"Path resolution failed: {e}, using fallback path")
        redshift_cfg = config.get("sources", {}).get("redshift", {})
        base_path = redshift_cfg.get("base_path", config.get("paths", {}).get("bronze_root", "") + "/redshift")
        files = redshift_cfg.get("files", {})
        behavior_path = f"{base_path}/{files.get('behavior', 'redshift_customer_behavior_50000.csv')}"
    
    df_behavior = load_with_fallback(spark, behavior_path, REDSHIFT_BEHAVIOR_SCHEMA)
    
    try:
        logger.info(f"  Behavior: {df_behavior.count():,} rows")
    except Exception as e:
        logger.warning(f"Could not count rows: {e}")
    
    return df_behavior


def load_kafka_bronze_data(
    spark: SparkSession,
    config: Dict[str, Any]
) -> DataFrame:
    """
    Load Kafka bronze data (from CSV seed file or actual Kafka stream).
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        
    Returns:
        Kafka events DataFrame
    """
    from project_a.utils.path_resolver import resolve_source_file_path
    from project_a.schemas.bronze_schemas import KAFKA_EVENTS_SCHEMA
    
    logger.info("📥 Loading Kafka bronze data...")
    
    try:
        kafka_path = resolve_source_file_path(config, "kafka_sim", "orders_seed")
    except (ValueError, KeyError) as e:
        logger.warning(f"Path resolution failed: {e}, using fallback path")
        kafka_cfg = config.get("sources", {}).get("kafka_sim", {})
        base_path = kafka_cfg.get("base_path", config.get("paths", {}).get("bronze_root", "") + "/kafka")
        files = kafka_cfg.get("files", {})
        kafka_path = f"{base_path}/{files.get('orders_seed', 'stream_kafka_events_100000.csv')}"
    
    df_kafka = load_with_fallback(spark, kafka_path, KAFKA_EVENTS_SCHEMA)
    
    try:
        logger.info(f"  Kafka events: {df_kafka.count():,} rows")
    except Exception as e:
        logger.warning(f"Could not count rows: {e}")
    
    return df_kafka

