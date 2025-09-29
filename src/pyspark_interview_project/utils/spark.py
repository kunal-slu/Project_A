"""
Spark session utilities with proper Delta Lake configuration.
"""

import os
import logging
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

logger = logging.getLogger(__name__)


def get_spark_session(
    app_name: str = "pyspark-interview-project",
    config: Optional[Dict[str, str]] = None,
    is_local: bool = False
) -> SparkSession:
    """
    Create a Spark session with Delta Lake configuration.
    
    Args:
        app_name: Name of the Spark application
        config: Additional Spark configuration
        is_local: Whether running in local mode
        
    Returns:
        Configured Spark session
    """
    logger.info(f"Creating Spark session: {app_name}")
    
    # Base configuration
    spark_conf = SparkConf()
    spark_conf.setAppName(app_name)
    
    # Delta Lake configuration (MANDATORY for all jobs)
    spark_conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    spark_conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    if is_local:
        # Local development configuration
        spark_conf.set("spark.sql.adaptive.enabled", "true")
        spark_conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark_conf.set("spark.driver.memory", "2g")
        spark_conf.set("spark.executor.memory", "2g")
        spark_conf.set("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    else:
        # Cloud configuration
        spark_conf.set("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        spark_conf.set("spark.sql.adaptive.enabled", "true")
        spark_conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark_conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    
    # Apply additional configuration
    if config:
        for key, value in config.items():
            spark_conf.set(key, value)
    
    # Create session
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    
    # Set log level from environment
    log_level = os.getenv("LOG_LEVEL", "INFO")
    spark.sparkContext.setLogLevel(log_level)
    
    logger.info(f"Spark session created successfully. Log level: {log_level}")
    return spark


def is_local() -> bool:
    """
    Check if running in local mode.
    
    Returns:
        True if running locally, False otherwise
    """
    return os.getenv("SPARK_ENV", "").lower() == "local"


def get_delta_config() -> Dict[str, str]:
    """
    Get standard Delta Lake configuration.
    
    Returns:
        Dictionary of Delta Lake configuration
    """
    return {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
    }