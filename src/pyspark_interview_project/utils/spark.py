"""
Spark session utilities with Delta Lake support.
"""

import logging
from typing import Dict, Optional
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str, extra_conf: Optional[Dict] = None) -> SparkSession:
    """
    Create and configure SparkSession with Delta Lake support.
    
    Args:
        app_name: Application name for Spark
        extra_conf: Additional Spark configuration
        
    Returns:
        Configured SparkSession
    """
    builder = SparkSession.builder.appName(app_name)
    
    # Performance optimizations
    builder = builder.config("spark.sql.adaptive.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    builder = builder.config("spark.sql.adaptive.skewJoin.enabled", "true")
    
    # Default shuffle partitions
    shuffle_partitions = 200
    if extra_conf and "spark.sql.shuffle.partitions" in extra_conf:
        shuffle_partitions = extra_conf["spark.sql.shuffle.partitions"]
    
    builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    
    # Apply extra configuration
    if extra_conf:
        for key, value in extra_conf.items():
            if key != "spark.sql.shuffle.partitions":  # Already set above
                builder = builder.config(key, str(value))
    
    try:
        spark = builder.getOrCreate()
        
        # Only configure Delta Lake if delta-spark is available
        try:
            import delta
            spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            logger.info("Delta Lake configured successfully")
        except ImportError:
            logger.info("Delta Lake not available, continuing without Delta Lake support")
        except Exception as e:
            logger.warning(f"Delta Lake configuration failed: {e}")
            logger.info("Continuing without Delta Lake support")
        
        logger.info(f"Spark session created successfully: {app_name}")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise


def is_local(conf: Dict) -> bool:
    """
    Determine if running in local environment based on configuration.
    
    Args:
        conf: Configuration dictionary
        
    Returns:
        True if local environment, False otherwise
    """
    env = conf.get("env", "local")
    return env == "local"