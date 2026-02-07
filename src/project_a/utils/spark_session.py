"""
Spark Session Builder

Creates and configures SparkSession with Delta Lake support.
"""
import logging
import os
from typing import Dict, Optional
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# MONKEY PATCH: Fix PySpark 3.4.2/3.5.0 + Java 17 HashMap constructor bug
# The issue is that SparkSession.__init__ tries to pass options as HashMap to Java
# but the Java API doesn't support that constructor signature
# Solution: Patch SparkSession.__init__ to ignore options when sparkContext is provided

_original_spark_session_init = SparkSession.__init__

def _patched_spark_session_init(self, sparkContext=None, jsparkSession=None, options=None):
    """Patched SparkSession.__init__ that bypasses HashMap bug by using empty options."""
    # CRITICAL FIX: The bug is that PySpark passes options as HashMap to Java
    # Solution: Always pass empty dict for options, then apply settings via applyModifiableSettings
    if sparkContext is not None:
        # Set up the session
        self._sc = sparkContext
        self._jsc = self._sc._jsc
        self._jvm = self._sc._jvm
        
        assert self._jvm is not None
        
        if jsparkSession is None:
            # Check for default session first (this path doesn't use HashMap constructor)
            default_session = self._jvm.SparkSession.getDefaultSession()
            if default_session.isDefined() and not default_session.get().sparkContext().isStopped():
                jsparkSession = default_session.get()
                # NOTE: Skip applying options - they're already set via .config() calls
                # applyModifiableSettings has HashMap bug, so we skip it
            else:
                # CRITICAL: Use Java builder API instead of constructor to avoid HashMap bug
                # The builder pattern doesn't use HashMap constructor
                java_builder = self._jvm.SparkSession.builder()
                java_builder = java_builder.appName(sparkContext.appName if hasattr(sparkContext, 'appName') else "project_a")
                java_builder = java_builder.master(sparkContext.master if hasattr(sparkContext, 'master') else "local[*]")
                # Build the session (this doesn't use HashMap constructor)
                jsparkSession = java_builder.getOrCreate()
                # NOTE: Skip applying options - they're already set via .config() calls on Python builder
                # applyModifiableSettings also has HashMap bug, so we skip it
        else:
            # jsparkSession provided - skip applying options (HashMap bug)
            pass
        
        # Set the Java session
        self._jsparkSession = jsparkSession
        # Initialize other attributes
        self._wrapped = None
    else:
        # Normal path - delegate to original
        _original_spark_session_init(self, sparkContext=sparkContext, jsparkSession=jsparkSession, options=options or {})

# Apply the patch
SparkSession.__init__ = _patched_spark_session_init


def build_spark(app_name: str = "project_a", config: Optional[Dict] = None) -> SparkSession:
    """
    Create and configure SparkSession with Delta Lake support.
    
    Args:
        app_name: Application name
        config: Configuration dictionary
        
    Returns:
        Configured SparkSession
    """
    if config is None:
        config = {}
    
    app_name = config.get("app_name", app_name)
    
    # Build SparkSession
    builder = SparkSession.builder.appName(app_name)
    
    # Configure Delta Lake
    spark_env = os.getenv("SPARK_ENV") or config.get("environment") or config.get("env") or "local"
    
    # Set master for local execution
    if spark_env == "local":
        builder = builder.master("local[*]")
        logger.info("Using local[*] master for local execution")
        # For local execution, skip Delta Lake extensions (JARs not available)
        # Will use Parquet format instead
        logger.info("Delta Lake extensions disabled for local execution (using Parquet)")
    else:
        # Enable Delta Lake for EMR/production
        builder = (
            builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        logger.info(f"Delta Lake extensions enabled for environment: {spark_env}")
    
    # Add extra Spark configs from config dict
    for k, v in config.get("spark", {}).items():
        builder = builder.config(k, v)
    
    # Set default Spark optimizations
    builder = (
        builder
        .config("spark.sql.shuffle.partitions", config.get("spark", {}).get("shuffle_partitions", 400))
        .config("spark.sql.adaptive.enabled", str(config.get("spark", {}).get("enable_aqe", True)).lower())
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", 64 * 1024 * 1024)
    )
    
    # Create SparkSession
    # CRITICAL: PySpark 3.5.0 has a bug with Java 17 where getOrCreate() fails
    # when there's an existing SparkContext. Solution: Force clean state first.
    try:
        from pyspark import SparkContext
        
        # CRITICAL: Stop ALL existing Spark sessions and contexts
        # This must be done BEFORE any builder calls
        try:
            existing_spark = SparkSession.getActiveSession()
            if existing_spark:
                logger.info("Stopping existing SparkSession...")
                existing_spark.stop()
                # Wait a moment for cleanup
                import time
                time.sleep(0.5)
        except Exception:
            pass
        
        try:
            sc = SparkContext._active_spark_context
            if sc:
                logger.info("Stopping existing SparkContext...")
                sc.stop()
                import time
                time.sleep(0.5)
        except Exception:
            pass
        
        # For local mode, use a workaround that avoids the HashMap bug
        if spark_env == "local":
            # Set environment variables to ensure clean state
            os.environ.setdefault("PYSPARK_PYTHON", "python3")
            os.environ.setdefault("PYSPARK_DRIVER_PYTHON", "python3")
            
            # Use builder with minimal config - this should work if no existing context
            # The key is ensuring we have NO existing SparkContext
            spark = SparkSession.builder \
                .appName(app_name) \
                .master("local[*]") \
                .config("spark.sql.shuffle.partitions", "2") \
                .config("spark.eventLog.enabled", "false") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .config("spark.driver.host", "localhost") \
                .config("spark.driver.bindAddress", "127.0.0.1") \
                .getOrCreate()
        else:
            # For EMR, use the full builder
            spark = builder.getOrCreate()
        
        logger.info(f"SparkSession created successfully ({spark_env} mode)")
    except Exception as e:
        logger.error(f"Failed to create SparkSession: {e}")
        logger.error("This is a known PySpark 3.5.0 + Java 17 compatibility issue.")
        logger.error("Workaround: Use PySpark 3.4.2 or Java 11")
        raise
    
    # Configure AWS S3 for both local and AWS execution
    # Check if any S3 paths are used (bronze_root, silver_root, etc.)
    has_s3_paths = False
    paths = config.get("paths", {})
    for path_value in paths.values():
        if isinstance(path_value, str) and path_value.startswith("s3://"):
            has_s3_paths = True
            break
    
    # Also check sources for S3 paths
    sources = config.get("sources", {})
    for source_config in sources.values():
        if isinstance(source_config, dict):
            for source_path in source_config.values():
                if isinstance(source_path, str) and source_path.startswith("s3://"):
                    has_s3_paths = True
                    break
    
    if has_s3_paths:
        try:
            spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
            # Use default credentials provider chain (works for both local and EMR)
            spark.conf.set(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
            # For EMR, also support instance profile
            if spark_env in {"emr", "prod", "aws"}:
                spark.conf.set(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
                )
            logger.info("AWS S3 configuration applied (S3 paths detected)")
        except Exception as e:
            logger.warning(f"AWS S3 configuration failed: {str(e)}")
    
    return spark


def get_spark(app_name: str = "project_a", config: Optional[Dict] = None) -> SparkSession:
    """
    Alias for build_spark for consistency.
    
    Args:
        app_name: Application name
        config: Configuration dictionary
        
    Returns:
        Configured SparkSession
    """
    return build_spark(app_name, config)
