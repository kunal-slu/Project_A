"""
Job Context - Spark + Delta Builder

Provides a unified context for all ETL jobs with:
- SparkSession management
- Delta Lake configuration
- Path resolution
- AWS S3 configuration
"""

import logging
import os
from typing import Dict, Optional

from pyspark.sql import SparkSession

from project_a.core.config import ProjectConfig
from project_a.utils.path_resolver import resolve_data_path, is_local_environment

logger = logging.getLogger(__name__)


class JobContext:
    """
    Context manager for ETL jobs.
    
    Handles:
    - SparkSession creation and configuration
    - Delta Lake setup
    - Path resolution (local vs S3)
    - AWS credentials
    """
    
    def __init__(self, config: ProjectConfig, app_name: str = "project_a"):
        """
        Initialize job context.
        
        Args:
            config: ProjectConfig instance
            app_name: Spark application name
        """
        self.config = config
        self.app_name = app_name
        self.spark: Optional[SparkSession] = None
        self._is_local = config.is_local()
    
    def __enter__(self) -> "JobContext":
        """Enter context - create SparkSession."""
        self.spark = self._build_spark()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context - stop SparkSession."""
        if self.spark:
            try:
                self.spark.stop()
            except Exception as e:
                logger.warning(f"Error stopping SparkSession: {e}")
    
    def _build_spark(self) -> SparkSession:
        """Build and configure SparkSession."""
        builder = SparkSession.builder.appName(self.app_name)
        
        if self._is_local:
            builder = builder.master("local[*]")
            logger.info("Using local[*] master for local execution")
        else:
            # Enable Delta Lake for EMR/production
            builder = (
                builder
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            )
            logger.info("Delta Lake extensions enabled for EMR")
        
        # Spark optimizations
        builder = (
            builder
            .config("spark.sql.shuffle.partitions", "400")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.autoBroadcastJoinThreshold", "67108864")
            .config("spark.eventLog.enabled", "false")  # Prevent log directory conflicts
        )
        
        # Stop existing session for local mode
        if self._is_local:
            try:
                from pyspark import SparkContext
                existing_sc = SparkContext._active_spark_context
                if existing_sc:
                    logger.info("Stopping existing SparkContext")
                    existing_sc.stop()
            except Exception:
                pass
            
            try:
                existing_spark = SparkSession.getActiveSession()
                if existing_spark:
                    logger.info("Stopping existing SparkSession")
                    existing_spark.stop()
            except Exception:
                pass
        
        spark = builder.getOrCreate()
        logger.info("SparkSession created successfully")
        
        # Configure AWS S3 if needed
        self._configure_s3(spark)
        
        return spark
    
    def _configure_s3(self, spark: SparkSession) -> None:
        """Configure AWS S3 access."""
        # Check if any S3 paths are used
        paths = self.config.paths
        has_s3 = any(
            isinstance(v, str) and v.startswith("s3://")
            for v in paths.values()
        )
        
        if not has_s3:
            sources = self.config.sources
            for source_config in sources.values():
                if isinstance(source_config, dict):
                    for v in source_config.values():
                        if isinstance(v, str) and v.startswith("s3://"):
                            has_s3 = True
                            break
                if has_s3:
                    break
        
        if has_s3:
            try:
                spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
                spark.conf.set(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
                )
                if not self._is_local:
                    spark.conf.set(
                        "spark.hadoop.fs.s3a.aws.credentials.provider",
                        "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
                    )
                logger.info("AWS S3 configuration applied")
            except Exception as e:
                logger.warning(f"AWS S3 configuration failed: {e}")
    
    def resolve_path(self, layer: str, source: Optional[str] = None, table: Optional[str] = None) -> str:
        """
        Resolve data path for a layer.
        
        Args:
            layer: Data layer (bronze, silver, gold)
            source: Optional source name (crm, snowflake, etc.)
            table: Optional table name
        
        Returns:
            Resolved path (file:// or s3://)
        """
        return resolve_data_path(self.config._config, layer, source, table)
    
    def get_kafka_bootstrap_servers(self) -> str:
        """Get Kafka bootstrap servers based on environment."""
        kafka_cfg = self.config.kafka
        
        if self._is_local:
            return kafka_cfg.get("local_bootstrap_servers", "localhost:9092")
        else:
            msk_bootstrap = kafka_cfg.get("msk_bootstrap_servers")
            if not msk_bootstrap:
                raise ValueError("MSK bootstrap servers not configured for AWS environment")
            return msk_bootstrap

