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

from pyspark.sql import SparkSession

from project_a.core.config import ProjectConfig
from project_a.iceberg_utils import DEFAULT_ICEBERG_PACKAGES, IcebergConfig
from project_a.utils.path_resolver import resolve_data_path

logger = logging.getLogger(__name__)


def _append_extension(existing: str | None, extension: str) -> str:
    if not existing:
        return extension
    parts = [part.strip() for part in existing.split(",") if part.strip()]
    if extension not in parts:
        parts.append(extension)
    return ",".join(parts)


def _merge_packages(existing: str | None, extra: str | None) -> str | None:
    if not extra and not existing:
        return None
    if not existing:
        return extra
    if not extra:
        return existing
    existing_parts = [part.strip() for part in existing.split(",") if part.strip()]
    extra_parts = [part.strip() for part in extra.split(",") if part.strip()]
    for part in extra_parts:
        if part not in existing_parts:
            existing_parts.append(part)
    return ",".join(existing_parts)


def _is_emr_runtime() -> bool:
    """Best-effort detection for EMR/EMR Serverless runtime."""
    emr_markers = (
        "EMR_JOB_ID",
        "EMR_SERVERLESS_APPLICATION_ID",
        "EMR_RELEASE_LABEL",
        "EMR_CLUSTER_ID",
    )
    return any(os.environ.get(marker) for marker in emr_markers)


def _config_uses_s3(config: ProjectConfig) -> bool:
    """Check whether any configured input/output path uses S3."""
    paths = config.paths
    if any(isinstance(v, str) and v.startswith("s3://") for v in paths.values()):
        return True

    for source_cfg in config.sources.values():
        if isinstance(source_cfg, dict):
            if any(isinstance(v, str) and v.startswith("s3://") for v in source_cfg.values()):
                return True
    return False


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
        self.spark: SparkSession | None = None
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
        if self._is_local and os.environ.get("PROJECT_A_FORCE_PYSPARK_HOME", "1") == "1":
            try:
                import pyspark

                pyspark_home = os.path.dirname(pyspark.__file__)
                os.environ["SPARK_HOME"] = pyspark_home
                logger.info("Using PySpark SPARK_HOME for local run: %s", pyspark_home)
            except Exception as exc:
                logger.warning("Unable to set PySpark SPARK_HOME: %s", exc)

        builder = SparkSession.builder.appName(self.app_name)

        extensions = None
        merged_packages = None
        runtime_on_emr = _is_emr_runtime()
        uses_s3 = _config_uses_s3(self.config)
        should_bootstrap_packages = self._is_local or not runtime_on_emr

        try:
            import importlib_metadata

            delta_version = importlib_metadata.version("delta_spark")
        except Exception:
            delta_version = None

        if self._is_local:
            local_master = str((self.config.get("spark", {}) or {}).get("master", "local[*]"))
            builder = builder.master(local_master)
            os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
            os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
            logger.info("Using %s master for local execution", local_master)

        # Enable Delta with environment-aware package loading.
        # - Local/non-EMR runtimes need explicit package coordinates.
        # - EMR runtimes can rely on platform-provided Delta when installed.
        if delta_version:
            extensions = _append_extension(extensions, "io.delta.sql.DeltaSparkSessionExtension")
            builder = builder.config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            if should_bootstrap_packages:
                delta_pkg = f"io.delta:delta-spark_2.12:{delta_version}"
                merged_packages = _merge_packages(merged_packages, delta_pkg)
            logger.info("Delta Lake enabled (delta_spark=%s)", delta_version)
        elif runtime_on_emr:
            extensions = _append_extension(extensions, "io.delta.sql.DeltaSparkSessionExtension")
            builder = builder.config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            logger.info(
                "Delta Lake extension enabled for EMR runtime (cluster-provided jars expected)"
            )
        else:
            logger.warning("delta_spark not installed; Delta Lake features disabled")

        # Add S3 filesystem connector jars when running off-EMR with S3 paths.
        if uses_s3 and should_bootstrap_packages:
            hadoop_aws_pkg = os.environ.get(
                "PROJECT_A_HADOOP_AWS_PACKAGE", "org.apache.hadoop:hadoop-aws:3.3.4"
            )
            aws_sdk_pkg = os.environ.get(
                "PROJECT_A_AWS_SDK_PACKAGE", "com.amazonaws:aws-java-sdk-bundle:1.12.262"
            )
            merged_packages = _merge_packages(
                merged_packages,
                f"{hadoop_aws_pkg},{aws_sdk_pkg}",
            )
            logger.info("S3 connector packages enabled for non-EMR runtime")

        # Optional Iceberg integration (via Spark packages)
        iceberg_cfg = self.config.get("iceberg", {})
        if iceberg_cfg.get("enabled"):
            catalog_name = iceberg_cfg.get("catalog_name", "local")
            catalog_type = iceberg_cfg.get("catalog_type", IcebergConfig.HADOOP_CATALOG)
            warehouse_path = iceberg_cfg.get("warehouse", "data/iceberg-warehouse")
            iceberg_spark_cfg = IcebergConfig.get_spark_config(
                catalog_type=catalog_type,
                warehouse_path=warehouse_path,
                catalog_name=catalog_name,
            )

            iceberg_extensions = iceberg_spark_cfg.pop("spark.sql.extensions", None)
            if iceberg_extensions:
                extensions = _append_extension(extensions, iceberg_extensions)

            for key, value in iceberg_spark_cfg.items():
                builder = builder.config(key, value)

            packages = iceberg_cfg.get("packages") or DEFAULT_ICEBERG_PACKAGES
            merged_packages = _merge_packages(merged_packages, packages)
            logger.info("Iceberg extensions enabled")

        if extensions:
            builder = builder.config("spark.sql.extensions", extensions)
        if merged_packages:
            builder = builder.config("spark.jars.packages", merged_packages)

        spark_cfg = self.config.get("spark", {}) or {}
        spark_conf = spark_cfg.get("conf", {}) if isinstance(spark_cfg, dict) else {}
        spark_tuning = spark_cfg.get("tuning", {}) if isinstance(spark_cfg, dict) else {}

        def _tuning_value(key: str, default):
            if isinstance(spark_tuning, dict) and key in spark_tuning:
                return spark_tuning.get(key)
            return spark_cfg.get(key, default)

        # Allow direct passthrough of explicit spark.* keys.
        for key, value in spark_cfg.items():
            if key in {
                "master",
                "driver_memory",
                "executor_memory",
                "shuffle_partitions",
                "enable_aqe",
                "enable_adaptive_join",
                "adaptive_coalesce_partitions",
                "broadcast_join_threshold",
                "conf",
                "tuning",
            }:
                continue
            if isinstance(key, str) and key.startswith("spark."):
                builder = builder.config(key, str(value))

        for key, value in spark_conf.items():
            if isinstance(key, str) and key.startswith("spark."):
                builder = builder.config(key, str(value))

        # Avoid Spark 3+ time parsing exceptions unless explicitly overridden.
        if (
            "spark.sql.legacy.timeParserPolicy" not in spark_cfg
            and "spark.sql.legacy.timeParserPolicy" not in spark_conf
        ):
            builder = builder.config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        shuffle_partitions = _tuning_value("shuffle_partitions", 400)
        adaptive_enabled = str(_tuning_value("enable_aqe", True)).lower()
        adaptive_skew = str(_tuning_value("enable_adaptive_join", True)).lower()
        adaptive_coalesce = str(_tuning_value("adaptive_coalesce_partitions", True)).lower()
        broadcast_threshold = _tuning_value("broadcast_join_threshold", "67108864")

        # Spark optimizations
        builder = (
            builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
            .config("spark.sql.adaptive.enabled", adaptive_enabled)
            .config("spark.sql.adaptive.skewJoin.enabled", adaptive_skew)
            .config("spark.sql.adaptive.coalescePartitions.enabled", adaptive_coalesce)
            .config("spark.sql.autoBroadcastJoinThreshold", str(broadcast_threshold))
            .config("spark.eventLog.enabled", "false")  # Prevent log directory conflicts
        )

        driver_memory = _tuning_value("driver_memory", None)
        if driver_memory:
            builder = builder.config("spark.driver.memory", str(driver_memory))
        executor_memory = _tuning_value("executor_memory", None)
        if executor_memory:
            builder = builder.config("spark.executor.memory", str(executor_memory))

        # Stop existing session for local mode
        if self._is_local and os.environ.get("PROJECT_A_DISABLE_SPARK_STOP") != "1":
            try:
                existing_spark = SparkSession.getActiveSession()
                if existing_spark:
                    logger.info("Stopping existing SparkSession")
                    existing_spark.stop()
            except Exception:
                pass

            try:
                from pyspark import SparkContext

                existing_sc = SparkContext._active_spark_context
                if existing_sc:
                    logger.info("Stopping existing SparkContext")
                    existing_sc.stop()
            except Exception:
                pass

            try:
                SparkContext._active_spark_context = None
            except Exception:
                pass

            try:
                SparkSession._instantiatedSession = None
                SparkSession._activeSession = None
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
        has_s3 = any(isinstance(v, str) and v.startswith("s3://") for v in paths.values())

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

    def resolve_path(self, layer: str, source: str | None = None, table: str | None = None) -> str:
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
