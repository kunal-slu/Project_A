"""
Spark Session Builder

Creates and configures SparkSession with Delta Lake support.
"""

import logging
import os

from pyspark.sql import SparkSession

from project_a.iceberg_utils import DEFAULT_ICEBERG_PACKAGES, IcebergConfig

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
    emr_markers = (
        "EMR_JOB_ID",
        "EMR_SERVERLESS_APPLICATION_ID",
        "EMR_RELEASE_LABEL",
        "EMR_CLUSTER_ID",
    )
    return any(os.environ.get(marker) for marker in emr_markers)


def _config_uses_s3_paths(config: dict) -> bool:
    paths = config.get("paths", {})
    if any(isinstance(v, str) and v.startswith("s3://") for v in paths.values()):
        return True

    sources = config.get("sources", {})
    for source_cfg in sources.values():
        if isinstance(source_cfg, dict):
            if any(isinstance(v, str) and v.startswith("s3://") for v in source_cfg.values()):
                return True
    return False


def build_spark(app_name: str = "project_a", config: dict | None = None) -> SparkSession:
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

    # Configure environment early
    spark_env = os.getenv("SPARK_ENV") or config.get("environment") or config.get("env") or "local"

    # Force local runs to use the PySpark-bundled Spark distribution
    if spark_env == "local" and os.environ.get("PROJECT_A_FORCE_PYSPARK_HOME", "1") == "1":
        try:
            import pyspark

            pyspark_home = os.path.dirname(pyspark.__file__)
            os.environ["SPARK_HOME"] = pyspark_home
            logger.info("Using PySpark SPARK_HOME for local run: %s", pyspark_home)
        except Exception as exc:
            logger.warning("Unable to set PySpark SPARK_HOME: %s", exc)

    spark_cfg = config.get("spark", {}) if isinstance(config, dict) else {}
    spark_conf = spark_cfg.get("conf", {}) if isinstance(spark_cfg, dict) else {}
    spark_tuning = spark_cfg.get("tuning", {}) if isinstance(spark_cfg, dict) else {}

    # Build SparkSession
    builder = SparkSession.builder.appName(app_name)

    # Configure Delta/Iceberg packaging & extensions
    extensions = None
    merged_packages = (
        config.get("spark", {}).get("spark.jars.packages") if isinstance(config, dict) else None
    )
    runtime_on_emr = _is_emr_runtime()
    should_bootstrap_packages = spark_env == "local" or not runtime_on_emr
    has_s3_paths = _config_uses_s3_paths(config)

    # Set master for local execution
    if spark_env == "local":
        local_master = str(spark_cfg.get("master", "local[*]"))
        builder = builder.master(local_master)
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
        logger.info("Using %s master for local execution", local_master)

    # Enable Delta with environment-aware package loading.
    try:
        import importlib_metadata

        delta_version = importlib_metadata.version("delta_spark")
    except Exception:
        delta_version = None

    if delta_version:
        extensions = _append_extension(extensions, "io.delta.sql.DeltaSparkSessionExtension")
        builder = builder.config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        if should_bootstrap_packages:
            delta_pkg = f"io.delta:delta-spark_2.12:{delta_version}"
            merged_packages = _merge_packages(merged_packages, delta_pkg)
        logger.info("Delta Lake enabled (delta_spark=%s)", delta_version)
    elif runtime_on_emr:
        extensions = _append_extension(extensions, "io.delta.sql.DeltaSparkSessionExtension")
        builder = builder.config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        logger.info("Delta Lake extension enabled for EMR runtime (cluster-provided jars expected)")
    else:
        logger.info("Delta Lake extensions disabled (delta_spark not found)")

    # Add S3 connector jars when running off-EMR with S3 paths.
    if has_s3_paths and should_bootstrap_packages:
        hadoop_aws_pkg = os.environ.get(
            "PROJECT_A_HADOOP_AWS_PACKAGE", "org.apache.hadoop:hadoop-aws:3.3.4"
        )
        aws_sdk_pkg = os.environ.get(
            "PROJECT_A_AWS_SDK_PACKAGE", "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        merged_packages = _merge_packages(merged_packages, f"{hadoop_aws_pkg},{aws_sdk_pkg}")
        logger.info("S3 connector packages enabled for non-EMR runtime")

    # Optional Iceberg integration (via Spark packages)
    iceberg_cfg = config.get("iceberg", {}) if isinstance(config, dict) else {}
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

    # Avoid Spark 3+ time parsing exceptions unless explicitly overridden.
    if (
        "spark.sql.legacy.timeParserPolicy" not in spark_cfg
        and "spark.sql.legacy.timeParserPolicy" not in spark_conf
    ):
        builder = builder.config("spark.sql.legacy.timeParserPolicy", "CORRECTED")

    # Apply explicit Spark passthrough keys only (to avoid non-Spark config warnings).
    reserved_cfg_keys = {
        "master",
        "driver_memory",
        "executor_memory",
        "shuffle_partitions",
        "enable_aqe",
        "enable_adaptive_join",
        "adaptive_coalesce_partitions",
        "broadcast_join_threshold",
    }
    for k, v in spark_cfg.items():
        if k in reserved_cfg_keys:
            continue
        if k in {"conf", "tuning"}:
            continue
        if k.startswith("spark."):
            builder = builder.config(k, str(v))
        else:
            logger.debug("Skipping non-Spark config key: %s", k)

    for k, v in spark_conf.items():
        if k.startswith("spark."):
            builder = builder.config(k, str(v))
        else:
            logger.debug("Skipping non-Spark spark.conf key: %s", k)

    def _tuning_value(key: str, default):
        if isinstance(spark_tuning, dict) and key in spark_tuning:
            return spark_tuning.get(key)
        return spark_cfg.get(key, default)

    # Set default Spark optimizations
    builder = (
        builder.config(
            "spark.sql.shuffle.partitions", str(_tuning_value("shuffle_partitions", 400))
        )
        .config(
            "spark.sql.adaptive.enabled",
            str(_tuning_value("enable_aqe", True)).lower(),
        )
        .config(
            "spark.sql.adaptive.skewJoin.enabled",
            str(_tuning_value("enable_adaptive_join", True)).lower(),
        )
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            str(_tuning_value("adaptive_coalesce_partitions", True)).lower(),
        )
        .config(
            "spark.sql.autoBroadcastJoinThreshold",
            str(_tuning_value("broadcast_join_threshold", 64 * 1024 * 1024)),
        )
        .config("spark.eventLog.enabled", "false")
    )

    driver_memory = _tuning_value("driver_memory", None)
    if driver_memory:
        builder = builder.config("spark.driver.memory", str(driver_memory))
    executor_memory = _tuning_value("executor_memory", None)
    if executor_memory:
        builder = builder.config("spark.executor.memory", str(executor_memory))

    # Create SparkSession
    # CRITICAL: PySpark 3.5.0 has a bug with Java 17 where getOrCreate() fails
    # when there's an existing SparkContext. Solution: Force clean state first.
    try:
        from pyspark import SparkContext

        # CRITICAL: Stop ALL existing Spark sessions and contexts
        # This must be done BEFORE any builder calls
        if os.environ.get("PROJECT_A_DISABLE_SPARK_STOP") == "1":
            logger.info("Skipping SparkContext stop (PROJECT_A_DISABLE_SPARK_STOP=1)")
        else:
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

            try:
                SparkContext._active_spark_context = None
            except Exception:
                pass

            try:
                SparkSession._instantiatedSession = None
                SparkSession._activeSession = None
            except Exception:
                pass

        # For local mode, use a workaround that avoids the HashMap bug
        if spark_env == "local":
            # Set environment variables to ensure clean state
            os.environ.setdefault("PYSPARK_PYTHON", "python3")
            os.environ.setdefault("PYSPARK_DRIVER_PYTHON", "python3")

            # Apply local-specific defaults without discarding existing configs
            builder = (
                builder.config("spark.eventLog.enabled", "false")
                .config("spark.driver.memory", str(_tuning_value("driver_memory", "2g")))
                .config("spark.executor.memory", str(_tuning_value("executor_memory", "2g")))
                .config("spark.sql.execution.arrow.pyspark.enabled", "false")
                .config("spark.driver.host", "localhost")
                .config("spark.driver.bindAddress", "127.0.0.1")
            )

        # Use the configured builder for all environments
        spark = builder.getOrCreate()

        logger.info(f"SparkSession created successfully ({spark_env} mode)")
    except Exception as e:
        logger.error(f"Failed to create SparkSession: {e}")
        logger.error("This is a known PySpark 3.5.0 + Java 17 compatibility issue.")
        logger.error("Workaround: Use PySpark 3.4.2 or Java 11")
        raise

    # Configure AWS S3 for both local and AWS execution
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


def get_spark(app_name: str = "project_a", config: dict | None = None) -> SparkSession:
    """
    Alias for build_spark for consistency.

    Args:
        app_name: Application name
        config: Configuration dictionary

    Returns:
        Configured SparkSession
    """
    return build_spark(app_name, config)
