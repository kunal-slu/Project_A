"""
Spark Session Builder

Creates and configures SparkSession with Delta Lake support.
"""

import logging
import os

from pyspark.sql import SparkSession

from project_a.iceberg_utils import DEFAULT_ICEBERG_PACKAGES, IcebergConfig

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
            use_default = (
                default_session.isDefined() and not default_session.get().sparkContext().isStopped()
            )

            # If options are provided, prefer a fresh session so configs apply
            if options:
                use_default = False

            if use_default:
                jsparkSession = default_session.get()
            else:
                # CRITICAL: Use Java builder API instead of constructor to avoid HashMap bug
                # The builder pattern doesn't use HashMap constructor
                java_builder = self._jvm.SparkSession.builder()
                java_builder = java_builder.appName(
                    sparkContext.appName if hasattr(sparkContext, "appName") else "project_a"
                )
                java_builder = java_builder.master(
                    sparkContext.master if hasattr(sparkContext, "master") else "local[*]"
                )
                # Apply options via builder.config to avoid HashMap constructor
                if options:
                    for key, value in options.items():
                        if value is not None:
                            java_builder = java_builder.config(key, str(value))
                # Build the session (this doesn't use HashMap constructor)
                jsparkSession = java_builder.getOrCreate()
        else:
            # jsparkSession provided - skip applying options (HashMap bug)
            pass

        # Set the Java session
        self._jsparkSession = jsparkSession
        # Initialize other attributes
        self._wrapped = None
    else:
        # Normal path - delegate to original
        _original_spark_session_init(
            self, sparkContext=sparkContext, jsparkSession=jsparkSession, options=options or {}
        )


# Apply the patch
SparkSession.__init__ = _patched_spark_session_init


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
        builder = builder.master("local[*]")
        logger.info("Using local[*] master for local execution")

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

    spark_cfg = config.get("spark", {}) if isinstance(config, dict) else {}

    # Avoid Spark 3+ time parsing exceptions unless the user explicitly overrides.
    if "spark.sql.legacy.timeParserPolicy" not in spark_cfg:
        builder = builder.config("spark.sql.legacy.timeParserPolicy", "CORRECTED")

    # Add extra Spark configs from config dict
    for k, v in spark_cfg.items():
        builder = builder.config(k, v)

    # Set default Spark optimizations
    builder = (
        builder.config(
            "spark.sql.shuffle.partitions", spark_cfg.get("shuffle_partitions", 400)
        )
        .config(
            "spark.sql.adaptive.enabled",
            str(spark_cfg.get("enable_aqe", True)).lower(),
        )
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

        # For local mode, use a workaround that avoids the HashMap bug
        if spark_env == "local":
            # Set environment variables to ensure clean state
            os.environ.setdefault("PYSPARK_PYTHON", "python3")
            os.environ.setdefault("PYSPARK_DRIVER_PYTHON", "python3")

            # Apply local-specific defaults without discarding existing configs
            builder = (
                builder.config("spark.sql.shuffle.partitions", "2")
                .config("spark.eventLog.enabled", "false")
                .config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
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
