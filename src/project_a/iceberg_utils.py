"""
Apache Iceberg Integration for Project_A

Why Iceberg at Silver/Gold?
- ACID transactions with snapshot isolation
- Schema evolution without rewrites
- Time travel for debugging and compliance
- Hidden partitioning for better query performance
- Multi-engine compatibility (Spark, Trino, Presto)

Where we use it:
- Silver Layer: orders_silver (high update frequency, needs ACID)
- Gold Layer: Optional for fact tables with complex updates

Where we DON'T use it:
- Bronze Layer: Keep as raw Parquet (immutable, append-only)
"""

import logging
import os

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

DEFAULT_ICEBERG_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"


class IcebergConfig:
    """Iceberg catalog configuration."""

    # Catalog types
    HADOOP_CATALOG = "hadoop"
    HIVE_CATALOG = "hive"
    GLUE_CATALOG = "glue"

    @staticmethod
    def get_spark_config(
        catalog_type: str = HADOOP_CATALOG,
        warehouse_path: str = "data/iceberg-warehouse",
        catalog_name: str = "local",
    ) -> dict[str, str]:
        """
        Get Spark configurations for Iceberg.

        Args:
            catalog_type: Type of catalog (hadoop, hive, glue)
            warehouse_path: Path to Iceberg warehouse
            catalog_name: Catalog name for Spark SQL

        Returns:
            Dictionary of Spark configurations
        """
        base_config = {
            # Iceberg Spark extensions
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            # Catalog configuration
            f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{catalog_name}.type": catalog_type,
            # Default catalog
            "spark.sql.defaultCatalog": catalog_name,
            # Iceberg format version
            "spark.sql.iceberg.handle-timestamp-without-timezone": "true",
        }

        if catalog_type == IcebergConfig.HADOOP_CATALOG:
            base_config[f"spark.sql.catalog.{catalog_name}.warehouse"] = warehouse_path

        elif catalog_type == IcebergConfig.GLUE_CATALOG:
            base_config.update(
                {
                    f"spark.sql.catalog.{catalog_name}.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
                    f"spark.sql.catalog.{catalog_name}.warehouse": warehouse_path,
                    f"spark.sql.catalog.{catalog_name}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                }
            )

        return base_config


class IcebergWriter:
    """
    Write data to Iceberg tables with ACID guarantees.

    Provides high-level API for common write patterns.
    """

    def __init__(self, spark: SparkSession, catalog_name: str = "local"):
        self.spark = spark
        self.catalog_name = catalog_name

    def write_overwrite(self, df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
        """
        Write DataFrame to Iceberg table (overwrite).

        Args:
            df: DataFrame to write
            table_name: Fully qualified table name (db.table)
            mode: Write mode (overwrite, append)
        """
        logger.info(f"Writing to Iceberg table: {table_name} (mode={mode})")

        df.writeTo(f"{self.catalog_name}.{table_name}").using("iceberg").createOrReplace()

        logger.info(f"Successfully wrote {df.count()} rows to {table_name}")

    def write_append(self, df: DataFrame, table_name: str) -> None:
        """
        Append data to Iceberg table.

        Args:
            df: DataFrame to append
            table_name: Fully qualified table name
        """
        logger.info(f"Appending to Iceberg table: {table_name}")

        df.writeTo(f"{self.catalog_name}.{table_name}").using("iceberg").append()

        logger.info(f"Successfully appended {df.count()} rows to {table_name}")

    def write_merge(
        self, df: DataFrame, table_name: str, merge_key: str, update_cols: list | None = None
    ) -> None:
        """
        Merge (upsert) data into Iceberg table.

        Handles late-arriving data and updates gracefully.

        Args:
            df: DataFrame with updates
            table_name: Fully qualified table name
            merge_key: Column(s) to match on (e.g., 'order_id')
            update_cols: Columns to update (None = all columns)
        """
        logger.info("Merging into Iceberg table: %s on key=%s", table_name, merge_key)

        # Create temp view for merge
        df.createOrReplaceTempView("updates")

        # Build merge statement
        all_cols = [c for c in df.columns if c != merge_key]
        if update_cols:
            update_target_cols = [c for c in update_cols if c != merge_key]
        else:
            update_target_cols = all_cols
        if not update_target_cols:
            update_target_cols = all_cols
        update_clause = ", ".join([f"t.{col} = s.{col}" for col in update_target_cols])

        merge_sql = f"""
        MERGE INTO {self.catalog_name}.{table_name} t
        USING updates s
        ON t.{merge_key} = s.{merge_key}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT *
        """
        try:
            self.spark.sql(merge_sql)
            logger.info("Successfully merged data into %s", table_name)
        except Exception:
            logger.info(
                "Merge target %s not available yet, creating table with overwrite", table_name
            )
            self.write_overwrite(df, table_name)

    def create_table(
        self,
        df: DataFrame,
        table_name: str,
        partition_by: list | None = None,
        properties: dict[str, str] | None = None,
    ) -> None:
        """
        Create new Iceberg table from DataFrame.

        Args:
            df: DataFrame with schema
            table_name: Fully qualified table name
            partition_by: List of partition columns
            properties: Table properties
        """
        logger.info(f"Creating Iceberg table: {table_name}")

        writer = df.writeTo(f"{self.catalog_name}.{table_name}").using("iceberg")

        if partition_by:
            writer = writer.partitionedBy(*partition_by)

        if properties:
            for key, value in properties.items():
                writer = writer.tableProperty(key, value)

        writer.create()
        logger.info(f"Successfully created table {table_name}")


class IcebergReader:
    """Read data from Iceberg tables with time travel support."""

    def __init__(self, spark: SparkSession, catalog_name: str = "local"):
        self.spark = spark
        self.catalog_name = catalog_name

    def read_current(self, table_name: str) -> DataFrame:
        """
        Read current snapshot of Iceberg table.

        Args:
            table_name: Fully qualified table name

        Returns:
            DataFrame
        """
        logger.info(f"Reading current snapshot from: {table_name}")
        return self.spark.table(f"{self.catalog_name}.{table_name}")

    def read_snapshot(self, table_name: str, snapshot_id: int) -> DataFrame:
        """
        Read specific snapshot of Iceberg table (time travel).

        Args:
            table_name: Fully qualified table name
            snapshot_id: Snapshot ID to read

        Returns:
            DataFrame
        """
        logger.info(f"Reading snapshot {snapshot_id} from: {table_name}")

        return (
            self.spark.read.format("iceberg")
            .option("snapshot-id", snapshot_id)
            .load(f"{self.catalog_name}.{table_name}")
        )

    def read_as_of_timestamp(self, table_name: str, timestamp: str) -> DataFrame:
        """
        Read table as of specific timestamp (time travel).

        Args:
            table_name: Fully qualified table name
            timestamp: Timestamp string (e.g., '2024-01-15 10:00:00')

        Returns:
            DataFrame
        """
        logger.info(f"Reading {table_name} as of {timestamp}")

        return (
            self.spark.read.format("iceberg")
            .option("as-of-timestamp", timestamp)
            .load(f"{self.catalog_name}.{table_name}")
        )

    def get_snapshots(self, table_name: str) -> DataFrame:
        """
        Get all snapshots for table (audit trail).

        Args:
            table_name: Fully qualified table name

        Returns:
            DataFrame with snapshot history
        """
        return self.spark.sql(f"SELECT * FROM {self.catalog_name}.{table_name}.snapshots")


def initialize_iceberg_spark(
    app_name: str = "project_a_iceberg",
    catalog_type: str = IcebergConfig.HADOOP_CATALOG,
    warehouse_path: str = "data/iceberg-warehouse",
    packages: str | None = None,
) -> SparkSession:
    """
    Initialize Spark session with Iceberg support.

    Args:
        app_name: Spark application name
        catalog_type: Iceberg catalog type
        warehouse_path: Warehouse location

    Returns:
        SparkSession with Iceberg configured
    """
    iceberg_config = IcebergConfig.get_spark_config(
        catalog_type=catalog_type, warehouse_path=warehouse_path
    )

    builder = SparkSession.builder.appName(app_name)

    # Add Iceberg configurations
    for key, value in iceberg_config.items():
        builder = builder.config(key, value)

    # Add Iceberg JARs via Spark packages (no separate install required)
    resolved_packages = packages or os.environ.get("ICEBERG_SPARK_PACKAGES") or DEFAULT_ICEBERG_PACKAGES
    if resolved_packages:
        builder = builder.config("spark.jars.packages", resolved_packages)

    spark = builder.getOrCreate()
    logger.info("Spark session initialized with Iceberg support")

    return spark
