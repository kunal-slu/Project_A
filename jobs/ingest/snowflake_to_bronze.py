"""
Snowflake to Bronze Ingestion Job

Ingests data from Snowflake into the Bronze layer.
"""

import logging
from pathlib import Path
from typing import Any

from project_a.core.base_job import BaseJob
from project_a.core.config import ProjectConfig
from project_a.schemas.bronze_schemas import (
    SNOWFLAKE_CUSTOMERS_SCHEMA,
    SNOWFLAKE_ORDERS_SCHEMA,
    SNOWFLAKE_PRODUCTS_SCHEMA,
)
from project_a.utils.dq_realism import check_date_realism
from project_a.utils.path_resolver import resolve_source_file_path

logger = logging.getLogger(__name__)

def _local_path_missing(path: str) -> bool:
    if path.startswith("file://"):
        return not Path(path.replace("file://", "", 1)).exists()
    return False


def _read_csv_with_incremental(spark, schema, snapshot_path: str, incremental_dir: str | None):
    reader = spark.read.schema(schema).option("header", "true")
    paths = [snapshot_path]
    if incremental_dir and not _local_path_missing(incremental_dir):
        reader = reader.option("recursiveFileLookup", "true")
        paths.append(incremental_dir)
    return reader.csv(paths)


class SnowflakeToBronzeJob(BaseJob):
    """Job to ingest data from Snowflake to Bronze layer."""

    def __init__(self, config: ProjectConfig):
        super().__init__(config)
        self.job_name = "snowflake_to_bronze"

    def run(self, ctx) -> dict[str, Any]:
        """Execute the Snowflake to Bronze ingestion."""
        logger.info("Starting Snowflake to Bronze ingestion...")

        try:
            # Get Spark session from context
            spark = ctx.spark

            # Get source and target paths from config
            paths_cfg = self.config.get("paths", {})
            bronze_root = paths_cfg.get("bronze_root") or paths_cfg.get("bronze") or "data/bronze"
            bronze_path = f"{bronze_root.rstrip('/')}/snowflake"

            # Ingest customers data
            logger.info("Ingesting customers data from Snowflake...")
            try:
                customers_path = resolve_source_file_path(self.config, "snowflake", "customers")
                if _local_path_missing(customers_path):
                    raise ValueError(f"Missing local file: {customers_path}")
            except ValueError:
                source_path = (
                    self.config.get("sources", {})
                    .get("snowflake", {})
                    .get("base_path", "data/samples/snowflake")
                )
                customers_path = f"{source_path}/snowflake_customers_50000.csv"
            incremental_dirs = (
                self.config.get("sources", {}).get("snowflake", {}).get("incremental_dirs", {})
            )
            customers_inc = incremental_dirs.get("customers")
            customers_df = _read_csv_with_incremental(
                spark, SNOWFLAKE_CUSTOMERS_SCHEMA, customers_path, customers_inc
            )
            customers_df.write.mode("overwrite").parquet(f"{bronze_path}/customers")

            # Ingest orders data
            logger.info("Ingesting orders data from Snowflake...")
            try:
                orders_path = resolve_source_file_path(self.config, "snowflake", "orders")
                if _local_path_missing(orders_path):
                    raise ValueError(f"Missing local file: {orders_path}")
            except ValueError:
                source_path = (
                    self.config.get("sources", {})
                    .get("snowflake", {})
                    .get("base_path", "data/samples/snowflake")
                )
                orders_path = f"{source_path}/snowflake_orders_100000.csv"
            orders_inc = incremental_dirs.get("orders")
            orders_df = _read_csv_with_incremental(
                spark, SNOWFLAKE_ORDERS_SCHEMA, orders_path, orders_inc
            )
            orders_df.write.mode("overwrite").parquet(f"{bronze_path}/orders")

            # Ingest products data
            logger.info("Ingesting products data from Snowflake...")
            try:
                products_path = resolve_source_file_path(self.config, "snowflake", "products")
                if _local_path_missing(products_path):
                    raise ValueError(f"Missing local file: {products_path}")
            except ValueError:
                source_path = (
                    self.config.get("sources", {})
                    .get("snowflake", {})
                    .get("base_path", "data/samples/snowflake")
                )
                products_path = f"{source_path}/snowflake_products_10000.csv"
            products_inc = incremental_dirs.get("products")
            products_df = _read_csv_with_incremental(
                spark, SNOWFLAKE_PRODUCTS_SCHEMA, products_path, products_inc
            )
            products_df.write.mode("overwrite").parquet(f"{bronze_path}/products")

            # Apply data quality checks
            logger.info("Applying data quality checks...")
            self.apply_dq_rules(customers_df, "bronze.snowflake.customers")
            self.apply_dq_rules(orders_df, "bronze.snowflake.orders")
            self.apply_dq_rules(products_df, "bronze.snowflake.products")
            check_date_realism(customers_df, "bronze.snowflake.customers", self.config)
            check_date_realism(orders_df, "bronze.snowflake.orders", self.config)
            check_date_realism(products_df, "bronze.snowflake.products", self.config)

            # Cache counts to avoid multiple computations
            customers_count = customers_df.count()
            orders_count = orders_df.count()
            products_count = products_df.count()

            # Log lineage
            self.log_lineage(
                source="snowflake",
                target="bronze.snowflake",
                records_processed={
                    "customers": customers_count,
                    "orders": orders_count,
                    "products": products_count,
                },
            )

            result = {
                "status": "success",
                "records_processed": {
                    "customers": customers_count,
                    "orders": orders_count,
                    "products": products_count,
                },
                "output_path": bronze_path,
            }

            logger.info(f"Snowflake to Bronze ingestion completed: {result}")
            return result

        except Exception as e:
            logger.error(f"Snowflake to Bronze ingestion failed: {e}")
            raise
