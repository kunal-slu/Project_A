"""
Automated Ingestion Pipeline: Bronze → Silver → Gold
Handles data quality, transformations, and orchestration between lakehouse layers.
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    current_timestamp,
    datediff,
    dayofmonth,
    lower,
    month,
    trim,
    when,
    year,
)

from project_a.config_loader import load_config_resolved
from project_a.delta_utils import read_delta, write_delta
from project_a.dq_checks import DQChecks
from project_a.incremental_loading import IncrementalLoader
from project_a.lineage_tracker import LineageTracker
from project_a.metrics_collector import MetricsCollector

logger = logging.getLogger(__name__)


class IngestionPipeline:
    """
    Orchestrates data flow from Bronze → Silver → Gold with:
    - Automated data quality validation
    - Incremental processing
    - Schema evolution handling
    - Performance optimization
    - Lineage tracking
    """

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        self.spark = spark
        self.config = config
        self.dq = DQChecks()
        self.metrics = MetricsCollector()
        self.lineage = LineageTracker()
        self.incremental_loader = IncrementalLoader(spark)

        # Paths
        self.bronze_base = config["output"]["bronze_path"]
        self.silver_base = config["output"]["silver_path"]
        self.gold_base = config["output"]["gold_path"]

        # Checkpoints for incremental processing
        self.checkpoint_base = "data/checkpoints/ingestion"
        os.makedirs(self.checkpoint_base, exist_ok=True)

    def run_full_pipeline(self) -> dict[str, Any]:
        """Execute complete ingestion pipeline."""
        start_time = datetime.now()
        pipeline_metrics = {}

        try:
            logger.info("🚀 Starting automated ingestion pipeline")

            # Bronze layer validation
            bronze_metrics = self._validate_bronze_layer()
            pipeline_metrics["bronze"] = bronze_metrics

            # Silver layer processing
            silver_metrics = self._process_silver_layer()
            pipeline_metrics["silver"] = silver_metrics

            # Gold layer aggregation
            gold_metrics = self._process_gold_layer()
            pipeline_metrics["gold"] = gold_metrics

            # Performance optimization
            optimization_metrics = self._optimize_performance()
            pipeline_metrics["optimization"] = optimization_metrics

            # Record lineage
            self._record_lineage(pipeline_metrics)

            # Save metrics
            self._save_pipeline_metrics(pipeline_metrics)

            duration = datetime.now() - start_time
            pipeline_metrics["pipeline_duration_seconds"] = duration.total_seconds()
            pipeline_metrics["status"] = "success"

            logger.info(f"✅ Ingestion pipeline completed in {duration}")
            return pipeline_metrics

        except Exception as e:
            duration = datetime.now() - start_time
            pipeline_metrics["pipeline_duration_seconds"] = duration.total_seconds()
            pipeline_metrics["status"] = "failed"
            pipeline_metrics["error"] = str(e)

            logger.error(f"❌ Ingestion pipeline failed: {e}")
            self._save_pipeline_metrics(pipeline_metrics)
            raise

    def _validate_bronze_layer(self) -> dict[str, Any]:
        """Validate data quality in bronze layer."""
        logger.info("🔍 Validating bronze layer data quality")
        metrics = {"records_processed": 0, "quality_issues": 0, "validation_time": 0}

        start_time = datetime.now()

        # Validate each bronze table
        bronze_tables = [
            "customers_raw",
            "products_raw",
            "orders_raw",
            "returns_raw",
            "fx_rates",
            "inventory_snapshots",
        ]

        for table in bronze_tables:
            table_path = f"{self.bronze_base}/{table}"
            if os.path.exists(table_path):
                df = read_delta(self.spark, table_path)
                table_metrics = self._validate_table(df, table)
                metrics["records_processed"] += table_metrics["record_count"]
                metrics["quality_issues"] += table_metrics["quality_issues"]

        metrics["validation_time"] = (datetime.now() - start_time).total_seconds()
        return metrics

    def _validate_table(self, df: DataFrame, table_name: str) -> dict[str, Any]:
        """Validate individual table data quality."""
        metrics = {"record_count": 0, "quality_issues": 0}

        try:
            record_count = df.count()
            metrics["record_count"] = record_count

            # Table-specific validations
            if table_name == "customers_raw":
                metrics.update(self._validate_customers(df))
            elif table_name == "orders_raw":
                metrics.update(self._validate_orders(df))
            elif table_name == "returns_raw":
                metrics.update(self._validate_returns(df))
            elif table_name == "products_raw":
                metrics.update(self._validate_products(df))

        except Exception as e:
            logger.warning(f"Validation failed for {table_name}: {e}")
            metrics["quality_issues"] += 1

        return metrics

    def _validate_customers(self, df: DataFrame) -> dict[str, Any]:
        """Validate customer data quality."""
        issues = 0

        # Check for null customer IDs
        null_customers = df.filter(col("customer_id").isNull()).count()
        if null_customers > 0:
            issues += null_customers
            logger.warning(f"Found {null_customers} customers with null IDs")

        # Check email format
        invalid_emails = df.filter(
            ~col("email").rlike(self.config["dq_rules"]["email_regex"])
        ).count()
        if invalid_emails > 0:
            issues += invalid_emails
            logger.warning(f"Found {invalid_emails} customers with invalid emails")

        # Check age range
        age_issues = df.filter(
            (col("age") < self.config["dq_rules"]["min_age"])
            | (col("age") > self.config["dq_rules"]["max_age"])
        ).count()
        if age_issues > 0:
            issues += age_issues
            logger.warning(f"Found {age_issues} customers with age outside valid range")

        return {"quality_issues": issues}

    def _validate_orders(self, df: DataFrame) -> dict[str, Any]:
        """Validate order data quality."""
        issues = 0

        # Check for null order IDs
        null_orders = df.filter(col("order_id").isNull()).count()
        if null_orders > 0:
            issues += null_orders

        # Check for negative amounts
        negative_amounts = df.filter(col("amount") < 0).count()
        if negative_amounts > 0:
            issues += negative_amounts
            logger.warning(f"Found {negative_amounts} orders with negative amounts")

        return {"quality_issues": issues}

    def _validate_returns(self, df: DataFrame) -> dict[str, Any]:
        """Validate returns data quality."""
        issues = 0

        # Check for null return IDs
        null_returns = df.filter(col("return_id").isNull()).count()
        if null_returns > 0:
            issues += null_returns

        # Check return reasons
        invalid_reasons = df.filter(
            ~col("return_reason").isin(["defective", "wrong_size", "not_as_described", "other"])
        ).count()
        if invalid_reasons > 0:
            issues += invalid_reasons
            logger.warning(f"Found {invalid_reasons} returns with invalid reasons")

        return {"quality_issues": issues}

    def _validate_products(self, df: DataFrame) -> dict[str, Any]:
        """Validate product data quality."""
        issues = 0

        # Check for null product IDs
        null_products = df.filter(col("product_id").isNull()).count()
        if null_products > 0:
            issues += null_products

        # Check for negative prices
        negative_prices = df.filter(col("price") < 0).count()
        if negative_prices > 0:
            issues += negative_prices
            logger.warning(f"Found {negative_prices} products with negative prices")

        return {"quality_issues": issues}

    def _process_silver_layer(self) -> dict[str, Any]:
        """Process data from bronze to silver layer."""
        logger.info("🔄 Processing silver layer transformations")
        metrics = {"tables_processed": 0, "records_transformed": 0, "processing_time": 0}

        start_time = datetime.now()

        try:
            # Process customers
            customers_silver = self._transform_customers_silver()
            if customers_silver:
                write_delta(customers_silver, f"{self.silver_base}/customers", mode="overwrite")
                metrics["tables_processed"] += 1
                metrics["records_transformed"] += customers_silver.count()

            # Process products
            products_silver = self._transform_products_silver()
            if products_silver:
                write_delta(products_silver, f"{self.silver_base}/products", mode="overwrite")
                metrics["tables_processed"] += 1
                metrics["records_transformed"] += products_silver.count()

            # Process orders
            orders_silver = self._transform_orders_silver()
            if orders_silver:
                write_delta(orders_silver, f"{self.silver_base}/orders", mode="overwrite")
                metrics["tables_processed"] += 1
                metrics["records_transformed"] += orders_silver.count()

            # Process returns
            returns_silver = self._transform_returns_silver()
            if returns_silver:
                write_delta(returns_silver, f"{self.silver_base}/returns", mode="overwrite")
                metrics["tables_processed"] += 1
                metrics["records_transformed"] += returns_silver.count()

            # Process inventory
            inventory_silver = self._transform_inventory_silver()
            if inventory_silver:
                write_delta(inventory_silver, f"{self.silver_base}/inventory", mode="overwrite")
                metrics["tables_processed"] += 1
                metrics["records_transformed"] += inventory_silver.count()

        except Exception as e:
            logger.error(f"Silver layer processing failed: {e}")
            raise

        metrics["processing_time"] = (datetime.now() - start_time).total_seconds()
        return metrics

    def _transform_customers_silver(self) -> DataFrame | None:
        """Transform customers to silver layer."""
        try:
            customers = read_delta(self.spark, f"{self.bronze_base}/customers_raw")

            # Clean and enrich
            customers_silver = (
                customers.withColumn(
                    "email_verified",
                    when(
                        col("email").rlike(self.config["dq_rules"]["email_regex"]), True
                    ).otherwise(False),
                )
                .withColumn(
                    "age_group",
                    when(col("age") < 25, "18-24")
                    .when(col("age") < 35, "25-34")
                    .when(col("age") < 45, "35-44")
                    .when(col("age") < 55, "45-54")
                    .otherwise("55+"),
                )
                .withColumn(
                    "customer_segment",
                    when(col("total_spent") > 1000, "premium")
                    .when(col("total_spent") > 500, "regular")
                    .otherwise("basic"),
                )
                .withColumn("processed_at", current_timestamp())
            )

            return customers_silver

        except Exception as e:
            logger.warning(f"Failed to transform customers: {e}")
            return None

    def _transform_products_silver(self) -> DataFrame | None:
        """Transform products to silver layer."""
        try:
            products = read_delta(self.spark, f"{self.bronze_base}/products_raw")

            # Clean and enrich
            products_silver = (
                products.withColumn(
                    "price_tier",
                    when(col("price") > 100, "high")
                    .when(col("price") > 50, "medium")
                    .otherwise("low"),
                )
                .withColumn("category_normalized", lower(trim(col("category"))))
                .withColumn("processed_at", current_timestamp())
            )

            return products_silver

        except Exception as e:
            logger.warning(f"Failed to transform products: {e}")
            return None

    def _transform_orders_silver(self) -> DataFrame | None:
        """Transform orders to silver layer."""
        try:
            orders = read_delta(self.spark, f"{self.bronze_base}/orders_raw")

            # Clean and enrich
            orders_silver = (
                orders.withColumn(
                    "order_status",
                    when(col("returned"), "returned")
                    .when(col("shipped"), "shipped")
                    .otherwise("pending"),
                )
                .withColumn(
                    "amount_usd",
                    when(col("currency") == "USD", col("amount"))
                    .when(col("currency") == "EUR", col("amount") * 1.1)
                    .when(col("currency") == "GBP", col("amount") * 1.3)
                    .otherwise(col("amount")),
                )
                .withColumn("processed_at", current_timestamp())
            )

            return orders_silver

        except Exception as e:
            logger.warning(f"Failed to transform orders: {e}")
            return None

    def _transform_returns_silver(self) -> DataFrame | None:
        """Transform returns to silver layer."""
        try:
            returns = read_delta(self.spark, f"{self.bronze_base}/returns_raw")

            # Clean and enrich
            returns_silver = (
                returns.withColumn(
                    "return_category",
                    when(col("return_reason") == "defective", "quality_issue")
                    .when(col("return_reason") == "wrong_size", "customer_error")
                    .when(col("return_reason") == "not_as_described", "product_mismatch")
                    .otherwise("other"),
                )
                .withColumn(
                    "return_value",
                    when(col("refund_amount").isNotNull(), col("refund_amount")).otherwise(
                        col("order_amount")
                    ),
                )
                .withColumn("processed_at", current_timestamp())
            )

            return returns_silver

        except Exception as e:
            logger.warning(f"Failed to transform returns: {e}")
            return None

    def _transform_inventory_silver(self) -> DataFrame | None:
        """Transform inventory to silver layer."""
        try:
            inventory = read_delta(self.spark, f"{self.bronze_base}/inventory_snapshots")

            # Clean and enrich
            inventory_silver = (
                inventory.withColumn(
                    "stock_level",
                    when(col("quantity") == 0, "out_of_stock")
                    .when(col("quantity") < 10, "low_stock")
                    .when(col("quantity") < 50, "medium_stock")
                    .otherwise("well_stocked"),
                )
                .withColumn(
                    "days_since_update", datediff(current_timestamp(), col("snapshot_date"))
                )
                .withColumn("processed_at", current_timestamp())
            )

            return inventory_silver

        except Exception as e:
            logger.warning(f"Failed to transform inventory: {e}")
            return None

    def _process_gold_layer(self) -> dict[str, Any]:
        """Process data from silver to gold layer (business metrics)."""
        logger.info("🏆 Processing gold layer aggregations")
        metrics = {"metrics_created": 0, "processing_time": 0}

        start_time = datetime.now()

        try:
            # Customer metrics
            customer_metrics = self._create_customer_metrics()
            if customer_metrics:
                write_delta(
                    customer_metrics, f"{self.gold_base}/customer_metrics", mode="overwrite"
                )
                metrics["metrics_created"] += 1

            # Product metrics
            product_metrics = self._create_product_metrics()
            if product_metrics:
                write_delta(product_metrics, f"{self.gold_base}/product_metrics", mode="overwrite")
                metrics["metrics_created"] += 1

            # Order metrics
            order_metrics = self._create_order_metrics()
            if order_metrics:
                write_delta(order_metrics, f"{self.gold_base}/order_metrics", mode="overwrite")
                metrics["metrics_created"] += 1

            # Return metrics
            return_metrics = self._create_return_metrics()
            if return_metrics:
                write_delta(return_metrics, f"{self.gold_base}/return_metrics", mode="overwrite")
                metrics["metrics_created"] += 1

            # Inventory metrics
            inventory_metrics = self._create_inventory_metrics()
            if inventory_metrics:
                write_delta(
                    inventory_metrics, f"{self.gold_base}/inventory_metrics", mode="overwrite"
                )
                metrics["metrics_created"] += 1

        except Exception as e:
            logger.error(f"Gold layer processing failed: {e}")
            raise

        metrics["processing_time"] = (datetime.now() - start_time).total_seconds()
        return metrics

    def _create_customer_metrics(self) -> DataFrame | None:
        """Create customer business metrics."""
        try:
            customers = read_delta(self.spark, f"{self.silver_base}/customers")
            orders = read_delta(self.spark, f"{self.silver_base}/orders")

            # Customer lifetime value and order patterns
            customer_metrics = (
                customers.join(orders, "customer_id", "left")
                .groupBy("customer_id", "customer_segment", "age_group")
                .agg(
                    count("order_id").alias("total_orders"),
                    sum("amount_usd").alias("total_spent"),
                    avg("amount_usd").alias("avg_order_value"),
                    max("order_date").alias("last_order_date"),
                )
                .withColumn("processed_at", current_timestamp())
            )

            return customer_metrics

        except Exception as e:
            logger.warning(f"Failed to create customer metrics: {e}")
            return None

    def _create_product_metrics(self) -> DataFrame | None:
        """Create product business metrics."""
        try:
            products = read_delta(self.spark, f"{self.silver_base}/products")
            orders = read_delta(self.spark, f"{self.silver_base}/orders")

            # Product performance metrics
            product_metrics = (
                products.join(orders, "product_id", "left")
                .groupBy("product_id", "category", "price_tier")
                .agg(
                    count("order_id").alias("total_orders"),
                    sum("amount_usd").alias("total_revenue"),
                    avg("amount_usd").alias("avg_order_value"),
                )
                .withColumn("processed_at", current_timestamp())
            )

            return product_metrics

        except Exception as e:
            logger.warning(f"Failed to create product metrics: {e}")
            return None

    def _create_order_metrics(self) -> DataFrame | None:
        """Create order business metrics."""
        try:
            orders = read_delta(self.spark, f"{self.silver_base}/orders")

            # Daily order metrics
            order_metrics = (
                orders.groupBy(
                    year("order_date").alias("year"),
                    month("order_date").alias("month"),
                    dayofmonth("order_date").alias("day"),
                )
                .agg(
                    count("order_id").alias("daily_orders"),
                    sum("amount_usd").alias("daily_revenue"),
                    avg("amount_usd").alias("avg_order_value"),
                    countDistinct("customer_id").alias("unique_customers"),
                )
                .withColumn("processed_at", current_timestamp())
            )

            return order_metrics

        except Exception as e:
            logger.warning(f"Failed to create order metrics: {e}")
            return None

    def _create_return_metrics(self) -> DataFrame | None:
        """Create return business metrics."""
        try:
            returns = read_delta(self.spark, f"{self.silver_base}/returns")

            # Return analysis metrics
            return_metrics = (
                returns.groupBy("return_category", "return_reason")
                .agg(
                    count("return_id").alias("return_count"),
                    sum("return_value").alias("total_return_value"),
                    avg("return_value").alias("avg_return_value"),
                )
                .withColumn("processed_at", current_timestamp())
            )

            return return_metrics

        except Exception as e:
            logger.warning(f"Failed to create return metrics: {e}")
            return None

    def _create_inventory_metrics(self) -> DataFrame | None:
        """Create inventory business metrics."""
        try:
            inventory = read_delta(self.spark, f"{self.silver_base}/inventory")

            # Inventory health metrics
            inventory_metrics = (
                inventory.groupBy("stock_level", "category")
                .agg(
                    count("product_id").alias("product_count"),
                    sum("quantity").alias("total_quantity"),
                    avg("quantity").alias("avg_quantity"),
                )
                .withColumn("processed_at", current_timestamp())
            )

            return inventory_metrics

        except Exception as e:
            logger.warning(f"Failed to create inventory metrics: {e}")
            return None

    def _optimize_performance(self) -> dict[str, Any]:
        """Optimize table performance with Z-ordering and partitioning."""
        logger.info("⚡ Optimizing table performance")
        metrics = {"tables_optimized": 0, "optimization_time": 0}

        start_time = datetime.now()

        try:
            # Optimize silver layer tables
            silver_tables = ["customers", "products", "orders", "returns", "inventory"]

            for table in silver_tables:
                table_path = f"{self.silver_base}/{table}"
                if os.path.exists(table_path):
                    self._optimize_table(table_path, table)
                    metrics["tables_optimized"] += 1

            # Optimize gold layer tables
            gold_tables = [
                "customer_metrics",
                "product_metrics",
                "order_metrics",
                "return_metrics",
                "inventory_metrics",
            ]

            for table in gold_tables:
                table_path = f"{self.gold_base}/{table}"
                if os.path.exists(table_path):
                    self._optimize_table(table_path, table)
                    metrics["tables_optimized"] += 1

        except Exception as e:
            logger.warning(f"Performance optimization failed: {e}")

        metrics["optimization_time"] = (datetime.now() - start_time).total_seconds()
        return metrics

    def _optimize_table(self, table_path: str, table_name: str):
        """Optimize individual table with Z-ordering and compaction."""
        try:
            # Z-order optimization for common query patterns
            if table_name in ["customers", "products"]:
                self.spark.sql(f"""
                    OPTIMIZE '{table_path}'
                    ZORDER BY (id, category)
                """)
            elif table_name in ["orders", "returns"]:
                self.spark.sql(f"""
                    OPTIMIZE '{table_path}'
                    ZORDER BY (customer_id, order_date)
                """)

            # Compact small files
            self.spark.sql(f"""
                OPTIMIZE '{table_path}'
            """)

            logger.info(f"Optimized table: {table_name}")

        except Exception as e:
            logger.warning(f"Failed to optimize {table_name}: {e}")

    def _record_lineage(self, pipeline_metrics: dict[str, Any]):
        """Record data lineage for the pipeline run."""
        try:
            self.lineage.record_pipeline_run(
                pipeline_name="automated_ingestion",
                start_time=datetime.now()
                - timedelta(seconds=pipeline_metrics["pipeline_duration_seconds"]),
                end_time=datetime.now(),
                metrics=pipeline_metrics,
                layers=["bronze", "silver", "gold"],
            )
        except Exception as e:
            logger.warning(f"Failed to record lineage: {e}")

    def _save_pipeline_metrics(self, metrics: dict[str, Any]):
        """Save pipeline metrics for monitoring."""
        try:
            metrics_file = (
                f"{self.checkpoint_base}/pipeline_metrics_"
                f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )

            with open(metrics_file, "w") as f:
                json.dump(metrics, f, indent=2, default=str)

            logger.info(f"Pipeline metrics saved to: {metrics_file}")

        except Exception as e:
            logger.warning(f"Failed to save pipeline metrics: {e}")


def main():
    """Main entry point for the ingestion pipeline."""
    from .utils import get_spark_session

    # Load configuration
    config = load_config_resolved("config/config-dev.yaml")

    # Create Spark session
    spark = get_spark_session(config)

    # Create and run pipeline
    pipeline = IngestionPipeline(spark, config)
    metrics = pipeline.run_full_pipeline()

    print(f"Pipeline completed with status: {metrics['status']}")
    print(f"Duration: {metrics['pipeline_duration_seconds']:.2f} seconds")
    print(f"Records processed: {metrics.get('bronze', {}).get('records_processed', 0)}")

    return metrics


if __name__ == "__main__":
    main()
