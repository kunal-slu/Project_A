"""
Silver to Gold Transformation Job

Transforms data from Silver layer to Gold layer with dimensional modeling and analytics.
"""

import logging
from typing import Any

from project_a.core.base_job import BaseJob
from project_a.core.config import ProjectConfig

logger = logging.getLogger(__name__)


class SilverToGoldJob(BaseJob):
    """Job to transform data from Silver to Gold layer."""

    def __init__(self, config: ProjectConfig):
        super().__init__(config)
        self.job_name = "silver_to_gold"

    def _storage_format(self) -> str:
        storage_fmt = (self.config.get("storage") or {}).get("format")
        if storage_fmt:
            return str(storage_fmt).lower()
        iceberg_cfg = self.config.get("iceberg", {})
        if iceberg_cfg.get("enabled"):
            return "iceberg"
        return "parquet"

    def _read_silver(self, spark, table_name: str, path: str):
        fmt = self._storage_format()
        if fmt == "iceberg":
            catalog = self.config.get("iceberg", {}).get("catalog_name", "local")
            try:
                return spark.read.format("iceberg").load(f"{catalog}.{table_name}")
            except Exception as exc:
                logger.warning(
                    "Iceberg table %s.%s not found, falling back to path read: %s (%s)",
                    catalog,
                    table_name,
                    path,
                    exc,
                )
                # Try delta first, then parquet
                try:
                    return spark.read.format("delta").load(path)
                except Exception:
                    return spark.read.parquet(path)
        if fmt == "delta":
            return spark.read.format("delta").load(path)
        return spark.read.parquet(path)

    def _gold_partitions(self, table_name: str) -> list[str]:
        if table_name == "fact_orders":
            return ["order_year", "order_month"]
        if table_name == "fact_opportunity":
            return ["close_year", "close_month"]
        if table_name == "fact_order_events":
            return ["event_year", "event_month"]
        return []

    def _optimize_output_partitions(self, df):
        perf_cfg = self.config.get("performance", {})
        max_parts = int(perf_cfg.get("max_partitions", 200))
        try:
            current_parts = df.rdd.getNumPartitions()
        except Exception:
            return df
        if current_parts > max_parts:
            logger.info(
                "Coalescing %s partitions -> %s for gold output", current_parts, max_parts
            )
            return df.coalesce(max_parts)
        return df

    def _prepare_fx_rates(self, fx_rates_df):
        from pyspark.sql import functions as F

        fx_base = fx_rates_df.select(
            F.col("trade_date").alias("fx_date"),
            F.col("base_ccy").alias("fx_base_ccy"),
            F.col("counter_ccy").alias("fx_counter_ccy"),
            F.col("rate").alias("fx_rate"),
        )

        direct = (
            fx_base.filter(F.col("fx_counter_ccy") == F.lit("USD"))
            .select(
                "fx_date",
                F.col("fx_base_ccy").alias("fx_currency"),
                F.col("fx_rate").alias("fx_rate"),
            )
        )
        inverted = (
            fx_base.filter(F.col("fx_base_ccy") == F.lit("USD"))
            .filter(F.col("fx_counter_ccy") != F.lit("USD"))
            .select(
                "fx_date",
                F.col("fx_counter_ccy").alias("fx_currency"),
                (F.lit(1.0) / F.col("fx_rate")).alias("fx_rate"),
            )
        )

        fx_rates_norm = (
            direct.unionByName(inverted, allowMissingColumns=True)
            .dropDuplicates(["fx_date", "fx_currency"])
        )
        return fx_rates_norm

    def _write_gold(self, spark, df, table_name: str, path: str) -> None:
        schema_evolution_cfg = self.config.get("schema_evolution", {})
        if schema_evolution_cfg.get("enabled"):
            from project_a.utils.schema_evolution import enforce_schema_evolution

            enforce_schema_evolution(
                df,
                table_name=table_name,
                layer="gold",
                config=schema_evolution_cfg,
            )

        df = self._optimize_output_partitions(df)
        partitions = self._gold_partitions(table_name)

        fmt = self._storage_format()
        if fmt == "iceberg":
            catalog = self.config.get("iceberg", {}).get("catalog_name", "local")
            writer = df.writeTo(f"{catalog}.{table_name}").using("iceberg")
            if partitions:
                writer = writer.partitionedBy(*partitions)
            writer.createOrReplace()
        elif fmt == "delta":
            writer = df.write.format("delta").mode("overwrite")
            if partitions:
                writer = writer.partitionBy(*partitions)
            writer.save(path)
        else:
            writer = df.write.mode("overwrite")
            if partitions:
                writer = writer.partitionBy(*partitions)
            writer.parquet(path)

    def _validate_gold_schema(self, df, table_name: str, expected_schema, pk: str | None = None):
        from project_a.utils.schema_validator import SchemaValidator

        strict = bool(self.config.get("dq.fail_on_error", True))
        SchemaValidator.validate_schema(df, expected_schema, table_name=table_name, strict=strict)
        if pk:
            SchemaValidator.validate_primary_key(df, pk, table_name=table_name)

    def _assert_non_null(self, df, columns: list[str], table_name: str) -> None:
        from functools import reduce
        from pyspark.sql.functions import col

        if not columns:
            return
        condition = reduce(lambda a, b: a | b, [col(c).isNull() for c in columns])
        null_count = df.filter(condition).count()
        if null_count > 0:
            if self.config.get("dq.fail_on_error", True):
                raise ValueError(
                    f"{table_name}: Found {null_count} rows with nulls in {columns}"
                )
            logger.warning(
                "%s: Found %s rows with nulls in %s", table_name, null_count, columns
            )

    def run(self, ctx) -> dict[str, Any]:
        """Execute the Silver to Gold transformation."""
        logger.info("Starting Silver to Gold transformation...")

        try:
            # Get Spark session from context
            spark = ctx.spark

            # Get silver and gold paths from config
            paths_cfg = self.config.get("paths", {})
            silver_path = paths_cfg.get("silver_root") or paths_cfg.get("silver") or "data/silver"
            gold_path = paths_cfg.get("gold_root") or paths_cfg.get("gold") or "data/gold"

            cache_enabled = bool(self.config.get("performance.cache_enabled", True))
            cached_frames: list = []

            def maybe_cache(df, name: str):
                if cache_enabled:
                    logger.info("Caching %s for reuse", name)
                    df = df.cache()
                    cached_frames.append(df)
                return df

            customers_df = maybe_cache(
                self._read_silver(spark, "customers_silver", f"{silver_path}/customers_silver"),
                "customers_silver",
            )
            products_df = maybe_cache(
                self._read_silver(spark, "products_silver", f"{silver_path}/products_silver"),
                "products_silver",
            )
            orders_df = maybe_cache(
                self._read_silver(spark, "orders_silver", f"{silver_path}/orders_silver"),
                "orders_silver",
            )
            behavior_df = maybe_cache(
                self._read_silver(
                    spark, "customer_behavior_silver", f"{silver_path}/customer_behavior_silver"
                ),
                "customer_behavior_silver",
            )
            fx_rates_df = maybe_cache(
                self._read_silver(spark, "fx_rates_silver", f"{silver_path}/fx_rates_silver"),
                "fx_rates_silver",
            )
            accounts_df = maybe_cache(
                self._read_silver(spark, "accounts_silver", f"{silver_path}/accounts_silver"),
                "accounts_silver",
            )
            contacts_df = maybe_cache(
                self._read_silver(spark, "contacts_silver", f"{silver_path}/contacts_silver"),
                "contacts_silver",
            )
            opportunities_df = maybe_cache(
                self._read_silver(
                    spark, "opportunities_silver", f"{silver_path}/opportunities_silver"
                ),
                "opportunities_silver",
            )
            order_events_df = maybe_cache(
                self._read_silver(
                    spark, "order_events_silver", f"{silver_path}/order_events_silver"
                ),
                "order_events_silver",
            )

            # Build Customer Dimension
            logger.info("Building Customer Dimension...")
            customer_dim = self.build_customer_dimension(spark, customers_df, gold_path)

            # Build Account Dimension
            logger.info("Building Account Dimension...")
            account_dim = self.build_account_dimension(spark, accounts_df, gold_path)

            # Build Contact Dimension
            logger.info("Building Contact Dimension...")
            contact_dim = self.build_contact_dimension(
                spark, contacts_df, account_dim, gold_path
            )

            # Build Product Dimension
            logger.info("Building Product Dimension...")
            product_dim = self.build_product_dimension(spark, products_df, gold_path)

            # Build Fact Orders
            logger.info("Building Fact Orders...")
            fact_orders = self.build_fact_orders(
                spark, orders_df, customer_dim, product_dim, fx_rates_df, gold_path
            )

            # Build Date Dimension
            logger.info("Building Date Dimension...")
            self.build_dim_date(spark, fact_orders, gold_path)

            # Build Product Performance
            logger.info("Building Product Performance...")
            self.build_product_performance(spark, product_dim, fact_orders, gold_path)

            # Build Customer 360 View
            logger.info("Building Customer 360 View...")
            self.build_customer_360(spark, customer_dim, fact_orders, behavior_df, gold_path)

            # Build Customer Behavior Analytics
            logger.info("Building Customer Behavior Analytics...")
            self.build_behavior_analytics(spark, behavior_df, gold_path)

            # Build Opportunity Fact
            logger.info("Building Opportunity Fact...")
            self.build_fact_opportunity(
                spark, opportunities_df, account_dim, contact_dim, gold_path
            )

            # Build Order Events Fact (Kafka)
            logger.info("Building Order Events Fact...")
            self.build_fact_order_events(
                spark, order_events_df, customer_dim, fact_orders, fx_rates_df, gold_path
            )

            # Apply data quality checks
            logger.info("Applying data quality checks...")
            self.apply_dq_rules(None, "gold.layer")

            # Log lineage
            self.log_lineage(source="silver", target="gold", records_processed={})

            result = {
                "status": "success",
                "output_path": gold_path,
                "models_created": [
                    "customer_dim",
                    "account_dim",
                    "contact_dim",
                    "product_dim",
                    "dim_date",
                    "fact_orders",
                    "customer_360",
                    "behavior_analytics",
                    "product_performance",
                    "fact_opportunity",
                    "fact_order_events",
                ],
            }

            logger.info(f"Silver to Gold transformation completed: {result}")
            return result

        except Exception as e:
            logger.error(f"Silver to Gold transformation failed: {e}")
            raise
        finally:
            for df in cached_frames:
                try:
                    df.unpersist()
                except Exception:
                    logger.warning("Failed to unpersist cached DataFrame", exc_info=True)

    def build_customer_dimension(self, spark, customers_df, gold_path: str):
        """Build Customer Dimension from Silver data."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            BooleanType,
            DateType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )
        from pyspark.sql.window import Window

        effective_exprs = []
        if "updated_at" in customers_df.columns:
            effective_exprs.append(F.col("updated_at").cast("timestamp"))
        if "registration_date" in customers_df.columns:
            effective_exprs.append(F.to_timestamp(F.col("registration_date")))
        effective_exprs.append(F.current_timestamp())

        updated_col = (
            F.col("updated_at").cast("timestamp")
            if "updated_at" in customers_df.columns
            else F.lit(None).cast("timestamp")
        )
        base = (
            customers_df.select(
                "customer_id",
                "first_name",
                "last_name",
                "email",
                "country",
                "registration_date",
                updated_col.alias("updated_at"),
            )
            .withColumn("registration_date", F.to_date("registration_date"))
            .withColumn("effective_start", F.coalesce(*effective_exprs))
            .dropDuplicates(["customer_id", "effective_start"])
        )

        scd_window = Window.partitionBy("customer_id").orderBy(F.col("effective_start").asc())
        customer_dim = base.withColumn("effective_end", F.lead("effective_start").over(scd_window))
        customer_dim = customer_dim.withColumn("is_current", F.col("effective_end").isNull())
        customer_dim = customer_dim.withColumn(
            "effective_end",
            F.coalesce(F.col("effective_end"), F.to_timestamp(F.lit("9999-12-31 00:00:00"))),
        )
        customer_dim = customer_dim.withColumn(
            "customer_sk",
            F.xxhash64(
                F.concat_ws(
                    "||",
                    F.col("customer_id"),
                    F.col("effective_start").cast("string"),
                )
            ),
        )
        customer_dim = customer_dim.drop("updated_at")

        expected_schema = StructType(
            [
                StructField("customer_sk", LongType(), True),
                StructField("customer_id", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("country", StringType(), True),
                StructField("registration_date", DateType(), True),
                StructField("effective_start", TimestampType(), True),
                StructField("effective_end", TimestampType(), True),
                StructField("is_current", BooleanType(), True),
            ]
        )
        self._assert_non_null(
            customer_dim, ["customer_id", "customer_sk", "effective_start", "effective_end"], "dim_customer"
        )
        self._validate_gold_schema(customer_dim, "dim_customer", expected_schema, pk="customer_sk")

        # Write to gold
        self._write_gold(spark, customer_dim, "dim_customer", f"{gold_path}/dim_customer")
        return customer_dim

    def build_product_dimension(self, spark, products_df, gold_path: str):
        """Build Product Dimension from Silver data."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            BooleanType,
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )
        from pyspark.sql.window import Window

        effective_exprs = []
        if "updated_at" in products_df.columns:
            effective_exprs.append(F.col("updated_at").cast("timestamp"))
        effective_exprs.append(F.current_timestamp())

        updated_col = (
            F.col("updated_at").cast("timestamp")
            if "updated_at" in products_df.columns
            else F.lit(None).cast("timestamp")
        )
        base = (
            products_df.select(
                "product_id",
                "product_name",
                "category",
                F.col("price_usd").cast("double").alias("price_usd"),
                F.col("cost_usd").cast("double").alias("cost_usd"),
                "supplier_id",
                updated_col.alias("updated_at"),
            )
            .withColumn("effective_start", F.coalesce(*effective_exprs))
            .dropDuplicates(["product_id", "effective_start"])
        )

        scd_window = Window.partitionBy("product_id").orderBy(F.col("effective_start").asc())
        product_dim = base.withColumn("effective_end", F.lead("effective_start").over(scd_window))
        product_dim = product_dim.withColumn("is_current", F.col("effective_end").isNull())
        product_dim = product_dim.withColumn(
            "effective_end",
            F.coalesce(F.col("effective_end"), F.to_timestamp(F.lit("9999-12-31 00:00:00"))),
        )
        product_dim = product_dim.withColumn(
            "product_sk",
            F.xxhash64(
                F.concat_ws(
                    "||",
                    F.col("product_id"),
                    F.col("effective_start").cast("string"),
                )
            ),
        )
        product_dim = product_dim.drop("updated_at")

        expected_schema = StructType(
            [
                StructField("product_sk", LongType(), True),
                StructField("product_id", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("price_usd", DoubleType(), True),
                StructField("cost_usd", DoubleType(), True),
                StructField("supplier_id", StringType(), True),
                StructField("effective_start", TimestampType(), True),
                StructField("effective_end", TimestampType(), True),
                StructField("is_current", BooleanType(), True),
            ]
        )
        self._assert_non_null(
            product_dim, ["product_id", "product_sk", "effective_start", "effective_end"], "dim_product"
        )
        self._validate_gold_schema(product_dim, "dim_product", expected_schema, pk="product_sk")

        # Write to gold
        self._write_gold(spark, product_dim, "dim_product", f"{gold_path}/dim_product")
        return product_dim

    def build_account_dimension(self, spark, accounts_df, gold_path: str):
        """Build Account Dimension from CRM Silver data."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            BooleanType,
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )
        from pyspark.sql.window import Window

        base = (
            accounts_df.select(
                "account_id",
                "account_name",
                "industry",
                "annual_revenue",
                "number_of_employees",
                "billing_city",
                "billing_state",
                "billing_country",
                "customer_segment",
                "geographic_region",
                "account_status",
                "website",
                "phone",
                F.to_timestamp("created_date").alias("created_date"),
                F.to_timestamp("last_modified_date").alias("last_modified_date"),
            )
            .withColumn(
                "effective_start",
                F.coalesce(
                    F.to_timestamp("last_modified_date"),
                    F.to_timestamp("created_date"),
                    F.current_timestamp(),
                ),
            )
            .dropDuplicates(["account_id", "effective_start"])
        )

        scd_window = Window.partitionBy("account_id").orderBy(F.col("effective_start").asc())
        account_dim = base.withColumn("effective_end", F.lead("effective_start").over(scd_window))
        account_dim = account_dim.withColumn("is_current", F.col("effective_end").isNull())
        account_dim = account_dim.withColumn(
            "effective_end",
            F.coalesce(F.col("effective_end"), F.to_timestamp(F.lit("9999-12-31 00:00:00"))),
        )
        account_dim = account_dim.withColumn(
            "account_sk",
            F.xxhash64(
                F.concat_ws(
                    "||",
                    F.col("account_id"),
                    F.col("effective_start").cast("string"),
                )
            ),
        )

        expected_schema = StructType(
            [
                StructField("account_sk", LongType(), True),
                StructField("account_id", StringType(), True),
                StructField("account_name", StringType(), True),
                StructField("industry", StringType(), True),
                StructField("annual_revenue", DoubleType(), True),
                StructField("number_of_employees", IntegerType(), True),
                StructField("billing_city", StringType(), True),
                StructField("billing_state", StringType(), True),
                StructField("billing_country", StringType(), True),
                StructField("customer_segment", StringType(), True),
                StructField("geographic_region", StringType(), True),
                StructField("account_status", StringType(), True),
                StructField("website", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("created_date", TimestampType(), True),
                StructField("last_modified_date", TimestampType(), True),
                StructField("effective_start", TimestampType(), True),
                StructField("effective_end", TimestampType(), True),
                StructField("is_current", BooleanType(), True),
            ]
        )
        self._assert_non_null(
            account_dim,
            ["account_id", "account_sk", "effective_start", "effective_end"],
            "dim_account",
        )
        self._validate_gold_schema(account_dim, "dim_account", expected_schema, pk="account_sk")

        self._write_gold(spark, account_dim, "dim_account", f"{gold_path}/dim_account")
        return account_dim

    def build_contact_dimension(self, spark, contacts_df, account_dim, gold_path: str):
        """Build Contact Dimension from CRM Silver data."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            BooleanType,
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )
        from pyspark.sql.window import Window

        base = (
            contacts_df.select(
                "contact_id",
                "account_id",
                "first_name",
                "last_name",
                "email",
                "phone",
                "title",
                "department",
                "lead_source",
                "engagement_score",
                "mailing_city",
                "mailing_state",
                "mailing_country",
                F.to_timestamp("created_date").alias("created_date"),
                F.to_timestamp("last_modified_date").alias("last_modified_date"),
            )
            .withColumn(
                "effective_start",
                F.coalesce(
                    F.to_timestamp("last_modified_date"),
                    F.to_timestamp("created_date"),
                    F.current_timestamp(),
                ),
            )
            .dropDuplicates(["contact_id", "effective_start"])
        )

        scd_window = Window.partitionBy("contact_id").orderBy(F.col("effective_start").asc())
        contact_dim = base.withColumn("effective_end", F.lead("effective_start").over(scd_window))
        contact_dim = contact_dim.withColumn("is_current", F.col("effective_end").isNull())
        contact_dim = contact_dim.withColumn(
            "effective_end",
            F.coalesce(F.col("effective_end"), F.to_timestamp(F.lit("9999-12-31 00:00:00"))),
        )
        contact_dim = contact_dim.withColumn(
            "contact_sk",
            F.xxhash64(
                F.concat_ws(
                    "||",
                    F.col("contact_id"),
                    F.col("effective_start").cast("string"),
                )
            ),
        )

        current_accounts = account_dim.filter(F.col("is_current") == F.lit(True)).select(
            "account_id", "account_sk"
        )
        contact_dim = contact_dim.join(current_accounts, "account_id", "left")

        expected_schema = StructType(
            [
                StructField("contact_sk", LongType(), True),
                StructField("contact_id", StringType(), True),
                StructField("account_id", StringType(), True),
                StructField("account_sk", LongType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("title", StringType(), True),
                StructField("department", StringType(), True),
                StructField("lead_source", StringType(), True),
                StructField("engagement_score", DoubleType(), True),
                StructField("mailing_city", StringType(), True),
                StructField("mailing_state", StringType(), True),
                StructField("mailing_country", StringType(), True),
                StructField("created_date", TimestampType(), True),
                StructField("last_modified_date", TimestampType(), True),
                StructField("effective_start", TimestampType(), True),
                StructField("effective_end", TimestampType(), True),
                StructField("is_current", BooleanType(), True),
            ]
        )
        self._assert_non_null(
            contact_dim,
            ["contact_id", "contact_sk", "effective_start", "effective_end"],
            "dim_contact",
        )
        self._validate_gold_schema(contact_dim, "dim_contact", expected_schema, pk="contact_sk")

        self._write_gold(spark, contact_dim, "dim_contact", f"{gold_path}/dim_contact")
        return contact_dim

    def build_fact_orders(self, spark, orders_df, customer_dim, product_dim, fx_rates_df, gold_path: str):
        """Build Fact Orders from Silver data."""
        from pyspark.sql import functions as F
        from pyspark.sql.functions import broadcast, col
        from pyspark.sql.types import (
            BooleanType,
            DateType,
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        orders_base = orders_df
        if "currency" not in orders_base.columns:
            orders_base = orders_base.withColumn("currency", F.lit("USD"))
        if "unit_price" not in orders_base.columns:
            orders_base = orders_base.withColumn("unit_price", F.lit(None).cast("double"))

        orders_enriched = (
            orders_base.withColumn("order_date", F.to_date("order_date"))
            .withColumn("currency", F.upper(F.coalesce(F.col("currency"), F.lit("USD"))))
            .withColumn("total_amount", F.col("total_amount").cast("double"))
            .withColumn(
                "unit_price",
                F.coalesce(
                    F.col("unit_price").cast("double"),
                    (F.col("total_amount").cast("double") / F.coalesce(F.col("quantity"), F.lit(1)))
                    .cast("double"),
                ),
            )
            .repartition("order_date")
        )

        fx_rates = self._prepare_fx_rates(fx_rates_df)

        orders_with_fx = orders_enriched.join(
            fx_rates,
            (orders_enriched.order_date == fx_rates.fx_date)
            & (orders_enriched.currency == fx_rates.fx_currency),
            "left",
        ).withColumn(
            "fx_rate",
            F.when(F.col("currency") == F.lit("USD"), F.lit(1.0)).otherwise(F.col("fx_rate")),
        ).drop("fx_date", "fx_currency")

        missing_fx = orders_with_fx.filter(
            (F.col("currency") != F.lit("USD")) & F.col("fx_rate").isNull()
        ).count()
        if missing_fx > 0:
            if self.config.get("dq.fail_on_error", True):
                raise ValueError(
                    f"Fact orders: {missing_fx} rows missing FX rates for non-USD orders"
                )
            logger.warning("Fact orders: %s rows missing FX rates for non-USD orders", missing_fx)

        orders_with_fx = orders_with_fx.withColumn(
            "order_amount_usd",
            (F.col("total_amount").cast("double") * F.col("fx_rate")).cast("double"),
        )
        orders_with_fx = orders_with_fx.withColumn("order_year", F.year("order_date")).withColumn(
            "order_month", F.month("order_date")
        )

        current_customers = customer_dim.filter(F.col("is_current") == F.lit(True)).select(
            "customer_id", "customer_sk", "country"
        )
        current_products = product_dim.filter(F.col("is_current") == F.lit(True)).select(
            "product_id", "product_sk", "category"
        )

        fact_orders = (
            orders_with_fx.join(broadcast(current_customers), "customer_id", "left")
            .join(broadcast(current_products), "product_id", "left")
        )

        missing_customers = fact_orders.filter(col("country").isNull()).count()
        if missing_customers > 0:
            if self.config.get("dq.fail_on_error", True):
                raise ValueError(
                    f"Fact orders: Referential integrity failure, {missing_customers} orders "
                    "have no matching customer"
                )
            logger.warning(
                f"Fact orders: {missing_customers} rows missing customer country after join"
            )

        missing_products = fact_orders.filter(col("product_sk").isNull()).count()
        if missing_products > 0:
            if self.config.get("dq.fail_on_error", True):
                raise ValueError(
                    f"Fact orders: Referential integrity failure, {missing_products} orders "
                    "have no matching product"
                )
            logger.warning(
                "Fact orders: %s rows missing product after join", missing_products
            )

        fact_orders = (
            fact_orders.withColumn("customer_sk", F.coalesce(F.col("customer_sk"), F.lit(-1)))
            .withColumn("product_sk", F.coalesce(F.col("product_sk"), F.lit(-1)))
        )

        fact_orders = fact_orders.select(
            "order_id",
            "customer_id",
            "customer_sk",
            "product_id",
            "product_sk",
            "order_date",
            "currency",
            "fx_rate",
            "total_amount",
            "order_amount_usd",
            "unit_price",
            "quantity",
            "status",
            "country",
            "order_year",
            "order_month",
        )

        duplicate_orders = (
            fact_orders.groupBy("order_id").count().filter(col("count") > 1).count()
        )
        if duplicate_orders > 0:
            raise ValueError(
                f"Fact orders: duplicate primary keys detected for order_id, count={duplicate_orders}"
            )

        expected_schema = StructType(
            [
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("customer_sk", LongType(), True),
                StructField("product_id", StringType(), True),
                StructField("product_sk", LongType(), True),
                StructField("order_date", DateType(), True),
                StructField("currency", StringType(), True),
                StructField("fx_rate", DoubleType(), True),
                StructField("total_amount", DoubleType(), True),
                StructField("order_amount_usd", DoubleType(), True),
                StructField("unit_price", DoubleType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("status", StringType(), True),
                StructField("country", StringType(), True),
                StructField("order_year", IntegerType(), True),
                StructField("order_month", IntegerType(), True),
            ]
        )
        self._assert_non_null(
            fact_orders,
            ["order_id", "customer_id", "product_id", "order_date", "total_amount"],
            "fact_orders",
        )
        self._validate_gold_schema(fact_orders, "fact_orders", expected_schema, pk="order_id")

        # Write to gold
        self._write_gold(spark, fact_orders, "fact_orders", f"{gold_path}/fact_orders")
        return fact_orders

    def build_dim_date(self, spark, fact_orders, gold_path: str):
        """Build Date Dimension from fact orders."""
        from datetime import date, timedelta

        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            BooleanType,
            DateType,
            IntegerType,
            StructField,
            StructType,
        )

        date_col = "order_date" if "order_date" in fact_orders.columns else None

        if date_col:
            min_max = fact_orders.agg(
                F.min(F.col(date_col)).alias("min_date"),
                F.max(F.col(date_col)).alias("max_date"),
            ).collect()[0]
            start_date = min_max["min_date"]
            end_date = min_max["max_date"]
        else:
            start_date = None
            end_date = None

        if start_date is None or end_date is None:
            end_date = date.today()
            start_date = end_date - timedelta(days=730)
        else:
            if hasattr(start_date, "date"):
                start_date = start_date.date()
            if hasattr(end_date, "date"):
                end_date = end_date.date()

            days_diff = (end_date - start_date).days
            if days_diff < 730:
                center = start_date + timedelta(days=days_diff // 2)
                start_date = center - timedelta(days=365)
                end_date = center + timedelta(days=365)

        num_days = (end_date - start_date).days + 1
        start_date_str = start_date.strftime("%Y-%m-%d")

        dim_date = (
            spark.range(0, num_days)
            .withColumn("date", F.expr(f"date_add('{start_date_str}', cast(id as int))"))
            .select("date")
            .filter(F.col("date").isNotNull())
            .withColumn("date_sk", F.date_format("date", "yyyyMMdd").cast("int"))
            .withColumn("year", F.year("date"))
            .withColumn("quarter", F.quarter("date"))
            .withColumn("month", F.month("date"))
            .withColumn("day_of_week", F.dayofweek("date"))
            .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), True).otherwise(False))
            .select("date_sk", "date", "year", "quarter", "month", "day_of_week", "is_weekend")
            .orderBy("date")
        )

        expected_schema = StructType(
            [
                StructField("date_sk", IntegerType(), True),
                StructField("date", DateType(), True),
                StructField("year", IntegerType(), True),
                StructField("quarter", IntegerType(), True),
                StructField("month", IntegerType(), True),
                StructField("day_of_week", IntegerType(), True),
                StructField("is_weekend", BooleanType(), True),
            ]
        )
        self._assert_non_null(dim_date, ["date_sk", "date"], "dim_date")
        self._validate_gold_schema(dim_date, "dim_date", expected_schema, pk="date_sk")

        self._write_gold(spark, dim_date, "dim_date", f"{gold_path}/dim_date")
        return dim_date

    def build_product_performance(self, spark, product_dim, fact_orders, gold_path: str):
        """Build Product Performance analytics table."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            BooleanType,
            DateType,
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        product_agg = fact_orders.groupBy("product_id").agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("order_amount_usd").alias("total_revenue_usd"),
            F.avg("order_amount_usd").alias("avg_order_value_usd"),
            F.sum("quantity").alias("total_quantity_sold"),
            F.max("order_date").alias("last_sale_date"),
        )

        current_products = product_dim.filter(F.col("is_current") == F.lit(True)).select(
            "product_id",
            "product_name",
            "category",
            "price_usd",
        )
        product_performance = current_products.join(product_agg, "product_id", "left")
        product_performance = product_performance.fillna(
            {
                "total_orders": 0,
                "total_revenue_usd": 0.0,
                "avg_order_value_usd": 0.0,
                "total_quantity_sold": 0,
            }
        ).withColumn(
            "is_active", F.when(F.col("total_orders") > 0, F.lit(True)).otherwise(F.lit(False))
        )

        expected_schema = StructType(
            [
                StructField("product_id", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("price_usd", DoubleType(), True),
                StructField("is_active", BooleanType(), True),
                StructField("total_orders", LongType(), True),
                StructField("total_revenue_usd", DoubleType(), True),
                StructField("avg_order_value_usd", DoubleType(), True),
                StructField("total_quantity_sold", LongType(), True),
                StructField("last_sale_date", DateType(), True),
            ]
        )
        self._assert_non_null(product_performance, ["product_id"], "product_performance")
        self._validate_gold_schema(
            product_performance, "product_performance", expected_schema, pk="product_id"
        )

        self._write_gold(
            spark, product_performance, "product_performance", f"{gold_path}/product_performance"
        )
        return product_performance

    def build_fact_opportunity(self, spark, opportunities_df, account_dim, contact_dim, gold_path: str):
        """Build CRM Opportunity Fact table."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            BooleanType,
            DateType,
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        opps = (
            opportunities_df.select(
                "opportunity_id",
                "account_id",
                "contact_id",
                "opportunity_name",
                "stage",
                "amount",
                "probability",
                "lead_source",
                "type",
                "forecast_category",
                "is_closed",
                "is_won",
                "sales_cycle",
                "product_interest",
                "budget",
                "timeline",
                "deal_size",
                "close_date",
                F.to_timestamp("created_date").alias("created_date"),
                F.to_timestamp("last_modified_date").alias("last_modified_date"),
            )
            .withColumn("close_date", F.to_date("close_date"))
            .withColumn("amount", F.col("amount").cast("double"))
            .withColumn("probability", F.col("probability").cast("int"))
            .withColumn("close_year", F.year("close_date"))
            .withColumn("close_month", F.month("close_date"))
        )

        current_accounts = account_dim.filter(F.col("is_current") == F.lit(True)).select(
            "account_id", "account_sk"
        )
        current_contacts = contact_dim.filter(F.col("is_current") == F.lit(True)).select(
            "contact_id", "contact_sk"
        )

        fact_opps = (
            opps.join(current_accounts, "account_id", "left")
            .join(current_contacts, "contact_id", "left")
            .withColumn("account_sk", F.coalesce(F.col("account_sk"), F.lit(-1)))
            .withColumn("contact_sk", F.coalesce(F.col("contact_sk"), F.lit(-1)))
        )

        expected_schema = StructType(
            [
                StructField("opportunity_id", StringType(), True),
                StructField("account_id", StringType(), True),
                StructField("account_sk", LongType(), True),
                StructField("contact_id", StringType(), True),
                StructField("contact_sk", LongType(), True),
                StructField("opportunity_name", StringType(), True),
                StructField("stage", StringType(), True),
                StructField("amount", DoubleType(), True),
                StructField("probability", IntegerType(), True),
                StructField("lead_source", StringType(), True),
                StructField("type", StringType(), True),
                StructField("forecast_category", StringType(), True),
                StructField("is_closed", BooleanType(), True),
                StructField("is_won", BooleanType(), True),
                StructField("sales_cycle", IntegerType(), True),
                StructField("product_interest", StringType(), True),
                StructField("budget", StringType(), True),
                StructField("timeline", StringType(), True),
                StructField("deal_size", StringType(), True),
                StructField("close_date", DateType(), True),
                StructField("created_date", TimestampType(), True),
                StructField("last_modified_date", TimestampType(), True),
                StructField("close_year", IntegerType(), True),
                StructField("close_month", IntegerType(), True),
            ]
        )
        self._assert_non_null(
            fact_opps, ["opportunity_id", "account_id", "close_date"], "fact_opportunity"
        )
        self._validate_gold_schema(fact_opps, "fact_opportunity", expected_schema, pk="opportunity_id")

        self._write_gold(spark, fact_opps, "fact_opportunity", f"{gold_path}/fact_opportunity")
        return fact_opps

    def build_fact_order_events(
        self, spark, order_events_df, customer_dim, fact_orders, fx_rates_df, gold_path: str
    ):
        """Build Kafka Order Events Fact table."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            DateType,
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        events = (
            order_events_df.select(
                "event_id",
                "order_id",
                "customer_id",
                "event_type",
                "event_ts",
                "amount",
                "currency",
                "channel",
                "session_id",
                "topic",
                "partition",
                "offset",
            )
            .withColumn("event_ts", F.to_timestamp("event_ts"))
            .withColumn("event_date", F.to_date("event_ts"))
            .withColumn("event_year", F.year("event_ts"))
            .withColumn("event_month", F.month("event_ts"))
            .withColumn("currency", F.upper(F.coalesce(F.col("currency"), F.lit("USD"))))
            .withColumn("amount", F.col("amount").cast("double"))
        )

        fx_rates = self._prepare_fx_rates(fx_rates_df)
        events = events.join(
            fx_rates,
            (events.event_date == fx_rates.fx_date) & (events.currency == fx_rates.fx_currency),
            "left",
        ).withColumn(
            "fx_rate",
            F.when(F.col("currency") == F.lit("USD"), F.lit(1.0)).otherwise(F.col("fx_rate")),
        ).drop("fx_date", "fx_currency")

        missing_fx = events.filter(
            (F.col("currency") != F.lit("USD")) & F.col("fx_rate").isNull()
        ).count()
        if missing_fx > 0:
            if self.config.get("dq.fail_on_error", True):
                raise ValueError(
                    f"Fact order events: {missing_fx} rows missing FX rates for non-USD events"
                )
            logger.warning(
                "Fact order events: %s rows missing FX rates for non-USD events", missing_fx
            )

        events = events.withColumn(
            "amount_usd",
            (F.col("amount").cast("double") * F.col("fx_rate")).cast("double"),
        )

        current_customers = customer_dim.filter(F.col("is_current") == F.lit(True)).select(
            "customer_id", "customer_sk"
        )
        events = events.join(current_customers, "customer_id", "left").withColumn(
            "customer_sk", F.coalesce(F.col("customer_sk"), F.lit(-1))
        )

        orders_lookup = fact_orders.select(
            "order_id",
            F.col("order_amount_usd").alias("order_amount_usd"),
            F.col("order_date").alias("order_date"),
        )
        events = events.join(orders_lookup, "order_id", "left")

        expected_schema = StructType(
            [
                StructField("event_id", StringType(), True),
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("customer_sk", LongType(), True),
                StructField("event_type", StringType(), True),
                StructField("event_ts", TimestampType(), True),
                StructField("event_date", DateType(), True),
                StructField("event_year", IntegerType(), True),
                StructField("event_month", IntegerType(), True),
                StructField("amount", DoubleType(), True),
                StructField("currency", StringType(), True),
                StructField("fx_rate", DoubleType(), True),
                StructField("amount_usd", DoubleType(), True),
                StructField("channel", StringType(), True),
                StructField("session_id", StringType(), True),
                StructField("topic", StringType(), True),
                StructField("partition", IntegerType(), True),
                StructField("offset", LongType(), True),
                StructField("order_amount_usd", DoubleType(), True),
                StructField("order_date", DateType(), True),
            ]
        )
        self._assert_non_null(
            events, ["event_id", "customer_id", "event_ts"], "fact_order_events"
        )
        self._validate_gold_schema(events, "fact_order_events", expected_schema, pk="event_id")

        self._write_gold(spark, events, "fact_order_events", f"{gold_path}/fact_order_events")
        return events

    def build_customer_360(self, spark, customer_dim, fact_orders, behavior_df, gold_path: str):
        """Build Customer 360 view combining multiple data sources."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

        # Aggregate order metrics
        order_metrics = (
            fact_orders.groupBy("customer_id")
            .agg(
                F.sum("order_amount_usd").alias("total_spent"),
                F.countDistinct("order_id").alias("order_count"),
            )
        )

        # Aggregate behavior metrics
        behavior_metrics = (
            behavior_df.groupBy("customer_id")
            .agg(
                F.sum(F.col("time_spent_seconds").cast("double")).alias("total_time_spent"),
                F.countDistinct("session_id").alias("session_count"),
            )
        )

        # Combine all data for customer 360 view
        current_customers = customer_dim.filter(F.col("is_current") == F.lit(True)).select(
            "customer_id",
            "customer_sk",
            "first_name",
            "last_name",
            "email",
            "country",
        )
        customer_360 = (
            current_customers.join(order_metrics, "customer_id", "left")
            .join(behavior_metrics, "customer_id", "left")
            .withColumn("total_spent", F.coalesce(F.col("total_spent"), F.lit(0.0)))
            .withColumn(
                "order_count",
                F.coalesce(F.col("order_count"), F.lit(0).cast("long")),
            )
            .withColumn("total_time_spent", F.coalesce(F.col("total_time_spent"), F.lit(0.0)))
            .withColumn(
                "session_count",
                F.coalesce(F.col("session_count"), F.lit(0).cast("long")),
            )
        )

        expected_schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("customer_sk", LongType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("country", StringType(), True),
                StructField("total_spent", DoubleType(), True),
                StructField("order_count", LongType(), True),
                StructField("total_time_spent", DoubleType(), True),
                StructField("session_count", LongType(), True),
            ]
        )
        self._assert_non_null(customer_360, ["customer_id", "customer_sk"], "customer_360")
        self._validate_gold_schema(customer_360, "customer_360", expected_schema)

        # Write to gold
        self._write_gold(spark, customer_360, "customer_360", f"{gold_path}/customer_360")

    def build_behavior_analytics(self, spark, behavior_df, gold_path: str):
        """Build Customer Behavior Analytics from Silver data."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

        # Create behavior analytics
        behavior_analytics = (
            behavior_df.withColumn("time_spent_seconds", F.col("time_spent_seconds").cast("double"))
            .groupBy("customer_id", "event_type", "device_type", "browser")
            .agg(
                F.avg("time_spent_seconds").alias("avg_time_spent"),
                F.countDistinct("session_id").alias("session_count"),
            )
        )

        expected_schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("device_type", StringType(), True),
                StructField("browser", StringType(), True),
                StructField("avg_time_spent", DoubleType(), True),
                StructField("session_count", LongType(), True),
            ]
        )
        self._assert_non_null(behavior_analytics, ["customer_id"], "behavior_analytics")
        self._validate_gold_schema(
            behavior_analytics, "behavior_analytics", expected_schema
        )

        # Write to gold
        self._write_gold(
            spark, behavior_analytics, "behavior_analytics", f"{gold_path}/behavior_analytics"
        )
