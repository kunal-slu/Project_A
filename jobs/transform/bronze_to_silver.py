"""
Bronze to Silver Transformation Job

Transforms data from Bronze layer to Silver layer with cleaning and standardization.
"""

import logging
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from project_a.contracts.runtime_contracts import load_table_contracts, validate_contract
from project_a.core.base_job import BaseJob
from project_a.core.config import ProjectConfig

logger = logging.getLogger(__name__)


class BronzeToSilverJob(BaseJob):
    """Job to transform data from Bronze to Silver layer."""

    def __init__(self, config: ProjectConfig):
        super().__init__(config)
        self.job_name = "bronze_to_silver"

    @staticmethod
    def _resolve_col(
        df: DataFrame, candidates: list[str], field_name: str, required: bool = True
    ) -> F.Column:
        for candidate in candidates:
            if candidate in df.columns:
                return F.col(candidate)
        if required:
            raise ValueError(
                f"Missing required field '{field_name}'. Tried candidates: {candidates}. "
                f"Available columns: {df.columns}"
            )
        return F.lit(None).cast("string")

    def _storage_format(self) -> str:
        storage_fmt = (self.config.get("storage") or {}).get("format")
        if storage_fmt:
            return str(storage_fmt).lower()
        iceberg_cfg = self.config.get("iceberg", {})
        if iceberg_cfg.get("enabled"):
            return "iceberg"
        return "parquet"

    def _write_silver(self, spark, df: DataFrame, table_name: str, path: str) -> None:
        schema_evolution_cfg = self.config.get("schema_evolution", {})
        if schema_evolution_cfg.get("enabled"):
            from project_a.utils.schema_evolution import enforce_schema_evolution

            enforce_schema_evolution(
                df,
                table_name=table_name,
                layer="silver",
                config=schema_evolution_cfg,
            )

        fmt = self._storage_format()
        if fmt == "iceberg":
            from project_a.iceberg_utils import IcebergWriter

            catalog = self.config.get("iceberg", {}).get("catalog_name", "local")
            IcebergWriter(spark, catalog).write_overwrite(df, table_name)
        elif fmt == "delta":
            df.write.format("delta").mode("overwrite").save(path)
        else:
            df.write.mode("overwrite").parquet(path)

    def run(self, ctx) -> dict[str, Any]:
        """Execute the Bronze to Silver transformation."""
        logger.info("Starting Bronze to Silver transformation...")

        try:
            # Get Spark session from context
            spark = ctx.spark

            # Get bronze and silver paths from config
            paths_cfg = self.config.get("paths", {})
            bronze_path = paths_cfg.get("bronze_root") or paths_cfg.get("bronze") or "data/bronze"
            silver_path = paths_cfg.get("silver_root") or paths_cfg.get("silver") or "data/silver"
            contracts_path = self.config.get(
                "contracts.path", "config/contracts/silver_contracts.yaml"
            )
            table_contracts = load_table_contracts(contracts_path)

            # Transform CRM data
            logger.info("Transforming CRM data...")
            self.transform_crm_data(spark, bronze_path, silver_path)

            # Transform Snowflake data
            logger.info("Transforming Snowflake data...")
            snowflake_metrics = self.transform_snowflake_data(
                spark, bronze_path, silver_path, table_contracts
            )

            # Transform Redshift data
            logger.info("Transforming Redshift data...")
            self.transform_redshift_data(spark, bronze_path, silver_path)

            # Transform FX data
            logger.info("Transforming FX data...")
            self.transform_fx_data(spark, bronze_path, silver_path)

            # Transform Kafka events
            logger.info("Transforming Kafka events...")
            self.transform_kafka_events(spark, bronze_path, silver_path)

            # Apply data quality checks
            logger.info("Applying data quality checks...")
            self.apply_dq_rules(None, "silver.layer")

            # Log lineage
            self.log_lineage(source="bronze", target="silver", records_processed={})

            result = {
                "status": "success",
                "output_path": silver_path,
                "layers_processed": ["crm", "snowflake", "redshift", "fx", "kafka"],
                "metrics": snowflake_metrics,
            }

            logger.info(f"Bronze to Silver transformation completed: {result}")
            return result

        except FileNotFoundError as e:
            logger.error(f"Required input data missing: {e}")
            raise
        except ValueError as e:
            logger.error(f"Data quality check failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Bronze to Silver transformation failed: {e}", exc_info=True)
            raise

    def transform_crm_data(self, spark, bronze_path: str, silver_path: str):
        """Transform CRM data from bronze to silver."""
        from pyspark.sql.functions import col, lower, trim
        from pyspark.sql.utils import AnalysisException

        # Validate input paths exist
        paths = {
            "accounts": f"{bronze_path}/crm/accounts",
            "contacts": f"{bronze_path}/crm/contacts",
            "opportunities": f"{bronze_path}/crm/opportunities",
        }

        for name, path in paths.items():
            try:
                test_df = spark.read.parquet(path)
                count = test_df.count()
                if count == 0:
                    logger.warning(f"CRM {name} is empty at {path}")
            except AnalysisException as exc:
                raise FileNotFoundError(f"Required CRM input not found: {path}") from exc

        # Read bronze CRM data
        accounts_df = spark.read.parquet(paths["accounts"])
        contacts_df = spark.read.parquet(paths["contacts"])
        opportunities_df = spark.read.parquet(paths["opportunities"])

        # Clean and standardize
        # Clean accounts
        accounts_clean = accounts_df.select(
            self._resolve_col(accounts_df, ["account_id", "Id", "id"], "account_id").alias(
                "account_id"
            ),
            trim(
                lower(self._resolve_col(accounts_df, ["account_name", "Name"], "account_name"))
            ).alias("account_name"),
            self._resolve_col(accounts_df, ["industry", "Industry"], "industry").alias("industry"),
            self._resolve_col(accounts_df, ["created_date", "CreatedDate"], "created_date").alias(
                "created_date"
            ),
        )

        # Validate no null primary keys
        null_pks = accounts_clean.filter(col("account_id").isNull()).count()
        if null_pks > 0:
            raise ValueError(f"CRM accounts: Found {null_pks} null account_ids - cannot proceed")

        # Clean contacts
        contacts_clean = contacts_df.select(
            self._resolve_col(contacts_df, ["contact_id", "Id", "id"], "contact_id").alias(
                "contact_id"
            ),
            self._resolve_col(contacts_df, ["account_id", "AccountId"], "account_id").alias(
                "account_id"
            ),
            trim(lower(self._resolve_col(contacts_df, ["first_name", "FirstName"], "first_name")))
            .alias("first_name"),
            trim(lower(self._resolve_col(contacts_df, ["last_name", "LastName"], "last_name")))
            .alias("last_name"),
            lower(self._resolve_col(contacts_df, ["email", "Email"], "email")).alias("email"),
            self._resolve_col(contacts_df, ["phone", "Phone", "MobilePhone"], "phone").alias(
                "phone"
            ),
        )

        # Validate no null primary keys
        null_pks = contacts_clean.filter(col("contact_id").isNull()).count()
        if null_pks > 0:
            raise ValueError(f"CRM contacts: Found {null_pks} null contact_ids - cannot proceed")

        # Clean opportunities
        opportunities_clean = opportunities_df.select(
            self._resolve_col(
                opportunities_df, ["opportunity_id", "Id", "id"], "opportunity_id"
            ).alias("opportunity_id"),
            self._resolve_col(opportunities_df, ["account_id", "AccountId"], "account_id").alias(
                "account_id"
            ),
            self._resolve_col(
                opportunities_df, ["contact_id", "ContactId"], "contact_id", required=False
            ).alias("contact_id"),
            self._resolve_col(
                opportunities_df, ["opportunity_name", "Name"], "opportunity_name"
            ).alias("opportunity_name"),
            self._resolve_col(opportunities_df, ["amount", "Amount"], "amount").cast("double").alias(
                "amount"
            ),
            self._resolve_col(opportunities_df, ["stage", "StageName"], "stage").alias("stage"),
            self._resolve_col(opportunities_df, ["close_date", "CloseDate"], "close_date").alias(
                "close_date"
            ),
        )

        # Validate no null primary keys
        null_pks = opportunities_clean.filter(col("opportunity_id").isNull()).count()
        if null_pks > 0:
            raise ValueError(
                f"CRM opportunities: Found {null_pks} null opportunity_ids - cannot proceed"
            )

        # Write to silver
        self._write_silver(
            spark, accounts_clean, "accounts_silver", f"{silver_path}/accounts_silver"
        )
        self._write_silver(
            spark, contacts_clean, "contacts_silver", f"{silver_path}/contacts_silver"
        )
        self._write_silver(
            spark,
            opportunities_clean,
            "opportunities_silver",
            f"{silver_path}/opportunities_silver",
        )

    def _upsert_orders_incremental(
        self, spark, incoming_orders: DataFrame, existing_path: str, lookback_days: int
    ) -> DataFrame:
        """
        Incremental strategy with late-data handling.

        Reprocess a rolling window and keep latest record per order_id.
        """
        try:
            existing_orders = spark.read.parquet(existing_path)
        except Exception:
            return incoming_orders

        if "order_date" not in incoming_orders.columns:
            return incoming_orders

        cutoff_date = F.current_date() - F.lit(lookback_days)

        existing_with_dates = existing_orders.withColumn("_order_date", F.to_date("order_date"))
        incoming_with_dates = incoming_orders.withColumn("_order_date", F.to_date("order_date"))

        unchanged_history = existing_with_dates.filter(F.col("_order_date") < cutoff_date).drop(
            "_order_date"
        )
        reprocess_existing = existing_with_dates.filter(F.col("_order_date") >= cutoff_date).drop(
            "_order_date"
        )
        reprocess_incoming = incoming_with_dates.filter(F.col("_order_date") >= cutoff_date).drop(
            "_order_date"
        )

        merge_candidates = reprocess_existing.unionByName(reprocess_incoming, allowMissingColumns=True)
        latest_ts = (
            F.coalesce(F.col("updated_at").cast("timestamp"), F.current_timestamp())
            if "updated_at" in merge_candidates.columns
            else F.current_timestamp()
        )
        dedupe_window = Window.partitionBy("order_id").orderBy(latest_ts.desc())
        deduped_recent = (
            merge_candidates.withColumn("_rn", F.row_number().over(dedupe_window))
            .filter(F.col("_rn") == F.lit(1))
            .drop("_rn")
        )

        return unchanged_history.unionByName(deduped_recent, allowMissingColumns=True)

    def transform_snowflake_data(
        self,
        spark,
        bronze_path: str,
        silver_path: str,
        table_contracts: dict[str, dict[str, Any]],
    ):
        """Transform Snowflake data from bronze to silver."""
        from pyspark.sql.functions import col, lower, to_date, to_timestamp, trim
        from pyspark.sql.utils import AnalysisException

        # Validate input paths exist
        paths = {
            "customers": f"{bronze_path}/snowflake/customers",
            "orders": f"{bronze_path}/snowflake/orders",
            "products": f"{bronze_path}/snowflake/products",
        }

        for name, path in paths.items():
            try:
                test_df = spark.read.parquet(path)
                if test_df.count() == 0:
                    logger.warning(f"Snowflake {name} is empty at {path}")
            except AnalysisException as exc:
                raise FileNotFoundError(f"Required Snowflake input not found: {path}") from exc

        # Read bronze Snowflake data
        customers_df = spark.read.parquet(paths["customers"])
        orders_df = spark.read.parquet(paths["orders"])
        products_df = spark.read.parquet(paths["products"])

        # Clean and standardize
        # Clean customers
        customers_clean = customers_df.select(
            self._resolve_col(customers_df, ["customer_id"], "customer_id").alias("customer_id"),
            trim(lower(self._resolve_col(customers_df, ["first_name"], "first_name"))).alias(
                "first_name"
            ),
            trim(lower(self._resolve_col(customers_df, ["last_name"], "last_name"))).alias(
                "last_name"
            ),
            lower(self._resolve_col(customers_df, ["email"], "email")).alias("email"),
            F.coalesce(
                self._resolve_col(customers_df, ["country"], "country", required=False),
                F.lit("unknown"),
            ).alias("country"),
            F.coalesce(
                self._resolve_col(
                    customers_df, ["registration_date", "created_at"], "registration_date", required=False
                ),
                F.current_date().cast("string"),
            ).alias("registration_date"),
        )

        # Validate no null primary keys
        null_pks = customers_clean.filter(col("customer_id").isNull()).count()
        if null_pks > 0:
            raise ValueError(
                f"Snowflake customers: Found {null_pks} null customer_ids - cannot proceed"
            )

        # Clean orders
        # Note: Bronze source has 'total_amount' column (from CSV)
        # We keep it as 'total_amount' for Silver (matches dbt schema)
        orders_clean = orders_df.select(
            self._resolve_col(orders_df, ["order_id"], "order_id").alias("order_id"),
            self._resolve_col(orders_df, ["customer_id"], "customer_id").alias("customer_id"),
            self._resolve_col(orders_df, ["product_id"], "product_id").alias("product_id"),
            to_date(self._resolve_col(orders_df, ["order_date"], "order_date")).alias("order_date"),
            self._resolve_col(orders_df, ["total_amount", "amount_usd", "amount"], "total_amount")
            .cast("double")
            .alias("total_amount"),
            self._resolve_col(orders_df, ["quantity"], "quantity").cast("int").alias("quantity"),
            self._resolve_col(orders_df, ["status"], "status").alias("status"),
            F.coalesce(
                to_timestamp(
                    self._resolve_col(
                        orders_df, ["updated_at", "order_timestamp"], "updated_at", required=False
                    )
                ),
                F.current_timestamp(),
            ).alias("updated_at"),
        )

        # Clean products
        products_clean = products_df.select(
            self._resolve_col(products_df, ["product_id"], "product_id").alias("product_id"),
            self._resolve_col(products_df, ["product_name"], "product_name").alias("product_name"),
            self._resolve_col(products_df, ["category"], "category").alias("category"),
            self._resolve_col(products_df, ["price_usd", "price"], "price_usd").cast("double").alias(
                "price_usd"
            ),
            self._resolve_col(products_df, ["cost_usd"], "cost_usd").cast("double").alias("cost_usd"),
            self._resolve_col(products_df, ["supplier_id"], "supplier_id").alias("supplier_id"),
        )

        # Validate no null primary keys
        null_pks = products_clean.filter(col("product_id").isNull()).count()
        if null_pks > 0:
            raise ValueError(
                f"Snowflake products: Found {null_pks} null product_ids - cannot proceed"
            )

        validate_contract(
            customers_clean,
            "customers_silver",
            table_contracts.get("customers_silver", {}),
        )
        validate_contract(
            products_clean,
            "products_silver",
            table_contracts.get("products_silver", {}),
        )

        lookback_days = int(
            table_contracts.get("orders_silver", {})
            .get("late_data", {})
            .get("lookback_days", 3)
        )
        orders_input = orders_clean
        if self.config.is_local():
            valid_customers = customers_clean.select("customer_id").dropDuplicates(["customer_id"])
            before_count = orders_clean.count()
            orders_input = orders_clean.join(valid_customers, on="customer_id", how="inner")
            dropped_count = before_count - orders_input.count()
            if dropped_count > 0:
                logger.warning(
                    "Local run: filtered %s orphan orders before contract validation",
                    dropped_count,
                )

        orders_output_path = f"{silver_path}/orders_silver"
        if self.config.is_local():
            orders_incremental = orders_input
        else:
            orders_incremental = self._upsert_orders_incremental(
                spark, orders_input, orders_output_path, lookback_days
            )
        validate_contract(
            orders_incremental,
            "orders_silver",
            table_contracts.get("orders_silver", {}),
            parent_frames={"customers_silver": customers_clean},
        )

        # Write to silver (idempotent overwrite after deterministic incremental merge)
        self._write_silver(
            spark, customers_clean, "customers_silver", f"{silver_path}/customers_silver"
        )
        self._write_silver(spark, orders_incremental, "orders_silver", orders_output_path)
        self._write_silver(
            spark, products_clean, "products_silver", f"{silver_path}/products_silver"
        )

        return {
            "customers_rows": customers_clean.count(),
            "orders_rows": orders_incremental.count(),
            "products_rows": products_clean.count(),
            "orders_lookback_days": lookback_days,
        }

    def transform_redshift_data(self, spark, bronze_path: str, silver_path: str):
        """Transform Redshift data from bronze to silver."""
        # Read bronze Redshift data
        behavior_df = spark.read.parquet(f"{bronze_path}/redshift/customer_behavior")

        # Clean and standardize
        behavior_clean = behavior_df.select(
            self._resolve_col(behavior_df, ["customer_id", "behavior_id"], "customer_id").alias(
                "customer_id"
            ),
            self._resolve_col(behavior_df, ["session_id", "customer_id"], "session_id").alias(
                "session_id"
            ),
            self._resolve_col(behavior_df, ["page_viewed", "event_name"], "page_viewed").alias(
                "page_viewed"
            ),
            self._resolve_col(
                behavior_df,
                ["time_spent_seconds", "duration_seconds", "event_timestamp"],
                "time_spent_seconds",
            )
            .cast("int")
            .alias("time_spent_seconds"),
            self._resolve_col(behavior_df, ["event_type", "session_id"], "event_type").alias(
                "event_type"
            ),
            self._resolve_col(behavior_df, ["event_date", "event_timestamp", "page_url"], "event_date")
            .alias("event_date"),
            self._resolve_col(behavior_df, ["device_type", "referrer"], "device_type").alias(
                "device_type"
            ),
            self._resolve_col(behavior_df, ["browser", "device_type"], "browser").alias("browser"),
        )

        # Write to silver
        self._write_silver(
            spark,
            behavior_clean,
            "customer_behavior_silver",
            f"{silver_path}/customer_behavior_silver",
        )

    def transform_fx_data(self, spark, bronze_path: str, silver_path: str):
        """Transform FX data from bronze to silver."""
        # Read bronze FX data
        fx_df = spark.read.parquet(f"{bronze_path}/fx/fx_rates")

        # Clean and standardize
        fx_clean = fx_df.select(
            self._resolve_col(fx_df, ["trade_date", "date"], "trade_date").alias("trade_date"),
            self._resolve_col(fx_df, ["base_ccy", "base_currency"], "base_ccy").alias("base_ccy"),
            self._resolve_col(fx_df, ["counter_ccy", "quote_ccy", "target_currency"], "counter_ccy")
            .alias("counter_ccy"),
            self._resolve_col(fx_df, ["rate", "exchange_rate"], "rate").cast("double").alias("rate"),
        )

        # Write to silver
        self._write_silver(spark, fx_clean, "fx_rates_silver", f"{silver_path}/fx_rates_silver")

    def transform_kafka_events(self, spark, bronze_path: str, silver_path: str):
        """Transform Kafka events from bronze to silver."""
        # Read bronze Kafka data
        events_df = spark.read.parquet(f"{bronze_path}/kafka/events")

        # Clean and standardize
        events_clean = events_df.select(
            self._resolve_col(events_df, ["event_id", "key"], "event_id").alias("event_id"),
            self._resolve_col(events_df, ["order_id"], "order_id").alias("order_id"),
            self._resolve_col(events_df, ["event_type"], "event_type").alias("event_type"),
            self._resolve_col(events_df, ["event_ts", "timestamp"], "event_ts").alias("event_ts"),
            self._resolve_col(events_df, ["amount"], "amount").cast("double").alias("amount"),
            self._resolve_col(events_df, ["currency"], "currency").alias("currency"),
            self._resolve_col(events_df, ["channel", "topic"], "channel").alias("channel"),
        )

        # Write to silver
        self._write_silver(
            spark,
            events_clean,
            "order_events_silver",
            f"{silver_path}/order_events_silver",
        )
