"""
Bronze to Silver Transformation Job

Transforms data from Bronze layer to Silver layer with cleaning and standardization.
"""

import logging
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
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

    @staticmethod
    def _pick_column(candidates: list[str], columns: list[str]) -> str | None:
        for candidate in candidates:
            if candidate in columns:
                return candidate
        return None

    @staticmethod
    def _coalesce_columns(
        df: DataFrame,
        candidates: list[str],
        field_name: str,
        required: bool = True,
    ) -> F.Column:
        existing = [candidate for candidate in candidates if candidate in df.columns]
        if not existing:
            if required:
                raise ValueError(
                    f"Missing required field '{field_name}'. Tried candidates: {candidates}. "
                    f"Available columns: {df.columns}"
                )
            return F.lit(None).cast("string")

        expr = F.col(existing[0])
        for candidate in existing[1:]:
            expr = F.coalesce(expr, F.col(candidate))
        return expr

    def _apply_cdc_if_present(
        self,
        df: DataFrame,
        key_cols: list[str],
        ts_col_candidates: list[str],
        op_col_candidates: list[str],
        delete_flag_candidates: list[str],
    ) -> DataFrame:
        op_col = self._pick_column(op_col_candidates, df.columns)
        delete_col = self._pick_column(delete_flag_candidates, df.columns)
        ts_col = self._pick_column(ts_col_candidates, df.columns)

        if not (op_col or delete_col or ts_col):
            return df

        df_cdc = df
        if op_col:
            df_cdc = df_cdc.withColumn("_cdc_op", F.upper(F.col(op_col)))
        elif delete_col:
            df_cdc = df_cdc.withColumn(
                "_cdc_op",
                F.when(F.col(delete_col).cast("boolean"), F.lit("DELETE")).otherwise("UPSERT"),
            )
        else:
            df_cdc = df_cdc.withColumn("_cdc_op", F.lit("UPSERT"))

        # Keep only latest version per key if timestamps exist
        if ts_col:
            window = Window.partitionBy(*key_cols).orderBy(F.col(ts_col).desc_nulls_last())
            df_cdc = df_cdc.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1)
            df_cdc = df_cdc.drop("_rn")

        # Drop deletes
        df_cdc = df_cdc.filter(~F.col("_cdc_op").isin("D", "DELETE"))
        return df_cdc

    def _storage_format(self) -> str:
        storage_fmt = (self.config.get("storage") or {}).get("format")
        if storage_fmt:
            return str(storage_fmt).lower()
        iceberg_cfg = self.config.get("iceberg", {})
        if iceberg_cfg.get("enabled"):
            return "iceberg"
        return "parquet"

    def _write_silver(
        self,
        spark,
        df: DataFrame,
        table_name: str,
        path: str,
        write_mode: str = "overwrite",
        merge_key: str | None = None,
    ) -> None:
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
        write_mode = (write_mode or "overwrite").lower()
        if fmt == "iceberg":
            from project_a.iceberg_utils import IcebergWriter

            catalog = self.config.get("iceberg", {}).get("catalog_name", "local")
            writer = IcebergWriter(spark, catalog)
            if write_mode == "merge":
                if not merge_key:
                    raise ValueError(f"{table_name}: merge write requested without merge_key")
                writer.write_merge(df, table_name, merge_key=merge_key)
            elif write_mode == "append":
                writer.write_append(df, table_name)
            else:
                writer.write_overwrite(df, table_name)
        elif fmt == "delta":
            mode = "append" if write_mode == "append" else "overwrite"
            if write_mode == "merge":
                logger.warning(
                    "%s: merge mode requested but Delta merge is not enabled in this job, using overwrite",
                    table_name,
                )
                mode = "overwrite"
            df.write.format("delta").mode(mode).save(path)
        else:
            mode = "append" if write_mode == "append" else "overwrite"
            if write_mode == "merge":
                logger.warning(
                    "%s: merge mode requested but parquet merge is not supported, using overwrite",
                    table_name,
                )
                mode = "overwrite"
            df.write.mode(mode).parquet(path)

    @staticmethod
    def _to_local_path(path: str) -> Path | None:
        if path.startswith("file://"):
            parsed = urlparse(path)
            return Path(unquote(parsed.path))
        if "://" in path:
            return None
        return Path(path)

    def _remove_empty_local_parquet_files(self, path: str) -> int:
        local_path = self._to_local_path(path)
        if local_path is None or not local_path.exists():
            return 0

        removed = 0
        for parquet_file in local_path.glob("*.parquet"):
            try:
                if parquet_file.stat().st_size == 0:
                    parquet_file.unlink()
                    removed += 1
            except OSError:
                logger.warning("Failed to inspect/remove candidate file: %s", parquet_file)
        return removed

    def _read_bronze_parquet(self, spark, path: str, table_name: str) -> DataFrame:
        removed = self._remove_empty_local_parquet_files(path)
        if removed > 0:
            logger.warning(
                "%s: removed %s empty parquet part files before read (%s)",
                table_name,
                removed,
                path,
            )

        try:
            return spark.read.parquet(path)
        except Exception as exc:
            error_text = str(exc)
            if "Conflicting directory structures detected" in error_text:
                logger.warning(
                    "%s: parquet read hit mixed directory layout; retrying with recursiveFileLookup",
                    table_name,
                )
                return (
                    spark.read.option("recursiveFileLookup", "true")
                    .option("pathGlobFilter", "*.parquet")
                    .parquet(path)
                )
            logger.warning(
                "%s: standard parquet read failed (%s). Retrying with ignoreCorruptFiles=true",
                table_name,
                exc,
            )
            recovered = spark.read.option("ignoreCorruptFiles", "true").parquet(path)
            if recovered.rdd.isEmpty():
                raise RuntimeError(
                    f"{table_name}: unable to read parquet data at {path}; no valid records available"
                ) from exc
            return recovered

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
            crm_metrics = self.transform_crm_data(spark, bronze_path, silver_path)

            # Transform Snowflake data
            logger.info("Transforming Snowflake data...")
            snowflake_metrics, customers_silver_df = self.transform_snowflake_data(
                spark, bronze_path, silver_path, table_contracts
            )

            # Transform Redshift data
            logger.info("Transforming Redshift data...")
            redshift_metrics = self.transform_redshift_data(
                spark,
                bronze_path,
                silver_path,
                table_contracts,
                customers_silver_df,
            )

            # Transform FX data
            logger.info("Transforming FX data...")
            fx_metrics = self.transform_fx_data(spark, bronze_path, silver_path, table_contracts)

            # Transform Kafka events
            logger.info("Transforming Kafka events...")
            kafka_metrics = self.transform_kafka_events(
                spark,
                bronze_path,
                silver_path,
                table_contracts,
                customers_silver_df,
            )

            # Apply data quality checks
            logger.info("Applying data quality checks...")
            logger.info(
                "Silver contracts validated within source-specific transforms "
                "(customers/orders/products/behavior/fx/events)"
            )

            # Log lineage
            self.log_lineage(source="bronze", target="silver", records_processed={})

            result = {
                "status": "success",
                "output_path": silver_path,
                "layers_processed": ["crm", "snowflake", "redshift", "fx", "kafka"],
                "metrics": {
                    **crm_metrics,
                    **snowflake_metrics,
                    **redshift_metrics,
                    **fx_metrics,
                    **kafka_metrics,
                },
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

        # Validate input paths exist
        paths = {
            "accounts": f"{bronze_path}/crm/accounts",
            "contacts": f"{bronze_path}/crm/contacts",
            "opportunities": f"{bronze_path}/crm/opportunities",
        }

        # Read bronze CRM data
        accounts_df = self._read_bronze_parquet(spark, paths["accounts"], "bronze.crm.accounts")
        contacts_df = self._read_bronze_parquet(spark, paths["contacts"], "bronze.crm.contacts")
        opportunities_df = self._read_bronze_parquet(
            spark, paths["opportunities"], "bronze.crm.opportunities"
        )

        for name, df in {
            "accounts": accounts_df,
            "contacts": contacts_df,
            "opportunities": opportunities_df,
        }.items():
            if df.rdd.isEmpty():
                logger.warning("CRM %s is empty at %s", name, paths[name])

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
            self._resolve_col(
                accounts_df, ["annual_revenue", "AnnualRevenue"], "annual_revenue", required=False
            )
            .cast("double")
            .alias("annual_revenue"),
            self._resolve_col(
                accounts_df,
                ["number_of_employees", "NumberOfEmployees"],
                "number_of_employees",
                required=False,
            )
            .cast("int")
            .alias("number_of_employees"),
            self._resolve_col(
                accounts_df, ["billing_city", "BillingCity"], "billing_city", required=False
            ).alias("billing_city"),
            self._resolve_col(
                accounts_df, ["billing_state", "BillingState"], "billing_state", required=False
            ).alias("billing_state"),
            self._resolve_col(
                accounts_df,
                ["billing_country", "BillingCountry"],
                "billing_country",
                required=False,
            ).alias("billing_country"),
            self._resolve_col(accounts_df, ["phone", "Phone"], "phone", required=False).alias(
                "phone"
            ),
            self._resolve_col(accounts_df, ["website", "Website"], "website", required=False).alias(
                "website"
            ),
            self._resolve_col(
                accounts_df,
                ["customer_segment", "CustomerSegment"],
                "customer_segment",
                required=False,
            ).alias("customer_segment"),
            self._resolve_col(
                accounts_df,
                ["geographic_region", "GeographicRegion"],
                "geographic_region",
                required=False,
            ).alias("geographic_region"),
            self._resolve_col(
                accounts_df, ["account_status", "AccountStatus"], "account_status", required=False
            ).alias("account_status"),
            self._resolve_col(
                accounts_df, ["created_date", "CreatedDate"], "created_date", required=False
            ).alias("created_date"),
            self._resolve_col(
                accounts_df,
                ["last_modified_date", "LastModifiedDate"],
                "last_modified_date",
                required=False,
            ).alias("last_modified_date"),
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
            trim(
                lower(self._resolve_col(contacts_df, ["first_name", "FirstName"], "first_name"))
            ).alias("first_name"),
            trim(
                lower(self._resolve_col(contacts_df, ["last_name", "LastName"], "last_name"))
            ).alias("last_name"),
            lower(self._resolve_col(contacts_df, ["email", "Email"], "email")).alias("email"),
            self._resolve_col(contacts_df, ["phone", "Phone", "MobilePhone"], "phone").alias(
                "phone"
            ),
            self._resolve_col(contacts_df, ["title", "Title"], "title", required=False).alias(
                "title"
            ),
            self._resolve_col(
                contacts_df, ["department", "Department"], "department", required=False
            ).alias("department"),
            self._resolve_col(
                contacts_df, ["lead_source", "LeadSource"], "lead_source", required=False
            ).alias("lead_source"),
            self._resolve_col(
                contacts_df,
                ["engagement_score", "EngagementScore"],
                "engagement_score",
                required=False,
            )
            .cast("double")
            .alias("engagement_score"),
            self._resolve_col(
                contacts_df, ["mailing_city", "MailingCity"], "mailing_city", required=False
            ).alias("mailing_city"),
            self._resolve_col(
                contacts_df, ["mailing_state", "MailingState"], "mailing_state", required=False
            ).alias("mailing_state"),
            self._resolve_col(
                contacts_df,
                ["mailing_country", "MailingCountry"],
                "mailing_country",
                required=False,
            ).alias("mailing_country"),
            self._resolve_col(
                contacts_df, ["created_date", "CreatedDate"], "created_date", required=False
            ).alias("created_date"),
            self._resolve_col(
                contacts_df,
                ["last_modified_date", "LastModifiedDate"],
                "last_modified_date",
                required=False,
            ).alias("last_modified_date"),
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
            self._resolve_col(opportunities_df, ["amount", "Amount"], "amount")
            .cast("double")
            .alias("amount"),
            self._resolve_col(opportunities_df, ["stage", "StageName"], "stage").alias("stage"),
            self._resolve_col(opportunities_df, ["close_date", "CloseDate"], "close_date").alias(
                "close_date"
            ),
            self._resolve_col(
                opportunities_df, ["probability", "Probability"], "probability", required=False
            )
            .cast("int")
            .alias("probability"),
            self._resolve_col(
                opportunities_df, ["lead_source", "LeadSource"], "lead_source", required=False
            ).alias("lead_source"),
            self._resolve_col(opportunities_df, ["type", "Type"], "type", required=False).alias(
                "type"
            ),
            self._resolve_col(
                opportunities_df,
                ["forecast_category", "ForecastCategory"],
                "forecast_category",
                required=False,
            ).alias("forecast_category"),
            self._resolve_col(
                opportunities_df, ["is_closed", "IsClosed"], "is_closed", required=False
            )
            .cast("boolean")
            .alias("is_closed"),
            self._resolve_col(opportunities_df, ["is_won", "IsWon"], "is_won", required=False)
            .cast("boolean")
            .alias("is_won"),
            self._resolve_col(
                opportunities_df, ["created_date", "CreatedDate"], "created_date", required=False
            ).alias("created_date"),
            self._resolve_col(
                opportunities_df,
                ["last_modified_date", "LastModifiedDate"],
                "last_modified_date",
                required=False,
            ).alias("last_modified_date"),
            self._resolve_col(
                opportunities_df, ["sales_cycle", "SalesCycle"], "sales_cycle", required=False
            )
            .cast("int")
            .alias("sales_cycle"),
            self._resolve_col(
                opportunities_df,
                ["product_interest", "ProductInterest"],
                "product_interest",
                required=False,
            ).alias("product_interest"),
            self._resolve_col(
                opportunities_df, ["budget", "Budget"], "budget", required=False
            ).alias("budget"),
            self._resolve_col(
                opportunities_df, ["timeline", "Timeline"], "timeline", required=False
            ).alias("timeline"),
            self._resolve_col(
                opportunities_df, ["deal_size", "DealSize"], "deal_size", required=False
            ).alias("deal_size"),
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
        return {
            "accounts_rows": accounts_clean.count(),
            "contacts_rows": contacts_clean.count(),
            "opportunities_rows": opportunities_clean.count(),
        }

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

        merge_candidates = reprocess_existing.unionByName(
            reprocess_incoming, allowMissingColumns=True
        )
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

        # Validate input paths exist
        paths = {
            "customers": f"{bronze_path}/snowflake/customers",
            "orders": f"{bronze_path}/snowflake/orders",
            "products": f"{bronze_path}/snowflake/products",
        }

        # Read bronze Snowflake data
        customers_df = self._read_bronze_parquet(
            spark, paths["customers"], "bronze.snowflake.customers"
        )
        orders_df = self._read_bronze_parquet(spark, paths["orders"], "bronze.snowflake.orders")
        products_df = self._read_bronze_parquet(
            spark, paths["products"], "bronze.snowflake.products"
        )

        for name, df in {
            "customers": customers_df,
            "orders": orders_df,
            "products": products_df,
        }.items():
            if df.rdd.isEmpty():
                logger.warning("Snowflake %s is empty at %s", name, paths[name])

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
                    customers_df,
                    ["registration_date", "created_at"],
                    "registration_date",
                    required=False,
                ),
                F.current_date().cast("string"),
            ).alias("registration_date"),
            to_timestamp(
                self._resolve_col(
                    customers_df,
                    ["updated_at", "last_modified_date", "modified_at"],
                    "updated_at",
                    required=False,
                )
            ).alias("updated_at"),
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
            F.upper(
                self._resolve_col(
                    orders_df, ["currency", "order_currency"], "currency", required=False
                )
            ).alias("currency"),
            self._resolve_col(orders_df, ["unit_price", "price"], "unit_price", required=False)
            .cast("double")
            .alias("unit_price"),
            self._resolve_col(
                orders_df, ["payment_method", "paymentType"], "payment_method", required=False
            ).alias("payment_method"),
            self._resolve_col(orders_df, ["total_amount", "amount_usd", "amount"], "total_amount")
            .cast("double")
            .alias("total_amount"),
            self._resolve_col(orders_df, ["quantity"], "quantity").cast("int").alias("quantity"),
            self._resolve_col(orders_df, ["status"], "status").alias("status"),
            self._resolve_col(orders_df, ["op", "_op", "operation"], "op", required=False).alias(
                "op"
            ),
            self._resolve_col(
                orders_df, ["is_deleted", "deleted", "_is_deleted"], "is_deleted", required=False
            )
            .cast("boolean")
            .alias("is_deleted"),
            F.coalesce(
                to_timestamp(
                    self._resolve_col(
                        orders_df, ["updated_at", "order_timestamp"], "updated_at", required=False
                    )
                ),
                F.current_timestamp(),
            ).alias("updated_at"),
        )

        # Apply CDC semantics if present (op/is_deleted/updated_at columns)
        orders_clean = self._apply_cdc_if_present(
            orders_clean,
            key_cols=["order_id"],
            ts_col_candidates=["updated_at", "event_ts", "event_timestamp"],
            op_col_candidates=["op", "_op", "operation"],
            delete_flag_candidates=["is_deleted", "deleted", "_is_deleted"],
        )

        # Clean products
        products_clean = products_df.select(
            self._resolve_col(products_df, ["product_id"], "product_id").alias("product_id"),
            self._resolve_col(products_df, ["product_name"], "product_name").alias("product_name"),
            self._resolve_col(products_df, ["category"], "category").alias("category"),
            self._resolve_col(products_df, ["price_usd", "price"], "price_usd")
            .cast("double")
            .alias("price_usd"),
            self._resolve_col(products_df, ["cost_usd"], "cost_usd")
            .cast("double")
            .alias("cost_usd"),
            self._resolve_col(products_df, ["supplier_id"], "supplier_id").alias("supplier_id"),
            to_timestamp(
                self._resolve_col(
                    products_df,
                    ["updated_at", "last_modified_date", "modified_at"],
                    "updated_at",
                    required=False,
                )
            ).alias("updated_at"),
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
            table_contracts.get("orders_silver", {}).get("late_data", {}).get("lookback_days", 3)
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

        # Write to silver:
        # - customers/products are full refresh dimensions
        # - orders_silver uses merge semantics where table format supports it
        self._write_silver(
            spark, customers_clean, "customers_silver", f"{silver_path}/customers_silver"
        )
        self._write_silver(
            spark,
            orders_incremental,
            "orders_silver",
            orders_output_path,
            write_mode="merge",
            merge_key="order_id",
        )
        self._write_silver(
            spark, products_clean, "products_silver", f"{silver_path}/products_silver"
        )

        return (
            {
                "snowflake_customers_rows": customers_clean.count(),
                "snowflake_orders_rows": orders_incremental.count(),
                "snowflake_products_rows": products_clean.count(),
                "orders_lookback_days": lookback_days,
            },
            customers_clean.select("customer_id").dropDuplicates(["customer_id"]),
        )

    def transform_redshift_data(
        self,
        spark,
        bronze_path: str,
        silver_path: str,
        table_contracts: dict[str, dict[str, Any]],
        customers_df: DataFrame,
    ):
        """Transform Redshift data from bronze to silver."""
        # Read bronze Redshift data
        behavior_df = self._read_bronze_parquet(
            spark,
            f"{bronze_path}/redshift/customer_behavior",
            "bronze.redshift.customer_behavior",
        )
        if behavior_df.rdd.isEmpty():
            raise ValueError(
                "Redshift behavior input is empty - cannot build customer_behavior_silver"
            )

        # Clean and standardize
        behavior_clean = behavior_df.select(
            self._resolve_col(behavior_df, ["customer_id"], "customer_id").alias("customer_id"),
            F.coalesce(
                self._resolve_col(behavior_df, ["session_id"], "session_id", required=False),
                self._resolve_col(behavior_df, ["behavior_id"], "behavior_id"),
            ).alias("session_id"),
            F.trim(
                F.lower(
                    F.coalesce(
                        self._resolve_col(behavior_df, ["event_type", "event_name"], "event_type"),
                        F.lit("unknown"),
                    )
                )
            ).alias("event_type"),
            F.to_timestamp(
                self._resolve_col(
                    behavior_df,
                    ["event_ts", "event_timestamp", "event_date"],
                    "event_ts",
                )
            ).alias("event_ts"),
            F.to_date(
                self._resolve_col(behavior_df, ["event_date", "event_timestamp"], "event_date")
            ).alias("event_date"),
            self._resolve_col(
                behavior_df, ["page_viewed", "page_url"], "page_viewed", required=False
            ).alias("page_viewed"),
            F.greatest(
                F.coalesce(
                    self._resolve_col(
                        behavior_df,
                        ["time_spent_seconds", "duration_seconds"],
                        "time_spent_seconds",
                    ).cast("int"),
                    F.lit(0),
                ),
                F.lit(0),
            ).alias("time_spent_seconds"),
            F.trim(
                F.lower(
                    F.coalesce(
                        self._resolve_col(
                            behavior_df, ["device_type", "referrer"], "device_type", required=False
                        ),
                        F.lit("unknown"),
                    )
                )
            ).alias("device_type"),
            F.trim(
                F.lower(
                    F.coalesce(
                        self._resolve_col(behavior_df, ["browser"], "browser", required=False),
                        F.lit("unknown"),
                    )
                )
            ).alias("browser"),
            self._resolve_col(
                behavior_df, ["conversion_value", "revenue"], "conversion_value", required=False
            )
            .cast(DoubleType())
            .alias("revenue"),
        ).withColumn(
            "event_date",
            F.coalesce(F.col("event_date"), F.to_date(F.col("event_ts"))),
        )

        null_customer_ids = behavior_clean.filter(F.col("customer_id").isNull()).count()
        if null_customer_ids > 0:
            raise ValueError(
                "customer_behavior_silver: found "
                f"{null_customer_ids} null customer_id records - cannot proceed"
            )

        null_event_ts = behavior_clean.filter(F.col("event_ts").isNull()).count()
        if null_event_ts > 0:
            if self.config.get("dq.fail_on_error", True):
                raise ValueError(
                    "customer_behavior_silver: found "
                    f"{null_event_ts} rows with invalid event timestamp"
                )
            logger.warning(
                "customer_behavior_silver: dropping %s rows with invalid event timestamp",
                null_event_ts,
            )
            behavior_clean = behavior_clean.filter(F.col("event_ts").isNotNull())

        dedupe_window = Window.partitionBy(
            "customer_id", "session_id", "event_ts", "event_type"
        ).orderBy(F.coalesce(F.col("revenue"), F.lit(0.0)).desc())
        behavior_clean = (
            behavior_clean.withColumn("_rn", F.row_number().over(dedupe_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        if self.config.is_local():
            before_count = behavior_clean.count()
            behavior_clean = behavior_clean.join(
                customers_df.select("customer_id").dropDuplicates(["customer_id"]),
                on="customer_id",
                how="inner",
            )
            dropped = before_count - behavior_clean.count()
            if dropped > 0:
                logger.warning(
                    "Local run: filtered %s orphan behavior events before contract validation",
                    dropped,
                )

        validate_contract(
            behavior_clean,
            "customer_behavior_silver",
            table_contracts.get("customer_behavior_silver", {}),
            parent_frames={
                "customers_silver": customers_df.select("customer_id").dropDuplicates(
                    ["customer_id"]
                )
            },
        )

        # Write to silver
        self._write_silver(
            spark,
            behavior_clean,
            "customer_behavior_silver",
            f"{silver_path}/customer_behavior_silver",
        )
        return {"redshift_behavior_rows": behavior_clean.count()}

    def transform_fx_data(
        self,
        spark,
        bronze_path: str,
        silver_path: str,
        table_contracts: dict[str, dict[str, Any]],
    ):
        """Transform FX data from bronze to silver."""
        # Read bronze FX data with environment-aware priority:
        # - tests/explicit path overrides -> prefer provided bronze_path input
        # - normal local pipeline -> prefer fx_json_to_bronze normalized delta reader
        cfg_paths = self.config.get("paths", {})
        configured_bronze_path = (
            cfg_paths.get("bronze_root") or cfg_paths.get("bronze") or "data/bronze"
        )
        prefer_explicit_input = str(bronze_path) != str(configured_bronze_path)

        def _read_from_explicit_path():
            fx_path = f"{bronze_path}/fx/fx_rates"
            try:
                candidate = self._read_bronze_parquet(spark, fx_path, "bronze.fx.fx_rates")
            except Exception as exc:
                logger.warning("Unable to read FX rates at %s (%s)", fx_path, exc)
                return None
            if candidate.rdd.isEmpty():
                logger.warning("FX rates input is empty at %s", fx_path)
                return None
            return candidate

        def _read_from_legacy_reader():
            from project_a.extract.fx_json_reader import read_fx_rates_from_bronze

            candidate = read_fx_rates_from_bronze(spark, self.config)
            if candidate.rdd.isEmpty():
                return None
            return candidate

        readers = (
            [_read_from_explicit_path, _read_from_legacy_reader]
            if prefer_explicit_input
            else [_read_from_legacy_reader, _read_from_explicit_path]
        )
        fx_df = None
        for reader in readers:
            fx_df = reader()
            if fx_df is not None:
                break

        if fx_df is None or fx_df.rdd.isEmpty():
            raise ValueError("FX rates input is empty - cannot build fx_rates_silver")

        # Clean and standardize
        fx_clean = fx_df.select(
            F.to_date(self._coalesce_columns(fx_df, ["trade_date", "date"], "trade_date")).alias(
                "trade_date"
            ),
            F.upper(
                F.trim(self._coalesce_columns(fx_df, ["base_ccy", "base_currency"], "base_ccy"))
            ).alias("base_ccy"),
            F.upper(
                F.trim(
                    self._coalesce_columns(
                        fx_df,
                        ["counter_ccy", "quote_ccy", "target_currency"],
                        "counter_ccy",
                    )
                )
            ).alias("counter_ccy"),
            F.coalesce(
                self._coalesce_columns(fx_df, ["rate", "exchange_rate", "mid_rate"], "rate").cast(
                    "double"
                ),
                F.lit(None).cast("double"),
            ).alias("rate"),
            self._resolve_col(fx_df, ["bid_rate"], "bid_rate", required=False)
            .cast("double")
            .alias("bid_rate"),
            self._resolve_col(fx_df, ["ask_rate"], "ask_rate", required=False)
            .cast("double")
            .alias("ask_rate"),
            self._coalesce_columns(
                fx_df, ["source", "record_source"], "source", required=False
            ).alias("source"),
            F.to_timestamp(
                self._coalesce_columns(
                    fx_df,
                    ["ingest_timestamp", "ingestion_timestamp"],
                    "ingest_timestamp",
                    required=False,
                )
            ).alias("ingest_timestamp"),
        )

        null_key_count = fx_clean.filter(
            F.col("trade_date").isNull()
            | F.col("base_ccy").isNull()
            | F.col("counter_ccy").isNull()
        ).count()
        if null_key_count > 0:
            raise ValueError(
                f"fx_rates_silver: found {null_key_count} rows with null primary key columns"
            )

        invalid_rate_count = fx_clean.filter(
            (F.col("rate").isNull()) | (F.col("rate") <= 0)
        ).count()
        if invalid_rate_count > 0:
            raise ValueError(
                f"fx_rates_silver: found {invalid_rate_count} rows with invalid non-positive rates"
            )

        dedupe_window = Window.partitionBy("trade_date", "base_ccy", "counter_ccy").orderBy(
            F.coalesce(F.col("ingest_timestamp"), F.current_timestamp()).desc(),
            F.col("rate").desc(),
        )
        fx_clean = (
            fx_clean.withColumn("_rn", F.row_number().over(dedupe_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        validate_contract(
            fx_clean,
            "fx_rates_silver",
            table_contracts.get("fx_rates_silver", {}),
        )

        # Write to silver
        self._write_silver(spark, fx_clean, "fx_rates_silver", f"{silver_path}/fx_rates_silver")
        return {"fx_rates_rows": fx_clean.count()}

    def transform_kafka_events(
        self,
        spark,
        bronze_path: str,
        silver_path: str,
        table_contracts: dict[str, dict[str, Any]],
        customers_df: DataFrame,
    ):
        """Transform Kafka events from bronze to silver."""
        # Read bronze Kafka data
        events_df = self._read_bronze_parquet(
            spark, f"{bronze_path}/kafka/events", "bronze.kafka.events"
        )
        if events_df.rdd.isEmpty():
            raise ValueError("Kafka events input is empty - cannot build order_events_silver")

        payload_schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("amount", DoubleType(), True),
                StructField("currency", StringType(), True),
                StructField("order_id", StringType(), True),
                StructField(
                    "metadata",
                    StructType(
                        [
                            StructField("source", StringType(), True),
                            StructField("version", StringType(), True),
                            StructField("session_id", StringType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )

        parsed_events = events_df.withColumn(
            "_payload",
            F.from_json(self._resolve_col(events_df, ["value"], "value"), payload_schema),
        )

        def _non_empty_string(col_expr: F.Column) -> F.Column:
            trimmed = F.trim(col_expr)
            return F.when(F.length(trimmed) > 0, trimmed)

        order_id_regex = _non_empty_string(
            F.regexp_extract(
                F.coalesce(F.col("value"), F.lit("")),
                r'order_id""\s*:\s*""([^"]+)""',
                1,
            )
        )
        customer_id_value_regex = _non_empty_string(
            F.regexp_extract(
                F.coalesce(F.col("value"), F.lit("")),
                r'customer_id""\s*:\s*""([^"]+)""',
                1,
            )
        )
        customer_id_headers_regex = _non_empty_string(
            F.regexp_extract(
                F.coalesce(F.col("headers"), F.lit("")),
                r'customer_id""\s*:\s*""([^"]+)""',
                1,
            )
        )
        event_type_regex = _non_empty_string(
            F.regexp_extract(
                F.coalesce(F.col("value"), F.lit("")),
                r'event_type""\s*:\s*""([^"]+)""',
                1,
            )
        )
        currency_regex = _non_empty_string(
            F.regexp_extract(
                F.coalesce(F.col("value"), F.lit("")),
                r'currency""\s*:\s*""([^"]+)""',
                1,
            )
        )
        amount_regex = _non_empty_string(
            F.regexp_extract(
                F.coalesce(F.col("value"), F.lit("")),
                r'amount""\s*:\s*([0-9]+(?:\\.[0-9]+)?)',
                1,
            )
        )

        # Clean and standardize
        events_clean = parsed_events.select(
            F.coalesce(
                self._resolve_col(parsed_events, ["event_id", "key"], "event_id"),
                F.lit(None).cast("string"),
            ).alias("event_id"),
            F.coalesce(F.col("_payload.order_id"), order_id_regex).alias("order_id"),
            F.coalesce(
                F.col("_payload.customer_id"),
                customer_id_value_regex,
                customer_id_headers_regex,
            ).alias("customer_id"),
            F.coalesce(
                F.trim(F.lower(F.col("_payload.event_type"))),
                F.lower(event_type_regex),
                F.lit("unknown"),
            ).alias("event_type"),
            F.to_timestamp(self._resolve_col(parsed_events, ["timestamp"], "timestamp")).alias(
                "event_ts"
            ),
            F.coalesce(
                F.col("_payload.amount").cast("double"), amount_regex.cast("double"), F.lit(0.0)
            ).alias("amount"),
            F.coalesce(
                F.upper(F.trim(F.col("_payload.currency"))), F.upper(currency_regex), F.lit("USD")
            ).alias("currency"),
            F.coalesce(F.col("_payload.metadata.source"), F.col("topic"), F.lit("kafka")).alias(
                "channel"
            ),
            F.col("_payload.metadata.session_id").alias("session_id"),
            F.col("topic").alias("topic"),
            F.col("partition").cast("int").alias("partition"),
            F.col("offset").cast("long").alias("offset"),
        ).withColumn("event_date", F.to_date(F.col("event_ts")))

        null_event_ids = events_clean.filter(F.col("event_id").isNull()).count()
        if null_event_ids > 0:
            raise ValueError(
                f"order_events_silver: found {null_event_ids} null event IDs - cannot proceed"
            )

        null_customer_ids = events_clean.filter(F.col("customer_id").isNull()).count()
        if null_customer_ids > 0:
            raise ValueError(
                f"order_events_silver: found {null_customer_ids} null customer IDs - cannot proceed"
            )

        null_timestamps = events_clean.filter(F.col("event_ts").isNull()).count()
        if null_timestamps > 0:
            if self.config.get("dq.fail_on_error", True):
                raise ValueError(
                    f"order_events_silver: found {null_timestamps} rows with invalid event_ts"
                )
            logger.warning(
                "order_events_silver: dropping %s rows with invalid event_ts", null_timestamps
            )
            events_clean = events_clean.filter(F.col("event_ts").isNotNull())

        invalid_amounts = events_clean.filter(F.col("amount") < 0).count()
        if invalid_amounts > 0:
            raise ValueError(
                f"order_events_silver: found {invalid_amounts} rows with negative amount"
            )

        dedupe_window = Window.partitionBy("event_id").orderBy(
            F.col("event_ts").desc_nulls_last(),
            F.col("offset").desc_nulls_last(),
        )
        events_clean = (
            events_clean.withColumn("_rn", F.row_number().over(dedupe_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        if self.config.is_local():
            before_count = events_clean.count()
            events_clean = events_clean.join(
                customers_df.select("customer_id").dropDuplicates(["customer_id"]),
                on="customer_id",
                how="inner",
            )
            dropped = before_count - events_clean.count()
            if dropped > 0:
                logger.warning(
                    "Local run: filtered %s orphan kafka events before contract validation",
                    dropped,
                )

        validate_contract(
            events_clean,
            "order_events_silver",
            table_contracts.get("order_events_silver", {}),
            parent_frames={
                "customers_silver": customers_df.select("customer_id").dropDuplicates(
                    ["customer_id"]
                )
            },
        )

        # Write to silver
        self._write_silver(
            spark,
            events_clean,
            "order_events_silver",
            f"{silver_path}/order_events_silver",
        )
        return {"kafka_events_rows": events_clean.count()}
