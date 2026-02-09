#!/usr/bin/env python3
# ruff: noqa: E402
"""
Comprehensive Data Quality Validation Job

Runs all DQ checks:
1. Schema drift
2. Referential integrity
3. Primary key uniqueness
4. Null analysis
5. Timestamp validation
6. Semantic validation
7. Distribution profiling
8. Incremental ETL readiness
9. Kafka streaming fitness
10. Performance optimization
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

# Add src to path
project_root = Path(__file__).parent.parent.parent
src_path = project_root / "src"
if src_path.exists() and str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from pyspark.sql import SparkSession

from project_a.config_loader import load_config_resolved
from project_a.dq.comprehensive_validator import ComprehensiveValidator
from project_a.schemas.bronze_schemas import (
    CRM_ACCOUNTS_SCHEMA,
    CRM_CONTACTS_SCHEMA,
    CRM_OPPORTUNITIES_SCHEMA,
    FX_RATES_BRONZE_SCHEMA,
    KAFKA_EVENTS_SCHEMA,
    REDSHIFT_BEHAVIOR_SCHEMA,
    SNOWFLAKE_CUSTOMERS_SCHEMA,
    SNOWFLAKE_ORDERS_SCHEMA,
    SNOWFLAKE_PRODUCTS_SCHEMA,
)
from project_a.utils.path_resolver import resolve_data_path
from project_a.utils.spark_session import build_spark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _is_strict_mode(config: dict[str, Any]) -> bool:
    dq_cfg = config.get("dq", {})
    return (
        dq_cfg.get("mode") == "strict"
        or dq_cfg.get("strict_mode", False)
        or dq_cfg.get("fail_on_error", False)
    )


def _layer_format(config: dict[str, Any], layer: str) -> str:
    if layer == "bronze":
        return "parquet"
    storage_fmt = (config.get("storage", {}) or {}).get("format")
    if storage_fmt:
        return str(storage_fmt).lower()
    iceberg_cfg = config.get("iceberg", {})
    if iceberg_cfg.get("enabled") and layer in ("silver", "gold"):
        return "iceberg"
    return "parquet"


def _read_table(
    spark: SparkSession,
    fmt: str,
    path: str,
    table_name: str | None = None,
    catalog_name: str | None = None,
    schema: Any | None = None,
):
    if fmt == "iceberg":
        if not table_name or not catalog_name:
            raise ValueError("Iceberg reads require catalog_name and table_name")
        return spark.read.format("iceberg").load(f"{catalog_name}.{table_name}")
    if fmt == "delta":
        return spark.read.format("delta").load(path)

    # Explicit CSV path
    if str(path).lower().endswith(".csv"):
        reader = spark.read.option("header", "true")
        if schema is not None:
            reader = reader.schema(schema)
        return reader.csv(path)

    # Default: try parquet first, then CSV fallback
    try:
        return spark.read.parquet(path)
    except Exception:
        reader = spark.read.option("header", "true")
        if schema is not None:
            reader = reader.schema(schema)
        return reader.csv(path)


def load_bronze_tables(spark: SparkSession, config: dict[str, Any]) -> dict[str, Any]:
    """Load all bronze tables."""
    bronze_root = resolve_data_path(config, "bronze")
    sources = config.get("sources", {})

    bronze_data = {}
    missing: list[str] = []
    fmt = _layer_format(config, "bronze")

    def _read_bronze_output(path: str) -> Any:
        # Attempt to read from Bronze output folder first (parquet/delta/iceberg).
        try:
            return spark.read.parquet(path)
        except Exception:
            try:
                return spark.read.format("delta").load(path)
            except Exception:
                return None

    def _read_csv_source(path: str, schema):
        return spark.read.option("header", "true").schema(schema).csv(path)

    # Load CRM
    try:
        crm_cfg = sources.get("crm", {})
        crm_base = crm_cfg.get("base_path", f"{bronze_root}/crm")
        crm_files = crm_cfg.get("files", {})
        accounts = _read_bronze_output(f"{bronze_root}/crm/accounts")
        if accounts is None:
            accounts = _read_csv_source(
                f"{crm_base}/{crm_files.get('accounts', 'accounts.csv')}", CRM_ACCOUNTS_SCHEMA
            )
        bronze_data["accounts"] = accounts

        contacts = _read_bronze_output(f"{bronze_root}/crm/contacts")
        if contacts is None:
            contacts = _read_csv_source(
                f"{crm_base}/{crm_files.get('contacts', 'contacts.csv')}", CRM_CONTACTS_SCHEMA
            )
        bronze_data["contacts"] = contacts

        opportunities = _read_bronze_output(f"{bronze_root}/crm/opportunities")
        if opportunities is None:
            opportunities = _read_csv_source(
                f"{crm_base}/{crm_files.get('opportunities', 'opportunities.csv')}",
                CRM_OPPORTUNITIES_SCHEMA,
            )
        bronze_data["opportunities"] = opportunities
    except Exception as e:
        missing.extend(["crm.accounts", "crm.contacts", "crm.opportunities"])
        logger.error(f"Could not load CRM bronze: {e}")

    # Load Snowflake
    try:
        snowflake_cfg = sources.get("snowflake", {})
        snowflake_base = snowflake_cfg.get("base_path", f"{bronze_root}/snowflake")
        snowflake_files = snowflake_cfg.get("files", {})
        customers = _read_bronze_output(f"{bronze_root}/snowflake/customers")
        if customers is None:
            customers = _read_csv_source(
                f"{snowflake_base}/{snowflake_files.get('customers', 'customers.csv')}",
                SNOWFLAKE_CUSTOMERS_SCHEMA,
            )
        bronze_data["customers"] = customers

        orders = _read_bronze_output(f"{bronze_root}/snowflake/orders")
        if orders is None:
            orders = _read_csv_source(
                f"{snowflake_base}/{snowflake_files.get('orders', 'orders.csv')}",
                SNOWFLAKE_ORDERS_SCHEMA,
            )
        bronze_data["orders"] = orders

        products = _read_bronze_output(f"{bronze_root}/snowflake/products")
        if products is None:
            products = _read_csv_source(
                f"{snowflake_base}/{snowflake_files.get('products', 'products.csv')}",
                SNOWFLAKE_PRODUCTS_SCHEMA,
            )
        bronze_data["products"] = products
    except Exception as e:
        missing.extend(["snowflake.customers", "snowflake.orders", "snowflake.products"])
        logger.error(f"Could not load Snowflake bronze: {e}")

    # Load Redshift
    try:
        redshift_cfg = sources.get("redshift", {})
        redshift_base = redshift_cfg.get("base_path", f"{bronze_root}/redshift")
        redshift_files = redshift_cfg.get("files", {})
        behavior = _read_bronze_output(f"{bronze_root}/redshift/customer_behavior")
        if behavior is None:
            behavior = _read_csv_source(
                f"{redshift_base}/{redshift_files.get('behavior', 'redshift_customer_behavior_50000.csv')}",
                REDSHIFT_BEHAVIOR_SCHEMA,
            )
        bronze_data["behavior"] = behavior
    except Exception as e:
        missing.append("redshift.behavior")
        logger.error(f"Could not load Redshift bronze: {e}")

    # Load Kafka
    try:
        kafka_cfg = sources.get("kafka_sim", {})
        kafka_base = kafka_cfg.get("base_path", f"{bronze_root}/kafka")
        kafka_files = kafka_cfg.get("files", {})
        events = _read_bronze_output(f"{bronze_root}/kafka/events")
        if events is None:
            kafka_path = f"{kafka_base}/{kafka_files.get('orders_seed', 'stream_kafka_events_100000.csv')}"
            events = _read_csv_source(kafka_path, KAFKA_EVENTS_SCHEMA)
        bronze_data["kafka_events"] = events
    except Exception as e:
        missing.append("kafka.events")
        logger.error(f"Could not load Kafka bronze: {e}")

    # Load FX
    try:
        from project_a.extract.fx_json_reader import read_fx_rates_from_bronze

        # Prefer bronze output path (delta/parquet) if present
        fx_output = _read_bronze_output(f"{bronze_root}/fx")
        if fx_output is None:
            fx_output = read_fx_rates_from_bronze(spark, config)
        bronze_data["fx_rates"] = fx_output
    except Exception as e:
        missing.append("fx.rates")
        logger.error(f"Could not load FX bronze: {e}")

    if missing and _is_strict_mode(config):
        raise RuntimeError(f"Missing required bronze datasets: {', '.join(sorted(set(missing)))}")

    return bronze_data


def load_silver_tables(spark: SparkSession, config: dict[str, Any]) -> dict[str, Any]:
    """Load all silver tables."""
    silver_root = resolve_data_path(config, "silver")
    tables_config = config.get("tables", {}).get("silver", {})
    fmt = _layer_format(config, "silver")
    catalog_name = config.get("iceberg", {}).get("catalog_name", "local")

    silver_data = {}
    missing: list[str] = []
    table_names = {
        "customers": tables_config.get("customers", "customers_silver"),
        "orders": tables_config.get("orders", "orders_silver"),
        "products": tables_config.get("products", "products_silver"),
        "behavior": tables_config.get("behavior", "customer_behavior_silver"),
        "fx_rates": tables_config.get("fx_rates", "fx_rates_silver"),
        "order_events": tables_config.get("order_events", "order_events_silver"),
    }

    for key, table_name in table_names.items():
        try:
            silver_data[key] = _read_table(
                spark, fmt, f"{silver_root}/{table_name}", table_name=table_name, catalog_name=catalog_name
            )
        except Exception as e:
            missing.append(f"silver.{table_name}")
            logger.error(f"Could not load {table_name}: {e}")

    if missing and _is_strict_mode(config):
        raise RuntimeError(f"Missing required silver datasets: {', '.join(sorted(set(missing)))}")

    return silver_data


def load_gold_tables(spark: SparkSession, config: dict[str, Any]) -> dict[str, Any]:
    """Load all gold tables."""
    gold_root = resolve_data_path(config, "gold")
    tables_config = config.get("tables", {}).get("gold", {})
    fmt = _layer_format(config, "gold")
    catalog_name = config.get("iceberg", {}).get("catalog_name", "local")

    gold_data = {}
    missing: list[str] = []
    table_names = {
        "fact_orders": tables_config.get("fact_orders", "fact_orders"),
        "dim_customer": tables_config.get("dim_customer", "dim_customer"),
        "dim_product": tables_config.get("dim_product", "dim_product"),
        "customer_360": tables_config.get("fact_customer_24m", "customer_360"),
    }

    for key, table_name in table_names.items():
        try:
            gold_data[key] = _read_table(
                spark, fmt, f"{gold_root}/{table_name}", table_name=table_name, catalog_name=catalog_name
            )
        except Exception as e:
            missing.append(f"gold.{table_name}")
            logger.error(f"Could not load {table_name}: {e}")

    if missing and _is_strict_mode(config):
        raise RuntimeError(f"Missing required gold datasets: {', '.join(sorted(set(missing)))}")

    return gold_data


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Comprehensive Data Quality Validation")
    parser.add_argument("--env", default="dev", help="Environment")
    parser.add_argument("--config", help="Config file path")
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="Layer to validate",
    )
    parser.add_argument("--output", help="Output path for report")
    args = parser.parse_args()

    # Load config
    if args.config:
        config = load_config_resolved(args.config)
    else:
        config_path = Path(f"config/{args.env}.yaml")
        if not config_path.exists():
            config_path = Path("config/local.yaml")
        config = load_config_resolved(str(config_path))

    # Build Spark
    spark = build_spark(app_name="project_a_dq", config=config)

    try:
        dq_cfg = config.get("dq", {})
        validator = ComprehensiveValidator(spark, dq_config=dq_cfg)

        # Load expected schemas
        expected_schemas = {
            "accounts": CRM_ACCOUNTS_SCHEMA,
            "contacts": CRM_CONTACTS_SCHEMA,
            "opportunities": CRM_OPPORTUNITIES_SCHEMA,
            "behavior": REDSHIFT_BEHAVIOR_SCHEMA,
            "customers": SNOWFLAKE_CUSTOMERS_SCHEMA,
            "orders": SNOWFLAKE_ORDERS_SCHEMA,
            "products": SNOWFLAKE_PRODUCTS_SCHEMA,
            "kafka_events": KAFKA_EVENTS_SCHEMA,
            "fx_rates": FX_RATES_BRONZE_SCHEMA,
        }

        # Validate based on layer
        if args.layer in ["bronze", "all"]:
            logger.info("Loading bronze tables...")
            bronze_data = load_bronze_tables(spark, config)
            validator.validate_bronze_layer(bronze_data, expected_schemas)

        if args.layer in ["silver", "all"]:
            logger.info("Loading silver tables...")
            silver_data = load_silver_tables(spark, config)
            bronze_data = (
                load_bronze_tables(spark, config) if args.layer == "silver" else bronze_data
            )
            validator.validate_silver_layer(silver_data, bronze_data)

        if args.layer in ["gold", "all"]:
            logger.info("Loading gold tables...")
            gold_data = load_gold_tables(spark, config)
            silver_data = load_silver_tables(spark, config) if args.layer == "gold" else silver_data
            validator.validate_gold_layer(gold_data, silver_data)

        # Generate report
        report = validator.generate_comprehensive_report()
        summary = validator.get_summary()

        logger.info("\n" + report)
        logger.info(f"\nSummary: {summary}")

        # Write report to file if output specified
        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(report, encoding="utf-8")
            logger.info(f"Report written to {args.output}")

        # Exit with error code if any layer failed
        if summary["layers_failed"] > 0:
            sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
