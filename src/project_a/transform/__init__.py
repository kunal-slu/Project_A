"""Compatibility exports for transformation helpers and entrypoints."""

from project_a.transform.examples import (
    broadcast_join_demo,
    build_customers_scd2,
    build_fact_orders,
    data_cleaning_examples,
    enrich_products,
    join_examples,
    join_inventory,
    join_returns,
    normalize_currency,
    partitioning_examples,
    select_and_filter,
    skew_mitigation_demo,
    sql_vs_dsl_demo,
    udf_examples,
    window_functions_demo,
)


def bronze_to_silver_complete(args):
    """Compatibility wrapper to local bronze -> silver job."""
    from project_a.jobs import bronze_to_silver

    return bronze_to_silver.main(args)


def silver_to_gold_complete(args):
    """Compatibility wrapper to local silver -> gold job."""
    from project_a.jobs import silver_to_gold

    return silver_to_gold.main(args)


__all__ = [
    "bronze_to_silver_complete",
    "silver_to_gold_complete",
    "select_and_filter",
    "join_examples",
    "broadcast_join_demo",
    "skew_mitigation_demo",
    "partitioning_examples",
    "window_functions_demo",
    "udf_examples",
    "data_cleaning_examples",
    "sql_vs_dsl_demo",
    "normalize_currency",
    "join_returns",
    "join_inventory",
    "enrich_products",
    "build_fact_orders",
    "build_customers_scd2",
]
