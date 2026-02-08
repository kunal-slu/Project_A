"""
Project A Extract Modules

Data extraction utilities for various sources.
"""

from .fx_json_reader import read_fx_json


def extract_customers(spark, path: str):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


def extract_products(spark, path: str):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


def extract_orders_json(spark, path: str):
    return spark.read.option("multiLine", True).json(path)


def extract_returns(spark, path: str):
    return spark.read.option("multiLine", True).json(path)


def extract_exchange_rates(spark, path: str):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


def extract_inventory_snapshots(spark, path: str):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


__all__ = [
    "read_fx_json",
    "extract_customers",
    "extract_products",
    "extract_orders_json",
    "extract_returns",
    "extract_exchange_rates",
    "extract_inventory_snapshots",
]
