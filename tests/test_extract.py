import os
import sys


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from pyspark_interview_project import extract


def test_extract_customers(spark):
    path = "tests/data/customers.csv"
    df = extract.extract_customers(spark, path)
    assert "customer_id" in df.columns
    assert df.count() > 0


def test_extract_products(spark):
    path = "tests/data/products.csv"
    df = extract.extract_products(spark, path)
    assert "product_id" in df.columns
    assert "price" in df.columns
    # Comment out below if your ETL does not have these columns!
    # assert "in_stock" in df.columns
    # assert df.filter(df.price.isNull()).count() == 0
    # assert df.filter(df.in_stock.isNull()).count() == 0


def test_extract_orders_json(spark):
    path = "tests/data/orders.json"
    df = extract.extract_orders_json(spark, path)
    assert "order_id" in df.columns
    assert "quantity" in df.columns
    # Comment out below if your ETL does not have these columns!
    # assert df.filter(df.quantity.isNull()).count() == 0
    # assert df.filter(df.total_amount.isNull()).count() == 0
    # assert "payment_method" in df.columns
    # assert "payment_status" in df.columns


def test_extract_returns(spark):
    p = "data/input_data/returns.json"
    df = extract.extract_returns(spark, p)
    assert set(["order_id", "return_date", "reason"]).issubset(set(df.columns))


def test_extract_exchange_rates(spark):
    p = "data/input_data/exchange_rates.csv"
    df = extract.extract_exchange_rates(spark, p)
    assert set(["currency", "usd_rate"]).issubset(set(df.columns))
    assert df.filter(df.currency == "USD").count() == 1


def test_extract_inventory_snapshots(spark):
    p = "data/input_data/inventory_snapshots.csv"
    df = extract.extract_inventory_snapshots(spark, p)
    assert set(["product_id", "on_hand", "warehouse"]).issubset(set(df.columns))
