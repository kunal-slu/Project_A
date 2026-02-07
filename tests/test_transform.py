import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from pyspark.sql import Row

from project_a.transform import (broadcast_join_demo,
                                                 build_customers_scd2,
                                                 build_fact_orders,
                                                 data_cleaning_examples,
                                                 enrich_products,
                                                 join_examples, join_inventory,
                                                 join_returns,
                                                 normalize_currency,
                                                 partitioning_examples,
                                                 select_and_filter,
                                                 skew_mitigation_demo,
                                                 sql_vs_dsl_demo, udf_examples,
                                                 window_functions_demo)


def test_select_and_filter(spark):
    rows = [
        Row(
            customer_id="C1",
            first_name="A",
            state="TX",
            registration_date="01/01/21",
            email=None,
        ),
        Row(
            customer_id="C2",
            first_name="B",
            state="CA",
            registration_date="02/02/21",
            email="b@example.com",
        ),
    ]
    df = spark.createDataFrame(rows)
    out = select_and_filter(df)
    assert out.count() == 1
    result = out.collect()[0]
    assert result.customer_id == "C2"
    assert result.first_name == "B"
    assert result.state == "CA"


def test_join_examples(spark):
    customers = spark.createDataFrame(
        [Row(customer_id="C1", first_name="A", last_name="Z", email="a@e.com", age=30)]
    )
    products = spark.createDataFrame(
        [Row(product_id="P1", product_name="X", category="Cat", brand="B", price=10.0)]
    )
    orders = spark.createDataFrame(
        [
            Row(
                order_id="O1",
                customer_id="C1",
                product_id="P1",
                quantity=2,
                total_amount=20.0,
                order_date="2021-01-01",
                shipping_address="Addr",
                shipping_city="City",
                shipping_state="ST",
                shipping_country="Country",
            )
        ]
    )
    out = join_examples(customers, products, orders)
    row = out.collect()[0]
    assert row.first_name == "A"
    assert row.product_name == "X"
    assert row.quantity == 2


def test_broadcast_join_demo(spark):
    customers = spark.createDataFrame([Row(customer_id="1")])
    products = spark.createDataFrame([Row(product_id="1")])
    out = broadcast_join_demo(customers, products)
    assert out.count() == 1


def test_skew_mitigation_demo(spark):
    customers = spark.createDataFrame([Row(customer_id="11"), Row(customer_id="22")])
    out = skew_mitigation_demo(customers)
    assert "salt" in out.columns
    salts = [r.salt for r in out.collect()]
    assert all(isinstance(s, int) for s in salts)


def test_partitioning_examples(spark):
    df = spark.createDataFrame([Row(customer_id="C1"), Row(customer_id="C2")])
    out = partitioning_examples(df)
    assert out.rdd.getNumPartitions() <= 2


def test_window_functions_demo(spark):
    df = spark.createDataFrame(
        [
            Row(customer_id="C1", order_date="2021-01-01", quantity=5),
            Row(customer_id="C1", order_date="2021-01-02", quantity=7),
        ]
    )
    out = window_functions_demo(df)
    vals = [r.running_total for r in out.orderBy("order_date").collect()]
    assert vals == [5, 12]


def test_udf_and_cleaning(spark):
    df = spark.createDataFrame(
        [
            Row(email="user@test.com", age=25, product_name="Name!@#"),
            Row(email=None, age=30, product_name="Test@@"),
        ]
    )
    udf_out = udf_examples(df)
    row = udf_out.filter(udf_out.email.isNotNull()).collect()[0]
    assert row.email_domain_py == "test.com"
    assert row.age_bucket == 2
    clean_out = data_cleaning_examples(df)
    assert clean_out.filter(clean_out.email.isNull()).count() == 0
    assert "product_name_clean" in clean_out.columns


def test_sql_vs_dsl_demo(spark):
    df = spark.createDataFrame(
        [
            Row(customer_id="C1", quantity=6),
            Row(customer_id="C1", quantity=7),
            Row(customer_id="C2", quantity=3),
        ]
    )
    sql_res, dsl_res = sql_vs_dsl_demo(spark, df)
    expected = {r.customer_id for r in sql_res.collect()}
    assert expected == {"C1"}


def test_normalize_currency_and_returns(spark):
    orders = spark.createDataFrame(
        [
            Row(
                order_id="O1",
                customer_id="C1",
                product_id="P1",
                order_date="2021-01-01T00:00:00",
                quantity=2,
                total_amount=100.0,
                currency="EUR",
            ),
            Row(
                order_id="O2",
                customer_id="C2",
                product_id="P2",
                order_date="2021-01-02T00:00:00",
                quantity=1,
                total_amount=50.0,
                currency="USD",
            ),
        ]
    )
    rates = spark.createDataFrame(
        [Row(currency="USD", usd_rate=1.0), Row(currency="EUR", usd_rate=1.1)]
    )
    returns = spark.createDataFrame([Row(order_id="O1")])
    out = normalize_currency(orders, rates)
    out = join_returns(out, returns)
    m = {r.order_id: (r.total_amount_usd, r.is_returned) for r in out.collect()}
    assert pytest.approx(m["O1"][0], 0.01) == 110.0 and m["O1"][1] is True
    assert pytest.approx(m["O2"][0], 0.01) == 50.0 and m["O2"][1] is False


def test_join_inventory_and_enrich(spark):
    products = spark.createDataFrame(
        [
            Row(
                product_id="P1",
                product_name="X",
                category="Electronics",
                brand="B",
                price=120.0,
                tags="new,promo",
            )
        ]
    )
    inv = spark.createDataFrame([Row(product_id="P1", on_hand=10, warehouse="W1")])
    out = join_inventory(enrich_products(products), inv)
    r = out.collect()[0]
    assert r.on_hand == 10 and r.price_band == "mid" and isinstance(r.tags_array, list)


def test_build_fact_orders(spark):
    df = spark.createDataFrame(
        [
            Row(
                customer_id="C1",
                product_id="P1",
                order_id="O1",
                order_date="2022-03-15T12:00:00",
                price=10.0,
                quantity=3,
            )
        ]
    )
    fact = build_fact_orders(df)
    r = fact.collect()[0]
    assert r.revenue == 30.0 and r.order_ym == "202203"


def test_build_customers_scd2(spark):
    customers = spark.createDataFrame(
        [
            Row(
                customer_id="C1",
                first_name="A",
                last_name="B",
                email="a@b.com",
                address="addr1",
                city="x",
                state="CA",
                country="USA",
                zip="1",
                phone="p",
                registration_date="01/01/21",
                gender="M",
                age=30,
            )
        ]
    )
    changes = spark.createDataFrame(
        [Row(customer_id="C1", new_address="addr2", effective_from="2022-01-01")]
    )
    scd2 = build_customers_scd2(customers, changes)
    assert set(["effective_from", "effective_to", "is_current"]).issubset(
        set(scd2.columns)
    )
    assert scd2.filter(scd2.is_current == True).count() == 1
