"""Tests for multisource BronzeToSilverJob transformations."""

from __future__ import annotations

import types

import pytest

from jobs.transform.bronze_to_silver import BronzeToSilverJob
from project_a.contracts.runtime_contracts import load_table_contracts
from project_a.core.config import ProjectConfig


@pytest.fixture
def bronze_to_silver_job() -> BronzeToSilverJob:
    config = ProjectConfig("local/config/local.yaml", env="local")
    return BronzeToSilverJob(config)


@pytest.fixture
def silver_contracts() -> dict:
    return load_table_contracts("config/contracts/silver_contracts.yaml")


def _capture_writes(job: BronzeToSilverJob) -> dict:
    writes: dict = {}

    def _fake_write(self, spark, df, table_name: str, path: str) -> None:
        writes[table_name] = df

    job._write_silver = types.MethodType(_fake_write, job)  # type: ignore[method-assign]
    return writes


def test_transform_kafka_events_parses_payload_and_dedupes(
    spark, tmp_path, bronze_to_silver_job, silver_contracts
):
    bronze_path = tmp_path / "bronze" / "kafka" / "events"
    bronze_path.mkdir(parents=True, exist_ok=True)

    events = [
        (
            "E1",
            "orders",
            0,
            100,
            "2025-01-01 10:00:00",
            "K1",
            '{"customer_id":"C1","event_type":"ORDER_CREATED","amount":10.0,"currency":"usd","metadata":{"source":"mobile_app","session_id":"S1"}}',
            "{}",
        ),
        (
            "E1",
            "orders",
            0,
            101,
            "2025-01-01 10:05:00",
            "K1",
            '{"customer_id":"C1","event_type":"order_created","amount":11.5,"currency":"usd","metadata":{"source":"mobile_app","session_id":"S1"}}',
            "{}",
        ),
        (
            "E2",
            "orders",
            1,
            1,
            "2025-01-02 12:00:00",
            "K2",
            '{"customer_id":"C2","event_type":"product_viewed","amount":1.0,"currency":"eur","metadata":{"source":"web","session_id":"S2"}}',
            "{}",
        ),
    ]
    spark.createDataFrame(
        events,
        "event_id string, topic string, partition int, offset long, timestamp string, key string, value string, headers string",
    ).write.mode("overwrite").parquet(str(bronze_path))

    customers_df = spark.createDataFrame([("C1",), ("C2",)], "customer_id string")
    writes = _capture_writes(bronze_to_silver_job)

    bronze_to_silver_job.transform_kafka_events(
        spark,
        str(tmp_path / "bronze"),
        str(tmp_path / "silver"),
        silver_contracts,
        customers_df,
    )

    out = writes["order_events_silver"]
    assert out.count() == 2
    assert out.filter("event_id = 'E1'").select("amount").collect()[0][0] == 11.5
    assert out.filter("event_id = 'E1'").select("channel").collect()[0][0] == "mobile_app"
    assert out.filter("event_id = 'E2'").select("currency").collect()[0][0] == "EUR"


def test_transform_fx_data_dedupes_latest_rate(
    spark, tmp_path, bronze_to_silver_job, silver_contracts
):
    bronze_path = tmp_path / "bronze" / "fx" / "fx_rates"
    bronze_path.mkdir(parents=True, exist_ok=True)

    fx_rows = [
        ("2025-01-01", "USD", "EUR", 0.90, "2025-01-01 01:00:00", "feed_a"),
        ("2025-01-01", "USD", "EUR", 0.95, "2025-01-01 02:00:00", "feed_b"),
        ("2025-01-01", "USD", "GBP", 0.80, "2025-01-01 01:00:00", "feed_a"),
    ]
    spark.createDataFrame(
        fx_rows,
        "date string, base_ccy string, quote_ccy string, rate double, ingest_timestamp string, source string",
    ).write.mode("overwrite").parquet(str(bronze_path))

    writes = _capture_writes(bronze_to_silver_job)
    bronze_to_silver_job.transform_fx_data(
        spark,
        str(tmp_path / "bronze"),
        str(tmp_path / "silver"),
        silver_contracts,
    )

    out = writes["fx_rates_silver"]
    assert out.count() == 2
    assert out.filter("counter_ccy = 'EUR'").select("rate").collect()[0][0] == 0.95


def test_transform_fx_data_fails_on_invalid_rate(
    spark, tmp_path, bronze_to_silver_job, silver_contracts
):
    bronze_path = tmp_path / "bronze" / "fx" / "fx_rates"
    bronze_path.mkdir(parents=True, exist_ok=True)

    spark.createDataFrame(
        [("2025-01-01", "USD", "EUR", -1.0)],
        "date string, base_ccy string, quote_ccy string, rate double",
    ).write.mode("overwrite").parquet(str(bronze_path))

    with pytest.raises(ValueError, match="invalid non-positive rates"):
        bronze_to_silver_job.transform_fx_data(
            spark,
            str(tmp_path / "bronze"),
            str(tmp_path / "silver"),
            silver_contracts,
        )


def test_transform_redshift_filters_local_orphans(
    spark, tmp_path, bronze_to_silver_job, silver_contracts
):
    bronze_path = tmp_path / "bronze" / "redshift" / "customer_behavior"
    bronze_path.mkdir(parents=True, exist_ok=True)

    behavior_rows = [
        (
            "B1",
            "C1",
            "Page_View",
            "2025-01-01 10:00:00",
            "S1",
            "/home",
            "Desktop",
            "Chrome",
            -5,
            10.0,
        ),
        (
            "B2",
            "C404",
            "Purchase",
            "2025-01-01 11:00:00",
            "S2",
            "/buy",
            "Mobile",
            "Safari",
            20,
            20.0,
        ),
    ]
    spark.createDataFrame(
        behavior_rows,
        "behavior_id string, customer_id string, event_name string, event_timestamp string, session_id string, page_url string, device_type string, browser string, duration_seconds int, conversion_value double",
    ).write.mode("overwrite").parquet(str(bronze_path))

    customers_df = spark.createDataFrame([("C1",)], "customer_id string")
    writes = _capture_writes(bronze_to_silver_job)
    bronze_to_silver_job.transform_redshift_data(
        spark,
        str(tmp_path / "bronze"),
        str(tmp_path / "silver"),
        silver_contracts,
        customers_df,
    )

    out = writes["customer_behavior_silver"]
    assert out.count() == 1
    row = out.collect()[0]
    assert row["customer_id"] == "C1"
    assert row["event_type"] == "page_view"
    assert row["time_spent_seconds"] == 0


def test_read_bronze_parquet_handles_mixed_nested_layout(spark, tmp_path, bronze_to_silver_job):
    orders_dir = tmp_path / "bronze" / "snowflake" / "orders"
    orders_dir.mkdir(parents=True, exist_ok=True)

    spark.createDataFrame(
        [("O1", "C1"), ("O2", "C2")],
        "order_id string, customer_id string",
    ).write.mode("overwrite").parquet(str(orders_dir))

    mixed_csv_dir = orders_dir / "daily" / "date=2026-01-01"
    mixed_csv_dir.mkdir(parents=True, exist_ok=True)
    (mixed_csv_dir / "orders_2026-01-01.csv").write_text(
        "order_id,customer_id\nO3,C3\n",
        encoding="utf-8",
    )

    read_df = bronze_to_silver_job._read_bronze_parquet(spark, str(orders_dir), "test.orders")
    assert read_df.count() == 2
