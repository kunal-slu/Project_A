import os
import pathlib

import pytest
from pyspark.sql import SparkSession

# Ensure PySpark uses the pip distribution and pre-loads Avro package only
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--packages org.apache.spark:spark-avro_2.12:3.4.2 pyspark-shell",
)
os.environ.pop("SPARK_HOME", None)


def _with_avro(builder: SparkSession.Builder) -> SparkSession.Builder:
    # Use both local JAR and packages to ensure availability
    project_root = pathlib.Path(__file__).resolve().parents[1]
    avro_jar = project_root / "jars" / "spark-avro_2.12-3.4.2.jar"
    cfg = builder.config(
        "spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.4.2"
    )
    if avro_jar.exists():
        jar_path = str(avro_jar)
        cfg = (
            cfg.config("spark.jars", jar_path)
            .config("spark.driver.extraClassPath", jar_path)
            .config("spark.executor.extraClassPath", jar_path)
        )
    return cfg


def _ensure_test_data():
    data_dir = pathlib.Path(__file__).parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    customers = data_dir / "customers.csv"
    products = data_dir / "products.csv"
    orders = data_dir / "orders.json"

    if not customers.exists():
        customers.write_text(
            """customer_id,first_name,last_name,email,address,city,state,country,zip,phone,registration_date,gender,age
C1,John,Doe,john@example.com,123 St,Austin,TX,USA,78701,111-222,01/01/21,M,30
"""
        )
    if not products.exists():
        products.write_text(
            """product_id,product_name,category,brand,price,in_stock,launch_date,rating,tags
P1,Widget,Cat,Brand,9.99,true,01/01/21,4.5,hot
"""
        )
    if not orders.exists():
        orders.write_text(
            """
[
  {"order_id":"O1","customer_id":"C1","product_id":"P1","order_date":"2021-01-01","quantity":2,"total_amount":19.98,
    "shipping_address":"123 St","shipping_city":"Austin","shipping_state":"TX","shipping_country":"USA",
    "payment":{"method":"card","status":"paid"}}
]
"""
        )


@pytest.fixture(scope="session", autouse=True)
def _pyspark_env_and_testdata():
    _ensure_test_data()


@pytest.fixture(scope="session")
def spark():
    # Ensure required packages are provided on submit
    os.environ.setdefault(
        "PYSPARK_SUBMIT_ARGS",
        "--packages io.delta:delta-spark_2.12:2.4.0,org.apache.spark:spark-avro_2.12:3.4.2 pyspark-shell",
    )
    # Force using PyPI PySpark distribution instead of a system Spark install
    os.environ.pop("SPARK_HOME", None)

    # Ensure no stale Spark (both Session and Context) persists without the required packages
    try:
        from pyspark import SparkContext

        # Stop active SparkSession
        active = SparkSession.getActiveSession()
        if active is not None:
            active.stop()
        # Stop active SparkContext if any (avoids reusing JVM without packages)
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()
    except Exception:
        pass

    builder = SparkSession.builder.master("local[1]").appName("pytest-pyspark")
    # Avro via packages (Delta optionally configured via helper below)
    builder = builder.config(
        "spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.4.2"
    )
    builder = _with_avro(builder)

    # Allow tests to simulate an environment without Delta for skip validation
    if os.getenv("DISABLE_DELTA_FOR_TEST") == "1":
        spark = builder.getOrCreate()
        yield spark
        spark.stop()
        return

    from delta import configure_spark_with_delta_pip

    builder = configure_spark_with_delta_pip(builder)
    spark = builder.getOrCreate()
    yield spark
    spark.stop()
