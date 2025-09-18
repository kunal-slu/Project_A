#!/usr/bin/env python3
"""
Enterprise AWS ETL Pipeline with 3 Real External Data Sources
This script implements a production ETL pipeline that ingests data from
3 key external sources: Snowflake, Salesforce API, and Kafka streaming.
"""

import sys
import os
import logging
import yaml
import boto3
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import snowflake.connector
from kafka import KafkaConsumer

# Reliability
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configure logging (enhanced)
try:
    # Prefer project logging with correlation IDs if available
    from pyspark_interview_project.logging_setup import get_logger, new_correlation_id
    _corr_id = new_correlation_id()
    logger = get_logger(__name__)
    logger.info("AWS ETL started", extra={"corr_id": _corr_id})
except Exception:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)


HTTP_TIMEOUT = 20  # seconds


def _resolve_env(value: Optional[str]) -> Optional[str]:
    """Resolve values like "${ENV_VAR}" to os.environ[ENV_VAR].
    If value is plain string without ${}, return as-is. If None, return None.
    """
    if value is None:
        return None
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        return os.getenv(value[2:-1])
    return value


def _secrets_client(region: Optional[str]):
    return boto3.client("secretsmanager", region_name=region) if region else boto3.client("secretsmanager")


def _get_secret_json(secrets_client, secret_name: Optional[str]) -> Dict[str, Any]:
    if not secret_name:
        return {}
    try:
        resp = secrets_client.get_secret_value(SecretId=secret_name)
        secret_str = resp.get("SecretString")
        if secret_str:
            return json.loads(secret_str)
    except Exception as e:
        logger.warning(f"Could not load secret {secret_name}: {e}")
    return {}


class EnterpriseSimpleETLPipeline:
    """Enterprise ETL pipeline with 3 key external data sources."""

    def __init__(self, config_path: str):
        """Initialize the ETL pipeline with configuration."""
        self.config = self._load_config(config_path)
        self.region = self.config.get('aws', {}).get('region')
        self.spark = self._create_spark_session()
        self.cloudwatch = boto3.client('cloudwatch', region_name=self.region) if self.region else boto3.client('cloudwatch')
        self.s3 = boto3.client('s3', region_name=self.region) if self.region else boto3.client('s3')
        self.secrets = _secrets_client(self.region)

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            logger.info(f"Configuration loaded from {config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with production configuration."""
        spark_config = self.config['spark']

        builder = (SparkSession.builder
            .appName(spark_config['app_name'])
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )

        # Add all Spark configurations
        for key, value in spark_config['config'].items():
            builder = builder.config(key, value)

        spark = builder.getOrCreate()
        logger.info("Spark session created successfully")
        return spark

    def send_metric(self, metric_name: str, value: float, unit: str = "Count"):
        """Send custom metric to CloudWatch."""
        try:
            self.cloudwatch.put_metric_data(
                Namespace=self.config['aws']['cloudwatch']['namespace'],
                MetricData=[{
                    'MetricName': metric_name,
                    'Value': value,
                    'Unit': unit,
                    'Timestamp': datetime.utcnow()
                }]
            )
        except Exception as e:
            logger.warning(f"Failed to send metric {metric_name}: {e}")

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=20),
           retry=retry_if_exception_type(Exception))
    def _snowflake_connect(self, cfg: Dict[str, Any]):
        return snowflake.connector.connect(
            account=cfg['account'],
            warehouse=cfg['warehouse'],
            database=cfg['database'],
            schema=cfg['schema'],
            user=cfg['user'],
            password=cfg['password'],
            role=cfg['role']
        )

    def ingest_from_snowflake(self):
        """Ingest data from Snowflake data warehouse."""
        logger.info("Starting Snowflake data ingestion...")
        start_time = datetime.now()

        try:
            snowflake_config = self.config['external_data_sources']['snowflake']

            # Resolve credentials from env or Secrets Manager
            secrets_name = snowflake_config.get('secrets_manager_secret')  # optional
            secret_map = _get_secret_json(self.secrets, secrets_name)
            username = _resolve_env(snowflake_config.get('username')) or secret_map.get('username')
            password = _resolve_env(snowflake_config.get('password')) or secret_map.get('password')

            conn = self._snowflake_connect({
                'account': snowflake_config['account'],
                'warehouse': snowflake_config['warehouse'],
                'database': snowflake_config['database'],
                'schema': snowflake_config['schema'],
                'user': username,
                'password': password,
                'role': snowflake_config['role']
            })

            total_records = 0

            for table_config in snowflake_config['tables']:
                table_name = table_config['name']
                description = table_config.get('description', table_name)

                logger.info(f"Ingesting {table_name}: {description}")

                # Read data from Snowflake
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, conn)

                if not df.empty:
                    spark_df = (self.spark.createDataFrame(df)
                        .withColumn("ingested_at", current_timestamp())
                        .withColumn("source_system", lit("snowflake"))
                        .withColumn("table_name", lit(table_name))
                    )

                    storage_config = self.config['storage_config']
                    if 'customer' in table_name or 'order' in table_name:
                        path = f"{storage_config['crm']['processed_path']}/snowflake/{table_name}/"
                    elif 'product' in table_name or 'financial' in table_name:
                        path = f"{storage_config['analytics']['processed_path']}/snowflake/{table_name}/"
                    else:
                        path = f"{self.config['data_lake']['bronze_path']}/snowflake/{table_name}/"

                    # Idempotent write: partition by ingestion timestamp; allow downstream overwrite per partition
                    spark_df.write.mode("append").partitionBy("ingested_at").parquet(path)
                    cnt = spark_df.count()
                    total_records += cnt
                    logger.info(f"Ingested {cnt} records from Snowflake table: {table_name}")
                else:
                    logger.warning(f"No data found in Snowflake table: {table_name}")

            conn.close()

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("SnowflakeIngestionTime", processing_time, "Seconds")
            self.send_metric("SnowflakeRecordsProcessed", total_records)

            logger.info("Snowflake data ingestion completed successfully")

        except Exception as e:
            logger.error(f"Snowflake data ingestion failed: {e}")
            self.send_metric("SnowflakeIngestionErrors", 1)
            raise

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=20),
           retry=retry_if_exception_type(Exception))
    def _http_post(self, url: str, data: Dict[str, Any]):
        return requests.post(url, data=data, timeout=HTTP_TIMEOUT)

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10),
           retry=retry_if_exception_type(Exception))
    def _http_get(self, url: str, headers: Dict[str, str]):
        return requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)

    def ingest_from_salesforce_api(self):
        """Ingest data from Salesforce API."""
        logger.info("Starting Salesforce API ingestion...")
        start_time = datetime.now()

        try:
            salesforce_config = self.config['external_data_sources']['salesforce']

            # Resolve from Secrets Manager if present
            secrets_name = salesforce_config.get('secrets_manager_secret')  # optional
            secret_map = _get_secret_json(self.secrets, secrets_name)

            token_url = f"{salesforce_config['base_url']}/services/oauth2/token"
            token_data = {
                'grant_type': 'refresh_token',
                'client_id': _resolve_env(salesforce_config.get('client_id')) or secret_map.get('client_id'),
                'client_secret': _resolve_env(salesforce_config.get('client_secret')) or secret_map.get('client_secret'),
                'refresh_token': _resolve_env(salesforce_config.get('refresh_token')) or secret_map.get('refresh_token'),
            }

            token_response = self._http_post(token_url, token_data)
            if token_response.status_code != 200:
                raise Exception(f"Failed to get Salesforce access token: {token_response.text}")

            access_token = token_response.json()['access_token']
            headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}

            total_records = 0

            for endpoint_config in salesforce_config['endpoints']:
                endpoint_name = endpoint_config['name']
                endpoint_path = endpoint_config['path']
                description = endpoint_config.get('description', endpoint_name)

                logger.info(f"Ingesting {endpoint_name}: {description}")

                url = f"{salesforce_config['base_url']}{endpoint_path}"
                response = self._http_get(url, headers)

                if response.status_code == 200:
                    data = response.json()
                    records = data.get('records', [])

                    if records:
                        df = pd.DataFrame(records)
                        spark_df = (self.spark.createDataFrame(df)
                            .withColumn("ingested_at", current_timestamp())
                            .withColumn("source_system", lit("salesforce"))
                            .withColumn("endpoint_name", lit(endpoint_name))
                        )
                        storage_config = self.config['storage_config']
                        path = f"{storage_config['crm']['processed_path']}/salesforce/{endpoint_name}/"
                        spark_df.write.mode("append").partitionBy("ingested_at").parquet(path)
                        cnt = spark_df.count()
                        total_records += cnt
                        logger.info(f"Ingested {cnt} records from Salesforce: {endpoint_name}")
                    else:
                        logger.warning(f"No records found in Salesforce endpoint: {endpoint_name}")
                else:
                    logger.error(f"Failed to call Salesforce API for {endpoint_name}: {response.status_code}")

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("SalesforceIngestionTime", processing_time, "Seconds")
            self.send_metric("SalesforceRecordsProcessed", total_records)

            logger.info("Salesforce API ingestion completed successfully")

        except Exception as e:
            logger.error(f"Salesforce API ingestion failed: {e}")
            self.send_metric("SalesforceIngestionErrors", 1)
            raise

    def ingest_from_kafka_streaming(self):
        """Ingest data from Kafka streaming platform."""
        logger.info("Starting Kafka streaming ingestion...")
        start_time = datetime.now()

        try:
            kafka_config = self.config['external_data_sources']['kafka']

            secrets_name = kafka_config.get('secrets_manager_secret')  # optional
            secret_map = _get_secret_json(self.secrets, secrets_name)
            kafka_username = _resolve_env(kafka_config.get('username')) or secret_map.get('username')
            kafka_password = _resolve_env(kafka_config.get('password')) or secret_map.get('password')

            consumer = KafkaConsumer(
                *[topic['name'] for topic in kafka_config['topics']],
                bootstrap_servers=kafka_config['bootstrap_servers'],
                security_protocol=kafka_config['security_protocol'],
                sasl_mechanism=kafka_config['sasl_mechanism'],
                sasl_plain_username=kafka_username,
                sasl_plain_password=kafka_password,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='etl-consumer-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )

            total_records = 0
            records_buffer = []
            end_time = datetime.now() + timedelta(minutes=5)
            logger.info("Starting Kafka message consumption...")

            for message in consumer:
                if datetime.now() > end_time:
                    logger.info("Kafka consumption time limit reached")
                    break

                topic = message.topic
                data = message.value
                data['topic'] = topic
                data['timestamp'] = message.timestamp
                data['partition'] = message.partition
                data['offset'] = message.offset
                records_buffer.append(data)

                if len(records_buffer) >= 1000:
                    df = pd.DataFrame(records_buffer)
                    spark_df = self.spark.createDataFrame(df).withColumn("ingested_at", current_timestamp()).withColumn("source_system", lit("kafka"))
                    storage_config = self.config['storage_config']
                    path = f"{storage_config['streaming']['processed_path']}/kafka/{topic}/"
                    spark_df.write.mode("append").partitionBy("ingested_at").parquet(path)
                    cnt = spark_df.count()
                    total_records += cnt
                    logger.info(f"Ingested {cnt} records from Kafka topic: {topic}")
                    records_buffer = []

            if records_buffer:
                df = pd.DataFrame(records_buffer)
                spark_df = self.spark.createDataFrame(df).withColumn("ingested_at", current_timestamp()).withColumn("source_system", lit("kafka"))
                storage_config = self.config['storage_config']
                for row in spark_df.select("topic").distinct().collect():
                    topic_name = row['topic']
                    topic_df = spark_df.filter(col("topic") == topic_name)
                    path = f"{storage_config['streaming']['processed_path']}/kafka/{topic_name}/"
                    topic_df.write.mode("append").partitionBy("ingested_at").parquet(path)
                    cnt = topic_df.count()
                    total_records += cnt
                    logger.info(f"Ingested {cnt} records from Kafka topic: {topic_name}")

            consumer.close()

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("KafkaIngestionTime", processing_time, "Seconds")
            self.send_metric("KafkaRecordsProcessed", total_records)

            logger.info("Kafka streaming ingestion completed successfully")

        except Exception as e:
            logger.error(f"Kafka streaming ingestion failed: {e}")
            self.send_metric("KafkaIngestionErrors", 1)
            raise

    def process_data_warehouse(self):
        """Process data for data warehouse."""
        logger.info("Processing data warehouse...")
        start_time = datetime.now()

        try:
            bronze_path = self.config['data_lake']['bronze_path']
            silver_path = self.config['data_lake']['silver_path']
            gold_path = self.config['data_lake']['gold_path']

            customer_sources = [
                f"{bronze_path}/snowflake/customer_orders/",
                f"{bronze_path}/salesforce/leads/",
                f"{bronze_path}/salesforce/accounts/",
            ]

            customer_dfs = []
            for source in customer_sources:
                try:
                    df = self.spark.read.parquet(source)
                    if df.take(1):
                        customer_dfs.append(df)
                        logger.info(f"Loaded customer data from {source}")
                except Exception as e:
                    logger.warning(f"Could not read from {source}: {e}")

            if customer_dfs:
                base_cols = set(customer_dfs[0].columns)
                aligned = []
                for df in customer_dfs:
                    cols = set(df.columns)
                    missing = list(base_cols - cols)
                    extra = list(cols - base_cols)
                    for c in missing:
                        df = df.withColumn(c, lit(None).cast(StringType()))
                    df = df.select(*sorted(list(base_cols.union(set(extra)))))
                    aligned.append(df)
                unified_customers = aligned[0]
                for df in aligned[1:]:
                    unified_customers = unified_customers.unionByName(df, allowMissingColumns=True)

                clean_customers = (unified_customers.dropDuplicates(['customer_id', 'email'])
                    .filter((col("customer_id").isNotNull()))
                    .withColumn("processed_at", current_timestamp()))

                clean_customers.write.mode("overwrite").partitionBy("processed_at").parquet(
                    f"{silver_path}/customers/"
                )

                customer_dim = (clean_customers.select(
                    col("customer_id"), col("email"), col("first_name"), col("last_name"),
                    col("phone"), col("address"), col("city"), col("state"), col("country"),
                    col("registration_date"), col("processed_at")
                ).withColumn("customer_key", expr("uuid()")))

                customer_dim.write.mode("overwrite").partitionBy("country", "state").parquet(
                    f"{gold_path}/dim_customers/"
                )

                logger.info(f"Processed {customer_dim.count()} customer records")

            sales_sources = [
                f"{bronze_path}/snowflake/customer_orders/",
                f"{bronze_path}/salesforce/opportunities/",
            ]

            sales_dfs = []
            for source in sales_sources:
                try:
                    df = self.spark.read.parquet(source)
                    if df.take(1):
                        sales_dfs.append(df)
                        logger.info(f"Loaded sales data from {source}")
                except Exception as e:
                    logger.warning(f"Could not read from {source}: {e}")

            if sales_dfs:
                unified_sales = sales_dfs[0]
                for df in sales_dfs[1:]:
                    unified_sales = unified_sales.unionByName(df, allowMissingColumns=True)

                clean_sales = (unified_sales.filter((col("total_amount") > 0) & (col("customer_id").isNotNull()))
                    .withColumn("processed_at", current_timestamp()))

                clean_sales.write.mode("append").partitionBy("processed_at").parquet(
                    f"{silver_path}/sales/"
                )

                sales_fact = (clean_sales
                    .join(customer_dim, "customer_id", "left")
                    .select(
                        col("order_id"), col("customer_id"), col("customer_key"), col("total_amount"),
                        col("order_date"), col("status"), col("payment_method"), col("processed_at")
                    )
                    .withColumn("order_key", expr("uuid()"))
                    .withColumn("order_year", year(col("order_date")))
                    .withColumn("order_month", month(col("order_date")))
                )

                sales_fact.write.mode("append").partitionBy("order_year", "order_month").parquet(
                    f"{gold_path}/fact_sales/"
                )

                logger.info(f"Processed {sales_fact.count()} sales records")

            streaming_sources = [
                f"{bronze_path}/kafka/user_events/",
                f"{bronze_path}/kafka/order_events/",
                f"{bronze_path}/kafka/inventory_updates/",
            ]

            for source in streaming_sources:
                try:
                    df = self.spark.read.parquet(source)
                    if df.take(1):
                        clean_streaming = (df.filter(col("timestamp").isNotNull())
                            .withColumn("processed_at", current_timestamp()))
                        clean_streaming.write.mode("append").partitionBy("processed_at").parquet(
                            f"{silver_path}/streaming/{source.split('/')[-2]}/"
                        )
                        logger.info(f"Processed {clean_streaming.count()} streaming records from {source}")
                except Exception as e:
                    logger.warning(f"Could not process streaming data from {source}: {e}")

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("DataWarehouseProcessingTime", processing_time, "Seconds")

            logger.info("Data warehouse processing completed successfully")

        except Exception as e:
            logger.error(f"Data warehouse processing failed: {e}")
            self.send_metric("DataWarehouseProcessingErrors", 1)
            raise

    def run_data_quality_checks(self):
        """Run comprehensive data quality checks."""
        logger.info("Running data quality checks...")
        start_time = datetime.now()

        try:
            quality_results = {}
            gold_path = self.config['data_lake']['gold_path']

            try:
                sales_df = self.spark.read.parquet(f"{gold_path}/fact_sales/")

                completeness = (sales_df.select([
                    (count(when(col(c).isNull(), c)) / count("*") * 100).alias(f"{c}_null_pct")
                    for c in sales_df.columns
                ])).collect()[0]

                validity_checks = sales_df.select(
                    count(when(col("total_amount") <= 0, True)).alias("invalid_amounts"),
                    count(when(col("order_date") > current_date(), True)).alias("future_dates"),
                    count(when(col("customer_id").isNull(), True)).alias("missing_customer_ids")
                ).collect()[0]

                uniqueness_checks = sales_df.select(
                    count("*").alias("total_rows"),
                    countDistinct("order_id").alias("unique_orders")
                ).collect()[0]

                quality_results['sales'] = {
                    'completeness': completeness,
                    'validity': validity_checks,
                    'uniqueness': uniqueness_checks
                }

                total_issues = sum([
                    validity_checks.invalid_amounts,
                    validity_checks.future_dates,
                    validity_checks.missing_customer_ids,
                ])

                quality_score = max(0, 100 - (total_issues / max(uniqueness_checks.total_rows, 1) * 100))
                self.send_metric("DataQualityScore", quality_score, "Percent")

                logger.info(f"Sales data quality score: {quality_score:.2f}%")

            except Exception as e:
                logger.warning(f"Could not perform sales data quality checks: {e}")

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("DataQualityCheckTime", processing_time, "Seconds")

            logger.info("Data quality checks completed successfully")
            return quality_results

        except Exception as e:
            logger.error(f"Data quality checks failed: {e}")
            self.send_metric("DataQualityCheckErrors", 1)
            raise

    def run_complete_pipeline(self):
        """Run the complete enterprise ETL pipeline with 3 data sources."""
        logger.info("Starting complete enterprise ETL pipeline with 3 data sources...")
        pipeline_start_time = datetime.now()

        try:
            logger.info("=== PHASE 1: SNOWFLAKE DATA WAREHOUSE INGESTION ===")
            self.ingest_from_snowflake()

            logger.info("=== PHASE 2: SALESFORCE CRM API INGESTION ===")
            self.ingest_from_salesforce_api()

            logger.info("=== PHASE 3: KAFKA STREAMING INGESTION ===")
            self.ingest_from_kafka_streaming()

            logger.info("=== PHASE 4: DATA WAREHOUSE PROCESSING ===")
            self.process_data_warehouse()

            logger.info("=== PHASE 5: DATA QUALITY ===")
            self.run_data_quality_checks()

            pipeline_time = (datetime.now() - pipeline_start_time).total_seconds()
            self.send_metric("TotalPipelineTime", pipeline_time, "Seconds")
            self.send_metric("PipelineSuccess", 1)

            logger.info(f"Complete enterprise ETL pipeline finished successfully in {pipeline_time:.2f} seconds")
            logger.info("✅ Successfully processed data from 3 external sources:")
            logger.info("   📊 Snowflake Data Warehouse")
            logger.info("   🏢 Salesforce CRM API")
            logger.info("   🌊 Kafka Streaming Platform")

        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            self.send_metric("PipelineErrors", 1)
            raise


def main():
    """Main function to run the enterprise ETL pipeline."""
    if len(sys.argv) != 2:
        print("Usage: python aws_enterprise_simple_etl.py <config_path>")
        sys.exit(1)

    config_path = sys.argv[1]

    try:
        pipeline = EnterpriseSimpleETLPipeline(config_path)
        pipeline.run_complete_pipeline()

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
