#!/usr/bin/env python3
"""
Enterprise AWS ETL Pipeline with Real External Data Sources
This script implements a production ETL pipeline that ingests data from
real external sources like Snowflake, databases, APIs, and streaming platforms.
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
from typing import Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import snowflake.connector
import psycopg2
import pymysql
import pymongo
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EnterpriseETLPipeline:
    """Enterprise ETL pipeline with real external data sources."""

    def __init__(self, config_path: str):
        """Initialize the ETL pipeline with configuration."""
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
        self.cloudwatch = boto3.client('cloudwatch')
        self.s3 = boto3.client('s3')

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

        builder = SparkSession.builder \
            .appName(spark_config['app_name']) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

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

    def ingest_from_snowflake(self):
        """Ingest data from Snowflake data warehouse."""
        logger.info("Starting Snowflake data ingestion...")
        start_time = datetime.now()

        try:
            snowflake_config = self.config['external_data_sources']['snowflake']

            # Connect to Snowflake
            conn = snowflake.connector.connect(
                account=snowflake_config['account'],
                warehouse=snowflake_config['warehouse'],
                database=snowflake_config['database'],
                schema=snowflake_config['schema'],
                user=os.getenv(snowflake_config['username']),
                password=os.getenv(snowflake_config['password']),
                role=snowflake_config['role']
            )

            total_records = 0

            # Ingest each table from Snowflake
            for table_config in snowflake_config['tables']:
                table_name = table_config['name']
                frequency = table_config['frequency']

                # Read data from Snowflake
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, conn)

                if not df.empty:
                    # Convert to Spark DataFrame
                    spark_df = self.spark.createDataFrame(df)
                    spark_df = spark_df.withColumn("ingested_at", current_timestamp())

                    # Write to S3
                    storage_config = self.config['storage_config']
                    if 'customer' in table_name:
                        path = f"{storage_config['crm']['processed_path']}/snowflake/{table_name}/"
                    elif 'product' in table_name:
                        path = f"{storage_config['ecommerce']['processed_path']}/snowflake/{table_name}/"
                    elif 'financial' in table_name:
                        path = f"{storage_config['finance']['processed_path']}/snowflake/{table_name}/"
                    else:
                        path = f"{self.config['data_lake']['bronze_path']}/snowflake/{table_name}/"

                    spark_df.write.mode("append").partitionBy("ingested_at").parquet(path)
                    total_records += spark_df.count()
                    logger.info(f"Ingested {spark_df.count()} records from Snowflake table: {table_name}")

            conn.close()

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("SnowflakeIngestionTime", processing_time, "Seconds")
            self.send_metric("SnowflakeRecordsProcessed", total_records)

            logger.info("Snowflake data ingestion completed successfully")

        except Exception as e:
            logger.error(f"Snowflake data ingestion failed: {e}")
            self.send_metric("SnowflakeIngestionErrors", 1)
            raise

    def ingest_from_postgresql(self):
        """Ingest data from PostgreSQL database."""
        logger.info("Starting PostgreSQL data ingestion...")
        start_time = datetime.now()

        try:
            postgres_config = self.config['external_data_sources']['postgresql']

            # Connect to PostgreSQL
            conn = psycopg2.connect(
                host=postgres_config['host'],
                port=postgres_config['port'],
                database=postgres_config['database'],
                user=os.getenv(postgres_config['username']),
                password=os.getenv(postgres_config['password']),
                sslmode=postgres_config['ssl_mode']
            )

            total_records = 0

            # Ingest each table from PostgreSQL
            for table_config in postgres_config['tables']:
                table_name = table_config['name']
                schema = table_config.get('schema', 'public')
                frequency = table_config['frequency']

                # Read data from PostgreSQL
                query = f"SELECT * FROM {schema}.{table_name}"
                df = pd.read_sql(query, conn)

                if not df.empty:
                    # Convert to Spark DataFrame
                    spark_df = self.spark.createDataFrame(df)
                    spark_df = spark_df.withColumn("ingested_at", current_timestamp())

                    # Write to S3
                    storage_config = self.config['storage_config']
                    if 'user' in table_name:
                        path = f"{storage_config['ecommerce']['processed_path']}/postgresql/{table_name}/"
                    elif 'subscription' in table_name:
                        path = f"{storage_config['finance']['processed_path']}/postgresql/{table_name}/"
                    elif 'inventory' in table_name:
                        path = f"{storage_config['erp']['processed_path']}/postgresql/{table_name}/"
                    else:
                        path = f"{self.config['data_lake']['bronze_path']}/postgresql/{table_name}/"

                    spark_df.write.mode("append").partitionBy("ingested_at").parquet(path)
                    total_records += spark_df.count()
                    logger.info(f"Ingested {spark_df.count()} records from PostgreSQL table: {table_name}")

            conn.close()

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("PostgreSQLIngestionTime", processing_time, "Seconds")
            self.send_metric("PostgreSQLRecordsProcessed", total_records)

            logger.info("PostgreSQL data ingestion completed successfully")

        except Exception as e:
            logger.error(f"PostgreSQL data ingestion failed: {e}")
            self.send_metric("PostgreSQLIngestionErrors", 1)
            raise

    def ingest_from_mysql(self):
        """Ingest data from MySQL database."""
        logger.info("Starting MySQL data ingestion...")
        start_time = datetime.now()

        try:
            mysql_config = self.config['external_data_sources']['mysql']

            # Connect to MySQL
            conn = pymysql.connect(
                host=mysql_config['host'],
                port=mysql_config['port'],
                database=mysql_config['database'],
                user=os.getenv(mysql_config['username']),
                password=os.getenv(mysql_config['password']),
                ssl={'ssl': mysql_config['ssl_mode']}
            )

            total_records = 0

            # Ingest each table from MySQL
            for table_config in mysql_config['tables']:
                table_name = table_config['name']
                frequency = table_config['frequency']

                # Read data from MySQL
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, conn)

                if not df.empty:
                    # Convert to Spark DataFrame
                    spark_df = self.spark.createDataFrame(df)
                    spark_df = spark_df.withColumn("ingested_at", current_timestamp())

                    # Write to S3
                    path = f"{self.config['data_lake']['bronze_path']}/mysql/{table_name}/"
                    spark_df.write.mode("append").partitionBy("ingested_at").parquet(path)
                    total_records += spark_df.count()
                    logger.info(f"Ingested {spark_df.count()} records from MySQL table: {table_name}")

            conn.close()

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("MySQLIngestionTime", processing_time, "Seconds")
            self.send_metric("MySQLRecordsProcessed", total_records)

            logger.info("MySQL data ingestion completed successfully")

        except Exception as e:
            logger.error(f"MySQL data ingestion failed: {e}")
            self.send_metric("MySQLIngestionErrors", 1)
            raise

    def ingest_from_salesforce_api(self):
        """Ingest data from Salesforce API."""
        logger.info("Starting Salesforce API ingestion...")
        start_time = datetime.now()

        try:
            salesforce_config = self.config['external_data_sources']['apis']['salesforce']

            # Get Salesforce access token
            token_url = f"{salesforce_config['base_url']}/services/oauth2/token"
            token_data = {
                'grant_type': 'refresh_token',
                'client_id': os.getenv(salesforce_config['client_id']),
                'client_secret': os.getenv(salesforce_config['client_secret']),
                'refresh_token': os.getenv(salesforce_config['refresh_token'])
            }

            token_response = requests.post(token_url, data=token_data)
            access_token = token_response.json()['access_token']

            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }

            total_records = 0

            # Ingest each endpoint from Salesforce
            for endpoint_config in salesforce_config['endpoints']:
                endpoint_name = endpoint_config['name']
                endpoint_path = endpoint_config['path']
                frequency = endpoint_config['frequency']

                # Call Salesforce API
                url = f"{salesforce_config['base_url']}{endpoint_path}"
                response = requests.get(url, headers=headers)

                if response.status_code == 200:
                    data = response.json()
                    records = data.get('records', [])

                    if records:
                        # Convert to Spark DataFrame
                        df = pd.DataFrame(records)
                        spark_df = self.spark.createDataFrame(df)
                        spark_df = spark_df.withColumn("ingested_at", current_timestamp())

                        # Write to S3
                        storage_config = self.config['storage_config']
                        path = f"{storage_config['crm']['processed_path']}/salesforce/{endpoint_name}/"
                        spark_df.write.mode("append").partitionBy("ingested_at").parquet(path)
                        total_records += spark_df.count()
                        logger.info(f"Ingested {spark_df.count()} records from Salesforce: {endpoint_name}")

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("SalesforceIngestionTime", processing_time, "Seconds")
            self.send_metric("SalesforceRecordsProcessed", total_records)

            logger.info("Salesforce API ingestion completed successfully")

        except Exception as e:
            logger.error(f"Salesforce API ingestion failed: {e}")
            self.send_metric("SalesforceIngestionErrors", 1)
            raise

    def ingest_from_stripe_api(self):
        """Ingest data from Stripe API."""
        logger.info("Starting Stripe API ingestion...")
        start_time = datetime.now()

        try:
            stripe_config = self.config['external_data_sources']['apis']['stripe']
            api_key = os.getenv(stripe_config['api_key'])

            headers = {
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }

            total_records = 0

            # Ingest each endpoint from Stripe
            for endpoint_config in stripe_config['endpoints']:
                endpoint_name = endpoint_config['name']
                endpoint_path = endpoint_config['path']
                frequency = endpoint_config['frequency']

                # Call Stripe API
                url = f"https://api.stripe.com{endpoint_path}"
                response = requests.get(url, headers=headers)

                if response.status_code == 200:
                    data = response.json()
                    records = data.get('data', [])

                    if records:
                        # Convert to Spark DataFrame
                        df = pd.DataFrame(records)
                        spark_df = self.spark.createDataFrame(df)
                        spark_df = spark_df.withColumn("ingested_at", current_timestamp())

                        # Write to S3
                        storage_config = self.config['storage_config']
                        path = f"{storage_config['payments']['processed_path']}/stripe/{endpoint_name}/"
                        spark_df.write.mode("append").partitionBy("ingested_at").parquet(path)
                        total_records += spark_df.count()
                        logger.info(f"Ingested {spark_df.count()} records from Stripe: {endpoint_name}")

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("StripeIngestionTime", processing_time, "Seconds")
            self.send_metric("StripeRecordsProcessed", total_records)

            logger.info("Stripe API ingestion completed successfully")

        except Exception as e:
            logger.error(f"Stripe API ingestion failed: {e}")
            self.send_metric("StripeIngestionErrors", 1)
            raise

    def ingest_from_kafka_streaming(self):
        """Ingest data from Kafka streaming platform."""
        logger.info("Starting Kafka streaming ingestion...")
        start_time = datetime.now()

        try:
            kafka_config = self.config['external_data_sources']['kafka']

            # Create Kafka consumer
            consumer = KafkaConsumer(
                *[topic['name'] for topic in kafka_config['topics']],
                bootstrap_servers=kafka_config['bootstrap_servers'],
                security_protocol=kafka_config['security_protocol'],
                sasl_mechanism=kafka_config['sasl_mechanism'],
                sasl_plain_username=os.getenv(kafka_config['username']),
                sasl_plain_password=os.getenv(kafka_config['password']),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='etl-consumer-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )

            total_records = 0
            records_buffer = []

            # Consume messages for a limited time
            end_time = datetime.now() + timedelta(minutes=5)  # 5 minutes of streaming

            for message in consumer:
                if datetime.now() > end_time:
                    break

                topic = message.topic
                data = message.value
                data['topic'] = topic
                data['timestamp'] = message.timestamp
                records_buffer.append(data)

                # Process in batches
                if len(records_buffer) >= 1000:
                    # Convert to Spark DataFrame
                    df = pd.DataFrame(records_buffer)
                    spark_df = self.spark.createDataFrame(df)
                    spark_df = spark_df.withColumn("ingested_at", current_timestamp())

                    # Write to S3
                    path = f"{self.config['data_lake']['bronze_path']}/kafka/{topic}/"
                    spark_df.write.mode("append").partitionBy("ingested_at").parquet(path)
                    total_records += spark_df.count()
                    logger.info(f"Ingested {spark_df.count()} records from Kafka topic: {topic}")

                    records_buffer = []

            # Process remaining records
            if records_buffer:
                df = pd.DataFrame(records_buffer)
                spark_df = self.spark.createDataFrame(df)
                spark_df = spark_df.withColumn("ingested_at", current_timestamp())

                # Group by topic and write
                for topic in spark_df.select("topic").distinct().collect():
                    topic_name = topic.topic
                    topic_df = spark_df.filter(col("topic") == topic_name)
                    path = f"{self.config['data_lake']['bronze_path']}/kafka/{topic_name}/"
                    topic_df.write.mode("append").partitionBy("ingested_at").parquet(path)
                    total_records += topic_df.count()
                    logger.info(f"Ingested {topic_df.count()} records from Kafka topic: {topic_name}")

            consumer.close()

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("KafkaIngestionTime", processing_time, "Seconds")
            self.send_metric("KafkaRecordsProcessed", total_records)

            logger.info("Kafka streaming ingestion completed successfully")

        except Exception as e:
            logger.error(f"Kafka streaming ingestion failed: {e}")
            self.send_metric("KafkaIngestionErrors", 1)
            raise

    def ingest_from_mongodb(self):
        """Ingest data from MongoDB NoSQL database."""
        logger.info("Starting MongoDB ingestion...")
        start_time = datetime.now()

        try:
            mongodb_config = self.config['external_data_sources']['mongodb']

            # Connect to MongoDB
            client = pymongo.MongoClient(
                mongodb_config['connection_string'],
                username=os.getenv(mongodb_config['username']),
                password=os.getenv(mongodb_config['password'])
            )

            db = client[mongodb_config['database']]
            total_records = 0

            # Ingest each collection from MongoDB
            for collection_config in mongodb_config['collections']:
                collection_name = collection_config['name']
                frequency = collection_config['frequency']

                # Read data from MongoDB
                collection = db[collection_name]
                cursor = collection.find({})
                records = list(cursor)

                if records:
                    # Convert to Spark DataFrame
                    df = pd.DataFrame(records)
                    spark_df = self.spark.createDataFrame(df)
                    spark_df = spark_df.withColumn("ingested_at", current_timestamp())

                    # Write to S3
                    storage_config = self.config['storage_config']
                    if 'session' in collection_name:
                        path = f"{storage_config['analytics']['processed_path']}/mongodb/{collection_name}/"
                    elif 'review' in collection_name:
                        path = f"{storage_config['ecommerce']['processed_path']}/mongodb/{collection_name}/"
                    else:
                        path = f"{self.config['data_lake']['bronze_path']}/mongodb/{collection_name}/"

                    spark_df.write.mode("append").partitionBy("ingested_at").parquet(path)
                    total_records += spark_df.count()
                    logger.info(f"Ingested {spark_df.count()} records from MongoDB collection: {collection_name}")

            client.close()

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("MongoDBIngestionTime", processing_time, "Seconds")
            self.send_metric("MongoDBRecordsProcessed", total_records)

            logger.info("MongoDB ingestion completed successfully")

        except Exception as e:
            logger.error(f"MongoDB ingestion failed: {e}")
            self.send_metric("MongoDBIngestionErrors", 1)
            raise

    def ingest_from_sftp(self):
        """Ingest data from SFTP file server."""
        logger.info("Starting SFTP file ingestion...")
        start_time = datetime.now()

        try:
            sftp_config = self.config['external_data_sources']['sftp']

            # Use paramiko for SFTP connection
            import paramiko

            # Create SSH client
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # Connect to SFTP server
            ssh.connect(
                hostname=sftp_config['host'],
                port=sftp_config['port'],
                username=os.getenv(sftp_config['username']),
                password=os.getenv(sftp_config['password'])
            )

            sftp = ssh.open_sftp()
            total_records = 0

            # Ingest each directory from SFTP
            for directory_config in sftp_config['directories']:
                directory_name = directory_config['name']
                directory_path = directory_config['path']
                frequency = directory_config['frequency']

                try:
                    # List files in directory
                    files = sftp.listdir(directory_path)

                    for file_name in files:
                        if file_name.endswith(('.csv', '.json', '.parquet')):
                            # Download file to local temp
                            local_path = f"/tmp/{file_name}"
                            remote_path = f"{directory_path}/{file_name}"
                            sftp.get(remote_path, local_path)

                            # Read file based on extension
                            if file_name.endswith('.csv'):
                                df = pd.read_csv(local_path)
                            elif file_name.endswith('.json'):
                                df = pd.read_json(local_path)
                            elif file_name.endswith('.parquet'):
                                df = pd.read_parquet(local_path)

                            if not df.empty:
                                # Convert to Spark DataFrame
                                spark_df = self.spark.createDataFrame(df)
                                spark_df = spark_df.withColumn("ingested_at", current_timestamp())
                                spark_df = spark_df.withColumn("source_file", lit(file_name))

                                # Write to S3
                                path = f"{self.config['data_lake']['bronze_path']}/sftp/{directory_name}/"
                                spark_df.write.mode("append").partitionBy("ingested_at").parquet(path)
                                total_records += spark_df.count()
                                logger.info(f"Ingested {spark_df.count()} records from SFTP file: {file_name}")

                            # Clean up local file
                            os.remove(local_path)

                except Exception as e:
                    logger.warning(f"Failed to process SFTP directory {directory_name}: {e}")

            sftp.close()
            ssh.close()

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("SFTPIngestionTime", processing_time, "Seconds")
            self.send_metric("SFTPRecordsProcessed", total_records)

            logger.info("SFTP file ingestion completed successfully")

        except Exception as e:
            logger.error(f"SFTP file ingestion failed: {e}")
            self.send_metric("SFTPIngestionErrors", 1)
            raise

    def process_data_warehouse(self):
        """Process data for data warehouse."""
        logger.info("Processing data warehouse...")
        start_time = datetime.now()

        try:
            # Read data from bronze layer
            bronze_path = self.config['data_lake']['bronze_path']
            silver_path = self.config['data_lake']['silver_path']
            gold_path = self.config['data_lake']['gold_path']

            # Process customer data from multiple sources
            customer_sources = [
                f"{bronze_path}/snowflake/customer_orders/",
                f"{bronze_path}/postgresql/users/",
                f"{bronze_path}/salesforce/leads/",
                f"{bronze_path}/stripe/customers/"
            ]

            customer_dfs = []
            for source in customer_sources:
                try:
                    df = self.spark.read.parquet(source)
                    customer_dfs.append(df)
                except Exception as e:
                    logger.warning(f"Could not read from {source}: {e}")

            if customer_dfs:
                # Union all customer data
                unified_customers = customer_dfs[0]
                for df in customer_dfs[1:]:
                    unified_customers = unified_customers.union(df)

                # Clean and deduplicate
                clean_customers = unified_customers.dropDuplicates(['customer_id', 'email']) \
                    .filter(col("customer_id").isNotNull()) \
                    .withColumn("processed_at", current_timestamp())

                # Write to silver
                clean_customers.write.mode("overwrite").partitionBy("processed_at").parquet(
                    f"{silver_path}/customers/"
                )

                # Build customer dimension
                customer_dim = clean_customers.select(
                    col("customer_id"),
                    col("email"),
                    col("first_name"),
                    col("last_name"),
                    col("phone"),
                    col("address"),
                    col("city"),
                    col("state"),
                    col("country"),
                    col("registration_date"),
                    col("processed_at")
                ).withColumn("customer_key", expr("uuid()"))

                # Write to gold
                customer_dim.write.mode("overwrite").partitionBy("country", "state").parquet(
                    f"{gold_path}/dim_customers/"
                )

                logger.info(f"Processed {customer_dim.count()} customer records")

            # Process sales data
            sales_sources = [
                f"{bronze_path}/snowflake/customer_orders/",
                f"{bronze_path}/stripe/charges/",
                f"{bronze_path}/shopify/orders/"
            ]

            sales_dfs = []
            for source in sales_sources:
                try:
                    df = self.spark.read.parquet(source)
                    sales_dfs.append(df)
                except Exception as e:
                    logger.warning(f"Could not read from {source}: {e}")

            if sales_dfs:
                # Union all sales data
                unified_sales = sales_dfs[0]
                for df in sales_dfs[1:]:
                    unified_sales = unified_sales.union(df)

                # Clean and process
                clean_sales = unified_sales.filter(
                    col("total_amount") > 0 &
                    col("customer_id").isNotNull()
                ).withColumn("processed_at", current_timestamp())

                # Write to silver
                clean_sales.write.mode("append").partitionBy("processed_at").parquet(
                    f"{silver_path}/sales/"
                )

                # Build sales fact table
                sales_fact = clean_sales.join(customer_dim, "customer_id", "left") \
                    .select(
                        col("order_id"),
                        col("customer_id"),
                        col("customer_key"),
                        col("total_amount"),
                        col("order_date"),
                        col("status"),
                        col("payment_method"),
                        col("processed_at")
                    ).withColumn("order_key", expr("uuid()")) \
                    .withColumn("order_year", year(col("order_date"))) \
                    .withColumn("order_month", month(col("order_date")))

                # Write to gold
                sales_fact.write.mode("append").partitionBy("order_year", "order_month").parquet(
                    f"{gold_path}/fact_sales/"
                )

                logger.info(f"Processed {sales_fact.count()} sales records")

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

            # Check sales data quality
            gold_path = self.config['data_lake']['gold_path']
            sales_df = self.spark.read.parquet(f"{gold_path}/fact_sales/")

            # Completeness checks
            completeness = sales_df.select([
                (count(when(col(c).isNull(), c)) / count("*") * 100).alias(f"{c}_null_pct")
                for c in sales_df.columns
            ]).collect()[0]

            # Validity checks
            validity_checks = sales_df.select(
                count(when(col("total_amount") <= 0, True)).alias("invalid_amounts"),
                count(when(col("order_date") > current_date(), True)).alias("future_dates"),
                count(when(col("customer_id").isNull(), True)).alias("missing_customer_ids")
            ).collect()[0]

            # Uniqueness checks
            uniqueness_checks = sales_df.select(
                count("*").alias("total_rows"),
                countDistinct("order_id").alias("unique_orders")
            ).collect()[0]

            quality_results['sales'] = {
                'completeness': completeness,
                'validity': validity_checks,
                'uniqueness': uniqueness_checks
            }

            # Calculate overall quality score
            total_issues = sum([
                validity_checks.invalid_amounts,
                validity_checks.future_dates,
                validity_checks.missing_customer_ids
            ])

            quality_score = max(0, 100 - (total_issues / uniqueness_checks.total_rows * 100))
            self.send_metric("DataQualityScore", quality_score, "Percent")

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("DataQualityCheckTime", processing_time, "Seconds")

            logger.info(f"Data quality checks completed. Score: {quality_score:.2f}%")
            return quality_results

        except Exception as e:
            logger.error(f"Data quality checks failed: {e}")
            self.send_metric("DataQualityCheckErrors", 1)
            raise

    def run_complete_pipeline(self):
        """Run the complete enterprise ETL pipeline."""
        logger.info("Starting complete enterprise ETL pipeline...")
        pipeline_start_time = datetime.now()

        try:
            # 1. External Database Ingestion
            logger.info("=== PHASE 1: EXTERNAL DATABASE INGESTION ===")
            self.ingest_from_snowflake()
            self.ingest_from_postgresql()
            self.ingest_from_mysql()
            self.ingest_from_mongodb()

            # 2. API Ingestion
            logger.info("=== PHASE 2: API INGESTION ===")
            self.ingest_from_salesforce_api()
            self.ingest_from_stripe_api()

            # 3. Streaming Ingestion
            logger.info("=== PHASE 3: STREAMING INGESTION ===")
            self.ingest_from_kafka_streaming()

            # 4. File Ingestion
            logger.info("=== PHASE 4: FILE INGESTION ===")
            self.ingest_from_sftp()

            # 5. Data Warehouse Processing
            logger.info("=== PHASE 5: DATA WAREHOUSE PROCESSING ===")
            self.process_data_warehouse()

            # 6. Data Quality
            logger.info("=== PHASE 6: DATA QUALITY ===")
            self.run_data_quality_checks()

            # Pipeline completion
            pipeline_time = (datetime.now() - pipeline_start_time).total_seconds()
            self.send_metric("TotalPipelineTime", pipeline_time, "Seconds")
            self.send_metric("PipelineSuccess", 1)

            logger.info(f"Complete enterprise ETL pipeline finished successfully in {pipeline_time:.2f} seconds")

        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            self.send_metric("PipelineErrors", 1)
            raise

def main():
    """Main function to run the enterprise ETL pipeline."""
    if len(sys.argv) != 2:
        print("Usage: python aws_enterprise_etl.py <config_path>")
        sys.exit(1)

    config_path = sys.argv[1]

    try:
        # Initialize and run the pipeline
        pipeline = EnterpriseETLPipeline(config_path)
        pipeline.run_complete_pipeline()

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
