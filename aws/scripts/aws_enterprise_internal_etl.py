#!/usr/bin/env python3
"""
Enterprise AWS ETL Pipeline with 1 External + 2 Internal Sources
This script implements a complete production ETL pipeline that ingests data from:
1. External: Snowflake Data Warehouse
2. Internal: PostgreSQL Production Database
3. Internal: Apache Kafka Streaming Platform
"""

import sys
import os
import logging
import yaml
import boto3
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import snowflake.connector
import psycopg2
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EnterpriseInternalETLPipeline:
    """Enterprise ETL pipeline with 1 external + 2 internal data sources."""

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

    def ingest_from_snowflake_external(self):
        """Ingest data from external Snowflake data warehouse."""
        logger.info("Starting EXTERNAL Snowflake data ingestion...")
        start_time = datetime.now()

        try:
            snowflake_config = self.config['data_sources']['external']['snowflake']

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
                description = table_config['description']

                logger.info(f"Ingesting EXTERNAL {table_name}: {description}")

                # Read data from Snowflake
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, conn)

                if not df.empty:
                    # Convert to Spark DataFrame
                    spark_df = self.spark.createDataFrame(df)
                    spark_df = spark_df.withColumn("ingested_at", current_timestamp())
                    spark_df = spark_df.withColumn("source_system", lit("snowflake_external"))
                    spark_df = spark_df.withColumn("table_name", lit(table_name))
                    spark_df = spark_df.withColumn("data_source_type", lit("external"))

                    # Write to S3
                    storage_config = self.config['storage_config']
                    path = f"{storage_config['external']['processed_path']}/snowflake/{table_name}/"
                    spark_df.write.mode("append").partitionBy("ingested_at").parquet(path)
                    total_records += spark_df.count()
                    logger.info(f"Ingested {spark_df.count()} records from EXTERNAL Snowflake table: {table_name}")
                else:
                    logger.warning(f"No data found in EXTERNAL Snowflake table: {table_name}")

            conn.close()

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("ExternalSnowflakeIngestionTime", processing_time, "Seconds")
            self.send_metric("ExternalSnowflakeRecordsProcessed", total_records)

            logger.info("EXTERNAL Snowflake data ingestion completed successfully")

        except Exception as e:
            logger.error(f"EXTERNAL Snowflake data ingestion failed: {e}")
            self.send_metric("ExternalSnowflakeIngestionErrors", 1)
            raise

    def ingest_from_postgresql_internal(self):
        """Ingest data from internal PostgreSQL database."""
        logger.info("Starting INTERNAL PostgreSQL data ingestion...")
        start_time = datetime.now()

        try:
            postgres_config = self.config['data_sources']['internal']['postgresql']

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
                description = table_config['description']

                logger.info(f"Ingesting INTERNAL {table_name}: {description}")

                # Read data from PostgreSQL
                query = f"SELECT * FROM {schema}.{table_name}"
                df = pd.read_sql(query, conn)

                if not df.empty:
                    # Convert to Spark DataFrame
                    spark_df = self.spark.createDataFrame(df)
                    spark_df = spark_df.withColumn("ingested_at", current_timestamp())
                    spark_df = spark_df.withColumn("source_system", lit("postgresql_internal"))
                    spark_df = spark_df.withColumn("table_name", lit(table_name))
                    spark_df = spark_df.withColumn("data_source_type", lit("internal"))

                    # Write to S3
                    storage_config = self.config['storage_config']
                    path = f"{storage_config['internal']['processed_path']}/postgresql/{table_name}/"
                    spark_df.write.mode("append").partitionBy("ingested_at").parquet(path)
                    total_records += spark_df.count()
                    logger.info(f"Ingested {spark_df.count()} records from INTERNAL PostgreSQL table: {table_name}")
                else:
                    logger.warning(f"No data found in INTERNAL PostgreSQL table: {table_name}")

            conn.close()

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("InternalPostgreSQLIngestionTime", processing_time, "Seconds")
            self.send_metric("InternalPostgreSQLRecordsProcessed", total_records)

            logger.info("INTERNAL PostgreSQL data ingestion completed successfully")

        except Exception as e:
            logger.error(f"INTERNAL PostgreSQL data ingestion failed: {e}")
            self.send_metric("InternalPostgreSQLIngestionErrors", 1)
            raise

    def ingest_from_kafka_internal(self):
        """Ingest data from internal Kafka streaming platform."""
        logger.info("Starting INTERNAL Kafka streaming ingestion...")
        start_time = datetime.now()

        try:
            kafka_config = self.config['data_sources']['internal_streaming']['kafka']

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
                group_id='etl-internal-consumer-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )

            total_records = 0
            records_buffer = []

            # Consume messages for a limited time (3 minutes for demo)
            end_time = datetime.now() + timedelta(minutes=3)

            logger.info("Starting INTERNAL Kafka message consumption...")

            for message in consumer:
                if datetime.now() > end_time:
                    logger.info("INTERNAL Kafka consumption time limit reached")
                    break

                topic = message.topic
                data = message.value
                data['topic'] = topic
                data['timestamp'] = message.timestamp
                data['partition'] = message.partition
                data['offset'] = message.offset
                records_buffer.append(data)

                # Process in batches
                if len(records_buffer) >= 500:
                    # Convert to Spark DataFrame
                    df = pd.DataFrame(records_buffer)
                    spark_df = self.spark.createDataFrame(df)
                    spark_df = spark_df.withColumn("ingested_at", current_timestamp())
                    spark_df = spark_df.withColumn("source_system", lit("kafka_internal"))
                    spark_df = spark_df.withColumn("data_source_type", lit("internal_streaming"))

                    # Write to S3
                    storage_config = self.config['storage_config']
                    path = f"{storage_config['streaming']['processed_path']}/kafka/{topic}/"
                    spark_df.write.mode("append").partitionBy("ingested_at").parquet(path)
                    total_records += spark_df.count()
                    logger.info(f"Ingested {spark_df.count()} records from INTERNAL Kafka topic: {topic}")

                    records_buffer = []

            # Process remaining records
            if records_buffer:
                df = pd.DataFrame(records_buffer)
                spark_df = self.spark.createDataFrame(df)
                spark_df = spark_df.withColumn("ingested_at", current_timestamp())
                spark_df = spark_df.withColumn("source_system", lit("kafka_internal"))
                spark_df = spark_df.withColumn("data_source_type", lit("internal_streaming"))

                # Group by topic and write
                for topic in spark_df.select("topic").distinct().collect():
                    topic_name = topic.topic
                    topic_df = spark_df.filter(col("topic") == topic_name)
                    path = f"{storage_config['streaming']['processed_path']}/kafka/{topic_name}/"
                    topic_df.write.mode("append").partitionBy("ingested_at").parquet(path)
                    total_records += topic_df.count()
                    logger.info(f"Ingested {topic_df.count()} records from INTERNAL Kafka topic: {topic_name}")

            consumer.close()

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("InternalKafkaIngestionTime", processing_time, "Seconds")
            self.send_metric("InternalKafkaRecordsProcessed", total_records)

            logger.info("INTERNAL Kafka streaming ingestion completed successfully")

        except Exception as e:
            logger.error(f"INTERNAL Kafka streaming ingestion failed: {e}")
            self.send_metric("InternalKafkaIngestionErrors", 1)
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

            # Process customer data from external and internal sources
            customer_sources = [
                f"{bronze_path}/external/snowflake/customer_orders/",
                f"{bronze_path}/internal/postgresql/users/"
            ]

            customer_dfs = []
            for source in customer_sources:
                try:
                    df = self.spark.read.parquet(source)
                    customer_dfs.append(df)
                    logger.info(f"Loaded customer data from {source}")
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

            # Process sales data from external source
            sales_sources = [
                f"{bronze_path}/external/snowflake/customer_orders/"
            ]

            sales_dfs = []
            for source in sales_sources:
                try:
                    df = self.spark.read.parquet(source)
                    sales_dfs.append(df)
                    logger.info(f"Loaded sales data from {source}")
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

            # Process internal user data
            user_sources = [
                f"{bronze_path}/internal/postgresql/users/",
                f"{bronze_path}/internal/postgresql/subscriptions/"
            ]

            user_dfs = []
            for source in user_sources:
                try:
                    df = self.spark.read.parquet(source)
                    user_dfs.append(df)
                    logger.info(f"Loaded user data from {source}")
                except Exception as e:
                    logger.warning(f"Could not read from {source}: {e}")

            if user_dfs:
                # Union all user data
                unified_users = user_dfs[0]
                for df in user_dfs[1:]:
                    unified_users = unified_users.union(df)

                # Clean and process
                clean_users = unified_users.dropDuplicates(['user_id', 'email']) \
                    .filter(col("user_id").isNotNull()) \
                    .withColumn("processed_at", current_timestamp())

                # Write to silver
                clean_users.write.mode("overwrite").partitionBy("processed_at").parquet(
                    f"{silver_path}/users/"
                )

                # Build user dimension
                user_dim = clean_users.select(
                    col("user_id"),
                    col("email"),
                    col("first_name"),
                    col("last_name"),
                    col("phone"),
                    col("address"),
                    col("city"),
                    col("state"),
                    col("country"),
                    col("registration_date"),
                    col("subscription_status"),
                    col("processed_at")
                ).withColumn("user_key", expr("uuid()"))

                # Write to gold
                user_dim.write.mode("overwrite").partitionBy("country", "state").parquet(
                    f"{gold_path}/dim_users/"
                )

                logger.info(f"Processed {user_dim.count()} user records")

            # Process streaming data from internal Kafka
            streaming_sources = [
                f"{bronze_path}/streaming/kafka/user_events/",
                f"{bronze_path}/streaming/kafka/order_events/",
                f"{bronze_path}/streaming/kafka/inventory_updates/"
            ]

            for source in streaming_sources:
                try:
                    df = self.spark.read.parquet(source)
                    if not df.rdd.isEmpty():
                        # Process streaming data
                        clean_streaming = df.filter(col("timestamp").isNotNull()) \
                            .withColumn("processed_at", current_timestamp())

                        # Write to silver
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

            # Check sales data quality
            gold_path = self.config['data_lake']['gold_path']

            try:
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
        """Run the complete enterprise ETL pipeline with 1 external + 2 internal sources."""
        logger.info("Starting complete enterprise ETL pipeline with 1 external + 2 internal sources...")
        pipeline_start_time = datetime.now()

        try:
            # 1. External Source - Snowflake Data Warehouse Ingestion
            logger.info("=== PHASE 1: EXTERNAL SNOWFLAKE DATA WAREHOUSE INGESTION ===")
            self.ingest_from_snowflake_external()

            # 2. Internal Source - PostgreSQL Database Ingestion
            logger.info("=== PHASE 2: INTERNAL POSTGRESQL DATABASE INGESTION ===")
            self.ingest_from_postgresql_internal()

            # 3. Internal Source - Kafka Streaming Ingestion
            logger.info("=== PHASE 3: INTERNAL KAFKA STREAMING INGESTION ===")
            self.ingest_from_kafka_internal()

            # 4. Data Warehouse Processing
            logger.info("=== PHASE 4: DATA WAREHOUSE PROCESSING ===")
            self.process_data_warehouse()

            # 5. Data Quality
            logger.info("=== PHASE 5: DATA QUALITY ===")
            self.run_data_quality_checks()

            # Pipeline completion
            pipeline_time = (datetime.now() - pipeline_start_time).total_seconds()
            self.send_metric("TotalPipelineTime", pipeline_time, "Seconds")
            self.send_metric("PipelineSuccess", 1)

            logger.info(f"Complete enterprise ETL pipeline finished successfully in {pipeline_time:.2f} seconds")
            logger.info("✅ Successfully processed data from 3 sources:")
            logger.info("   📊 EXTERNAL: Snowflake Data Warehouse")
            logger.info("   🗄️ INTERNAL: PostgreSQL Production Database")
            logger.info("   🌊 INTERNAL: Apache Kafka Streaming Platform")

        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            self.send_metric("PipelineErrors", 1)
            raise

def main():
    """Main function to run the enterprise ETL pipeline."""
    if len(sys.argv) != 2:
        print("Usage: python aws_enterprise_internal_etl.py <config_path>")
        sys.exit(1)

    config_path = sys.argv[1]

    try:
        # Initialize and run the pipeline
        pipeline = EnterpriseInternalETLPipeline(config_path)
        pipeline.run_complete_pipeline()

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
