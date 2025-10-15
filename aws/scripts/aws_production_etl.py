from pyspark_interview_project.pipeline import run_pipeline
#!/usr/bin/env python3
"""
Production AWS ETL Pipeline for Real-World Data Sources
This script implements a complete ETL pipeline for production use with real data sources.
"""

import sys
import logging
import yaml
import boto3
from datetime import datetime
from typing import Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProductionETLPipeline:
    """Production ETL pipeline for real-world data sources."""

    def __init__(self, config_path: str):
        """Initialize the ETL pipeline with configuration."""
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
        self.cloudwatch = boto3.client('cloudwatch')

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

    def ingest_ecommerce_data(self):
        """Ingest e-commerce data from multiple sources."""
        logger.info("Starting e-commerce data ingestion...")
        start_time = datetime.now()

        try:
            # Ingest orders data
            orders_source = self.config['data_sources']['ecommerce']['orders']['source']
            orders_df = self.spark.read.json(orders_source)
            orders_df = orders_df.withColumn("ingested_at", current_timestamp())
            orders_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{self.config['data_lake']['bronze_path']}/orders/"
            )
            logger.info(f"Ingested {orders_df.count()} orders")

            # Ingest customers data
            customers_source = self.config['data_sources']['ecommerce']['customers']['source']
            customers_df = self.spark.read.csv(customers_source, header=True)
            customers_df = customers_df.withColumn("ingested_at", current_timestamp())
            customers_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(
                f"{self.config['data_lake']['bronze_path']}/customers/"
            )
            logger.info(f"Ingested {customers_df.count()} customers")

            # Ingest products data
            products_source = self.config['data_sources']['ecommerce']['products']['source']
            products_df = self.spark.read.parquet(products_source)
            products_df = products_df.withColumn("ingested_at", current_timestamp())
            products_df.write.mode("overwrite").partitionBy("category").parquet(
                f"{self.config['data_lake']['bronze_path']}/products/"
            )
            logger.info(f"Ingested {products_df.count()} products")

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("EcommerceIngestionTime", processing_time, "Seconds")
            self.send_metric("EcommerceRecordsProcessed", orders_df.count() + customers_df.count() + products_df.count())

            logger.info("E-commerce data ingestion completed successfully")

        except Exception as e:
            logger.error(f"E-commerce data ingestion failed: {e}")
            self.send_metric("EcommerceIngestionErrors", 1)
            raise

    def ingest_payment_data(self):
        """Ingest payment processing data."""
        logger.info("Starting payment data ingestion...")
        start_time = datetime.now()

        try:
            # Ingest Stripe payments
            stripe_source = self.config['data_sources']['payments']['stripe']['source']
            stripe_df = self.spark.read.json(stripe_source)
            stripe_df = stripe_df.withColumn("ingested_at", current_timestamp())
            stripe_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{self.config['data_lake']['bronze_path']}/payments/stripe/"
            )
            logger.info(f"Ingested {stripe_df.count()} Stripe payments")

            # Ingest PayPal payments
            paypal_source = self.config['data_sources']['payments']['paypal']['source']
            paypal_df = self.spark.read.csv(paypal_source, header=True)
            paypal_df = paypal_df.withColumn("ingested_at", current_timestamp())
            paypal_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{self.config['data_lake']['bronze_path']}/payments/paypal/"
            )
            logger.info(f"Ingested {paypal_df.count()} PayPal payments")

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("PaymentIngestionTime", processing_time, "Seconds")
            self.send_metric("PaymentRecordsProcessed", stripe_df.count() + paypal_df.count())

            logger.info("Payment data ingestion completed successfully")

        except Exception as e:
            logger.error(f"Payment data ingestion failed: {e}")
            self.send_metric("PaymentIngestionErrors", 1)
            raise

    def ingest_analytics_data(self):
        """Ingest analytics data from various platforms."""
        logger.info("Starting analytics data ingestion...")
        start_time = datetime.now()

        try:
            # Ingest Google Analytics 4 data
            ga4_source = self.config['data_sources']['analytics']['google_analytics']['source']
            ga4_df = self.spark.read.parquet(ga4_source)
            ga4_df = ga4_df.withColumn("ingested_at", current_timestamp())
            ga4_df.write.mode("append").partitionBy("event_date").parquet(
                f"{self.config['data_lake']['bronze_path']}/analytics/ga4/"
            )
            logger.info(f"Ingested {ga4_df.count()} GA4 events")

            # Ingest Firebase Analytics data
            firebase_source = self.config['data_sources']['analytics']['firebase']['source']
            firebase_df = self.spark.read.parquet(firebase_source)
            firebase_df = firebase_df.withColumn("ingested_at", current_timestamp())
            firebase_df.write.mode("append").partitionBy("event_date").parquet(
                f"{self.config['data_lake']['bronze_path']}/analytics/firebase/"
            )
            logger.info(f"Ingested {firebase_df.count()} Firebase events")

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("AnalyticsIngestionTime", processing_time, "Seconds")
            self.send_metric("AnalyticsRecordsProcessed", ga4_df.count() + firebase_df.count())

            logger.info("Analytics data ingestion completed successfully")

        except Exception as e:
            logger.error(f"Analytics data ingestion failed: {e}")
            self.send_metric("AnalyticsIngestionErrors", 1)
            raise

    def ingest_marketing_data(self):
        """Ingest marketing campaign data."""
        logger.info("Starting marketing data ingestion...")
        start_time = datetime.now()

        try:
            # Ingest Google Ads data
            google_ads_source = self.config['data_sources']['marketing']['google_ads']['source']
            google_ads_df = self.spark.read.csv(google_ads_source, header=True)
            google_ads_df = google_ads_df.withColumn("ingested_at", current_timestamp())
            google_ads_df.write.mode("append").partitionBy("date").parquet(
                f"{self.config['data_lake']['bronze_path']}/marketing/google_ads/"
            )
            logger.info(f"Ingested {google_ads_df.count()} Google Ads records")

            # Ingest Facebook Ads data
            facebook_ads_source = self.config['data_sources']['marketing']['facebook_ads']['source']
            facebook_ads_df = self.spark.read.json(facebook_ads_source)
            facebook_ads_df = facebook_ads_df.withColumn("ingested_at", current_timestamp())
            facebook_ads_df.write.mode("append").partitionBy("date").parquet(
                f"{self.config['data_lake']['bronze_path']}/marketing/facebook_ads/"
            )
            logger.info(f"Ingested {facebook_ads_df.count()} Facebook Ads records")

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("MarketingIngestionTime", processing_time, "Seconds")
            self.send_metric("MarketingRecordsProcessed", google_ads_df.count() + facebook_ads_df.count())

            logger.info("Marketing data ingestion completed successfully")

        except Exception as e:
            logger.error(f"Marketing data ingestion failed: {e}")
            self.send_metric("MarketingIngestionErrors", 1)
            raise

    def ingest_support_data(self):
        """Ingest customer support data."""
        logger.info("Starting support data ingestion...")
        start_time = datetime.now()

        try:
            # Ingest Zendesk tickets
            zendesk_source = self.config['data_sources']['support']['zendesk']['source']
            zendesk_df = self.spark.read.json(zendesk_source)
            zendesk_df = zendesk_df.withColumn("ingested_at", current_timestamp())
            zendesk_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{self.config['data_lake']['bronze_path']}/support/zendesk/"
            )
            logger.info(f"Ingested {zendesk_df.count()} Zendesk tickets")

            # Ingest Intercom conversations
            intercom_source = self.config['data_sources']['support']['intercom']['source']
            intercom_df = self.spark.read.json(intercom_source)
            intercom_df = intercom_df.withColumn("ingested_at", current_timestamp())
            intercom_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{self.config['data_lake']['bronze_path']}/support/intercom/"
            )
            logger.info(f"Ingested {intercom_df.count()} Intercom conversations")

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("SupportIngestionTime", processing_time, "Seconds")
            self.send_metric("SupportRecordsProcessed", zendesk_df.count() + intercom_df.count())

            logger.info("Support data ingestion completed successfully")

        except Exception as e:
            logger.error(f"Support data ingestion failed: {e}")
            self.send_metric("SupportIngestionErrors", 1)
            raise

    def process_orders_silver(self):
        """Process and clean order data for silver layer."""
        logger.info("Processing orders for silver layer...")
        start_time = datetime.now()

        try:
            # Read bronze orders
            orders_df = self.spark.read.parquet(f"{self.config['data_lake']['bronze_path']}/orders/")

            # Clean and transform
            clean_orders = orders_df.select(
                col("order_id"),
                col("customer_id"),
                col("order_date"),
                col("total_amount"),
                col("status"),
                col("payment_method"),
                col("shipping_address"),
                col("items"),
                col("ingested_at")
            ).filter(
                col("order_id").isNotNull() &
                col("customer_id").isNotNull() &
                col("total_amount") > 0
            ).withColumn("processed_at", current_timestamp())

            # Write to silver
            clean_orders.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{self.config['data_lake']['silver_path']}/orders/"
            )

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("OrdersProcessingTime", processing_time, "Seconds")
            self.send_metric("OrdersProcessed", clean_orders.count())

            logger.info("Orders processing completed successfully")

        except Exception as e:
            logger.error(f"Orders processing failed: {e}")
            self.send_metric("OrdersProcessingErrors", 1)
            raise

    def process_customers_silver(self):
        """Process and enrich customer data for silver layer."""
        logger.info("Processing customers for silver layer...")
        start_time = datetime.now()

        try:
            # Read bronze customers
            customers_df = self.spark.read.parquet(f"{self.config['data_lake']['bronze_path']}/customers/")

            # Clean and enrich
            clean_customers = customers_df.select(
                col("customer_id"),
                concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
                col("email"),
                col("phone"),
                col("address"),
                col("city"),
                col("state"),
                col("country"),
                col("zip_code"),
                col("registration_date"),
                col("ingested_at")
            ).filter(
                col("customer_id").isNotNull() &
                col("email").isNotNull()
            ).withColumn("processed_at", current_timestamp())

            # Write to silver
            clean_customers.write.mode("overwrite").partitionBy("country", "state").parquet(
                f"{self.config['data_lake']['silver_path']}/customers/"
            )

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("CustomersProcessingTime", processing_time, "Seconds")
            self.send_metric("CustomersProcessed", clean_customers.count())

            logger.info("Customers processing completed successfully")

        except Exception as e:
            logger.error(f"Customers processing failed: {e}")
            self.send_metric("CustomersProcessingErrors", 1)
            raise

    def process_analytics_silver(self):
        """Process analytics data for silver layer."""
        logger.info("Processing analytics for silver layer...")
        start_time = datetime.now()

        try:
            # Process GA4 data
            ga4_df = self.spark.read.parquet(f"{self.config['data_lake']['bronze_path']}/analytics/ga4/")

            # Aggregate user sessions
            sessions_df = ga4_df.groupBy(
                "user_pseudo_id",
                "session_id",
                date_trunc("day", col("event_timestamp")).alias("session_date")
            ).agg(
                count("*").alias("event_count"),
                min("event_timestamp").alias("session_start"),
                max("event_timestamp").alias("session_end"),
                collect_list("event_name").alias("events")
            ).withColumn("session_duration",
                unix_timestamp(col("session_end")) - unix_timestamp(col("session_start"))
            ).withColumn("processed_at", current_timestamp())

            # Write to silver
            sessions_df.write.mode("append").partitionBy("session_date").parquet(
                f"{self.config['data_lake']['silver_path']}/analytics/sessions/"
            )

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("AnalyticsProcessingTime", processing_time, "Seconds")
            self.send_metric("AnalyticsSessionsProcessed", sessions_df.count())

            logger.info("Analytics processing completed successfully")

        except Exception as e:
            logger.error(f"Analytics processing failed: {e}")
            self.send_metric("AnalyticsProcessingErrors", 1)
            raise

    def build_sales_fact_table(self):
        """Build sales fact table for gold layer."""
        logger.info("Building sales fact table...")
        start_time = datetime.now()

        try:
            # Read silver data
            orders_df = self.spark.read.parquet(f"{self.config['data_lake']['silver_path']}/orders/")
            customers_df = self.spark.read.parquet(f"{self.config['data_lake']['silver_path']}/customers/")

            # Build sales fact table
            sales_fact = orders_df.join(customers_df, "customer_id", "left") \
                .select(
                    col("order_id"),
                    col("customer_id"),
                    col("full_name").alias("customer_name"),
                    col("order_date"),
                    col("total_amount"),
                    col("status"),
                    col("payment_method"),
                    col("country"),
                    col("state"),
                    col("city"),
                    col("processed_at")
                ).withColumn("order_year", year(col("order_date"))) \
                .withColumn("order_month", month(col("order_date"))) \
                .withColumn("order_day", dayofmonth(col("order_date")))

            # Write to gold
            sales_fact.write.mode("append").partitionBy("order_year", "order_month").parquet(
                f"{self.config['data_lake']['gold_path']}/fact_sales/"
            )

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("SalesFactBuildTime", processing_time, "Seconds")
            self.send_metric("SalesFactRecords", sales_fact.count())

            logger.info("Sales fact table built successfully")

        except Exception as e:
            logger.error(f"Sales fact table build failed: {e}")
            self.send_metric("SalesFactBuildErrors", 1)
            raise

    def build_customer_dimension(self):
        """Build customer dimension table with SCD Type 2."""
        logger.info("Building customer dimension table...")
        start_time = datetime.now()

        try:
            # Read silver customers
            customers_df = self.spark.read.parquet(f"{self.config['data_lake']['silver_path']}/customers/")

            # Add SCD Type 2 columns
            customer_dim = customers_df.withColumn("effective_from", col("processed_at")) \
                .withColumn("effective_to", lit(None).cast("timestamp")) \
                .withColumn("is_current", lit(True)) \
                .withColumn("surrogate_key", expr("uuid()"))

            # Write to gold
            customer_dim.write.mode("overwrite").parquet(
                f"{self.config['data_lake']['gold_path']}/dim_customers/"
            )

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("CustomerDimBuildTime", processing_time, "Seconds")
            self.send_metric("CustomerDimRecords", customer_dim.count())

            logger.info("Customer dimension table built successfully")

        except Exception as e:
            logger.error(f"Customer dimension table build failed: {e}")
            self.send_metric("CustomerDimBuildErrors", 1)
            raise

    def build_marketing_attribution(self):
        """Build marketing attribution model."""
        logger.info("Building marketing attribution model...")
        start_time = datetime.now()

        try:
            # Read analytics data
            sessions_df = self.spark.read.parquet(f"{self.config['data_lake']['silver_path']}/analytics/sessions/")
            marketing_df = self.spark.read.parquet(f"{self.config['data_lake']['bronze_path']}/marketing/google_ads/")
            orders_df = self.spark.read.parquet(f"{self.config['data_lake']['silver_path']}/orders/")

            # Join sessions with marketing data
            attribution_df = sessions_df.join(marketing_df,
                sessions_df.session_date == marketing_df.date, "left") \
                .join(orders_df,
                    sessions_df.user_pseudo_id == orders_df.customer_id, "left") \
                .select(
                    col("user_pseudo_id"),
                    col("session_id"),
                    col("session_date"),
                    col("campaign_name"),
                    col("ad_group"),
                    col("keyword"),
                    col("clicks"),
                    col("impressions"),
                    col("cost"),
                    col("order_id"),
                    col("total_amount"),
                    col("processed_at")
                ).withColumn("conversion", col("order_id").isNotNull()) \
                .withColumn("attribution_date", current_date())

            # Write to gold
            attribution_df.write.mode("append").partitionBy("session_date").parquet(
                f"{self.config['data_lake']['gold_path']}/marketing_attribution/"
            )

            processing_time = (datetime.now() - start_time).total_seconds()
            self.send_metric("MarketingAttributionBuildTime", processing_time, "Seconds")
            self.send_metric("MarketingAttributionRecords", attribution_df.count())

            logger.info("Marketing attribution model built successfully")

        except Exception as e:
            logger.error(f"Marketing attribution model build failed: {e}")
            self.send_metric("MarketingAttributionBuildErrors", 1)
            raise

    def run_data_quality_checks(self):
        """Run comprehensive data quality checks."""
        logger.info("Running data quality checks...")
        start_time = datetime.now()

        try:
            quality_results = {}

            # Check sales data quality
            sales_df = self.spark.read.parquet(f"{self.config['data_lake']['gold_path']}/fact_sales/")

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
        """Run the complete ETL pipeline."""
        logger.info("Starting complete ETL pipeline...")
        pipeline_start_time = datetime.now()

        try:
            # 1. Data Ingestion
            logger.info("=== PHASE 1: DATA INGESTION ===")
            self.ingest_ecommerce_data()
            self.ingest_payment_data()
            self.ingest_analytics_data()
            self.ingest_marketing_data()
            self.ingest_support_data()

            # 2. Data Processing
            logger.info("=== PHASE 2: DATA PROCESSING ===")
            self.process_orders_silver()
            self.process_customers_silver()
            self.process_analytics_silver()

            # 3. Data Warehouse
            logger.info("=== PHASE 3: DATA WAREHOUSE ===")
            self.build_sales_fact_table()
            self.build_customer_dimension()
            self.build_marketing_attribution()

            # 4. Data Quality
            logger.info("=== PHASE 4: DATA QUALITY ===")
            self.run_data_quality_checks()

            # Pipeline completion
            pipeline_time = (datetime.now() - pipeline_start_time).total_seconds()
            self.send_metric("TotalPipelineTime", pipeline_time, "Seconds")
            self.send_metric("PipelineSuccess", 1)

            logger.info(f"Complete ETL pipeline finished successfully in {pipeline_time:.2f} seconds")

        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            self.send_metric("PipelineErrors", 1)
            raise

def main():
    """Main function to run the production ETL pipeline."""
    if len(sys.argv) != 2:
        print("Usage: python aws_production_etl.py <config_path>")
        sys.exit(1)

    config_path = sys.argv[1]

    try:
        # Initialize and run the pipeline
        pipeline = ProductionETLPipeline(config_path)
        pipeline.run_complete_pipeline()

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
