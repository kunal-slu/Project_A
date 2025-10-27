#!/usr/bin/env python3
"""
Real-World AWS ETL Pipeline with Separate Data Sources and Storage
This script implements a production ETL pipeline with different data sources
and separate storage locations for each data type.
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

class RealWorldETLPipeline:
    """Real-world ETL pipeline with separate data sources and storage."""

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

    def ingest_ecommerce_data(self):
        """Ingest e-commerce data from separate storage bucket."""
        logger.info("Starting e-commerce data ingestion...")
        start_time = datetime.now()

        try:
            # Get e-commerce storage config
            ecommerce_config = self.config['storage_config']['ecommerce']
            raw_path = ecommerce_config['raw_path']
            processed_path = ecommerce_config['processed_path']

            # Ingest orders data
            orders_source = f"{raw_path}/orders/"
            orders_df = self.spark.read.json(orders_source)
            orders_df = orders_df.withColumn("ingested_at", current_timestamp())
            orders_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/orders/"
            )
            logger.info(f"Ingested {orders_df.count()} orders from {orders_source}")

            # Ingest customers data
            customers_source = f"{raw_path}/customers/"
            customers_df = self.spark.read.csv(customers_source, header=True)
            customers_df = customers_df.withColumn("ingested_at", current_timestamp())
            customers_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/customers/"
            )
            logger.info(f"Ingested {customers_df.count()} customers from {customers_source}")

            # Ingest products data
            products_source = f"{raw_path}/products/"
            products_df = self.spark.read.parquet(products_source)
            products_df = products_df.withColumn("ingested_at", current_timestamp())
            products_df.write.mode("overwrite").partitionBy("category").parquet(
                f"{processed_path}/products/"
            )
            logger.info(f"Ingested {products_df.count()} products from {products_source}")

            # Ingest inventory data
            inventory_source = f"{raw_path}/inventory/"
            inventory_df = self.spark.read.json(inventory_source)
            inventory_df = inventory_df.withColumn("ingested_at", current_timestamp())
            inventory_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/inventory/"
            )
            logger.info(f"Ingested {inventory_df.count()} inventory records from {inventory_source}")

            processing_time = (datetime.now() - start_time).total_seconds()
            total_records = orders_df.count() + customers_df.count() + products_df.count() + inventory_df.count()

            self.send_metric("EcommerceIngestionTime", processing_time, "Seconds")
            self.send_metric("EcommerceRecordsProcessed", total_records)

            logger.info("E-commerce data ingestion completed successfully")

        except Exception as e:
            logger.error(f"E-commerce data ingestion failed: {e}")
            self.send_metric("EcommerceIngestionErrors", 1)
            raise

    def ingest_marketing_data(self):
        """Ingest marketing data from separate storage bucket."""
        logger.info("Starting marketing data ingestion...")
        start_time = datetime.now()

        try:
            # Get marketing storage config
            marketing_config = self.config['storage_config']['marketing']
            raw_path = marketing_config['raw_path']
            processed_path = marketing_config['processed_path']

            # Ingest Google Ads data
            google_ads_source = f"{raw_path}/google-ads/"
            google_ads_df = self.spark.read.csv(google_ads_source, header=True)
            google_ads_df = google_ads_df.withColumn("ingested_at", current_timestamp())
            google_ads_df.write.mode("append").partitionBy("date").parquet(
                f"{processed_path}/google-ads/"
            )
            logger.info(f"Ingested {google_ads_df.count()} Google Ads records from {google_ads_source}")

            # Ingest Facebook Ads data
            facebook_ads_source = f"{raw_path}/facebook-ads/"
            facebook_ads_df = self.spark.read.json(facebook_ads_source)
            facebook_ads_df = facebook_ads_df.withColumn("ingested_at", current_timestamp())
            facebook_ads_df.write.mode("append").partitionBy("date").parquet(
                f"{processed_path}/facebook-ads/"
            )
            logger.info(f"Ingested {facebook_ads_df.count()} Facebook Ads records from {facebook_ads_source}")

            # Ingest LinkedIn Ads data
            linkedin_ads_source = f"{raw_path}/linkedin-ads/"
            linkedin_ads_df = self.spark.read.csv(linkedin_ads_source, header=True)
            linkedin_ads_df = linkedin_ads_df.withColumn("ingested_at", current_timestamp())
            linkedin_ads_df.write.mode("append").partitionBy("date").parquet(
                f"{processed_path}/linkedin-ads/"
            )
            logger.info(f"Ingested {linkedin_ads_df.count()} LinkedIn Ads records from {linkedin_ads_source}")

            # Ingest Email Campaigns data
            email_campaigns_source = f"{raw_path}/email-campaigns/"
            email_campaigns_df = self.spark.read.json(email_campaigns_source)
            email_campaigns_df = email_campaigns_df.withColumn("ingested_at", current_timestamp())
            email_campaigns_df.write.mode("append").partitionBy("date").parquet(
                f"{processed_path}/email-campaigns/"
            )
            logger.info(f"Ingested {email_campaigns_df.count()} Email Campaigns records from {email_campaigns_source}")

            processing_time = (datetime.now() - start_time).total_seconds()
            total_records = google_ads_df.count() + facebook_ads_df.count() + linkedin_ads_df.count() + email_campaigns_df.count()

            self.send_metric("MarketingIngestionTime", processing_time, "Seconds")
            self.send_metric("MarketingRecordsProcessed", total_records)

            logger.info("Marketing data ingestion completed successfully")

        except Exception as e:
            logger.error(f"Marketing data ingestion failed: {e}")
            self.send_metric("MarketingIngestionErrors", 1)
            raise

    def ingest_analytics_data(self):
        """Ingest analytics data from separate storage bucket."""
        logger.info("Starting analytics data ingestion...")
        start_time = datetime.now()

        try:
            # Get analytics storage config
            analytics_config = self.config['storage_config']['analytics']
            raw_path = analytics_config['raw_path']
            processed_path = analytics_config['processed_path']

            # Ingest Google Analytics data
            ga_source = f"{raw_path}/google-analytics/"
            ga_df = self.spark.read.parquet(ga_source)
            ga_df = ga_df.withColumn("ingested_at", current_timestamp())
            ga_df.write.mode("append").partitionBy("event_date").parquet(
                f"{processed_path}/google-analytics/"
            )
            logger.info(f"Ingested {ga_df.count()} Google Analytics events from {ga_source}")

            # Ingest Firebase data
            firebase_source = f"{raw_path}/firebase/"
            firebase_df = self.spark.read.parquet(firebase_source)
            firebase_df = firebase_df.withColumn("ingested_at", current_timestamp())
            firebase_df.write.mode("append").partitionBy("event_date").parquet(
                f"{processed_path}/firebase/"
            )
            logger.info(f"Ingested {firebase_df.count()} Firebase events from {firebase_source}")

            # Ingest Mixpanel data
            mixpanel_source = f"{raw_path}/mixpanel/"
            mixpanel_df = self.spark.read.json(mixpanel_source)
            mixpanel_df = mixpanel_df.withColumn("ingested_at", current_timestamp())
            mixpanel_df.write.mode("append").partitionBy("event_date").parquet(
                f"{processed_path}/mixpanel/"
            )
            logger.info(f"Ingested {mixpanel_df.count()} Mixpanel events from {mixpanel_source}")

            # Ingest Amplitude data
            amplitude_source = f"{raw_path}/amplitude/"
            amplitude_df = self.spark.read.json(amplitude_source)
            amplitude_df = amplitude_df.withColumn("ingested_at", current_timestamp())
            amplitude_df.write.mode("append").partitionBy("event_date").parquet(
                f"{processed_path}/amplitude/"
            )
            logger.info(f"Ingested {amplitude_df.count()} Amplitude events from {amplitude_source}")

            processing_time = (datetime.now() - start_time).total_seconds()
            total_records = ga_df.count() + firebase_df.count() + mixpanel_df.count() + amplitude_df.count()

            self.send_metric("AnalyticsIngestionTime", processing_time, "Seconds")
            self.send_metric("AnalyticsRecordsProcessed", total_records)

            logger.info("Analytics data ingestion completed successfully")

        except Exception as e:
            logger.error(f"Analytics data ingestion failed: {e}")
            self.send_metric("AnalyticsIngestionErrors", 1)
            raise

    def ingest_payment_data(self):
        """Ingest payment data from separate encrypted storage bucket."""
        logger.info("Starting payment data ingestion...")
        start_time = datetime.now()

        try:
            # Get payment storage config
            payment_config = self.config['storage_config']['payments']
            raw_path = payment_config['raw_path']
            processed_path = payment_config['processed_path']

            # Ingest Stripe payments
            stripe_source = f"{raw_path}/stripe/"
            stripe_df = self.spark.read.json(stripe_source)
            stripe_df = stripe_df.withColumn("ingested_at", current_timestamp())
            stripe_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/stripe/"
            )
            logger.info(f"Ingested {stripe_df.count()} Stripe payments from {stripe_source}")

            # Ingest PayPal payments
            paypal_source = f"{raw_path}/paypal/"
            paypal_df = self.spark.read.csv(paypal_source, header=True)
            paypal_df = paypal_df.withColumn("ingested_at", current_timestamp())
            paypal_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/paypal/"
            )
            logger.info(f"Ingested {paypal_df.count()} PayPal payments from {paypal_source}")

            # Ingest Square payments
            square_source = f"{raw_path}/square/"
            square_df = self.spark.read.json(square_source)
            square_df = square_df.withColumn("ingested_at", current_timestamp())
            square_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/square/"
            )
            logger.info(f"Ingested {square_df.count()} Square payments from {square_source}")

            # Ingest Adyen payments
            adyen_source = f"{raw_path}/adyen/"
            adyen_df = self.spark.read.json(adyen_source)
            adyen_df = adyen_df.withColumn("ingested_at", current_timestamp())
            adyen_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/adyen/"
            )
            logger.info(f"Ingested {adyen_df.count()} Adyen payments from {adyen_source}")

            processing_time = (datetime.now() - start_time).total_seconds()
            total_records = stripe_df.count() + paypal_df.count() + square_df.count() + adyen_df.count()

            self.send_metric("PaymentIngestionTime", processing_time, "Seconds")
            self.send_metric("PaymentRecordsProcessed", total_records)

            logger.info("Payment data ingestion completed successfully")

        except Exception as e:
            logger.error(f"Payment data ingestion failed: {e}")
            self.send_metric("PaymentIngestionErrors", 1)
            raise

    def ingest_support_data(self):
        """Ingest customer support data from separate storage bucket."""
        logger.info("Starting support data ingestion...")
        start_time = datetime.now()

        try:
            # Get support storage config
            support_config = self.config['storage_config']['support']
            raw_path = support_config['raw_path']
            processed_path = support_config['processed_path']

            # Ingest Zendesk tickets
            zendesk_source = f"{raw_path}/zendesk/"
            zendesk_df = self.spark.read.json(zendesk_source)
            zendesk_df = zendesk_df.withColumn("ingested_at", current_timestamp())
            zendesk_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/zendesk/"
            )
            logger.info(f"Ingested {zendesk_df.count()} Zendesk tickets from {zendesk_source}")

            # Ingest Intercom conversations
            intercom_source = f"{raw_path}/intercom/"
            intercom_df = self.spark.read.json(intercom_source)
            intercom_df = intercom_df.withColumn("ingested_at", current_timestamp())
            intercom_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/intercom/"
            )
            logger.info(f"Ingested {intercom_df.count()} Intercom conversations from {intercom_source}")

            # Ingest Freshdesk tickets
            freshdesk_source = f"{raw_path}/freshdesk/"
            freshdesk_df = self.spark.read.json(freshdesk_source)
            freshdesk_df = freshdesk_df.withColumn("ingested_at", current_timestamp())
            freshdesk_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/freshdesk/"
            )
            logger.info(f"Ingested {freshdesk_df.count()} Freshdesk tickets from {freshdesk_source}")

            # Ingest Salesforce Service data
            salesforce_source = f"{raw_path}/salesforce-service/"
            salesforce_df = self.spark.read.csv(salesforce_source, header=True)
            salesforce_df = salesforce_df.withColumn("ingested_at", current_timestamp())
            salesforce_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/salesforce-service/"
            )
            logger.info(f"Ingested {salesforce_df.count()} Salesforce Service records from {salesforce_source}")

            processing_time = (datetime.now() - start_time).total_seconds()
            total_records = zendesk_df.count() + intercom_df.count() + freshdesk_df.count() + salesforce_df.count()

            self.send_metric("SupportIngestionTime", processing_time, "Seconds")
            self.send_metric("SupportRecordsProcessed", total_records)

            logger.info("Support data ingestion completed successfully")

        except Exception as e:
            logger.error(f"Support data ingestion failed: {e}")
            self.send_metric("SupportIngestionErrors", 1)
            raise

    def ingest_hr_data(self):
        """Ingest HR data from separate restricted storage bucket."""
        logger.info("Starting HR data ingestion (restricted access)...")
        start_time = datetime.now()

        try:
            # Get HR storage config
            hr_config = self.config['storage_config']['hr']
            raw_path = hr_config['raw_path']
            processed_path = hr_config['processed_path']

            # Ingest Workday data
            workday_source = f"{raw_path}/workday/"
            workday_df = self.spark.read.csv(workday_source, header=True)
            workday_df = workday_df.withColumn("ingested_at", current_timestamp())
            workday_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/workday/"
            )
            logger.info(f"Ingested {workday_df.count()} Workday records from {workday_source}")

            # Ingest BambooHR data
            bamboohr_source = f"{raw_path}/bamboohr/"
            bamboohr_df = self.spark.read.csv(bamboohr_source, header=True)
            bamboohr_df = bamboohr_df.withColumn("ingested_at", current_timestamp())
            bamboohr_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/bamboohr/"
            )
            logger.info(f"Ingested {bamboohr_df.count()} BambooHR records from {bamboohr_source}")

            # Ingest ADP data
            adp_source = f"{raw_path}/adp/"
            adp_df = self.spark.read.csv(adp_source, header=True)
            adp_df = adp_df.withColumn("ingested_at", current_timestamp())
            adp_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/adp/"
            )
            logger.info(f"Ingested {adp_df.count()} ADP records from {adp_source}")

            processing_time = (datetime.now() - start_time).total_seconds()
            total_records = workday_df.count() + bamboohr_df.count() + adp_df.count()

            self.send_metric("HRIngestionTime", processing_time, "Seconds")
            self.send_metric("HRRecordsProcessed", total_records)

            logger.info("HR data ingestion completed successfully")

        except Exception as e:
            logger.error(f"HR data ingestion failed: {e}")
            self.send_metric("HRIngestionErrors", 1)
            raise

    def ingest_finance_data(self):
        """Ingest finance data from separate high-security storage bucket."""
        logger.info("Starting finance data ingestion (high security)...")
        start_time = datetime.now()

        try:
            # Get finance storage config
            finance_config = self.config['storage_config']['finance']
            raw_path = finance_config['raw_path']
            processed_path = finance_config['processed_path']

            # Ingest QuickBooks data
            quickbooks_source = f"{raw_path}/quickbooks/"
            quickbooks_df = self.spark.read.csv(quickbooks_source, header=True)
            quickbooks_df = quickbooks_df.withColumn("ingested_at", current_timestamp())
            quickbooks_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/quickbooks/"
            )
            logger.info(f"Ingested {quickbooks_df.count()} QuickBooks records from {quickbooks_source}")

            # Ingest Xero data
            xero_source = f"{raw_path}/xero/"
            xero_df = self.spark.read.csv(xero_source, header=True)
            xero_df = xero_df.withColumn("ingested_at", current_timestamp())
            xero_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/xero/"
            )
            logger.info(f"Ingested {xero_df.count()} Xero records from {xero_source}")

            # Ingest NetSuite data
            netsuite_source = f"{raw_path}/netsuite/"
            netsuite_df = self.spark.read.csv(netsuite_source, header=True)
            netsuite_df = netsuite_df.withColumn("ingested_at", current_timestamp())
            netsuite_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/netsuite/"
            )
            logger.info(f"Ingested {netsuite_df.count()} NetSuite records from {netsuite_source}")

            # Ingest SAP data
            sap_source = f"{raw_path}/sap/"
            sap_df = self.spark.read.csv(sap_source, header=True)
            sap_df = sap_df.withColumn("ingested_at", current_timestamp())
            sap_df.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{processed_path}/sap/"
            )
            logger.info(f"Ingested {sap_df.count()} SAP records from {sap_source}")

            processing_time = (datetime.now() - start_time).total_seconds()
            total_records = quickbooks_df.count() + xero_df.count() + netsuite_df.count() + sap_df.count()

            self.send_metric("FinanceIngestionTime", processing_time, "Seconds")
            self.send_metric("FinanceRecordsProcessed", total_records)

            logger.info("Finance data ingestion completed successfully")

        except Exception as e:
            logger.error(f"Finance data ingestion failed: {e}")
            self.send_metric("FinanceIngestionErrors", 1)
            raise

    def process_ecommerce_silver(self):
        """Process e-commerce data for silver layer."""
        logger.info("Processing e-commerce data for silver layer...")
        start_time = datetime.now()

        try:
            # Get e-commerce storage config
            ecommerce_config = self.config['storage_config']['ecommerce']
            processed_path = ecommerce_config['processed_path']
            silver_path = self.config['data_lake']['silver_path']

            # Read bronze e-commerce data
            orders_df = self.spark.read.parquet(f"{processed_path}/orders/")
            customers_df = self.spark.read.parquet(f"{processed_path}/customers/")
            products_df = self.spark.read.parquet(f"{processed_path}/products/")

            # Clean and transform orders
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

            # Clean and transform customers
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
            clean_orders.write.mode("append").partitionBy("year", "month", "day").parquet(
                f"{silver_path}/ecommerce/orders/"
            )
            clean_customers.write.mode("overwrite").partitionBy("country", "state").parquet(
                f"{silver_path}/ecommerce/customers/"
            )

            processing_time = (datetime.now() - start_time).total_seconds()
            total_records = clean_orders.count() + clean_customers.count()

            self.send_metric("EcommerceProcessingTime", processing_time, "Seconds")
            self.send_metric("EcommerceProcessed", total_records)

            logger.info("E-commerce data processing completed successfully")

        except Exception as e:
            logger.error(f"E-commerce data processing failed: {e}")
            self.send_metric("EcommerceProcessingErrors", 1)
            raise

    def build_sales_fact_table(self):
        """Build sales fact table for gold layer."""
        logger.info("Building sales fact table...")
        start_time = datetime.now()

        try:
            # Read silver data
            silver_path = self.config['data_lake']['silver_path']
            gold_path = self.config['data_lake']['gold_path']

            orders_df = self.spark.read.parquet(f"{silver_path}/ecommerce/orders/")
            customers_df = self.spark.read.parquet(f"{silver_path}/ecommerce/customers/")

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
                f"{gold_path}/ecommerce/fact_sales/"
            )

            processing_time = (datetime.now() - start_time).total_seconds()
            total_records = sales_fact.count()

            self.send_metric("SalesFactBuildTime", processing_time, "Seconds")
            self.send_metric("SalesFactRecords", total_records)

            logger.info("Sales fact table built successfully")

        except Exception as e:
            logger.error(f"Sales fact table build failed: {e}")
            self.send_metric("SalesFactBuildErrors", 1)
            raise

    def build_marketing_attribution(self):
        """Build marketing attribution model."""
        logger.info("Building marketing attribution model...")
        start_time = datetime.now()

        try:
            # Read analytics and marketing data
            silver_path = self.config['data_lake']['silver_path']
            gold_path = self.config['data_lake']['gold_path']

            # Read analytics sessions
            analytics_config = self.config['storage_config']['analytics']
            analytics_processed = analytics_config['processed_path']

            sessions_df = self.spark.read.parquet(f"{analytics_processed}/google-analytics/")
            marketing_df = self.spark.read.parquet(f"{silver_path}/marketing/google-ads/")
            orders_df = self.spark.read.parquet(f"{silver_path}/ecommerce/orders/")

            # Build attribution model
            attribution_df = sessions_df.join(marketing_df,
                sessions_df.event_date == marketing_df.date, "left") \
                .join(orders_df,
                    sessions_df.user_pseudo_id == orders_df.customer_id, "left") \
                .select(
                    col("user_pseudo_id"),
                    col("session_id"),
                    col("event_date").alias("session_date"),
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
                f"{gold_path}/marketing/attribution/"
            )

            processing_time = (datetime.now() - start_time).total_seconds()
            total_records = attribution_df.count()

            self.send_metric("MarketingAttributionBuildTime", processing_time, "Seconds")
            self.send_metric("MarketingAttributionRecords", total_records)

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
            gold_path = self.config['data_lake']['gold_path']
            sales_df = self.spark.read.parquet(f"{gold_path}/ecommerce/fact_sales/")

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
        """Run the complete ETL pipeline with separate data sources."""
        logger.info("Starting complete real-world ETL pipeline...")
        pipeline_start_time = datetime.now()

        try:
            # 1. Data Ingestion from separate sources
            logger.info("=== PHASE 1: DATA INGESTION FROM SEPARATE SOURCES ===")
            self.ingest_ecommerce_data()
            self.ingest_marketing_data()
            self.ingest_analytics_data()
            self.ingest_payment_data()
            self.ingest_support_data()
            self.ingest_hr_data()
            self.ingest_finance_data()

            # 2. Data Processing
            logger.info("=== PHASE 2: DATA PROCESSING ===")
            self.process_ecommerce_silver()

            # 3. Data Warehouse
            logger.info("=== PHASE 3: DATA WAREHOUSE ===")
            self.build_sales_fact_table()
            self.build_marketing_attribution()

            # 4. Data Quality
            logger.info("=== PHASE 4: DATA QUALITY ===")
            self.run_data_quality_checks()

            # Pipeline completion
            pipeline_time = (datetime.now() - pipeline_start_time).total_seconds()
            self.send_metric("TotalPipelineTime", pipeline_time, "Seconds")
            self.send_metric("PipelineSuccess", 1)

            logger.info(f"Complete real-world ETL pipeline finished successfully in {pipeline_time:.2f} seconds")

        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            self.send_metric("PipelineErrors", 1)
            raise

def main():
    """Main function to run the real-world ETL pipeline."""
    if len(sys.argv) != 2:
        print("Usage: python aws_real_world_etl.py <config_path>")
        sys.exit(1)

    config_path = sys.argv[1]

    try:
        # Initialize and run the pipeline
        pipeline = RealWorldETLPipeline(config_path)
        pipeline.run_complete_pipeline()

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
