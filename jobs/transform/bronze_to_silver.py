"""
Bronze to Silver Transformation Job

Transforms data from Bronze layer to Silver layer with cleaning and standardization.
"""
import logging
from typing import Dict, Any
from project_a.core.base_job import BaseJob
from project_a.core.config import ProjectConfig


logger = logging.getLogger(__name__)


class BronzeToSilverJob(BaseJob):
    """Job to transform data from Bronze to Silver layer."""
    
    def __init__(self, config: ProjectConfig):
        super().__init__(config)
        self.job_name = "bronze_to_silver"
    
    def run(self, ctx) -> Dict[str, Any]:
        """Execute the Bronze to Silver transformation."""
        logger.info("Starting Bronze to Silver transformation...")
        
        try:
            # Get Spark session from context
            spark = ctx.spark
            
            # Get bronze and silver paths from config
            bronze_path = self.config.get('paths', {}).get('bronze_root', 'data/bronze')
            silver_path = self.config.get('paths', {}).get('silver_root', 'data/silver')
            
            # Transform CRM data
            logger.info("Transforming CRM data...")
            self.transform_crm_data(spark, bronze_path, silver_path)
            
            # Transform Snowflake data
            logger.info("Transforming Snowflake data...")
            self.transform_snowflake_data(spark, bronze_path, silver_path)
            
            # Transform Redshift data
            logger.info("Transforming Redshift data...")
            self.transform_redshift_data(spark, bronze_path, silver_path)
            
            # Transform FX data
            logger.info("Transforming FX data...")
            self.transform_fx_data(spark, bronze_path, silver_path)
            
            # Transform Kafka events
            logger.info("Transforming Kafka events...")
            self.transform_kafka_events(spark, bronze_path, silver_path)
            
            # Apply data quality checks
            logger.info("Applying data quality checks...")
            self.apply_dq_rules(None, "silver.layer")
            
            # Log lineage
            self.log_lineage(
                source="bronze",
                target="silver",
                records_processed={}
            )
            
            result = {
                "status": "success",
                "output_path": silver_path,
                "layers_processed": ["crm", "snowflake", "redshift", "fx", "kafka"]
            }
            
            logger.info(f"Bronze to Silver transformation completed: {result}")
            return result
            
        except FileNotFoundError as e:
            logger.error(f"Required input data missing: {e}")
            raise
        except ValueError as e:
            logger.error(f"Data quality check failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Bronze to Silver transformation failed: {e}", exc_info=True)
            raise
    
    def transform_crm_data(self, spark, bronze_path: str, silver_path: str):
        """Transform CRM data from bronze to silver."""
        from pyspark.sql.utils import AnalysisException
        from pyspark.sql.functions import trim, lower, col
        
        # Validate input paths exist
        paths = {
            "accounts": f"{bronze_path}/crm/accounts",
            "contacts": f"{bronze_path}/crm/contacts",
            "opportunities": f"{bronze_path}/crm/opportunities"
        }
        
        for name, path in paths.items():
            try:
                test_df = spark.read.parquet(path)
                count = test_df.count()
                if count == 0:
                    logger.warning(f"CRM {name} is empty at {path}")
            except AnalysisException:
                raise FileNotFoundError(f"Required CRM input not found: {path}")
        
        # Read bronze CRM data
        accounts_df = spark.read.parquet(paths["accounts"])
        contacts_df = spark.read.parquet(paths["contacts"])
        opportunities_df = spark.read.parquet(paths["opportunities"])
        
        # Clean and standardize
        # Clean accounts
        accounts_clean = accounts_df.select(
            "account_id",
            trim(lower("account_name")).alias("account_name"),
            "industry",
            "created_date"
        )
        
        # Validate no null primary keys
        null_pks = accounts_clean.filter(col("account_id").isNull()).count()
        if null_pks > 0:
            raise ValueError(f"CRM accounts: Found {null_pks} null account_ids - cannot proceed")
        
        # Clean contacts
        contacts_clean = contacts_df.select(
            "contact_id",
            "account_id",
            trim(lower("first_name")).alias("first_name"),
            trim(lower("last_name")).alias("last_name"),
            lower("email").alias("email"),
            "phone"
        )
        
        # Validate no null primary keys
        null_pks = contacts_clean.filter(col("contact_id").isNull()).count()
        if null_pks > 0:
            raise ValueError(f"CRM contacts: Found {null_pks} null contact_ids - cannot proceed")
        
        # Clean opportunities
        opportunities_clean = opportunities_df.select(
            "opportunity_id",
            "account_id",
            "contact_id",
            "opportunity_name",
            "amount",
            "stage",
            "close_date"
        )
        
        # Validate no null primary keys
        null_pks = opportunities_clean.filter(col("opportunity_id").isNull()).count()
        if null_pks > 0:
            raise ValueError(f"CRM opportunities: Found {null_pks} null opportunity_ids - cannot proceed")
        
        # Write to silver
        accounts_clean.write.mode("overwrite").parquet(f"{silver_path}/accounts_silver")
        contacts_clean.write.mode("overwrite").parquet(f"{silver_path}/contacts_silver")
        opportunities_clean.write.mode("overwrite").parquet(f"{silver_path}/opportunities_silver")
    
    def transform_snowflake_data(self, spark, bronze_path: str, silver_path: str):
        """Transform Snowflake data from bronze to silver."""
        from pyspark.sql.utils import AnalysisException
        from pyspark.sql.functions import trim, lower, col
        
        # Validate input paths exist
        paths = {
            "customers": f"{bronze_path}/snowflake/customers",
            "orders": f"{bronze_path}/snowflake/orders",
            "products": f"{bronze_path}/snowflake/products"
        }
        
        for name, path in paths.items():
            try:
                test_df = spark.read.parquet(path)
                if test_df.count() == 0:
                    logger.warning(f"Snowflake {name} is empty at {path}")
            except AnalysisException:
                raise FileNotFoundError(f"Required Snowflake input not found: {path}")
        
        # Read bronze Snowflake data
        customers_df = spark.read.parquet(paths["customers"])
        orders_df = spark.read.parquet(paths["orders"])
        products_df = spark.read.parquet(paths["products"])
        
        # Clean and standardize
        # Clean customers
        customers_clean = customers_df.select(
            "customer_id",
            trim(lower("first_name")).alias("first_name"),
            trim(lower("last_name")).alias("last_name"),
            lower("email").alias("email"),
            "country",
            "registration_date"
        )
        
        # Validate no null primary keys
        null_pks = customers_clean.filter(col("customer_id").isNull()).count()
        if null_pks > 0:
            raise ValueError(f"Snowflake customers: Found {null_pks} null customer_ids - cannot proceed")
        
        # Clean orders
        # Note: Bronze source has 'total_amount' column (from CSV)
        # We keep it as 'total_amount' for Silver (matches dbt schema)
        orders_clean = orders_df.select(
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "total_amount",  # Source column name (from Snowflake CSV)
            "quantity",
            "status",
            "updated_at"
        )
        
        # Validate no null primary keys
        null_pks = orders_clean.filter(col("order_id").isNull()).count()
        if null_pks > 0:
            raise ValueError(f"Snowflake orders: Found {null_pks} null order_ids - cannot proceed")
        
        # Validate no null foreign keys
        null_fks = orders_clean.filter(col("customer_id").isNull()).count()
        if null_fks > 0:
            logger.warning(f"Snowflake orders: Found {null_fks} null customer_ids (orphaned orders)")
        
        # Clean products
        products_clean = products_df.select(
            "product_id",
            "product_name",
            "category",
            "price_usd",
            "cost_usd",
            "supplier_id"
        )
        
        # Validate no null primary keys
        null_pks = products_clean.filter(col("product_id").isNull()).count()
        if null_pks > 0:
            raise ValueError(f"Snowflake products: Found {null_pks} null product_ids - cannot proceed")
        
        # Write to silver
        customers_clean.write.mode("overwrite").parquet(f"{silver_path}/customers_silver")
        orders_clean.write.mode("overwrite").parquet(f"{silver_path}/orders_silver")
        products_clean.write.mode("overwrite").parquet(f"{silver_path}/products_silver")
    
    def transform_redshift_data(self, spark, bronze_path: str, silver_path: str):
        """Transform Redshift data from bronze to silver."""
        # Read bronze Redshift data
        behavior_df = spark.read.parquet(f"{bronze_path}/redshift/customer_behavior")
        
        # Clean and standardize
        from pyspark.sql.functions import trim, lower, regexp_replace
        
        behavior_clean = behavior_df.select(
            "customer_id",
            "session_id",
            "page_viewed",
            "time_spent_seconds",
            "event_type",
            "event_date",
            "device_type",
            "browser"
        )
        
        # Write to silver
        behavior_clean.write.mode("overwrite").parquet(f"{silver_path}/customer_behavior_silver")
    
    def transform_fx_data(self, spark, bronze_path: str, silver_path: str):
        """Transform FX data from bronze to silver."""
        # Read bronze FX data
        fx_df = spark.read.parquet(f"{bronze_path}/fx/fx_rates")
        
        # Clean and standardize
        fx_clean = fx_df.select(
            "trade_date",
            "base_ccy",
            "counter_ccy",
            "rate"
        )
        
        # Write to silver
        fx_clean.write.mode("overwrite").parquet(f"{silver_path}/fx_rates_silver")
    
    def transform_kafka_events(self, spark, bronze_path: str, silver_path: str):
        """Transform Kafka events from bronze to silver."""
        # Read bronze Kafka data
        events_df = spark.read.parquet(f"{bronze_path}/kafka/events")
        
        # Clean and standardize
        events_clean = events_df.select(
            "event_id",
            "order_id",
            "event_type",
            "event_ts",
            "amount",
            "currency",
            "channel"
        )
        
        # Write to silver
        events_clean.write.mode("overwrite").parquet(f"{silver_path}/order_events_silver")