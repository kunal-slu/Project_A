"""
Referential Integrity Validator

Checks that foreign key relationships are maintained:
- Every order.customer_id exists in customers
- Every order.product_id exists in products
- Every opportunity.account_id exists in accounts
- Behavior rows match customers 1:1
- Kafka events reference real customer IDs
"""
from typing import Dict, List, Any, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


class ReferentialIntegrityChecker:
    """Check referential integrity between related tables."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.issues: List[Dict[str, Any]] = []
    
    def check_foreign_key(
        self,
        fact_df: DataFrame,
        dim_df: DataFrame,
        fact_key: str,
        dim_key: str,
        relationship_name: str,
        allow_null: bool = False
    ) -> Dict[str, Any]:
        """
        Check that all foreign key values in fact table exist in dimension table.
        
        Args:
            fact_df: Fact table DataFrame
            dim_df: Dimension table DataFrame
            fact_key: Foreign key column name in fact table
            dim_key: Primary key column name in dimension table
            relationship_name: Descriptive name for the relationship
            allow_null: Whether NULL foreign keys are allowed
        
        Returns:
            Dictionary with validation results
        """
        logger.info(f"Checking referential integrity: {relationship_name}")
        
        # Get distinct foreign keys from fact table
        fact_keys = fact_df.select(fact_key).distinct()
        
        if allow_null:
            fact_keys = fact_keys.filter(F.col(fact_key).isNotNull())
        
        # Get distinct primary keys from dimension table
        dim_keys = dim_df.select(dim_key).distinct()
        
        # Find orphaned foreign keys
        orphaned = fact_keys.join(
            dim_keys,
            fact_keys[fact_key] == dim_keys[dim_key],
            "left_anti"
        )
        
        orphaned_count = orphaned.count()
        
        result = {
            "relationship": relationship_name,
            "fact_key": fact_key,
            "dim_key": dim_key,
            "orphaned_count": orphaned_count,
            "valid": orphaned_count == 0,
            "orphaned_keys": []
        }
        
        if orphaned_count > 0:
            # Sample orphaned keys (limit to 100 for performance)
            orphaned_sample = orphaned.limit(100).collect()
            result["orphaned_keys"] = [row[fact_key] for row in orphaned_sample]
            
            self.issues.append(result)
            logger.warning(
                f"❌ Referential integrity violation in {relationship_name}: "
                f"{orphaned_count} orphaned {fact_key} values"
            )
        else:
            logger.info(f"✅ Referential integrity OK for {relationship_name}")
        
        return result
    
    def check_orders_customers(
        self,
        orders_df: DataFrame,
        customers_df: DataFrame
    ) -> Dict[str, Any]:
        """Check that all order.customer_id exist in customers."""
        return self.check_foreign_key(
            orders_df,
            customers_df,
            "customer_id",
            "customer_id",
            "orders → customers"
        )
    
    def check_orders_products(
        self,
        orders_df: DataFrame,
        products_df: DataFrame
    ) -> Dict[str, Any]:
        """Check that all order.product_id exist in products."""
        return self.check_foreign_key(
            orders_df,
            products_df,
            "product_id",
            "product_id",
            "orders → products"
        )
    
    def check_opportunities_accounts(
        self,
        opportunities_df: DataFrame,
        accounts_df: DataFrame
    ) -> Dict[str, Any]:
        """Check that all opportunity.account_id exist in accounts."""
        return self.check_foreign_key(
            opportunities_df,
            accounts_df,
            "account_id",
            "account_id",
            "opportunities → accounts"
        )
    
    def check_behavior_customers(
        self,
        behavior_df: DataFrame,
        customers_df: DataFrame
    ) -> Dict[str, Any]:
        """Check that all behavior.customer_id exist in customers."""
        return self.check_foreign_key(
            behavior_df,
            customers_df,
            "customer_id",
            "customer_id",
            "behavior → customers"
        )
    
    def check_kafka_customers(
        self,
        kafka_df: DataFrame,
        customers_df: DataFrame
    ) -> Dict[str, Any]:
        """Check that all kafka event customer_ids exist in customers."""
        return self.check_foreign_key(
            kafka_df,
            customers_df,
            "customer_id",
            "customer_id",
            "kafka_events → customers",
            allow_null=True  # Some events might not have customer_id
        )
    
    def check_duplicate_ids(
        self,
        df: DataFrame,
        id_column: str,
        table_name: str
    ) -> Dict[str, Any]:
        """Check for duplicate primary keys."""
        logger.info(f"Checking for duplicate {id_column} in {table_name}")
        
        duplicates = df.groupBy(id_column).count().filter(F.col("count") > 1)
        duplicate_count = duplicates.count()
        
        result = {
            "table": table_name,
            "id_column": id_column,
            "duplicate_count": duplicate_count,
            "valid": duplicate_count == 0,
            "duplicate_ids": []
        }
        
        if duplicate_count > 0:
            duplicate_sample = duplicates.limit(100).collect()
            result["duplicate_ids"] = [
                {"id": row[id_column], "count": row["count"]}
                for row in duplicate_sample
            ]
            
            self.issues.append(result)
            logger.warning(
                f"❌ Duplicate {id_column} found in {table_name}: {duplicate_count} duplicates"
            )
        else:
            logger.info(f"✅ No duplicate {id_column} in {table_name}")
        
        return result
    
    def get_all_issues(self) -> List[Dict[str, Any]]:
        """Get all detected referential integrity issues."""
        return self.issues
    
    def generate_report(self) -> str:
        """Generate a human-readable report of all issues."""
        if not self.issues:
            return "✅ No referential integrity issues detected."
        
        report = ["🔍 Referential Integrity Report", "=" * 50, ""]
        
        for issue in self.issues:
            if "relationship" in issue:
                report.append(f"Relationship: {issue['relationship']}")
                report.append(f"  Orphaned keys: {issue['orphaned_count']}")
                if issue["orphaned_keys"]:
                    report.append(f"  Sample: {issue['orphaned_keys'][:10]}")
            elif "table" in issue:
                report.append(f"Table: {issue['table']}")
                report.append(f"  Duplicate {issue['id_column']}: {issue['duplicate_count']}")
                if issue["duplicate_ids"]:
                    report.append(f"  Sample: {[d['id'] for d in issue['duplicate_ids'][:10]]}")
            report.append("")
        
        return "\n".join(report)

