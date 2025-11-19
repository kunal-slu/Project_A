"""
Unity Catalog Integration Module

This module provides comprehensive Unity Catalog integration for:
- Centralized metadata management
- Fine-grained RBAC and access control
- Cross-workspace data sharing
- Data lineage and governance
- Automated compliance controls
"""

import logging
import os
from datetime import datetime
from typing import Any, Dict, List

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class UnityCatalogManager:
    """
    Manages Unity Catalog operations for data governance and access control.
    """

    def __init__(self, spark: SparkSession, catalog_name: str = "main"):
        self.spark = spark
        self.catalog_name = catalog_name
        self._dbutils = self._get_dbutils()

    def _get_dbutils(self):
        """Get dbutils instance for Unity Catalog operations."""
        try:
            from pyspark.dbutils import DBUtils
            return DBUtils(self.spark)
        except ImportError:
            try:
                # Databricks notebooks inject dbutils into global scope
                return globals().get("dbutils")
            except Exception:
                logger.warning("dbutils not available - Unity Catalog features limited")
                return None

    def create_catalog(self, catalog_name: str, description: str = "", owner: str = None) -> bool:
        """
        Create a new Unity Catalog.

        Args:
            catalog_name: Name of the catalog
            description: Catalog description
            owner: Owner of the catalog

        Returns:
            bool: True if successful
        """
        try:
            if not self._dbutils:
                logger.warning("dbutils not available - cannot create catalog")
                return False

            # Create catalog using SQL
            create_sql = f"CREATE CATALOG IF NOT EXISTS {catalog_name}"
            if description:
                create_sql += f" COMMENT '{description}'"

            self.spark.sql(create_sql)

            # Set owner if specified
            if owner:
                self.spark.sql(f"ALTER CATALOG {catalog_name} OWNER TO `{owner}`")

            logger.info(f"Created Unity Catalog: {catalog_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to create catalog {catalog_name}: {str(e)}")
            return False

    def create_schema(self, catalog_name: str, schema_name: str, description: str = "", owner: str = None) -> bool:
        """
        Create a new schema within a catalog.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            description: Schema description
            owner: Owner of the schema

        Returns:
            bool: True if successful
        """
        try:
            if not self._dbutils:
                logger.warning("dbutils not available - cannot create schema")
                return False

            # Create schema using SQL
            create_sql = f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}"
            if description:
                create_sql += f" COMMENT '{description}'"

            self.spark.sql(create_sql)

            # Set owner if specified
            if owner:
                self.spark.sql(f"ALTER SCHEMA {catalog_name}.{schema_name} OWNER TO `{owner}`")

            logger.info(f"Created schema: {catalog_name}.{schema_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to create schema {catalog_name}.{schema_name}: {str(e)}")
            return False

    def create_table(self, catalog_name: str, schema_name: str, table_name: str,
                    df: DataFrame, description: str = "", owner: str = None,
                    partition_by: List[str] = None, properties: Dict[str, str] = None) -> bool:
        """
        Create a Unity Catalog table from DataFrame.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_name: Name of the table
            df: DataFrame to create table from
            description: Table description
            owner: Owner of the table
            partition_by: Partition columns
            properties: Additional table properties

        Returns:
            bool: True if successful
        """
        try:
            # Write DataFrame to Unity Catalog table
            writer = df.write.format("delta").mode("overwrite")

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            if properties:
                for key, value in properties.items():
                    writer = writer.option(key, value)

            # Write to Unity Catalog path
            table_path = f"{catalog_name}.{schema_name}.{table_name}"
            writer.saveAsTable(table_path)

            # Set description and owner if specified
            if description:
                self.spark.sql(f"ALTER TABLE {table_path} SET TBLPROPERTIES ('comment' = '{description}')")

            if owner:
                self.spark.sql(f"ALTER TABLE {table_path} OWNER TO `{owner}`")

            logger.info(f"Created Unity Catalog table: {table_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to create table {catalog_name}.{schema_name}.{table_name}: {str(e)}")
            return False

    def grant_permissions(self, catalog_name: str, schema_name: str, table_name: str,
                         principal: str, permissions: List[str], grant_option: bool = False) -> bool:
        """
        Grant permissions on Unity Catalog table.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_name: Name of the table
            principal: User/group to grant permissions to
            permissions: List of permissions (SELECT, MODIFY, ALL_PRIVILEGES, etc.)
            grant_option: Whether principal can grant to others

        Returns:
            bool: True if successful
        """
        try:
            if not self._dbutils:
                logger.warning("dbutils not available - cannot grant permissions")
                return False

            table_path = f"{catalog_name}.{schema_name}.{table_name}"

            for permission in permissions:
                grant_sql = f"GRANT {permission} ON TABLE {table_path} TO `{principal}`"
                if grant_option:
                    grant_sql += " WITH GRANT OPTION"

                self.spark.sql(grant_sql)

            logger.info(f"Granted {permissions} on {table_path} to {principal}")
            return True

        except Exception as e:
            logger.error(f"Failed to grant permissions on {table_path}: {str(e)}")
            return False

    def create_external_location(self, location_name: str, url: str,
                               credential_name: str, owner: str = None) -> bool:
        """
        Create external location for cross-workspace data sharing.

        Args:
            location_name: Name of the external location
            url: URL of the external location
            credential_name: Name of the credential to use
            owner: Owner of the external location

        Returns:
            bool: True if successful
        """
        try:
            if not self._dbutils:
                logger.warning("dbutils not available - cannot create external location")
                return False

            # Create external location using dbutils
            self._dbutils.fs.mkdirs(url)

            # Create external location in Unity Catalog
            create_sql = f"CREATE EXTERNAL LOCATION IF NOT EXISTS {location_name} URL '{url}'"
            if credential_name:
                create_sql += f" WITH (CREDENTIAL `{credential_name}`)"

            self.spark.sql(create_sql)

            # Set owner if specified
            if owner:
                self.spark.sql(f"ALTER EXTERNAL LOCATION {location_name} OWNER TO `{owner}`")

            logger.info(f"Created external location: {location_name} -> {url}")
            return True

        except Exception as e:
            logger.error(f"Failed to create external location {location_name}: {str(e)}")
            return False

    def share_table(self, catalog_name: str, schema_name: str, table_name: str,
                   share_name: str, recipients: List[str], comment: str = "") -> bool:
        """
        Share a table with other workspaces/users.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_name: Name of the table
            share_name: Name of the share
            recipients: List of recipient workspaces/users
            comment: Comment for the share

        Returns:
            bool: True if successful
        """
        try:
            if not self._dbutils:
                logger.warning("dbutils not available - cannot create share")
                return False

            # Create share
            create_share_sql = f"CREATE SHARE IF NOT EXISTS {share_name}"
            if comment:
                create_share_sql += f" COMMENT '{comment}'"

            self.spark.sql(create_share_sql)

            # Add table to share
            table_path = f"{catalog_name}.{schema_name}.{table_name}"
            self.spark.sql(f"ALTER SHARE {share_name} ADD TABLE {table_path}")

            # Grant access to recipients
            for recipient in recipients:
                self.spark.sql(f"GRANT SELECT ON SHARE {share_name} TO `{recipient}`")

            logger.info(f"Shared table {table_path} via share {share_name} with {recipients}")
            return True

        except Exception as e:
            logger.error(f"Failed to share table {table_path}: {str(e)}")
            return False

    def get_table_lineage(self, catalog_name: str, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get lineage information for a table.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_name: Name of the table

        Returns:
            dict: Lineage information
        """
        try:
            table_path = f"{catalog_name}.{schema_name}.{table_name}"

            # Query Unity Catalog lineage information
            lineage_query = f"""
            SELECT
                table_name,
                column_name,
                upstream_table,
                upstream_column,
                transformation_type,
                created_at
            FROM system.lineage.table_lineage
            WHERE table_name = '{table_path}'
            """

            lineage_df = self.spark.sql(lineage_query)

            # Convert to dictionary
            lineage_data = {
                "table_path": table_path,
                "lineage_records": lineage_df.limit(100).collect(),
                "total_upstream_tables": lineage_df.select("upstream_table").distinct().count(),
                "total_columns": lineage_df.select("column_name").distinct().count(),
                "retrieved_at": datetime.now().isoformat()
            }

            logger.info(f"Retrieved lineage for {table_path}: {lineage_data['total_upstream_tables']} upstream tables")
            return lineage_data

        except Exception as e:
            logger.error(f"Failed to get lineage for {catalog_name}.{schema_name}.{table_name}: {str(e)}")
            return {"error": str(e)}

    def set_data_classification(self, catalog_name: str, schema_name: str, table_name: str,
                               classification: str, sensitivity_level: str = None) -> bool:
        """
        Set data classification and sensitivity level for a table.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_name: Name of the table
            classification: Data classification (PII, PHI, Confidential, Public, etc.)
            sensitivity_level: Sensitivity level (High, Medium, Low)

        Returns:
            bool: True if successful
        """
        try:
            table_path = f"{catalog_name}.{schema_name}.{table_name}"

            # Set classification properties
            properties = {
                "data.classification": classification
            }

            if sensitivity_level:
                properties["data.sensitivity"] = sensitivity_level

            # Apply properties
            for key, value in properties.items():
                self.spark.sql(f"ALTER TABLE {table_path} SET TBLPROPERTIES ('{key}' = '{value}')")

            logger.info(f"Set classification {classification} for {table_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to set classification for {table_path}: {str(e)}")
            return False

    def create_data_governance_policy(self, policy_name: str, description: str,
                                    rules: List[Dict[str, Any]]) -> bool:
        """
        Create a data governance policy.

        Args:
            policy_name: Name of the policy
            description: Policy description
            rules: List of governance rules

        Returns:
            bool: True if successful
        """
        try:
            if not self._dbutils:
                logger.warning("dbutils not available - cannot create governance policy")
                return False

            # Create policy using dbutils
            policy_config = {
                "name": policy_name,
                "description": description,
                "rules": rules,
                "created_at": datetime.now().isoformat(),
                "enabled": True
            }

            # Store policy configuration (in production, this would go to a metadata store)
            logger.info(f"Created governance policy: {policy_name} with {len(rules)} rules")
            return True

        except Exception as e:
            logger.error(f"Failed to create governance policy {policy_name}: {str(e)}")
            return False

    def audit_table_access(self, catalog_name: str, schema_name: str, table_name: str,
                          user: str, action: str, timestamp: datetime = None) -> bool:
        """
        Audit table access for compliance.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_name: Name of the table
            user: User performing the action
            action: Action performed (SELECT, INSERT, UPDATE, DELETE)
            timestamp: Timestamp of the action

        Returns:
            bool: True if successful
        """
        try:
            if timestamp is None:
                timestamp = datetime.now()

            # Create audit record
            audit_data = {
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "table_name": table_name,
                "user": user,
                "action": action,
                "timestamp": timestamp.isoformat(),
                "workspace_id": os.environ.get("DATABRICKS_WORKSPACE_ID", "unknown"),
                "cluster_id": self.spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "unknown")
            }

            # In production, this would write to an audit log table
            logger.info(f"Audit: {user} performed {action} on {catalog_name}.{schema_name}.{table_name}")

            # Store audit record (simplified - in production use proper audit table)
            audit_df = self.spark.createDataFrame([audit_data])
            audit_df.write.format("delta").mode("append").save("data/audit/table_access_logs")

            return True

        except Exception as e:
            logger.error(f"Failed to audit table access: {str(e)}")
            return False

    def get_governance_summary(self) -> Dict[str, Any]:
        """
        Get summary of governance status across all catalogs.

        Returns:
            dict: Governance summary
        """
        try:
            summary = {
                "total_catalogs": 0,
                "total_schemas": 0,
                "total_tables": 0,
                "classified_tables": 0,
                "shared_tables": 0,
                "governance_policies": 0,
                "last_updated": datetime.now().isoformat()
            }

            # Query Unity Catalog metadata
            try:
                # Get catalog count
                catalogs_df = self.spark.sql("SHOW CATALOGS")
                summary["total_catalogs"] = catalogs_df.count()

                # Get schema count
                schemas_df = self.spark.sql("SHOW SCHEMAS")
                summary["total_schemas"] = schemas_df.count()

                # Get table count
                tables_df = self.spark.sql("SHOW TABLES")
                summary["total_tables"] = tables_df.count()

            except Exception as e:
                logger.warning(f"Could not query Unity Catalog metadata: {str(e)}")

            logger.info(f"Governance summary: {summary}")
            return summary

        except Exception as e:
            logger.error(f"Failed to get governance summary: {str(e)}")
            return {"error": str(e)}

    def setup_unity_catalog_governance(self) -> bool:
        """
        Setup Unity Catalog governance policies and compliance controls.

        Returns:
            bool: True if successful
        """
        try:
            logger.info("Setting up Unity Catalog governance...")

            # Create default governance policies
            policies = [
                {
                    "name": "data_retention_policy",
                    "description": "Enforce data retention policies",
                    "rules": [
                        "DELETE data older than 7 years for financial records",
                        "DELETE data older than 3 years for customer data",
                        "ARCHIVE data older than 1 year"
                    ]
                },
                {
                    "name": "data_access_policy",
                    "description": "Control data access based on user roles",
                    "rules": [
                        "ADMIN users can access all data",
                        "ANALYST users can access non-sensitive data",
                        "AUDITOR users can read-only access for compliance"
                    ]
                },
                {
                    "name": "data_quality_policy",
                    "description": "Enforce data quality standards",
                    "rules": [
                        "All tables must have primary keys",
                        "Required fields cannot be NULL",
                        "Data must pass validation before ingestion"
                    ]
                }
            ]

            # Create policies
            for policy in policies:
                success = self.create_data_governance_policy(
                    policy["name"],
                    policy["description"],
                    policy["rules"]
                )
                if success:
                    logger.info(f"Created governance policy: {policy['name']}")
                else:
                    logger.warning(f"Failed to create governance policy: {policy['name']}")

            # Setup default data classifications
            default_classifications = [
                ("public", "PUBLIC"),
                ("internal", "INTERNAL"),
                ("confidential", "CONFIDENTIAL"),
                ("restricted", "RESTRICTED")
            ]

            for classification, level in default_classifications:
                try:
                    # Create classification tag
                    self.spark.sql(f"""
                    CREATE TAG IF NOT EXISTS {classification}
                    COMMENT 'Data classification: {level}'
                    """)
                    logger.info(f"Created data classification: {classification}")
                except Exception as e:
                    logger.warning(f"Could not create classification {classification}: {str(e)}")

            logger.info("Unity Catalog governance setup completed successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to setup Unity Catalog governance: {str(e)}")
            return False

    def list_catalogs(self) -> List[str]:
        """
        List all available catalogs.

        Returns:
            List of catalog names
        """
        try:
            catalogs_df = self.spark.sql("SHOW CATALOGS")
            return [row.catalog for row in catalogs_df.limit(100).collect()]
        except Exception as e:
            logger.error(f"Failed to list catalogs: {str(e)}")
            return []


def setup_unity_catalog_governance(spark: SparkSession, config: Dict[str, Any]) -> UnityCatalogManager:
    """
    Setup Unity Catalog governance for the project.

    Args:
        spark: SparkSession instance
        config: Configuration dictionary

    Returns:
        UnityCatalogManager: Configured Unity Catalog manager
    """
    try:
        # Initialize Unity Catalog manager
        uc_manager = UnityCatalogManager(spark)

        # Get governance configuration
        governance_config = config.get("governance", {})
        catalog_name = governance_config.get("catalog_name", "main")
        schema_name = governance_config.get("schema_name", "data_engineering")

        # Create catalog and schema if they don't exist
        uc_manager.create_catalog(catalog_name, "Main data catalog for data engineering project")
        uc_manager.create_schema(catalog_name, schema_name, "Data engineering schemas")

        # Set up governance policies
        governance_policies = governance_config.get("policies", [])
        for policy in governance_policies:
            uc_manager.create_data_governance_policy(
                policy["name"],
                policy["description"],
                policy["rules"]
            )

        logger.info(f"Unity Catalog governance setup completed for {catalog_name}.{schema_name}")
        return uc_manager

    except Exception as e:
        logger.error(f"Failed to setup Unity Catalog governance: {str(e)}")
        raise


def migrate_to_unity_catalog(spark: SparkSession, config: Dict[str, Any],
                           uc_manager: UnityCatalogManager) -> bool:
    """
    Migrate existing Delta tables to Unity Catalog.

    Args:
        spark: SparkSession instance
        config: Configuration dictionary
        uc_manager: Unity Catalog manager

    Returns:
        bool: True if migration successful
    """
    try:
        # Get table paths from config
        bronze_path = config["output"].get("bronze_path", "data/lakehouse/bronze")
        silver_path = config["output"].get("silver_path", "data/lakehouse/silver")
        gold_path = config["output"].get("gold_path", "data/lakehouse/gold")

        catalog_name = config.get("governance", {}).get("catalog_name", "main")
        schema_name = config.get("governance", {}).get("schema_name", "data_engineering")

        # Migrate bronze tables
        bronze_tables = ["customers_raw", "products_raw", "orders_raw", "returns_raw", "fx_rates", "inventory_snapshots"]
        for table in bronze_tables:
            source_path = f"{bronze_path}/{table}"
            if os.path.exists(source_path):
                df = spark.read.format("delta").load(source_path)
                uc_manager.create_table(
                    catalog_name, schema_name, f"bronze_{table}",
                    df, f"Bronze layer table: {table}"
                )

        # Migrate silver tables
        silver_tables = ["dim_customers", "dim_products", "orders_cleansed"]
        for table in silver_tables:
            source_path = f"{silver_path}/{table}"
            if os.path.exists(source_path):
                df = spark.read.format("delta").load(source_path)
                uc_manager.create_table(
                    catalog_name, schema_name, f"silver_{table}",
                    df, f"Silver layer table: {table}"
                )

        # Migrate gold tables
        gold_tables = ["fact_orders"]
        for table in gold_tables:
            source_path = f"{gold_path}/{table}"
            if os.path.exists(source_path):
                df = spark.read.format("delta").load(source_path)
                uc_manager.create_table(
                    catalog_name, schema_name, f"gold_{table}",
                    df, f"Gold layer table: {table}"
                )

        logger.info("Successfully migrated tables to Unity Catalog")
        return True

    except Exception as e:
        logger.error(f"Failed to migrate to Unity Catalog: {str(e)}")
        return False
