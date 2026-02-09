"""
Disaster Recovery Executor
Handles backup strategies, replication, and recovery operations for Azure data platform.
"""

import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Azure imports
try:
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient
    from azure.storage.blob import BlobServiceClient

    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    logger.warning("Azure SDK not available, using mock DR operations")


@dataclass
class BackupStrategy:
    """Backup strategy configuration."""

    strategy_name: str
    backup_type: str  # full, incremental, differential
    frequency: str  # daily, weekly, monthly
    retention_days: int
    storage_account: str
    container_name: str
    compression: bool = True
    encryption: bool = True
    cross_region: bool = False
    secondary_region: str | None = None


@dataclass
class ReplicationConfig:
    """Replication configuration."""

    config_name: str
    source_storage: str
    target_storage: str
    replication_type: str  # sync, async, geo
    tables: list[str]
    frequency_minutes: int
    enabled: bool = True
    monitoring: bool = True


class DisasterRecoveryExecutor:
    """
    Executes disaster recovery operations including backups and replication.
    """

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        self.spark = spark
        self.config = config
        self.dr_config = config.get("disaster_recovery", {})

        # Initialize Azure clients if available
        if AZURE_AVAILABLE:
            self._init_azure_clients()
        else:
            self._init_mock_clients()

        # Create DR directories
        self.dr_base = Path("data/disaster_recovery")
        self.dr_base.mkdir(parents=True, exist_ok=True)

        # Initialize metrics
        self.backup_metrics = {}
        self.replication_metrics = {}

    def _init_azure_clients(self):
        """Initialize Azure service clients."""
        try:
            credential = DefaultAzureCredential()

            # Storage clients
            self.primary_storage_client = BlobServiceClient(
                account_url=f"https://{self.dr_config.get('primary_storage_account')}.blob.core.windows.net/",
                credential=credential,
            )

            if self.dr_config.get("secondary_storage_account"):
                self.secondary_storage_client = BlobServiceClient(
                    account_url=f"https://{self.dr_config.get('secondary_storage_account')}.blob.core.windows.net/",
                    credential=credential,
                )

            # Key Vault client
            key_vault_name = self.dr_config.get("key_vault_name")
            if key_vault_name:
                key_vault_url = f"https://{key_vault_name}.vault.azure.net/"
                self.key_vault_client = SecretClient(vault_url=key_vault_url, credential=credential)

            logger.info("Azure clients initialized successfully")

        except Exception as e:
            logger.warning(f"Failed to initialize Azure clients: {e}")
            self._init_mock_clients()

    def _init_mock_clients(self):
        """Initialize mock clients for testing."""
        self.primary_storage_client = None
        self.secondary_storage_client = None
        self.key_vault_client = None
        logger.info("Using mock Azure clients")

    def load_backup_strategy(
        self, strategy_path: str = "data/disaster_recovery/backup_strategy.json"
    ) -> BackupStrategy:
        """Load backup strategy from configuration file."""
        try:
            if os.path.exists(strategy_path):
                with open(strategy_path) as f:
                    strategy_data = json.load(f)

                strategy = BackupStrategy(**strategy_data)
                logger.info(f"Loaded backup strategy: {strategy.strategy_name}")
                return strategy
            else:
                # Create default strategy
                default_strategy = BackupStrategy(
                    strategy_name="default_backup",
                    backup_type="full",
                    frequency="daily",
                    retention_days=30,
                    storage_account=self.dr_config.get("primary_storage_account", "default"),
                    container_name="backups",
                    compression=True,
                    encryption=True,
                    cross_region=self.dr_config.get("enable_cross_region_replication", False),
                    secondary_region=self.dr_config.get("secondary_region"),
                )

                # Save default strategy
                self.save_backup_strategy(default_strategy, strategy_path)
                logger.info("Created default backup strategy")
                return default_strategy

        except Exception as e:
            logger.error(f"Failed to load backup strategy: {e}")
            raise

    def save_backup_strategy(self, strategy: BackupStrategy, strategy_path: str):
        """Save backup strategy to configuration file."""
        try:
            strategy_dir = Path(strategy_path).parent
            strategy_dir.mkdir(parents=True, exist_ok=True)

            with open(strategy_path, "w") as f:
                json.dump(asdict(strategy), f, indent=2)

            logger.info(f"Backup strategy saved to {strategy_path}")

        except Exception as e:
            logger.error(f"Failed to save backup strategy: {e}")
            raise

    def load_replication_configs(
        self, config_dir: str = "data/disaster_recovery/replication_configs"
    ) -> list[ReplicationConfig]:
        """Load replication configurations from directory."""
        configs = []
        config_path = Path(config_dir)

        try:
            if config_path.exists():
                for config_file in config_path.glob("*.json"):
                    try:
                        with open(config_file) as f:
                            config_data = json.load(f)

                        config = ReplicationConfig(**config_data)
                        configs.append(config)
                        logger.info(f"Loaded replication config: {config.config_name}")

                    except Exception as e:
                        logger.warning(f"Failed to load config {config_file}: {e}")
                        continue
            else:
                # Create default replication configs
                configs = self._create_default_replication_configs(config_path)
                logger.info("Created default replication configurations")

            return configs

        except Exception as e:
            logger.error(f"Failed to load replication configs: {e}")
            return []

    def _create_default_replication_configs(self, config_dir: Path) -> list[ReplicationConfig]:
        """Create default replication configurations."""
        configs = []

        # Default configs for common tables
        default_tables = ["customers", "orders", "products", "returns"]

        configs.append(
            ReplicationConfig(
                config_name="core_tables_replication",
                source_storage=self.dr_config.get("primary_storage_account", "primary"),
                target_storage=self.dr_config.get("secondary_storage_account", "secondary"),
                replication_type="sync",
                tables=default_tables,
                frequency_minutes=15,
                enabled=True,
                monitoring=True,
            )
        )

        configs.append(
            ReplicationConfig(
                config_name="analytics_tables_replication",
                source_storage=self.dr_config.get("primary_storage_account", "primary"),
                target_storage=self.dr_config.get("secondary_storage_account", "secondary"),
                replication_type="async",
                tables=["metrics", "aggregations", "reports"],
                frequency_minutes=60,
                enabled=True,
                monitoring=True,
            )
        )

        # Save default configs
        for config in configs:
            config_file = config_dir / f"{config.config_name}.json"
            config_dir.mkdir(parents=True, exist_ok=True)

            with open(config_file, "w") as f:
                json.dump(asdict(config), f, indent=2)

        return configs

    def execute_backup(self, strategy: BackupStrategy, tables: list[str]) -> dict[str, Any]:
        """Execute backup operation based on strategy."""
        backup_result = {
            "backup_id": f"backup_{int(time.time())}",
            "strategy_name": strategy.strategy_name,
            "backup_type": strategy.backup_type,
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "status": "running",
            "tables_backed_up": [],
            "backup_size_bytes": 0,
            "errors": [],
            "warnings": [],
        }

        try:
            logger.info(f"Starting backup operation: {backup_result['backup_id']}")

            # Create backup directory
            backup_dir = self.dr_base / "backups" / backup_result["backup_id"]
            backup_dir.mkdir(parents=True, exist_ok=True)

            # Backup each table
            for table_name in tables:
                try:
                    table_backup_result = self._backup_table(table_name, backup_dir, strategy)
                    backup_result["tables_backed_up"].append(table_backup_result)
                    backup_result["backup_size_bytes"] += table_backup_result.get("size_bytes", 0)

                except Exception as e:
                    error_msg = f"Failed to backup table {table_name}: {e}"
                    backup_result["errors"].append(error_msg)
                    logger.error(error_msg)

            # Compress backup if enabled
            if strategy.compression:
                self._compress_backup(backup_dir)

            # Upload to storage if Azure is available
            if AZURE_AVAILABLE and self.primary_storage_client:
                self._upload_backup_to_storage(backup_dir, strategy)

            # Clean up old backups based on retention
            self._cleanup_old_backups(strategy)

            backup_result["status"] = "completed"
            backup_result["end_time"] = datetime.now().isoformat()

            logger.info(f"Backup operation completed: {backup_result['backup_id']}")

        except Exception as e:
            backup_result["status"] = "failed"
            backup_result["end_time"] = datetime.now().isoformat()
            backup_result["errors"].append(f"Backup operation failed: {e}")
            logger.error(f"Backup operation failed: {e}")

        # Save backup metrics
        self.backup_metrics[backup_result["backup_id"]] = backup_result
        self._save_backup_metrics()

        return backup_result

    def _backup_table(
        self, table_name: str, backup_dir: Path, strategy: BackupStrategy
    ) -> dict[str, Any]:
        """Backup individual table."""
        table_backup = {
            "table_name": table_name,
            "backup_path": None,
            "size_bytes": 0,
            "record_count": 0,
            "backup_time": datetime.now().isoformat(),
        }

        try:
            # Determine table path based on table name
            table_path = self._get_table_path(table_name)
            if not table_path or not os.path.exists(table_path):
                raise FileNotFoundError(f"Table path not found: {table_path}")

            # Create table backup directory
            table_backup_dir = backup_dir / table_name
            table_backup_dir.mkdir(exist_ok=True)

            # Copy table data
            if table_path.endswith(".parquet"):
                # For Parquet files, copy directly
                import shutil

                backup_file = table_backup_dir / f"{table_name}.parquet"
                shutil.copy2(table_path, backup_file)

                # Get file size and record count
                table_backup["backup_path"] = str(backup_file)
                table_backup["size_bytes"] = backup_file.stat().st_size

                # Count records using PySpark
                try:
                    df = self.spark.read.parquet(table_path)
                    table_backup["record_count"] = df.count()
                except:
                    table_backup["record_count"] = 0

            elif table_path.endswith(".csv"):
                # For CSV files, convert to Parquet for better compression
                backup_file = table_backup_dir / f"{table_name}.parquet"
                df = self.spark.read.csv(table_path, header=True, inferSchema=True)
                df.write.mode("overwrite").parquet(str(backup_file))

                table_backup["backup_path"] = str(backup_file)
                table_backup["size_bytes"] = backup_file.stat().st_size
                table_backup["record_count"] = df.count()

            logger.info(f"Table {table_name} backed up successfully")

        except Exception as e:
            logger.error(f"Failed to backup table {table_name}: {e}")
            raise

        return table_backup

    def _get_table_path(self, table_name: str) -> str | None:
        """Get the file path for a table."""
        table_mappings = {
            "customers": "data/input_data/customers.csv",
            "products": "data/input_data/products.csv",
            "orders": "data/input_data/orders.json",
            "returns": "data/input_data/returns.json",
            "inventory": "data/input_data/inventory_snapshots.csv",
            "fx_rates": "data/input_data/exchange_rates.csv",
        }

        return table_mappings.get(table_name)

    def _compress_backup(self, backup_dir: Path):
        """Compress backup directory."""
        try:
            import zipfile

            zip_file = backup_dir.parent / f"{backup_dir.name}.zip"
            with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zipf:
                for file_path in backup_dir.rglob("*"):
                    if file_path.is_file():
                        zipf.write(file_path, file_path.relative_to(backup_dir))

            # Remove uncompressed directory
            import shutil

            shutil.rmtree(backup_dir)

            logger.info(f"Backup compressed to {zip_file}")

        except Exception as e:
            logger.warning(f"Failed to compress backup: {e}")

    def _upload_backup_to_storage(self, backup_dir: Path, strategy: BackupStrategy):
        """Upload backup to Azure Storage."""
        try:
            if not self.primary_storage_client:
                logger.warning("Storage client not available, skipping upload")
                return

            container_client = self.primary_storage_client.get_container_client(
                strategy.container_name
            )

            # Find backup file (either directory or zip)
            backup_file = None
            if backup_dir.exists():
                backup_file = backup_dir
            else:
                zip_file = backup_dir.parent / f"{backup_dir.name}.zip"
                if zip_file.exists():
                    backup_file = zip_file

            if backup_file:
                blob_name = f"backups/{backup_file.name}"
                with open(backup_file, "rb") as f:
                    container_client.upload_blob(blob_name, f, overwrite=True)

                logger.info(f"Backup uploaded to storage: {blob_name}")

        except Exception as e:
            logger.warning(f"Failed to upload backup to storage: {e}")

    def _cleanup_old_backups(self, strategy: BackupStrategy):
        """Clean up old backups based on retention policy."""
        try:
            backup_dir = self.dr_base / "backups"
            if not backup_dir.exists():
                return

            cutoff_date = datetime.now() - timedelta(days=strategy.retention_days)

            for backup_item in backup_dir.iterdir():
                if backup_item.is_dir():
                    # Check creation time
                    creation_time = datetime.fromtimestamp(backup_item.stat().st_ctime)
                    if creation_time < cutoff_date:
                        import shutil

                        shutil.rmtree(backup_item)
                        logger.info(f"Removed old backup: {backup_item.name}")

                elif backup_item.suffix == ".zip":
                    # Check creation time for zip files
                    creation_time = datetime.fromtimestamp(backup_item.stat().st_ctime)
                    if creation_time < cutoff_date:
                        backup_item.unlink()
                        logger.info(f"Removed old backup: {backup_item.name}")

        except Exception as e:
            logger.warning(f"Failed to cleanup old backups: {e}")

    def execute_replication(self, config: ReplicationConfig) -> dict[str, Any]:
        """Execute replication operation based on configuration."""
        replication_result = {
            "replication_id": f"repl_{int(time.time())}",
            "config_name": config.config_name,
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "status": "running",
            "tables_replicated": [],
            "total_size_bytes": 0,
            "errors": [],
            "warnings": [],
        }

        try:
            logger.info(f"Starting replication: {replication_result['replication_id']}")

            # Replicate each table
            for table_name in config.tables:
                try:
                    table_repl_result = self._replicate_table(table_name, config)
                    replication_result["tables_replicated"].append(table_repl_result)
                    replication_result["total_size_bytes"] += table_repl_result.get("size_bytes", 0)

                except Exception as e:
                    error_msg = f"Failed to replicate table {table_name}: {e}"
                    replication_result["errors"].append(error_msg)
                    logger.error(error_msg)

            replication_result["status"] = "completed"
            replication_result["end_time"] = datetime.now().isoformat()

            logger.info(f"Replication completed: {replication_result['replication_id']}")

        except Exception as e:
            replication_result["status"] = "failed"
            replication_result["end_time"] = datetime.now().isoformat()
            replication_result["errors"].append(f"Replication failed: {e}")
            logger.error(f"Replication failed: {e}")

        # Save replication metrics
        self.replication_metrics[replication_result["replication_id"]] = replication_result
        self._save_replication_metrics()

        return replication_result

    def _replicate_table(self, table_name: str, config: ReplicationConfig) -> dict[str, Any]:
        """Replicate individual table."""
        table_repl = {
            "table_name": table_name,
            "source_path": None,
            "target_path": None,
            "size_bytes": 0,
            "record_count": 0,
            "replication_time": datetime.now().isoformat(),
        }

        try:
            # Get source table path
            source_path = self._get_table_path(table_name)
            if not source_path or not os.path.exists(source_path):
                raise FileNotFoundError(f"Source table not found: {source_path}")

            # Create target directory
            target_dir = Path(f"data/disaster_recovery/replicated/{config.target_storage}")
            target_dir.mkdir(parents=True, exist_ok=True)

            # Copy table to target
            if source_path.endswith(".parquet"):
                target_file = target_dir / f"{table_name}.parquet"
                import shutil

                shutil.copy2(source_path, target_file)

                table_repl["source_path"] = source_path
                table_repl["target_path"] = str(target_file)
                table_repl["size_bytes"] = target_file.stat().st_size

                # Count records
                try:
                    df = self.spark.read.parquet(source_path)
                    table_repl["record_count"] = df.count()
                except:
                    table_repl["record_count"] = 0

            logger.info(f"Table {table_name} replicated successfully")

        except Exception as e:
            logger.error(f"Failed to replicate table {table_name}: {e}")
            raise

        return table_repl

    def _save_backup_metrics(self):
        """Save backup metrics to file."""
        try:
            metrics_file = self.dr_base / "backup_metrics.json"
            with open(metrics_file, "w") as f:
                json.dump(self.backup_metrics, f, indent=2, default=str)

        except Exception as e:
            logger.warning(f"Failed to save backup metrics: {e}")

    def _save_replication_metrics(self):
        """Save replication metrics to file."""
        try:
            metrics_file = self.dr_base / "replication_metrics.json"
            with open(metrics_file, "w") as f:
                json.dump(self.replication_metrics, f, indent=2, default=str)

        except Exception as e:
            logger.warning(f"Failed to save replication metrics: {e}")

    def get_dr_status(self) -> dict[str, Any]:
        """Get overall disaster recovery status."""
        status = {
            "timestamp": datetime.now().isoformat(),
            "backup_status": {
                "total_backups": len(self.backup_metrics),
                "successful_backups": sum(
                    1 for b in self.backup_metrics.values() if b.get("status") == "completed"
                ),
                "failed_backups": sum(
                    1 for b in self.backup_metrics.values() if b.get("status") == "failed"
                ),
                "last_backup": None,
            },
            "replication_status": {
                "total_replications": len(self.replication_metrics),
                "successful_replications": sum(
                    1 for r in self.replication_metrics.values() if r.get("status") == "completed"
                ),
                "failed_replications": sum(
                    1 for r in self.replication_metrics.values() if r.get("status") == "failed"
                ),
                "last_replication": None,
            },
            "storage_status": {
                "primary_available": self.primary_storage_client is not None,
                "secondary_available": self.secondary_storage_client is not None,
                "cross_region_enabled": self.dr_config.get(
                    "enable_cross_region_replication", False
                ),
            },
        }

        # Get last backup time
        if self.backup_metrics:
            last_backup = max(self.backup_metrics.values(), key=lambda x: x.get("start_time", ""))
            status["backup_status"]["last_backup"] = last_backup.get("start_time")

        # Get last replication time
        if self.replication_metrics:
            last_replication = max(
                self.replication_metrics.values(), key=lambda x: x.get("start_time", "")
            )
            status["replication_status"]["last_replication"] = last_replication.get("start_time")

        return status

    def run_dr_workflow(self) -> dict[str, Any]:
        """Run complete disaster recovery workflow."""
        workflow_result = {
            "workflow_id": f"dr_workflow_{int(time.time())}",
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "status": "running",
            "backup_result": None,
            "replication_result": None,
            "overall_status": "unknown",
        }

        try:
            logger.info(f"Starting DR workflow: {workflow_result['workflow_id']}")

            # Load configurations
            backup_strategy = self.load_backup_strategy()
            replication_configs = self.load_replication_configs()

            # Execute backup
            tables_to_backup = [
                "customers",
                "products",
                "orders",
                "returns",
                "inventory",
                "fx_rates",
            ]
            workflow_result["backup_result"] = self.execute_backup(
                backup_strategy, tables_to_backup
            )

            # Execute replication for enabled configs
            enabled_configs = [c for c in replication_configs if c.enabled]
            if enabled_configs:
                workflow_result["replication_result"] = self.execute_replication(enabled_configs[0])

            # Determine overall status
            backup_success = workflow_result["backup_result"].get("status") == "completed"
            replication_success = (
                workflow_result["replication_result"] is None
                or workflow_result["replication_result"].get("status") == "completed"
            )

            if backup_success and replication_success:
                workflow_result["overall_status"] = "success"
                workflow_result["status"] = "completed"
            else:
                workflow_result["overall_status"] = "partial_failure"
                workflow_result["status"] = "completed_with_errors"

            workflow_result["end_time"] = datetime.now().isoformat()

            logger.info(f"DR workflow completed: {workflow_result['overall_status']}")

        except Exception as e:
            workflow_result["status"] = "failed"
            workflow_result["end_time"] = datetime.now().isoformat()
            workflow_result["overall_status"] = "failed"
            logger.error(f"DR workflow failed: {e}")

        return workflow_result


def main():
    """Main entry point for disaster recovery operations."""
    from .config_loader import load_config_resolved
    from .utils import get_spark_session

    # Load configuration
    config = load_config_resolved("config/config-dev.yaml")

    # Create Spark session
    spark = get_spark_session(config)

    # Initialize DR executor
    dr_executor = DisasterRecoveryExecutor(spark, config)

    # Run DR workflow
    print("Running disaster recovery workflow...")
    workflow_result = dr_executor.run_dr_workflow()

    print(f"DR Workflow Status: {workflow_result['overall_status']}")
    print(
        f"Backup Status: {workflow_result['backup_result']['status'] if workflow_result['backup_result'] else 'N/A'}"
    )
    print(
        f"Replication Status: {workflow_result['replication_result']['status'] if workflow_result['replication_result'] else 'N/A'}"
    )

    # Get overall DR status
    status = dr_executor.get_dr_status()
    print(f"Overall DR Status: {json.dumps(status, indent=2)}")

    print("Disaster recovery workflow completed!")


if __name__ == "__main__":
    main()
