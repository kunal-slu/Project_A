import os
from pathlib import Path

# Base data root (override with env var)
DATA_ROOT = Path(os.getenv("DATA_ROOT", "data/lakehouse"))

BRONZE = DATA_ROOT / "bronze"
SILVER = DATA_ROOT / "silver"
GOLD = DATA_ROOT / "gold"

# Metrics & DR configs
METRICS_FILE = Path(os.getenv("PIPELINE_METRICS_JSON", "data/metrics/pipeline_metrics.json"))
DR_ROOT = Path(os.getenv("DR_ROOT", "data/dr"))
BACKUP_STRATEGY = DR_ROOT / "backup_strategies" / "backup_strategy.json"
REPL_PRIMARY = DR_ROOT / "replication_configs" / "primary-storage_replication.json"
REPL_SECONDARY = DR_ROOT / "replication_configs" / "secondary-storage_replication.json"
