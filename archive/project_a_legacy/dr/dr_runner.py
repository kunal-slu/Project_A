import json
import time
from pathlib import Path

from ..logging_setup import get_logger

log = get_logger("dr")


def _read_json(p: Path) -> dict:
    return json.loads(p.read_text()) if p.exists() else {}


def run_backup_and_replication(backup_strategy: Path, primary: Path, secondary: Path):
    bs = _read_json(backup_strategy)
    pr = _read_json(primary)
    sr = _read_json(secondary)

    log.info("Starting backup/replication", extra={"corr_id": None})

    # Simulated actions (replace with cloud SDK/CLI calls in real impl)
    if bs:
        log.info("Applying backup strategy", extra={"plan": bs})
        time.sleep(0.2)
    if pr:
        log.info("Replicating to primary", extra={"replication": pr})
        time.sleep(0.2)
    if sr:
        log.info("Replicating to secondary", extra={"replication": sr})
        time.sleep(0.2)

    log.info("DR completed")
