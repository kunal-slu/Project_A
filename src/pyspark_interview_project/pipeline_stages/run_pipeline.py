import argparse
from ..logging_setup import get_logger, new_correlation_id
from ..metrics.metrics_exporter import start_metrics_server
from ..metrics.ingest_pipeline_metrics import ingest_metrics_json
from ..config.paths import METRICS_FILE, BACKUP_STRATEGY, REPL_PRIMARY, REPL_SECONDARY
from .bronze_to_silver import run as b2s
from .silver_to_gold import run as s2g
from ..dr.dr_runner import run_backup_and_replication

log = get_logger("runner")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--with-dr", action="store_true", help="Run backup/replication after ETL")
    parser.add_argument("--ingest-metrics-json", action="store_true", help="Ingest metrics JSON")
    args = parser.parse_args()

    new_correlation_id()
    port = start_metrics_server()
    log.info(f"Metrics at /metrics on :{port}")

    silver = b2s()
    gold = s2g()

    if args.ingest_metrics_json:
        ingest_metrics_json(METRICS_FILE)

    if args.with_dr:
        run_backup_and_replication(BACKUP_STRATEGY, REPL_PRIMARY, REPL_SECONDARY)

    log.info("Pipeline completed", extra={"silver": silver, "gold": gold})


if __name__ == "__main__":
    main()
