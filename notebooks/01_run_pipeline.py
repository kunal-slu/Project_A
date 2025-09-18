"""Databricks thin orchestration notebook (Python script style).
Calls packaged code with resolved config; keep all logic in the package.
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, "src"))

from pyspark_interview_project.config_loader import load_config_resolved
from pyspark_interview_project.pipeline import main as run_main


def run(env: str | None = None) -> None:
    # Resolve once to validate secrets; pipeline loads its config path
    _ = load_config_resolved(env=env)
    # Choose a config path resolved by loader for transparency if needed
    # Here we pass the selected explicit path to keep logs clear in jobs
    run_main(config_path=os.environ.get("CONFIG_PATH", "config/config-azure-dev.yaml"))


if __name__ == "__main__":
    run(os.environ.get("APP_ENV"))
