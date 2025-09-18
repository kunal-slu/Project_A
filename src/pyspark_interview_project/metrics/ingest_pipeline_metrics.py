import json
from pathlib import Path
from .metrics_exporter import PIPELINE_RUNS, ROWS_PROCESSED, STAGE_DURATION


def ingest_metrics_json(path: Path):
    if not path.exists():
        return
    with open(path, "r") as f:
        payload = json.load(f)
    # Expecting e.g. {"stages":[{"name":"bronze_to_silver","rows":123,"duration_sec":4.5}, ...]}
    for s in payload.get("stages", []):
        name = s.get("name")
        if not name:
            continue
        PIPELINE_RUNS.labels(stage=name).inc()
        if "rows" in s:
            ROWS_PROCESSED.labels(stage=name).inc(int(s["rows"]))
        if "duration_sec" in s:
            STAGE_DURATION.labels(stage=name).set(float(s["duration_sec"]))
