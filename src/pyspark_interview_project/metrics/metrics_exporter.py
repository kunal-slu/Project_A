from prometheus_client import Counter, Gauge, start_http_server


# Pipeline execution metrics
PIPELINE_RUNS = Counter('pipeline_runs_total', 'Total pipeline runs', ['stage'])
ROWS_PROCESSED = Counter('rows_processed_total', 'Total rows processed', ['stage'])
STAGE_DURATION = Gauge('stage_duration_seconds', 'Stage execution duration', ['stage'])


def start_metrics_server(port: int = 8000) -> int:
    """Start Prometheus metrics server on specified port."""
    try:
        start_http_server(port)
        return port
    except OSError:
        # Port might be in use, try next available
        for p in range(port + 1, port + 10):
            try:
                start_http_server(p)
                return p
            except OSError:
                continue
        raise RuntimeError(f"Could not start metrics server on ports {port}-{port+9}")
