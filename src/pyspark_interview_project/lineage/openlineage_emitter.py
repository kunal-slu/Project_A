import os
import requests


def emit(event: dict, cfg: dict):
    if not cfg.get("lineage", {}).get("enabled"):
        return
    url = cfg["lineage"]["url"].rstrip("/") + "/api/v1/lineage"
    try:
        requests.post(url, json=event, timeout=2)
    except Exception:
        pass









