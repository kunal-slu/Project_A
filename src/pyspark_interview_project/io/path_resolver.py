from __future__ import annotations
from typing import Dict


def resolve(uri: str, cfg: Dict[str, dict]) -> str:
    """
    Map logical lake:// URIs to cloud paths.
    Example: lake://bronze/returns -> s3a://... or abfss://...
    """
    if not uri.startswith("lake://"):
        return uri
    root = cfg["paths"]["lake_root"].rstrip("/")
    return f"{root}/{uri.replace('lake://','',1)}".rstrip("/")












