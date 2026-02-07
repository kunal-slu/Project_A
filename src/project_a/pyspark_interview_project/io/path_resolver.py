from __future__ import annotations
from typing import Dict


def resolve(uri: str, cfg: Dict[str, dict]) -> str:
    """
    Map logical lake:// URIs to cloud paths.
    Example: lake://bronze/returns -> s3a://... or abfss://...
    
    Ensures consistent path resolution across environments.
    """
    if not uri.startswith("lake://"):
        return uri
    
    # Get lake root from config
    lake_root = cfg.get("paths", {}).get("lake_root", "")
    if not lake_root:
        # Fallback to S3 bucket from config
        bucket = cfg.get("aws", {}).get("s3_bucket", "data-lake-bucket")
        cloud = cfg.get("cloud", "aws")
        if cloud == "aws":
            lake_root = f"s3a://{bucket}"
        else:
            lake_root = f"s3a://{bucket}"  # Default to S3 for now
    
    # Ensure lake_root doesn't end with slash
    root = lake_root.rstrip("/")
    
    # Remove lake:// prefix and build path
    relative_path = uri.replace('lake://', '', 1)
    full_path = f"{root}/{relative_path}".rstrip("/")
    
    return full_path







