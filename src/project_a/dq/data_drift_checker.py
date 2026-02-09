"""
Data drift checker based on profiling snapshots.

Compares the current profile with a stored baseline and flags significant deltas.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class DataDriftChecker:
    def __init__(self, baseline_root: str, thresholds: dict[str, Any] | None = None):
        self.remote_root = str(baseline_root).startswith("s3://") or str(baseline_root).startswith(
            "s3a://"
        )
        self.baseline_root = Path(baseline_root) if not self.remote_root else None
        self.baseline_root_str = (
            str(baseline_root).replace("s3a://", "s3://") if self.remote_root else ""
        )
        self.thresholds = thresholds or {}

    def _baseline_path(self, layer: str, table: str) -> Path:
        if self.baseline_root is None:
            raise ValueError("Remote baseline path not supported in local drift checker.")
        return self.baseline_root / layer / f"{table}.json"

    def load_baseline(self, layer: str, table: str) -> dict[str, Any] | None:
        if self.remote_root:
            import boto3
            from botocore.exceptions import ClientError

            _, _, bucket_key = self.baseline_root_str.partition("s3://")
            bucket, _, key = bucket_key.partition("/")
            object_key = f"{key.rstrip('/')}/{layer}/{table}.json"
            try:
                obj = boto3.client("s3").get_object(Bucket=bucket, Key=object_key)
                return json.loads(obj["Body"].read().decode("utf-8"))
            except ClientError:
                return None
        path = self._baseline_path(layer, table)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def save_baseline(self, layer: str, table: str, profile: dict[str, Any]) -> None:
        if self.remote_root:
            import boto3

            _, _, bucket_key = self.baseline_root_str.partition("s3://")
            bucket, _, key = bucket_key.partition("/")
            object_key = f"{key.rstrip('/')}/{layer}/{table}.json"
            boto3.client("s3").put_object(
                Bucket=bucket,
                Key=object_key,
                Body=json.dumps(profile, indent=2, default=str).encode("utf-8"),
            )
            return
        path = self._baseline_path(layer, table)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(profile, indent=2, default=str), encoding="utf-8")

    def compare(self, layer: str, table: str, profile: dict[str, Any]) -> dict[str, Any]:
        baseline = self.load_baseline(layer, table)
        if baseline is None:
            self.save_baseline(layer, table, profile)
            return {"status": "BASELINE_CREATED", "drift_detected": False, "issues": []}

        thresholds = {
            "null_pct_delta": float(self.thresholds.get("null_pct_delta", 5.0)),
            "distinct_pct_delta": float(self.thresholds.get("distinct_pct_delta", 10.0)),
            "avg_pct_delta": float(self.thresholds.get("avg_pct_delta", 20.0)),
        }

        issues = []
        for col, curr_stats in (profile.get("columns") or {}).items():
            base_stats = (baseline.get("columns") or {}).get(col)
            if not base_stats:
                continue

            # Null pct drift
            curr_null = float(curr_stats.get("null_pct", 0))
            base_null = float(base_stats.get("null_pct", 0))
            if abs(curr_null - base_null) >= thresholds["null_pct_delta"]:
                issues.append(
                    {
                        "column": col,
                        "metric": "null_pct",
                        "baseline": base_null,
                        "current": curr_null,
                    }
                )

            # Distinct count drift
            base_distinct = float(base_stats.get("distinct_count", 0) or 0)
            curr_distinct = float(curr_stats.get("distinct_count", 0) or 0)
            if base_distinct > 0:
                delta_pct = abs(curr_distinct - base_distinct) / base_distinct * 100
                if delta_pct >= thresholds["distinct_pct_delta"]:
                    issues.append(
                        {
                            "column": col,
                            "metric": "distinct_count",
                            "baseline": base_distinct,
                            "current": curr_distinct,
                            "delta_pct": round(delta_pct, 2),
                        }
                    )

            # Average drift (numeric only)
            if "avg" in curr_stats and "avg" in base_stats:
                try:
                    base_avg = float(base_stats.get("avg"))
                    curr_avg = float(curr_stats.get("avg"))
                    if base_avg != 0:
                        avg_delta_pct = abs(curr_avg - base_avg) / abs(base_avg) * 100
                        if avg_delta_pct >= thresholds["avg_pct_delta"]:
                            issues.append(
                                {
                                    "column": col,
                                    "metric": "avg",
                                    "baseline": base_avg,
                                    "current": curr_avg,
                                    "delta_pct": round(avg_delta_pct, 2),
                                }
                            )
                except Exception:
                    # ignore non-numeric avg values
                    pass

        drift_detected = len(issues) > 0
        return {
            "status": "DRIFT_CHECKED",
            "drift_detected": drift_detected,
            "issues": issues,
        }
