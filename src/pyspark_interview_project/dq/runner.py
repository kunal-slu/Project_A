from dataclasses import dataclass
from pyspark.sql import DataFrame
from typing import Dict


@dataclass
class DQResult:
    critical_fail: bool
    stats: Dict[str, float]
    issues: Dict[str, int]


def run_dq(df: DataFrame, key_cols: list[str], required_cols: list[str]) -> DQResult:
    n = df.count()
    null_keys = df.filter(" OR ".join([f"{c} IS NULL" for c in key_cols])).count()
    issues = {"null_keys": null_keys}
    critical = null_keys > 0
    return DQResult(critical, {"rows": n}, issues)






