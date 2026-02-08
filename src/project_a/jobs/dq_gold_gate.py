"""
Compatibility wrapper for Gold-layer DQ gate.
"""

from __future__ import annotations

from ._compat import call_module_main


def main(args):
    """Run DQ gate with a sensible Gold default table."""
    table = getattr(args, "table", None) or "fact_orders"
    return call_module_main(
        "jobs.dq.dq_gate",
        args,
        extra={"layer": "gold", "table": table},
        arg_keys=(),
    )
