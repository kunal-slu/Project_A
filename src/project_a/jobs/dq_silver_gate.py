"""
Compatibility wrapper for Silver-layer DQ gate.
"""

from __future__ import annotations

from ._compat import call_module_main


def main(args):
    """Run DQ gate with a sensible Silver default table."""
    table = getattr(args, "table", None) or "orders_silver"
    return call_module_main(
        "jobs.dq.dq_gate",
        args,
        extra={"layer": "silver", "table": table},
        arg_keys=(),
    )
