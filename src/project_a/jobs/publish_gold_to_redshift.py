"""
Compatibility wrapper for publishing Gold data to Redshift.
"""

from __future__ import annotations

from ._compat import call_module_main


def main(args):
    """Run Redshift publish script."""
    return call_module_main(
        "jobs.publish.publish_gold_to_redshift",
        args,
        arg_keys=("env", "config"),
    )
