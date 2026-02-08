"""
Compatibility wrapper for publishing Gold data to Snowflake.
"""

from __future__ import annotations

from ._compat import call_module_main


def main(args):
    """Run Snowflake publish script."""
    return call_module_main(
        "jobs.publish.publish_gold_to_snowflake",
        args,
        arg_keys=("env", "config"),
    )
