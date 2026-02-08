"""
Compatibility helpers for project_a.jobs wrappers.
"""

from __future__ import annotations

import inspect
import sys
from contextlib import contextmanager
from importlib import import_module
from typing import Any

from project_a.core.config import ProjectConfig

_KNOWN_ARG_KEYS = (
    "env",
    "config",
    "run_date",
    "table",
    "layer",
    "mode",
    "checkpoint",
)


def _arg_value(args: Any, key: str) -> Any:
    if hasattr(args, key):
        return getattr(args, key)
    return None


def namespace_to_argv(
    args: Any,
    extra: dict[str, Any] | None = None,
    arg_keys: tuple[str, ...] = _KNOWN_ARG_KEYS,
) -> list[str]:
    """Build CLI argv from an argparse.Namespace-like object."""
    argv: list[str] = ["project_a-job"]

    for key in arg_keys:
        value = _arg_value(args, key)
        if value is None:
            continue

        flag = f"--{key.replace('_', '-')}"
        if isinstance(value, bool):
            if value:
                argv.append(flag)
        else:
            argv.extend([flag, str(value)])

    if extra:
        for key, value in extra.items():
            if value is None:
                continue
            flag = f"--{key.replace('_', '-')}"
            if isinstance(value, bool):
                if value:
                    argv.append(flag)
            else:
                argv.extend([flag, str(value)])

    return argv


@contextmanager
def patched_argv(argv: list[str]):
    """Temporarily patch sys.argv for legacy main() functions."""
    previous = sys.argv[:]
    sys.argv = argv
    try:
        yield
    finally:
        sys.argv = previous


def call_module_main(
    module_name: str,
    args: Any,
    extra: dict[str, Any] | None = None,
    arg_keys: tuple[str, ...] = _KNOWN_ARG_KEYS,
) -> Any:
    """
    Call module.main.

    Supports both signatures:
    - main(args)
    - main() reading from argparse
    """
    module = import_module(module_name)
    main_fn = module.main
    signature = inspect.signature(main_fn)

    if len(signature.parameters) == 0:
        with patched_argv(namespace_to_argv(args, extra=extra, arg_keys=arg_keys)):
            return main_fn()

    return main_fn(args)


def run_job_class(module_name: str, class_name: str, args: Any) -> Any:
    """
    Instantiate a BaseJob-style class and execute it.
    """
    env = _arg_value(args, "env")
    config_path = _arg_value(args, "config")
    if not config_path:
        raise ValueError("--config is required to run class-based jobs")

    module = import_module(module_name)
    job_cls = getattr(module, class_name)
    config = ProjectConfig(str(config_path), env=env)
    job = job_cls(config)
    return job.execute()
