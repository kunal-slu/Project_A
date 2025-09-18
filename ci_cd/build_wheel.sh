#!/usr/bin/env bash
set -euo pipefail

echo "[build_wheel] Creating wheel..."
python -m pip install --upgrade pip build wheel setuptools
python -m build
ls -lh dist


