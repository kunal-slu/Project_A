import sys
from pathlib import Path

"""
Compatibility CLI wrapper.

Canonical runner lives in:
    `project_a.pipeline.run_pipeline`
"""

PROJECT_ROOT = Path(__file__).parent.parent.resolve()
SRC_ROOT = PROJECT_ROOT / "src"
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SRC_ROOT))

from project_a.pipeline.run_pipeline import main


if __name__ == "__main__":
    main()
