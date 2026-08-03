"""Run the quickstart: ``python -m examples.quickstart``.

The module body lives in `pipeline.py`; this file only supplies the entry point.
"""

from __future__ import annotations

import sys

from .pipeline import main

if __name__ == "__main__":
    sys.exit(main())
