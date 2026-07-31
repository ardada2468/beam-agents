"""Run the console demo: ``python -m examples.console_demo``.

The module body lives in `pipeline.py`, which `docs/examples/console-demo.md`
renders verbatim; this file only supplies the package entry point.
"""

from __future__ import annotations

from .pipeline import main

if __name__ == "__main__":
    raise SystemExit(main())
