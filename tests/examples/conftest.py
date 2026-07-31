"""Path setup for the doc-contract tests: examples are importable, not installed.

`examples/` is sample code outside the wheel (see the slack-approval change,
design D1), so the doc-contract tests reach it by putting the repo root on
`sys.path` and importing `examples.<name>` as a plain package. Out-of-tree
copies of an example import nothing from here.
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
