#!/usr/bin/env python3
"""Fail if branch coverage regresses vs. the committed baseline.

coverage.xml is gitignored (it's a regenerated build artifact), so comparing
against `git show origin/main:coverage.xml` can never find a baseline -- it
has never existed in git history. The baseline instead lives in
coverage-baseline.toml, committed in the same ratchet style as
mutation-baseline.toml: this script tells you to raise it by hand when
coverage improves, so a gain is locked in deliberately rather than silently.

Branch rate, not line rate, is the gated metric. Line rate rewards condensed
conditionals that branch coverage does not, and branch coverage is the metric
CONTRIBUTING.md and openspec/project.md say is enforced.
"""

from __future__ import annotations

import sys
import tomllib
import xml.etree.ElementTree as ET
from pathlib import Path

COVERAGE_PATH = Path("coverage.xml")
BASELINE_PATH = Path("coverage-baseline.toml")


def branch_rate(xml_path: Path) -> float:
    root = ET.parse(xml_path).getroot()
    return float(root.attrib["branch-rate"])


def load_baseline() -> float:
    data = tomllib.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    rate = data.get("branch_rate")
    if not isinstance(rate, (int, float)) or isinstance(rate, bool):
        raise SystemExit(f"error: {BASELINE_PATH} must set a numeric branch_rate")
    return float(rate)


def main() -> int:
    if not COVERAGE_PATH.exists():
        print(f"error: {COVERAGE_PATH} not found; run tests with coverage first", file=sys.stderr)
        return 1
    if not BASELINE_PATH.exists():
        print(
            f"error: {BASELINE_PATH} not found -- it records the branch-coverage rate "
            "to beat and must be committed.",
            file=sys.stderr,
        )
        return 1

    current = branch_rate(COVERAGE_PATH)
    baseline = load_baseline()

    if current + 1e-9 < baseline:
        print(
            f"error: branch coverage dropped from {baseline:.2%} to {current:.2%}",
            file=sys.stderr,
        )
        return 1

    if current > baseline + 1e-9:
        print(
            f"branch coverage {current:.2%} is above baseline {baseline:.2%}; "
            f"raise branch_rate to {current:.4f} in {BASELINE_PATH} to lock in the gain."
        )
    else:
        print(f"branch coverage {current:.2%} is at baseline")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
