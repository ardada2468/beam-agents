#!/usr/bin/env python3
"""Fail if branch coverage is lower than the last coverage.xml committed on main."""

from __future__ import annotations

import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def line_rate(xml_path: Path) -> float:
    root = ET.parse(xml_path).getroot()
    return float(root.attrib["line-rate"])


def baseline_line_rate(ref: str) -> float | None:
    result = subprocess.run(
        ["git", "show", f"{ref}:coverage.xml"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return float(ET.fromstring(result.stdout).attrib["line-rate"])


def main() -> int:
    current_path = Path("coverage.xml")
    if not current_path.exists():
        print("error: coverage.xml not found; run tests with coverage first", file=sys.stderr)
        return 1

    current = line_rate(current_path)
    baseline = baseline_line_rate("origin/main")
    if baseline is None:
        print(f"no baseline on origin/main yet; current coverage {current:.2%} sets it")
        return 0

    if current + 1e-9 < baseline:
        print(
            f"error: coverage dropped from {baseline:.2%} to {current:.2%}",
            file=sys.stderr,
        )
        return 1

    print(f"coverage {current:.2%} >= baseline {baseline:.2%}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
