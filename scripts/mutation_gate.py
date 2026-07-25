#!/usr/bin/env python3
"""Fail the build on surviving mutants in core/, and ratchet the un-mutation-tested surface.

`mutmut run` exits 0 no matter what it finds (`mutmut/__main__.py::_run` returns
None), so it cannot gate anything on its own. This script reads the per-mutant
results mutmut writes to `mutants/<path>.py.meta` and turns them into a verdict.

Two separate judgements:

1. Any mutant with a failing status (survived, timeout, suspicious, segfault, or
   an unfinished check) fails the build. `mutation-exclusions.toml` may exempt an
   equivalent survivor with a written reason; it cannot suppress an indeterminate
   result, and stale or unnecessary entries fail the gate.
2. Mutants mutmut reports as "no tests" cannot be killed by the configured test
   selection -- they are on lines only the deselected Beam pipeline suites reach
   (see the [tool.mutmut] comment in pyproject.toml). Failing on them is
   unsatisfiable, so instead their count is ratcheted against
   `mutation-baseline.toml` per module: the existing gap stays visible, and an
   improvement in one module cannot hide a regression in another.
"""

from __future__ import annotations

import json
import sys
import tomllib
from pathlib import Path

try:
    # Authoritative exit-code -> status mapping, imported rather than copied: it
    # contains non-obvious entries (a duplicate -24 key where "timeout" wins over
    # "killed", and a defaultdict fallback to "suspicious") that are easy to
    # transcribe wrongly. A loud ImportError if mutmut moves this is the right
    # failure mode for a gate -- far better than a silently wrong verdict.
    from mutmut.__main__ import status_by_exit_code
except ImportError as exc:  # pragma: no cover - environment error, not logic
    print(
        f"error: cannot import mutmut's status table ({exc}). "
        "Install the test dependency group: uv sync --group test",
        file=sys.stderr,
    )
    raise SystemExit(1) from exc

MUTANTS_DIR = Path("mutants")
CORE_SOURCE_DIR = Path("src/beam_agents/core")
BASELINE_PATH = Path("mutation-baseline.toml")
EXCLUSIONS_PATH = Path("mutation-exclusions.toml")

# mutmut separates class from method in a mangled mutant name with this character.
CLASS_NAME_SEPARATOR = "ǁ"

# A mutant the tests should have killed, or a result we cannot interpret as a
# kill. "not checked" and the interrupted status mean the run did not finish;
# treating an absent result as a pass is how a gate silently stops gating.
FAILING_STATUSES = frozenset(
    {
        "survived",
        "timeout",
        "suspicious",
        "segfault",
        "not checked",
        "check was interrupted by user",
    }
)
# "skipped" and "caught by type check" are legitimately not a test's job.
PASSING_STATUSES = frozenset({"killed", "skipped", "caught by type check"})
# Counted and ratcheted rather than failed -- see the module docstring.
RATCHETED_STATUS = "no tests"


class GateError(Exception):
    """A condition that fails the gate before any mutant is even considered."""


def qualified_function(mutant_name: str) -> str:
    """Turn `pkg.mod.xǁClsǁmeth__mutmut_7` into a readable `Cls.meth`."""
    mangled = mutant_name.partition("__mutmut_")[0]
    tail = mangled.rpartition(".")[2]
    if tail.startswith("x"):
        tail = tail[1:]
    if CLASS_NAME_SEPARATOR in tail:
        return ".".join(p for p in tail.split(CLASS_NAME_SEPARATOR) if p)
    return tail


def _read_toml(path: Path) -> dict[str, object]:
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise GateError(f"cannot read {path}: {exc}") from exc
    return data


def load_results() -> dict[str, tuple[str, Path]]:
    """Map every core mutant name to its (status, source file).

    Reads mutmut's `.meta` sidecars directly. They are plain JSON
    (`{"exit_code_by_key": {...}, ...}`), so this stays free of mutmut's
    internal data classes, which have moved between versions.
    """
    meta_dir = MUTANTS_DIR / CORE_SOURCE_DIR
    if not MUTANTS_DIR.is_dir():
        raise GateError(
            f"{MUTANTS_DIR}/ not found -- mutmut has not run. "
            "The gate must never pass on a missing run; use `make mutation`."
        )
    meta_paths = sorted(meta_dir.glob("*.py.meta"))
    if not meta_paths:
        raise GateError(
            f"no *.py.meta files under {meta_dir}/ -- mutmut generated no core mutants. "
            "Check that [tool.mutmut] only_mutate still matches src/beam_agents/core/."
        )

    results: dict[str, tuple[str, Path]] = {}
    for meta_path in meta_paths:
        source = CORE_SOURCE_DIR / meta_path.name.removesuffix(".meta")
        try:
            data = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise GateError(f"cannot read {meta_path}: {exc}") from exc
        if not isinstance(data, dict) or not isinstance(data.get("exit_code_by_key"), dict):
            raise GateError(f"{meta_path} has no valid exit_code_by_key mapping")
        exit_codes = data["exit_code_by_key"]
        for mutant_name, exit_code in exit_codes.items():
            if not isinstance(mutant_name, str) or (
                exit_code is not None
                and (not isinstance(exit_code, int) or isinstance(exit_code, bool))
            ):
                raise GateError(f"{meta_path} has an invalid exit_code_by_key entry")
            if mutant_name in results:
                raise GateError(f"duplicate mutant {mutant_name!r} in mutation metadata")
            results[mutant_name] = (status_by_exit_code[exit_code], source)
    return results


def load_exclusions() -> dict[str, str]:
    """Read declared-equivalent mutants as {mutant name: reason}."""
    if not EXCLUSIONS_PATH.exists():
        return {}
    data = _read_toml(EXCLUSIONS_PATH)
    mutants = data.get("mutants", {})
    if not isinstance(mutants, dict):
        raise GateError(f"{EXCLUSIONS_PATH} [mutants] must be a table")
    bad = sorted(
        str(name)
        for name, reason in mutants.items()
        if not isinstance(name, str) or not isinstance(reason, str) or not reason.strip()
    )
    if bad:
        raise GateError(f"{EXCLUSIONS_PATH} entries need a non-empty reason: {', '.join(bad)}")
    return {name: reason for name, reason in mutants.items()}


def load_baseline() -> dict[str, int]:
    """Read committed per-module ceilings for the `no tests` count."""
    if not BASELINE_PATH.exists():
        raise GateError(
            f"{BASELINE_PATH} not found -- it records how many core mutants are "
            "outside the mutation-tested surface and must be committed."
        )
    data = _read_toml(BASELINE_PATH)
    baseline = data.get("no_tests")
    if not isinstance(baseline, dict):
        raise GateError(f"{BASELINE_PATH} [no_tests] must be a table")
    invalid = sorted(
        str(module)
        for module, count in baseline.items()
        if not isinstance(module, str)
        or not isinstance(count, int)
        or isinstance(count, bool)
        or count < 0
    )
    if invalid:
        raise GateError(
            f"{BASELINE_PATH} entries must be non-negative integers: {', '.join(invalid)}"
        )
    return {module: count for module, count in baseline.items()}


def report_failures(failures: dict[str, tuple[str, Path]]) -> None:
    """Print failing mutants grouped by source file, survivors distinct from the rest."""
    by_source: dict[Path, list[tuple[str, str]]] = {}
    for name, (status, source) in failures.items():
        by_source.setdefault(source, []).append((status, name))

    for source in sorted(by_source):
        entries = sorted(by_source[source])
        print(f"\n{source} ({len(entries)}):", file=sys.stderr)
        for status, name in entries:
            label = "survived" if status == "survived" else f"{status} !"
            print(f"  [{label}] {qualified_function(name)}  <- {name}", file=sys.stderr)


def exclusions_are_valid(
    results: dict[str, tuple[str, Path]],
    exclusions: dict[str, str],
) -> bool:
    """Report exclusions that are missing or no longer represent a survivor."""
    valid = True
    missing = sorted(set(exclusions) - set(results))
    if missing:
        print(
            f"\nerror: {EXCLUSIONS_PATH} names {len(missing)} mutant(s) that no longer exist. "
            "Remove them rather than leaving the list to accumulate:",
            file=sys.stderr,
        )
        for name in missing:
            print(f"  {name}", file=sys.stderr)
        valid = False

    unnecessary = sorted(
        (name, results[name][0])
        for name in exclusions.keys() & results.keys()
        if results[name][0] != "survived"
    )
    if unnecessary:
        print(
            f"\nerror: {EXCLUSIONS_PATH} may exempt only live survivors. "
            "Remove or investigate these entries:",
            file=sys.stderr,
        )
        for name, status in unnecessary:
            print(f"  [{status}] {name}", file=sys.stderr)
        valid = False
    return valid


def ratchet_passes(
    results: dict[str, tuple[str, Path]],
    baseline: dict[str, int],
) -> bool:
    """Compare no-tests counts independently for every source module."""
    live_no_tests: dict[str, int] = {}
    for status, source in results.values():
        if status == RATCHETED_STATUS:
            live_no_tests[source.name] = live_no_tests.get(source.name, 0) + 1

    passes = True
    for module in sorted(set(baseline) | set(live_no_tests)):
        live = live_no_tests.get(module, 0)
        ceiling = baseline.get(module, 0)
        if live > ceiling:
            print(
                f"\nerror: un-mutation-tested mutants in {module} rose from "
                f"{ceiling} to {live}. New core code is not reached by the mutation "
                f"test selection -- cover it, or justify raising its ceiling in {BASELINE_PATH}.",
                file=sys.stderr,
            )
            passes = False
        elif live < ceiling:
            print(
                f"'{RATCHETED_STATUS}' count for {module} is {live}, below its baseline "
                f"{ceiling}; lower `{module}` to {live} in {BASELINE_PATH} to lock in the gain."
            )
        else:
            print(f"'{RATCHETED_STATUS}' count for {module} is at the baseline ({live})")
    return passes


def main() -> int:
    try:
        results = load_results()
        exclusions = load_exclusions()
        baseline = load_baseline()
    except GateError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    counts: dict[str, int] = {}
    for status, _ in results.values():
        counts[status] = counts.get(status, 0) + 1
    summary = ", ".join(f"{status}: {n}" for status, n in sorted(counts.items()))
    print(f"{len(results)} core mutants -- {summary}")

    exit_code = 0 if exclusions_are_valid(results, exclusions) else 1

    failures = {
        name: (status, source)
        for name, (status, source) in results.items()
        if status in FAILING_STATUSES and not (status == "survived" and name in exclusions)
    }
    if failures:
        n_survived = sum(1 for status, _ in failures.values() if status == "survived")
        n_other = len(failures) - n_survived
        detail = f"{n_survived} survived"
        if n_other:
            detail += f", {n_other} indeterminate (timeout/suspicious/unchecked)"
        print(f"\nerror: {len(failures)} mutant(s) not killed -- {detail}", file=sys.stderr)
        report_failures(failures)
        print(
            "\nWrite a test derived from the owning spec scenario for each. If a mutant is "
            f"genuinely equivalent to the original, add it to {EXCLUSIONS_PATH} with a reason. "
            "Never weaken or deselect a test to make one pass.",
            file=sys.stderr,
        )
        exit_code = 1

    unexpected = sorted(
        {
            status
            for status, _ in results.values()
            if status not in FAILING_STATUSES
            and status not in PASSING_STATUSES
            and status != RATCHETED_STATUS
        }
    )
    if unexpected:
        print(
            f"\nerror: unclassified mutmut status(es) {unexpected}; mutation_gate.py needs "
            "updating to say whether they pass or fail.",
            file=sys.stderr,
        )
        exit_code = 1

    if not ratchet_passes(results, baseline):
        exit_code = 1

    if exit_code == 0:
        print("mutation gate passed")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
