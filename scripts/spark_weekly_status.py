"""Report the Spark promotion window from the weekly workflow's run history.

Spark is best-effort (`openspec/project.md`). Promoting it to *supported* is
defined as a claim about sustained behavior — four consecutive green
**scheduled** weekly runs with zero spark `Skip` declarations added during the
window — and demoting it as the inverse trend, two consecutive red scheduled
weeks. This script is what makes those windows mechanical to assess: it runs
at the end of every `spark-weekly` run and writes the streak, the cadence
verdict, the current spark skip inventory, any skip added inside the window,
and a `PROMOTION READY` / `NOT READY (...)` line to the job summary.

Four rules the logic exists to enforce, each a spec scenario:

* **Scheduled runs only.** `workflow_dispatch` iterations neither extend nor
  break the streak — per-PR-style ad-hoc runs would make "consecutive"
  meaningless, and investigating a red week must not cost the window.
* **Final conclusions.** A run re-run to green counts as green: the harness's
  infra/verdict separation exists precisely so stack breakage is not a Spark
  verdict. (The rerun has to land before the next scheduled run to count,
  which the cadence rule enforces by itself.)
* **Consecutive means weekly.** Adjacent scheduled runs more than eight days
  apart break the streak, converting the known GitHub failure mode where a
  schedule is silently disabled after 60 days of repository inactivity into
  an explicit broken window rather than a phantom streak.
* **Skip drift resets the clock.** A spark `Skip` added inside the window
  means the leg's coverage shrank mid-window, so the streak buys nothing.

**This script reports; it does not and must not change anything.** It never
edits the support statement, the specs, or any repository content, and it
always exits 0 — a not-ready verdict is information, not a build failure. The
flip itself is a reviewed OpenSpec change that re-verifies this evidence.

Everything except `main()`'s API call, git invocation, and summary write is a
pure function over data it is handed, so the streak, cadence, dispatch
exclusion, and drift detection are unit-tested offline
(`tests/scripts/test_spark_weekly_status.py`).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = "tests/conformance/_spec.py"
WORKFLOW_FILE = "spark-weekly.yml"

SCHEDULE_EVENT = "schedule"
#: Consecutive green scheduled weeks that authorize a promotion change.
PROMOTION_WEEKS = 4
#: Consecutive red scheduled weeks that trigger a demotion change.
DEMOTION_WEEKS = 2
#: Adjacent scheduled runs further apart than this are not consecutive weeks.
MAX_CADENCE_GAP = timedelta(days=8)
#: The trailing window the skip-drift scan covers (the promotion window).
WINDOW_DAYS = 7 * PROMOTION_WEEKS

#: An added leg declaration marking a scenario unrunnable on spark. Matched on
#: diff `+` lines only. A heuristic by construction (a rename-and-re-add could
#: evade it) — the printed inventory and the promotion review are the backstop.
_ADDED_SPARK_SKIP = re.compile(r"^\+.*\bSPARK\s*:\s*Skip\s*\(")
_COMMIT_LINE = re.compile(r"^commit ([0-9a-f]{7,40})")


# -- runs ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkflowRun:
    """One completed workflow run, as the promotion window sees it."""

    run_id: int
    event: str
    conclusion: str
    created_at: datetime
    url: str

    @property
    def green(self) -> bool:
        return self.conclusion == "success"

    @property
    def red(self) -> bool:
        # Anything that finished non-green is red for the demotion trend:
        # `cancelled` and `timed_out` weeks produced no evidence either.
        return not self.green

    def describe(self) -> str:
        return f"run {self.run_id} ({self.created_at:%Y-%m-%d}, {self.conclusion})"


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def parse_runs(pages: Iterable[Mapping[str, Any]]) -> list[WorkflowRun]:
    """Normalize GitHub `workflow_runs` API pages into runs, newest first."""
    runs: list[WorkflowRun] = []
    for payload in pages:
        for entry in payload.get("workflow_runs", ()):
            runs.append(
                WorkflowRun(
                    run_id=int(entry["id"]),
                    event=str(entry.get("event", "")),
                    conclusion=str(entry.get("conclusion") or "pending"),
                    created_at=_parse_timestamp(str(entry["created_at"])),
                    url=str(entry.get("html_url", "")),
                )
            )
    return sorted(runs, key=lambda run: run.created_at, reverse=True)


def scheduled_runs(runs: Sequence[WorkflowRun]) -> list[WorkflowRun]:
    """Only the scheduled runs — the window's unambiguous clock."""
    return [run for run in runs if run.event == SCHEDULE_EVENT]


def with_current_run(
    runs: Sequence[WorkflowRun],
    *,
    run_id: int,
    event: str,
    conclusion: str,
    created_at: datetime,
    url: str,
) -> list[WorkflowRun]:
    """Fold the in-flight run's own conclusion (from `needs`) into the history.

    A dispatch run is dropped rather than prepended: it is excluded from the
    window by the same rule that excludes every other dispatch run. A
    scheduled run replaces any API entry with the same id, because the listing
    reports the still-running job's conclusion as null.
    """
    if event != SCHEDULE_EVENT:
        return list(runs)
    current = WorkflowRun(
        run_id=run_id, event=event, conclusion=conclusion, created_at=created_at, url=url
    )
    others = [run for run in runs if run.run_id != run_id]
    return sorted([current, *others], key=lambda run: run.created_at, reverse=True)


# -- streaks ------------------------------------------------------------------------


@dataclass(frozen=True)
class Streak:
    """A run of consecutive same-verdict scheduled weeks, newest first."""

    length: int
    runs: tuple[WorkflowRun, ...]
    #: Why the streak stopped growing; "" when it ran out of history.
    broken_by: str


def _streak(runs: Sequence[WorkflowRun], *, want_green: bool) -> Streak:
    counted: list[WorkflowRun] = []
    broken_by = ""
    for run in runs:
        if run.green is not want_green:
            broken_by = f"{run.describe()} concluded {run.conclusion!r}"
            break
        if counted:
            gap = counted[-1].created_at - run.created_at
            if gap > MAX_CADENCE_GAP:
                broken_by = (
                    f"cadence gap of {gap.days} days between {run.describe()} and "
                    f"{counted[-1].describe()} — consecutive means weekly, so the "
                    f"window is broken, not bridged"
                )
                break
        counted.append(run)
    return Streak(length=len(counted), runs=tuple(counted), broken_by=broken_by)


def green_streak(runs: Sequence[WorkflowRun]) -> Streak:
    """Consecutive green scheduled weeks, walking backward from the newest."""
    return _streak(runs, want_green=True)


def red_streak(runs: Sequence[WorkflowRun]) -> Streak:
    """Consecutive red scheduled weeks — the demotion trend."""
    return _streak(runs, want_green=False)


# -- skip drift ---------------------------------------------------------------------


@dataclass(frozen=True)
class SkipAddition:
    """One spark `Skip` declaration added inside the window."""

    commit: str
    line: str


def added_spark_skips(git_log_output: str) -> list[SkipAddition]:
    """Spark `Skip` declarations *added* in a `git log -p` diff over the spec."""
    additions: list[SkipAddition] = []
    commit = "(unknown)"
    for raw in git_log_output.splitlines():
        match = _COMMIT_LINE.match(raw)
        if match:
            commit = match.group(1)
            continue
        if raw.startswith("+++"):  # diff header, never content
            continue
        if _ADDED_SPARK_SKIP.match(raw):
            additions.append(SkipAddition(commit=commit, line=raw.strip()))
    return additions


def git_log_spec_diff(days: int = WINDOW_DAYS, repo_root: Path = REPO_ROOT) -> str:
    """`git log -p` over the conformance spec for the trailing window.

    Best-effort: a shallow clone or a missing history makes the scan blind,
    and a blind scan must not fail the status step — the printed inventory
    still shows what is skipped today.
    """
    try:
        result = subprocess.run(
            ["git", "log", f"--since={days} days ago", "-p", "--", SPEC_PATH],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - diagnostics
        return f"(git log unavailable: {exc})"
    return result.stdout


def current_skip_inventory() -> dict[str, str]:
    """The spark skips declared right now, scenario -> reason.

    Read from the declarations themselves, not from a copy: the inventory is
    the backstop for the diff-scan heuristic, so it must not be able to drift
    from what the matrix actually declares. Run as a script, ``sys.path[0]``
    is ``scripts/``, so the repo root is prepended for the ``tests`` package.
    """
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    # PLC0415-is-the-point: the import must follow the sys.path insertion above.
    from tests.conformance._spec import SPARK, skip_inventory  # noqa: PLC0415

    return skip_inventory(SPARK)


# -- verdicts -----------------------------------------------------------------------


@dataclass(frozen=True)
class Verdict:
    """A reported assessment. `ready` never triggers anything by itself."""

    ready: bool
    reason: str
    ready_label: str = "PROMOTION READY"
    not_ready_label: str = "NOT READY"

    @property
    def line(self) -> str:
        return self.ready_label if self.ready else f"{self.not_ready_label} ({self.reason})"


def promotion_verdict(streak: Streak, additions: Sequence[SkipAddition]) -> Verdict:
    """Ready iff four consecutive green scheduled weeks and no skip drift.

    Drift is checked first: a skip added mid-window means the promotion clock
    resets regardless of how the runs concluded.
    """
    if additions:
        where = ", ".join(sorted({a.commit[:7] for a in additions}))
        return Verdict(
            ready=False,
            reason=(
                f"{len(additions)} spark Skip declaration(s) added in the last "
                f"{WINDOW_DAYS} days (commits {where}) — the promotion clock restarts"
            ),
        )
    if streak.length < PROMOTION_WEEKS:
        detail = f"; {streak.broken_by}" if streak.broken_by else ""
        return Verdict(
            ready=False,
            reason=f"green streak {streak.length}/{PROMOTION_WEEKS} scheduled weeks{detail}",
        )
    return Verdict(ready=True, reason="")


def demotion_verdict(red: Streak) -> Verdict:
    """Triggered iff two consecutive red scheduled weeks.

    One red week opens an investigation and demotes nothing: a single red is
    too often infra, and the promotion evidence was itself trend-based.
    """
    if red.length >= DEMOTION_WEEKS:
        return Verdict(
            ready=True,
            reason="",
            ready_label=(
                f"DEMOTION TRIGGERED ({red.length} consecutive red scheduled weeks) — "
                f"author the demotion change"
            ),
        )
    if red.length == 1:
        return Verdict(
            ready=False,
            reason=(
                f"1 red scheduled week ({red.runs[0].describe()}) — investigate; demotion "
                f"needs {DEMOTION_WEEKS} consecutive"
            ),
            not_ready_label="NO DEMOTION",
        )
    return Verdict(ready=False, reason="no red scheduled weeks", not_ready_label="NO DEMOTION")


# -- rendering ----------------------------------------------------------------------


def render_summary(
    *,
    streak: Streak,
    red: Streak,
    additions: Sequence[SkipAddition],
    inventory: Mapping[str, str],
    verdict: Verdict,
    demotion: Verdict,
) -> str:
    """The job-summary markdown: everything the promotion/demotion checklists
    in `docs/ci.md` need, answerable from this text alone."""
    lines: list[str] = ["## Spark promotion window", ""]
    lines.append(
        f"Consecutive green **scheduled** weekly runs: **{streak.length}/{PROMOTION_WEEKS}**"
    )
    if streak.broken_by:
        lines.append(f"- streak ends at: {streak.broken_by}")
    lines.append("")
    if streak.runs:
        lines.append("Qualifying runs (newest first):")
        lines.extend(
            f"- {run.created_at:%Y-%m-%d} — [{run.conclusion}]({run.url})" for run in streak.runs
        )
        lines.append("")

    lines.append(f"### Spark skip inventory ({len(inventory)})")
    if inventory:
        lines.extend(f"- `{name}`: {reason}" for name, reason in sorted(inventory.items()))
    else:
        lines.append("- (none — every scenario is declared runnable on spark)")
    lines.append("")

    lines.append(f"### Skip drift in the last {WINDOW_DAYS} days")
    if additions:
        lines.extend(f"- `{a.commit[:7]}`: {a.line}" for a in additions)
    else:
        lines.append("- none")
    lines.append("")

    lines.append(f"### Verdict\n\n{verdict.line}")
    lines.append("")
    lines.append(f"### Demotion watch\n\n{demotion.line}")
    if red.runs:
        # The demotion checklist has to confirm both reds are scheduled runs at
        # their final conclusion, so the summary must carry their links too —
        # not just the streak length (found by dry-running the checklist).
        lines.append("")
        lines.extend(
            f"- {run.created_at:%Y-%m-%d} — [{run.conclusion}]({run.url})" for run in red.runs
        )
    lines.append("")
    lines.append(
        "_This step reports only. Promotion and demotion are reviewed OpenSpec "
        "changes; see the checklists in `docs/ci.md`._"
    )
    return "\n".join(lines)


# -- the impure edge ----------------------------------------------------------------


def fetch_runs(repo: str, workflow: str, token: str, per_page: int = 100) -> list[dict[str, Any]]:
    """One page of completed scheduled runs for the workflow (report-only, so a
    failed API call degrades to an empty history rather than a red step)."""
    url = (
        f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/runs"
        f"?event={SCHEDULE_EVENT}&status=completed&per_page={per_page}"
    )
    request = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload: dict[str, Any] = json.load(response)
    except (urllib.error.URLError, OSError, ValueError) as exc:
        print(f"warning: could not read run history: {exc}", file=sys.stderr)
        return []
    return [payload]


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument("--workflow", default=WORKFLOW_FILE)
    parser.add_argument("--token", default=os.environ.get("GITHUB_TOKEN", ""))
    parser.add_argument(
        "--runs-json",
        default="",
        help="read the API payload from this file instead of the network "
        "(dry-running the promotion/demotion checklists against fixtures)",
    )
    parser.add_argument("--current-run-id", type=int, default=0)
    parser.add_argument("--current-event", default=os.environ.get("GITHUB_EVENT_NAME", ""))
    parser.add_argument(
        "--current-conclusion",
        default="",
        help="the test job's conclusion, passed in from `needs` (success/failure/...)",
    )
    parser.add_argument("--summary-path", default=os.environ.get("GITHUB_STEP_SUMMARY", ""))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)

    if args.runs_json:
        pages = [json.loads(Path(args.runs_json).read_text())]
    elif args.repo:
        pages = fetch_runs(args.repo, args.workflow, args.token)
    else:
        print(
            "warning: no --repo and no --runs-json; reporting on an empty history", file=sys.stderr
        )
        pages = []

    runs = scheduled_runs(parse_runs(pages))
    if args.current_conclusion and args.current_event:
        runs = with_current_run(
            runs,
            run_id=args.current_run_id,
            event=args.current_event,
            conclusion=args.current_conclusion,
            created_at=datetime.now(UTC),
            url=f"https://github.com/{args.repo}/actions/runs/{args.current_run_id}",
        )

    additions = added_spark_skips(git_log_spec_diff())
    inventory = current_skip_inventory()
    green = green_streak(runs)
    red = red_streak(runs)
    summary = render_summary(
        streak=green,
        red=red,
        additions=additions,
        inventory=inventory,
        verdict=promotion_verdict(green, additions),
        demotion=demotion_verdict(red),
    )

    print(summary)
    if args.summary_path:
        with Path(args.summary_path).open("a", encoding="utf-8") as handle:
            handle.write(summary + "\n")
    # Always 0: the promotion window is information, never a build verdict.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
