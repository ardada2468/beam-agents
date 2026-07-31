"""Unit tests for the Spark promotion-window reporter's pure logic.

`scripts/spark_weekly_status.py` is what makes the promotion gate mechanical
to *assess*: every weekly run publishes the consecutive-green streak over
scheduled runs, the weekly-cadence verdict, the current spark skip inventory,
and any skip added inside the trailing window. The script is pure Python over
JSON and git-log text it is handed, so all of that is exercised here offline —
no network, no GitHub, no docker.

Spec: openspec/changes/promote-spark-runner, capability `spark-runner-support`,
requirement "Weekly runs report the promotion window mechanically".
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from scripts.spark_weekly_status import (
    DEMOTION_WEEKS,
    PROMOTION_WEEKS,
    SCHEDULE_EVENT,
    added_spark_skips,
    demotion_verdict,
    green_streak,
    parse_runs,
    promotion_verdict,
    red_streak,
    render_summary,
    scheduled_runs,
    with_current_run,
)

NOW = datetime(2026, 7, 27, 6, 0, tzinfo=UTC)  # a Monday 06:00 UTC


def api_run(
    *,
    days_ago: float,
    conclusion: str = "success",
    event: str = SCHEDULE_EVENT,
    run_id: int | None = None,
) -> dict[str, Any]:
    """One GitHub `workflow_runs` entry, in the shape the API returns."""
    created = NOW - timedelta(days=days_ago)
    return {
        "id": run_id if run_id is not None else int(days_ago * 100) + 1,
        "event": event,
        "status": "completed",
        "conclusion": conclusion,
        "created_at": created.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "html_url": f"https://github.com/o/r/actions/runs/{int(days_ago * 100) + 1}",
    }


def page(*runs: dict[str, Any]) -> dict[str, Any]:
    return {"total_count": len(runs), "workflow_runs": list(runs)}


def weekly(conclusions: list[str], event: str = SCHEDULE_EVENT) -> list[dict[str, Any]]:
    """Newest-first weekly cadence: index 0 is today, then -7d, -14d, ..."""
    return [
        api_run(days_ago=7 * i, conclusion=c, event=event, run_id=1000 + i)
        for i, c in enumerate(conclusions)
    ]


class TestParsing:
    def test_runs_are_normalized_and_ordered_newest_first(self) -> None:
        runs = parse_runs([page(*weekly(["success", "failure", "success"]))])
        assert [r.conclusion for r in runs] == ["success", "failure", "success"]
        assert runs[0].created_at > runs[1].created_at > runs[2].created_at
        assert runs[0].green and not runs[1].green

    def test_multiple_api_pages_are_merged_and_sorted(self) -> None:
        older = page(*weekly(["success", "success"])[1:])
        newer = page(weekly(["success", "success"])[0])
        runs = parse_runs([older, newer])
        assert len(runs) == 2
        assert runs[0].created_at > runs[1].created_at

    def test_only_scheduled_runs_are_kept_for_the_window(self) -> None:
        mixed = [
            api_run(days_ago=0, event="workflow_dispatch", run_id=1),
            api_run(days_ago=1, event=SCHEDULE_EVENT, run_id=2),
        ]
        kept = scheduled_runs(parse_runs([page(*mixed)]))
        assert [r.run_id for r in kept] == [2]


class TestConsecutiveGreenStreak:
    """From scenario: Consecutive green scheduled runs accumulate a streak."""

    def test_four_green_weekly_runs_are_a_streak_of_four(self) -> None:
        runs = parse_runs([page(*weekly(["success"] * 4))])
        streak = green_streak(runs)
        assert streak.length == PROMOTION_WEEKS
        assert streak.broken_by == ""

    def test_verdict_is_ready_at_four_green_with_no_skip_drift(self) -> None:
        streak = green_streak(parse_runs([page(*weekly(["success"] * 4))]))
        verdict = promotion_verdict(streak, [])
        assert verdict.ready
        assert verdict.line == "PROMOTION READY"

    def test_three_green_weeks_is_not_yet_ready(self) -> None:
        streak = green_streak(parse_runs([page(*weekly(["success"] * 3))]))
        verdict = promotion_verdict(streak, [])
        assert not verdict.ready
        assert "3" in verdict.reason and str(PROMOTION_WEEKS) in verdict.reason

    def test_a_rerun_that_landed_green_counts_at_its_final_conclusion(self) -> None:
        # An InfraFailure week re-run to green is legitimate: the harness's
        # infra/verdict separation exists precisely so stack breakage is not a
        # Spark verdict. The API reports the final conclusion, so nothing
        # special is needed — this pins that we read `conclusion`, not
        # `run_attempt`.
        runs = weekly(["success"] * 4)
        runs[2]["run_attempt"] = 3
        streak = green_streak(parse_runs([page(*runs)]))
        assert streak.length == PROMOTION_WEEKS


class TestRedWeekResetsTheStreak:
    """From scenario: A red week resets the streak."""

    def test_newest_run_red_gives_a_zero_streak(self) -> None:
        streak = green_streak(parse_runs([page(*weekly(["failure", "success", "success"]))]))
        assert streak.length == 0
        assert "failure" in streak.broken_by

    def test_older_red_truncates_the_streak_at_it(self) -> None:
        streak = green_streak(
            parse_runs([page(*weekly(["success", "success", "failure", "success"]))])
        )
        assert streak.length == 2

    def test_no_partial_credit_is_carried_across_the_reset(self) -> None:
        # Scenario: Gate not satisfied leaves best-effort in place — the runs
        # before the red week do not count toward the new window.
        streak = green_streak(
            parse_runs([page(*weekly(["success", "failure", "success", "success", "success"]))])
        )
        assert streak.length == 1
        assert not promotion_verdict(streak, []).ready

    def test_a_cancelled_run_is_not_green(self) -> None:
        streak = green_streak(parse_runs([page(*weekly(["cancelled", "success"]))]))
        assert streak.length == 0


class TestCadenceGapBreaksConsecutiveness:
    """From scenario: A missed week breaks consecutiveness."""

    def test_a_gap_longer_than_eight_days_breaks_the_streak(self) -> None:
        runs = parse_runs(
            [
                page(
                    api_run(days_ago=0, run_id=1),
                    api_run(days_ago=7, run_id=2),
                    # Two weeks missing here: the schedule was disabled.
                    api_run(days_ago=35, run_id=3),
                    api_run(days_ago=42, run_id=4),
                )
            ]
        )
        streak = green_streak(runs)
        assert streak.length == 2
        assert "gap" in streak.broken_by.lower()

    def test_a_gap_is_never_silently_bridged_into_a_ready_verdict(self) -> None:
        runs = parse_runs(
            [
                page(
                    api_run(days_ago=0, run_id=1),
                    api_run(days_ago=7, run_id=2),
                    api_run(days_ago=30, run_id=3),
                    api_run(days_ago=37, run_id=4),
                )
            ]
        )
        assert not promotion_verdict(green_streak(runs), []).ready

    def test_a_slightly_late_run_within_eight_days_still_counts(self) -> None:
        # Scheduler jitter and a rerun landing a day late must not read as a
        # missed week; only a genuinely skipped cadence slot does.
        runs = parse_runs(
            [
                page(
                    api_run(days_ago=0, run_id=1),
                    api_run(days_ago=7.5, run_id=2),
                    api_run(days_ago=15, run_id=3),
                    api_run(days_ago=22, run_id=4),
                )
            ]
        )
        assert green_streak(runs).length == PROMOTION_WEEKS


class TestManualDispatchIsExcluded:
    """From scenario: Manual dispatch does not affect the promotion window."""

    def test_a_red_dispatch_run_does_not_break_a_green_streak(self) -> None:
        runs = weekly(["success"] * 4)
        runs.insert(1, api_run(days_ago=2, conclusion="failure", event="workflow_dispatch"))
        streak = green_streak(scheduled_runs(parse_runs([page(*runs)])))
        assert streak.length == PROMOTION_WEEKS

    def test_a_green_dispatch_run_does_not_extend_a_streak(self) -> None:
        runs = weekly(["success"] * 2)
        runs.insert(0, api_run(days_ago=1, conclusion="success", event="workflow_dispatch"))
        assert green_streak(scheduled_runs(parse_runs([page(*runs)]))).length == 2

    def test_the_in_flight_run_is_folded_in_only_when_it_is_scheduled(self) -> None:
        history = parse_runs([page(*weekly(["success"] * 3)[1:])])
        dispatched = with_current_run(
            history,
            run_id=99,
            event="workflow_dispatch",
            conclusion="success",
            created_at=NOW,
            url="u",
        )
        assert [r.run_id for r in dispatched] == [r.run_id for r in history]

        scheduled = with_current_run(
            history,
            run_id=99,
            event=SCHEDULE_EVENT,
            conclusion="success",
            created_at=NOW,
            url="u",
        )
        assert scheduled[0].run_id == 99
        assert green_streak(scheduled).length == 3

    def test_the_in_flight_run_replaces_its_own_api_entry(self) -> None:
        # The API listing may already carry the in-flight run (with a null
        # conclusion); the conclusion passed in from `needs` wins.
        history = parse_runs(
            [
                page(
                    *weekly(
                        ["success"] * 2,
                    )
                )
            ]
        )
        merged = with_current_run(
            history,
            run_id=history[0].run_id,
            event=SCHEDULE_EVENT,
            conclusion="failure",
            created_at=history[0].created_at,
            url="u",
        )
        assert len(merged) == len(history)
        assert merged[0].conclusion == "failure"
        assert green_streak(merged).length == 0


class TestSkipDrift:
    """From scenario: A skip added mid-window resets the promotion clock."""

    GIT_LOG = """commit 1111111111111111111111111111111111111111
Author: A <a@example.com>
Date:   Mon Jul 20 10:00:00 2026 +0000

    Record the spike finding for suspension_resume

diff --git a/tests/conformance/_spec.py b/tests/conformance/_spec.py
--- a/tests/conformance/_spec.py
+++ b/tests/conformance/_spec.py
@@ -230,7 +230,9 @@
-        SPARK: Run(),
+        SPARK: Skip(
+            "the Spark portable runner drops REAL_TIME timer firings in streaming mode"
+        ),
"""

    def test_an_added_spark_skip_is_detected_with_its_commit(self) -> None:
        additions = added_spark_skips(self.GIT_LOG)
        assert len(additions) == 1
        assert additions[0].commit.startswith("1111111")
        assert "SPARK" in additions[0].line

    def test_a_removed_spark_skip_is_not_an_addition(self) -> None:
        log = self.GIT_LOG.replace("+        SPARK: Skip(", "-        SPARK: Skip(")
        assert added_spark_skips(log) == []

    def test_an_added_flink_skip_is_not_spark_drift(self) -> None:
        log = self.GIT_LOG.replace("+        SPARK: Skip(", "+        FLINK: Skip(")
        assert added_spark_skips(log) == []

    def test_the_diff_header_lines_are_never_mistaken_for_additions(self) -> None:
        log = "commit abc\n+++ b/tests/conformance/_spec.py\n"
        assert added_spark_skips(log) == []

    def test_an_empty_log_means_no_drift(self) -> None:
        assert added_spark_skips("") == []

    def test_drift_makes_the_verdict_not_ready_regardless_of_run_conclusions(self) -> None:
        streak = green_streak(parse_runs([page(*weekly(["success"] * 4))]))
        verdict = promotion_verdict(streak, added_spark_skips(self.GIT_LOG))
        assert not verdict.ready
        assert "skip" in verdict.reason.lower()
        assert verdict.line.startswith("NOT READY")


class TestDemotionSignal:
    """From scenarios: Two consecutive red weeks demote / One red week does not."""

    def test_two_consecutive_red_scheduled_runs_trigger_demotion(self) -> None:
        red = red_streak(parse_runs([page(*weekly(["failure", "failure", "success"]))]))
        assert red.length == DEMOTION_WEEKS
        assert demotion_verdict(red).ready

    def test_one_red_week_does_not_demote(self) -> None:
        red = red_streak(parse_runs([page(*weekly(["failure", "success", "success"]))]))
        assert red.length == 1
        verdict = demotion_verdict(red)
        assert not verdict.ready
        assert "investigat" in verdict.reason.lower()

    def test_a_green_latest_run_means_no_demotion_signal(self) -> None:
        red = red_streak(parse_runs([page(*weekly(["success", "failure", "failure"]))]))
        assert red.length == 0
        assert not demotion_verdict(red).ready

    def test_a_cadence_gap_breaks_the_red_streak_too(self) -> None:
        runs = parse_runs(
            [
                page(
                    api_run(days_ago=0, conclusion="failure", run_id=1),
                    api_run(days_ago=30, conclusion="failure", run_id=2),
                )
            ]
        )
        assert red_streak(runs).length == 1


class TestSummaryRendering:
    def _summary(self, conclusions: list[str], additions: list[Any] | None = None) -> str:
        runs = parse_runs([page(*weekly(conclusions))])
        green = green_streak(runs)
        red = red_streak(runs)
        drift = additions if additions is not None else []
        return render_summary(
            streak=green,
            red=red,
            additions=drift,
            inventory={"ttl_expiry": "no idle-partition watermark control"},
            verdict=promotion_verdict(green, drift),
            demotion=demotion_verdict(red),
        )

    def test_ready_summary_reports_streak_and_verdict(self) -> None:
        summary = self._summary(["success"] * 4)
        assert "PROMOTION READY" in summary
        assert f"{PROMOTION_WEEKS}" in summary

    def test_summary_always_prints_the_current_skip_inventory(self) -> None:
        # The diff scan is a heuristic; the printed inventory is what makes a
        # refactor-shaped evasion visible week over week (design D5).
        summary = self._summary(["success"] * 4)
        assert "ttl_expiry" in summary
        assert "no idle-partition watermark control" in summary

    def test_not_ready_summary_names_the_reason(self) -> None:
        summary = self._summary(["failure", "success"])
        assert "NOT READY" in summary
        assert "failure" in summary

    def test_summary_reports_the_demotion_signal(self) -> None:
        summary = self._summary(["failure", "failure"])
        assert "DEMOTION" in summary.upper()

    def test_summary_links_the_red_runs_a_demotion_would_cite(self) -> None:
        # The demotion checklist confirms both reds are scheduled runs at their
        # final conclusion, so their links must be in the summary too.
        summary = self._summary(["failure", "failure", "success"])
        demotion_section = summary.split("### Demotion watch", 1)[1]
        assert demotion_section.count("https://github.com/o/r/actions/runs/") == DEMOTION_WEEKS

    def test_summary_links_the_qualifying_runs(self) -> None:
        # The promotion change must cite four run links; the summary is where
        # its author gets them.
        summary = self._summary(["success"] * 4)
        assert summary.count("https://github.com/o/r/actions/runs/") >= PROMOTION_WEEKS
