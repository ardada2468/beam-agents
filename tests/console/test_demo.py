"""The demo pipeline, driven end to end on the DirectRunner over the fake provider.

These assertions are about the *records the pipeline emits* — real `TraceEvent`s
and `ActivationError`s off `RunAgentOutputs` — because that is what the demo owns.
The store and the `console://` sink are separate units, so every test here injects
a recording delivery instead: the demo's contract is "produce the full event
vocabulary", not "write it to SQLite".

One cached round feeds most of the suite. A demo round is a real streaming Beam
pipeline with two `RunAgent` transforms and a scripted `TestStream`; running it
once per assertion would spend minutes proving one thing.
"""

from __future__ import annotations

from functools import cache

import pytest

from beam_agents._protos import TraceEvent
from beam_agents.console._demo import (
    DEFAULT_ENTITY_COUNT,
    SCENARIOS,
    UNROUTABLE_TOOL,
    DemoRecords,
    entity_key_for,
    main,
    render_summary,
    run_demo,
    scenario_of,
    summarize,
)

# A demo round runs a streaming pipeline with two stateful `RunAgent`s and a
# scripted clock; the repo-wide 30s per-test timeout is sized for unit tests, not
# for that.
pytestmark = pytest.mark.timeout(600)


class _Recorder:
    """A delivery that keeps every round's records instead of storing them."""

    def __init__(self) -> None:
        self.rounds: list[DemoRecords] = []

    def deliver(self, records: DemoRecords) -> None:
        self.rounds.append(records)


def _run(**options: object) -> _Recorder:
    recorder = _Recorder()
    run_demo(delivery=recorder, **options)  # type: ignore[arg-type]
    return recorder


@cache
def _one_round() -> DemoRecords:
    recorder = _Recorder()
    run_demo(delivery=recorder, seed=0)
    return recorder.rounds[0]


@pytest.fixture
def records() -> DemoRecords:
    """One demo round over every scenario, at the default seed.

    Function-scoped over a cached round rather than `scope="module"`, which is
    the obvious way to write this and is wrong: pytest sets a module-scoped
    fixture up *before* the function-scoped autouse `clean_registry`, so the
    pipeline's `register_coders()` would land outside that snapshot and leak a
    mutated global coder registry into every test that ran afterwards.
    """
    return _one_round()


def _events(records: DemoRecords, scenario: str) -> list[TraceEvent]:
    return [e for e in records.traces if scenario_of(e.entity_key) == scenario]


def _types(records: DemoRecords, scenario: str) -> list[str]:
    return [TraceEvent.EventType.Name(e.event_type) for e in _events(records, scenario)]


def _reasons(records: DemoRecords, scenario: str) -> list[str]:
    return [e.reason for e in records.errors if scenario_of(e.entity_key) == scenario]


def _error_trace(records: DemoRecords, scenario: str) -> TraceEvent:
    return next(e for e in _events(records, scenario) if e.event_type == TraceEvent.ERROR)


def _detail(records: DemoRecords, scenario: str, reason: str) -> str:
    return next(
        e.detail
        for e in records.errors
        if scenario_of(e.entity_key) == scenario and e.reason == reason
    )


# --- Requirement: the demo drives every scenario in SCENARIOS ------------------


def test_every_scenario_produces_records(records: DemoRecords) -> None:
    # Scenario: Every scenario produces records. An empty console teaches
    # nothing, so the demo's whole contract is that no name in `SCENARIOS` comes
    # back with nothing behind it.
    summary = summarize(records)
    assert tuple(summary) == SCENARIOS
    empty = [name for name, entry in summary.items() if not entry.event_types and not entry.reasons]
    assert empty == []


def test_default_entity_count_covers_each_scenario_once(records: DemoRecords) -> None:
    # Scenario: Default entity count covers each scenario once. The default is
    # one entity per scenario, so a first-time console shows the whole
    # vocabulary without a scenario appearing twice.
    assert len(SCENARIOS) == DEFAULT_ENTITY_COUNT
    keys = {e.entity_key for e in records.traces} | {e.entity_key for e in records.errors}
    assert sorted(scenario_of(key) for key in keys) == sorted(SCENARIOS)


# --- Requirement: the successful activations trace their full bracket ----------


def test_completion_scenario_brackets_one_model_call(records: DemoRecords) -> None:
    # Scenario: Completion brackets one model call. The simplest activation:
    # start, one LLM_CALL, end with status `completed`, and nothing on `.errors`.
    assert _types(records, "completion") == ["ACTIVATION_START", "LLM_CALL", "ACTIVATION_END"]
    end = _events(records, "completion")[-1]
    assert end.attributes["beam_agents.activation.status"] == "completed"
    assert _reasons(records, "completion") == []


def test_multi_tool_scenario_traces_one_tool_call_per_inline_tool(records: DemoRecords) -> None:
    # Scenario: Multi-tool traces one TOOL_CALL per inline tool. The console's
    # per-tool volume panel is sourced from `beam_agents.tool_name`, so each
    # inline call has to arrive as its own span.
    calls = [e for e in _events(records, "multi_tool") if e.event_type == TraceEvent.TOOL_CALL]
    # Not order-asserted: `run_tool` does not advance the step cursor, so two
    # tools called at one step carry the same `step_index` and their relative
    # order is not recoverable from the trace.
    assert sorted(e.attributes["beam_agents.tool_name"] for e in calls) == [
        "lookup_customer",
        "risk_score",
    ]
    assert len({e.span_id for e in calls}) == len(calls)


def test_cache_hit_scenario_traces_a_billed_call_then_an_unbilled_hit(
    records: DemoRecords,
) -> None:
    # Scenario: Cache hit traces a billed call then an unbilled hit. The
    # cache-hit ratio the console renders comes from `beam_agents.cache_hit`,
    # and a hit still reports real token counts because the stored response is
    # decoded.
    calls = [e for e in _events(records, "cache_hit") if e.event_type == TraceEvent.LLM_CALL]
    assert [e.attributes["beam_agents.cache_hit"] for e in calls] == ["false", "true"]
    assert [e.attributes["beam_agents.billed"] for e in calls] == ["true", "false"]
    for call in calls:
        assert int(call.attributes["gen_ai.usage.input_tokens"]) > 0
        assert int(call.attributes["gen_ai.usage.output_tokens"]) > 0


# --- Requirement: the approval queue has something in it ----------------------


def test_suspension_approved_scenario_resumes_inside_one_trace(records: DemoRecords) -> None:
    # Scenario: Approved suspension resumes inside one trace. Trace identity is
    # `uuid5(entity_key, seq)`, so suspend and resume share a trace and the
    # console can render the cycle as one activation with two attempts.
    events = _events(records, "suspension_approved")
    assert len({e.trace_id for e in events}) == 1
    kinds = [
        e.attributes["beam_agents.activation.kind"]
        for e in events
        if e.event_type == TraceEvent.ACTIVATION_START
    ]
    assert kinds == ["start", "resume"]
    statuses = [
        e.attributes["beam_agents.activation.status"]
        for e in events
        if e.event_type == TraceEvent.ACTIVATION_END
    ]
    assert statuses == ["suspended", "completed"]
    assert b"approved:" in b"".join(records.outputs)


def test_suspension_denied_scenario_resumes_with_the_operator_denial(
    records: DemoRecords,
) -> None:
    # Scenario: Denied suspension resumes with the operator denial. A denial is
    # an answered approval, not a timeout: the resumed activation completes and
    # nothing reaches `.errors`.
    statuses = [
        e.attributes["beam_agents.activation.status"]
        for e in _events(records, "suspension_denied")
        if e.event_type == TraceEvent.ACTIVATION_END
    ]
    assert statuses == ["suspended", "completed"]
    assert _reasons(records, "suspension_denied") == []
    assert b"denied:" in b"".join(records.outputs)


def test_suspension_timeout_scenario_dead_letters_hitl_timeout(records: DemoRecords) -> None:
    # Scenario: Unanswered suspension dead-letters `hitl_timeout`. The scripted
    # processing-time advance elapses the deadline and the demo's drop route
    # puts the record on `.errors`, which is the only way the console's
    # timed-out-approval view is ever populated.
    assert _reasons(records, "suspension_timeout") == ["hitl_timeout"]
    error = _error_trace(records, "suspension_timeout")
    assert error.attributes["beam_agents.reason"] == "hitl_timeout"


# --- Requirement: the closed error vocabulary is actually produced -------------


def test_tool_error_scenario_names_the_tool_exception_type(records: DemoRecords) -> None:
    # Scenario: A failing inline tool names its exception type. An inline tool
    # raises before its TOOL_CALL span is staged, so the only record is the
    # activation failure — and `error.type` is what tells it apart from an
    # ordinary agent bug under the same `activation_error` reason.
    assert _reasons(records, "tool_error") == ["activation_error"]
    error = _error_trace(records, "tool_error")
    assert error.attributes["error.type"] == "ToolError"


def test_activation_error_scenario_carries_the_failure_position(records: DemoRecords) -> None:
    # Scenario: A raising agent carries its failure position. The
    # `beam_agents.failure.*` scalars are what the console's failure-context
    # panel renders, and they exist only on this route.
    assert _reasons(records, "activation_error") == ["activation_error"]
    error = _error_trace(records, "activation_error")
    assert error.attributes["error.type"] == "RuntimeError"
    assert error.attributes["beam_agents.failure.llm_calls"] == "1"
    assert error.attributes["beam_agents.failure.last_event"] == "LLM_CALL"


def test_budget_exceeded_scenario_is_its_own_reason(records: DemoRecords) -> None:
    # Scenario: A tripped token budget is its own reason. `budget_exceeded` is
    # deliberately distinct from `activation_error` — the triage is a cost
    # question, not a stack trace — so the console groups it separately.
    assert _reasons(records, "budget_exceeded") == ["budget_exceeded"]
    error = _error_trace(records, "budget_exceeded")
    assert error.attributes["error.type"] == "BudgetExceeded"


def test_orphaned_result_scenario_reports_no_continuation(records: DemoRecords) -> None:
    # Scenario: A result with nothing to resume is orphaned. The demo delivers a
    # tool result for a key whose activation already completed; admission
    # refuses it and says which of the four ways it failed.
    assert _reasons(records, "orphaned_result") == ["orphaned_result"]
    detail = _detail(records, "orphaned_result", "orphaned_result")
    assert detail.startswith("no_continuation:")


def test_intent_dead_letter_scenario_reaches_errors_through_the_outbox(
    records: DemoRecords,
) -> None:
    # Scenario: An unroutable intent reaches `.errors` as a dead letter. The
    # record is built by the runtime's own `intent_dead_letter_to_error`, off
    # the `WriteIntentsResult.dead_letter` branch `RunAgent` exposes.
    assert _reasons(records, "intent_dead_letter") == ["intent_dead_letter"]
    detail = _detail(records, "intent_dead_letter", "intent_dead_letter")
    assert UNROUTABLE_TOOL in detail


def test_batch_overflow_scenario_sheds_events_past_the_buffer_cap(records: DemoRecords) -> None:
    # Scenario: A deferred buffer sheds events past its cap. Overflow is
    # reachable only while a suspension defers flushing, so the demo's batching
    # branch suspends first and then keeps the burst coming: of eight events, two
    # flush the first batch (which suspends), four fill the deferred buffer to
    # its cap, and the last two are shed.
    reasons = _reasons(records, "batch_overflow")
    assert reasons.count("batch_buffer_overflow") == 2
    assert "cap=" in _detail(records, "batch_overflow", "batch_buffer_overflow")
    # The still-suspended key is also what working-memory GC reaches at the
    # watermark advance, so the round covers two more reasons for free.
    assert reasons.count("ttl_wiped_suspension") == 1
    assert reasons.count("ttl_wiped_batch") == 4


# --- Requirement: the same seed produces the same console ---------------------


def test_same_seed_produces_byte_identical_records() -> None:
    # Scenario: The same seed produces byte-identical records. Trace identity is
    # `uuid5(entity_key, seq)` and the fake provider replays a script, so the
    # docs' screenshots are reproducible only if the whole round is.
    first = _run(seed=7, scenarios=("completion", "cache_hit", "activation_error"), entities=3)
    second = _run(seed=7, scenarios=("completion", "cache_hit", "activation_error"), entities=3)
    assert first.rounds[0].digest() == second.rounds[0].digest()
    assert render_summary(first.rounds[0]) == render_summary(second.rounds[0])


def test_a_different_seed_moves_the_whole_round() -> None:
    # Scenario: A different seed moves the whole round. Loop rounds must not
    # collide in the store, which dedups on `(trace_id, span_id, event_type)`;
    # the seed is in the entity key, so it moves every trace id with it.
    first = _run(seed=1, scenarios=("completion",), entities=1)
    second = _run(seed=2, scenarios=("completion",), entities=1)
    assert first.rounds[0].digest() != second.rounds[0].digest()
    assert {e.trace_id for e in first.rounds[0].traces}.isdisjoint(
        {e.trace_id for e in second.rounds[0].traces}
    )


def test_entity_key_carries_scenario_seed_and_index() -> None:
    # Scenario: An entity key carries its scenario, seed, and index. The console
    # lists activations by `entity_key`, so the key is the demo's only way to
    # say which scenario a row came from.
    key = entity_key_for("cache_hit", 3, 9)
    assert scenario_of(key) == "cache_hit"
    assert key != entity_key_for("cache_hit", 3, 10)
    assert key != entity_key_for("cache_hit", 4, 9)


# --- Requirement: run_demo's delivery target and loop are caller-controlled ----


def test_loop_delivers_one_batch_per_round() -> None:
    # Scenario: Looping delivers one batch per round. `docker compose up` should
    # show traffic arriving rather than a static snapshot, so each round is its
    # own delivery with its own seed.
    recorder = _Recorder()
    run_demo(
        delivery=recorder,
        loop=True,
        rounds=2,
        interval_s=0.0,
        scenarios=("completion",),
        entities=1,
        seed=100,
    )
    assert len(recorder.rounds) == 2
    assert recorder.rounds[0].digest() != recorder.rounds[1].digest()


def test_run_demo_returns_the_committed_activation_count() -> None:
    # Scenario: `run_demo` returns the committed activation count. An activation
    # that failed commits nothing and traces no bracket, so only the ones the
    # console can show as activations are counted.
    recorder = _Recorder()
    produced = run_demo(delivery=recorder, scenarios=("completion",), entities=2, seed=3)
    starts = [e for e in recorder.rounds[0].traces if e.event_type == TraceEvent.ACTIVATION_START]
    assert produced == len(starts) == 2


def test_run_demo_without_a_delivery_target_is_a_configuration_error() -> None:
    # Scenario: No delivery target is a configuration error. Silently running a
    # pipeline whose records go nowhere is the one outcome a demo must not have.
    with pytest.raises(ValueError, match="needs somewhere to put its records"):
        run_demo(scenarios=("completion",), entities=1)


def test_an_unknown_scenario_is_rejected_before_a_pipeline_exists() -> None:
    # Scenario: An unknown scenario is rejected up front. A typo names nothing
    # the agent can drive, and failing at the call site beats an empty console.
    with pytest.raises(ValueError, match="not a demo scenario"):
        run_demo(delivery=_Recorder(), scenarios=("compltion",), entities=1)


def test_a_scenario_subset_drives_only_the_named_scenarios() -> None:
    # Scenario: A subset drives only what it names. The docs walk one scenario
    # at a time, which is only readable if the round contains nothing else.
    recorder = _Recorder()
    run_demo(delivery=recorder, scenarios=("multi_tool",), entities=1, seed=11)
    summary = summarize(recorder.rounds[0])
    assert tuple(summary) == ("multi_tool",)


# --- Requirement: `python -m beam_agents.console._demo` runs it ---------------


def test_main_prints_a_per_scenario_summary(capsys: pytest.CaptureFixture[str]) -> None:
    # Scenario: The module entry point prints a per-scenario summary. With no
    # console and no store there is still something to see, which is what makes
    # the demo usable as a first look and as the docs' pasted output.
    exit_code = main(["--scenarios", "completion,tool_error", "--entities", "2", "--seed", "5"])
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "completion" in out
    assert "tool_error" in out
    assert "activation_error" in out  # the reason a failing inline tool produces
