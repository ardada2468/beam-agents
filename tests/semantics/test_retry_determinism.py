"""Retry-determinism gate (project.md's `-m semantics` promise).

Verifies: a chaos-forced bundle retry of a resumed activation adds zero
additional real provider calls (the resume's repeated LLM call reads the
`LLM_CACHE` committed at suspend time, both on the discarded failed attempt
and on Beam's own retry), and commits the deterministically-expected
`ToolIntent`.

See openspec/changes/add-retry-determinism-gate/design.md for the empirical
findings behind this shape: Beam's classic (streaming) DirectRunner already
retries a failed bundle with genuine per-key state rollback, so the chaos
helper only needs to fail one targeted commit once; the cache key bakes in
`seq`, so "zero additional calls" is only achievable across a suspend->resume
pair (same seq), not a raw single-shot activation's own retry.
"""

from __future__ import annotations

import pytest
from apache_beam.metrics.metric import MetricResults, MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.transforms.window import TimestampedValue

from beam_agents._protos import AgentEnvelope, ToolIntent, ToolResult, TraceEvent
from beam_agents.core.agent import Complete, Suspend, intent_id_for
from beam_agents.core.batching import BatchPolicy
from beam_agents.core.context import ActivationContext
from beam_agents.core.loop import ActivationResult
from beam_agents.core.transform import AgentConfig, RunAgent
from beam_agents.memory import SummarizationView, SummarizeCompactor
from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import FakeLLM
from beam_agents.observability import trace_id_for
from beam_agents.observability.metrics import NAMESPACE
from beam_agents.testing.chaos import fail_first_matching_commit
from tests.core._context_helpers import decode_len_based
from tests.core._dofn_helpers import keyed, make_pong_provider
from tests.semantics._helpers import (
    batch_act_agent,
    batch_suspend_then_recall_agent,
    budgeted_suspend_then_recall_agent,
    suspend_then_recall_agent,
)

pytestmark = pytest.mark.semantics

_ENTITY_KEY = b"k"
_SEQ = 0
# call_model() consumes step_index=0 (the pre-suspend call); act() consumes
# step_index=1. These are pure functions of call order, per agent-context.
_INTENT_STEP_INDEX = 1
_EXPECTED_INTENT_ID = intent_id_for(_ENTITY_KEY, _SEQ, _INTENT_STEP_INDEX)
# Derived from the activation scope alone, so suspend and resume — which
# share `seq` — land in the same trace with nothing carried on the wire.
_EXPECTED_TRACE_ID = trace_id_for(_ENTITY_KEY, _SEQ)


def _streaming_pipeline() -> BeamTestPipeline:
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    return BeamTestPipeline(options=options)


def _event(key: bytes, payload: bytes, t_ms: int) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=key, event_time_ms=t_ms, external_event=payload)
    return TimestampedValue(env, t_ms / 1000)


def _tool_result(key: bytes, intent_id: str, t_ms: int) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=key, event_time_ms=t_ms)
    env.tool_result.intent_id = intent_id
    env.tool_result.entity_key = key
    env.tool_result.payload = b"tool-done"
    env.tool_result.status = ToolResult.OK
    return TimestampedValue(env, t_ms / 1000)


def _is_resume_commit(result: ActivationResult) -> bool:
    # The resume is the only "completed" commit in this scenario; the
    # pre-suspend activation commits with status "suspended".
    return result.status == "completed"


def _assert_zero_additional_calls(traces: object) -> None:
    """The invariant under test: exactly one real provider call (the
    unavoidable pre-suspend miss) and exactly one cache hit (the resume),
    regardless of how many attempts Beam needed to commit the resume.
    """
    llm_events = [t for t in traces if t.event_type == TraceEvent.LLM_CALL]  # type: ignore[attr-defined]
    cache_hits = [e.attributes["beam_agents.cache_hit"] for e in llm_events]
    assert cache_hits.count("false") == 1, (
        f"expected exactly one real provider call, got trace cache_hit values {cache_hits!r}"
    )
    assert cache_hits.count("true") == 1, (
        f"expected exactly one cache hit (the resume), got trace cache_hit values {cache_hits!r}"
    )
    assert len(llm_events) == 2, f"expected exactly two LLM_CALL traces, got {len(llm_events)}"


def _check_output(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert items == [b"resumed:pong"], f"unexpected output: {items!r}"


def _check_no_errors(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert items == [], f"unexpected errors: {items!r}"


def _check_committed_intent(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert len(items) == 1, f"expected exactly one committed intent, got {items!r}"
    intent = items[0]
    assert intent.intent_id == _EXPECTED_INTENT_ID
    assert intent.seq == _SEQ
    assert intent.step_index == _INTENT_STEP_INDEX
    # The intent's whole encoding is the determinism claim, not just its ID:
    # `trace_id` rides on it now, and a non-deterministic one would make a
    # retried bundle's intents differ byte-for-byte while every field the
    # earlier assertions look at still matched.
    assert intent.trace_id == _EXPECTED_TRACE_ID
    assert intent.SerializeToString(deterministic=True) == _expected_intent_bytes(intent)


def _expected_intent_bytes(intent: object) -> bytes:
    """Re-mint the intent from its own scope and encode it.

    Independent of the committed message: everything here is derived from
    `(entity_key, seq, step_index)` and the agent's own arguments, so a match
    proves the commit was reproducible rather than merely self-consistent.
    """
    expected = ToolIntent(
        intent_id=_EXPECTED_INTENT_ID,
        entity_key=_ENTITY_KEY,
        seq=_SEQ,
        step_index=_INTENT_STEP_INDEX,
        tool_name=intent.tool_name,  # type: ignore[attr-defined]
        args_json=intent.args_json,  # type: ignore[attr-defined]
        created_at_ms=intent.created_at_ms,  # type: ignore[attr-defined]
        expires_at_ms=intent.expires_at_ms,  # type: ignore[attr-defined]
        attempt=0,
        kind=intent.kind,  # type: ignore[attr-defined]
        trace_id=_EXPECTED_TRACE_ID,
    )
    return expected.SerializeToString(deterministic=True)


def _check_one_trace_spans_the_suspension(actual: object) -> None:
    """Every event of the suspend/resume cycle shares one trace ID.

    The resume runs under the suspended activation's `seq`, so it recomputes
    the same trace with nothing carried on the wire — including across the
    chaos-forced retry, which re-emits byte-identical events.
    """
    trace_ids = {event.trace_id for event in actual}  # type: ignore[attr-defined]
    assert trace_ids == {_EXPECTED_TRACE_ID}, f"expected one trace, got {trace_ids!r}"


def test_chaos_forced_resume_retry_adds_zero_calls_and_commits_deterministic_intent() -> None:
    with fail_first_matching_commit(_is_resume_commit), _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(_ENTITY_KEY, b"go", 1000)])
            .add_elements([_tool_result(_ENTITY_KEY, _EXPECTED_INTENT_ID, 2000)])
            .advance_watermark_to_infinity()
        )
        envs = p | stream
        out = keyed(envs) | RunAgent(
            suspend_then_recall_agent,
            config=AgentConfig(provider_factory=make_pong_provider),
        )
        # `assert_that` matchers must assert (and raise) on the collected
        # values themselves — a closure that copies values out to an
        # outer list does not reliably survive Beam's own serialization
        # of the assertion DoFn, even on the in-process DirectRunner.
        assert_that(out.output, _check_output, label="output")
        assert_that(out.errors, _check_no_errors, label="errors")
        assert_that(out.intents, _check_committed_intent, label="intents")
        assert_that(out.traces, _assert_zero_additional_calls, label="traces")
        assert_that(out.traces, _check_one_trace_spans_the_suspension, label="trace-id")


def test_measurement_does_not_change_what_a_retried_bundle_commits() -> None:
    # Scenario: Replay determinism is unaffected by measurement.
    #
    # Durations are read from a monotonic clock *inside* the activation, so this
    # gate is where that has to be shown harmless: across a chaos-forced bundle
    # retry, with the two attempts measuring different elapsed times, the
    # committed side must be identical -- one real provider call, one
    # byte-identical intent, one output. Measurement is not a decision input.
    #
    # The counters themselves are deliberately not asserted on. This pipeline
    # runs on the classic DirectRunner (a REAL_TIME timer spec rules out the
    # FnApiRunner), whose metrics implementation reports one bundle's updates
    # and drops the rest, so the retried resume's increments are not visible
    # here regardless of what the code does. What the retry *does* to counters
    # -- attempted values, so a double count is expected and harmless -- is a
    # documented property of Beam metrics, not something this runner can
    # demonstrate.
    with fail_first_matching_commit(_is_resume_commit), _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(_ENTITY_KEY, b"go", 1000)])
            .add_elements([_tool_result(_ENTITY_KEY, _EXPECTED_INTENT_ID, 2000)])
            .advance_watermark_to_infinity()
        )
        out = keyed(p | stream) | RunAgent(
            suspend_then_recall_agent,
            config=AgentConfig(provider_factory=make_pong_provider),
        )
        assert_that(out.output, _check_output, label="output")
        assert_that(out.errors, _check_no_errors, label="errors")
        assert_that(out.intents, _check_committed_intent, label="intents")
        assert_that(out.traces, _assert_zero_additional_calls, label="traces")

    # The metric surface is live in this pipeline (not stubbed out), so the
    # determinism above is measured *with* recording switched on.
    query = p.result.metrics().query(MetricsFilter().with_namespace(NAMESPACE))
    counters = {m.key.metric.name: m.result for m in query[MetricResults.COUNTERS]}
    assert counters["activations"] >= 1


# --- Requirement: the budget charges every consumed response (token-budgets) ---
#
# The budget is a DECISION, taken mid-activation and upstream of every intent
# the activation goes on to mint, so its input has to be a pure function of the
# deterministic walk. The retried walk here serves its call from the replay
# cache, where the original missed: a billed-only meter would charge 0 where the
# original charged N, take a different branch, and break the byte-identical
# intents this file exists to protect. Charging consumed responses -- cache hits
# included -- is what makes the two walks agree.

# `make_pong_provider` answers b"pong"; `decode_len_based` reports `2 * len`, so
# each consumed response charges 8 and the resume's own attempt charges exactly
# that (its single call, served from the cache the suspension committed).
_BUDGET_CHARGE_PER_CALL = 8
_BUDGET_LIMIT = 1_000


def _budgeted_config() -> AgentConfig:
    return AgentConfig(
        provider_factory=make_pong_provider,
        decode=decode_len_based,
        max_tokens_per_activation=_BUDGET_LIMIT,
    )


def _check_the_retry_charged_the_same_total(actual: object) -> None:
    """One output, carrying the charge the committed attempt made.

    The resume is the chaos-failed bundle, so whichever attempt commits, the
    number on this output is the budget decision that survived — and it has to
    be the cache-served walk's, because that is the attempt Beam retried.
    """
    items = list(actual)  # type: ignore[call-overload]
    expected = b"resumed:pong#" + str(_BUDGET_CHARGE_PER_CALL).encode()
    assert items == [expected], f"unexpected output: {items!r}"


def test_a_chaos_forced_bundle_retry_makes_the_identical_budget_decision() -> None:
    # Scenario: A chaos-forced bundle retry makes the identical budget decision.
    with fail_first_matching_commit(_is_resume_commit), _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(_ENTITY_KEY, b"go", 1000)])
            .add_elements([_tool_result(_ENTITY_KEY, _EXPECTED_INTENT_ID, 2000)])
            .advance_watermark_to_infinity()
        )
        out = keyed(p | stream) | RunAgent(
            budgeted_suspend_then_recall_agent, config=_budgeted_config()
        )
        assert_that(out.output, _check_the_retry_charged_the_same_total, label="output")
        assert_that(out.errors, _check_no_errors, label="errors")
        # The rest of the gate, unchanged under a configured budget: the same
        # byte-identical intent, zero additional provider calls, one trace.
        assert_that(out.intents, _check_committed_intent, label="intents")
        assert_that(out.traces, _assert_zero_additional_calls, label="traces")
        assert_that(out.traces, _check_one_trace_spans_the_suspension, label="trace-id")


# ---------------------------------------------------------------------------------
# The same gate, with a summarizing agent (memory-compaction, design D2)
# ---------------------------------------------------------------------------------
#
# `SummarizeCompactor` runs INSIDE the activation and calls the model only
# through `ctx.call_model`, so its request is keyed by `(content, key, seq)`,
# staged in the replay cache, and committed with the bundle. This is where that
# placement is proven rather than asserted in prose: the suspend commits the
# summarization's cache entry, the resume's identical summarization request is
# served from it, and the chaos-forced retry of the resume re-derives the same
# request from the same rolled-back state and is served from it again — so the
# whole run reaches the provider exactly once, on the suspending activation.
#
# The provider is a single shared `FakeLLM` (the factory hands out one instance
# to every DoFn setup in the run, as `test_longterm_retry_determinism` does with
# its store), so the count below spans the DISCARDED attempt too — the trace
# assertions cannot see it, and it is exactly the walk a regression would
# re-call the provider on.

_SUMMARY_KEY = "summary"
# Equal to the FakeLLM's scripted response, which makes the fold a fixed point:
# summarizing twice yields the same summary, so the resume's summarization has
# the same `(items, prior_summary)` inputs as the suspend's and therefore the
# same cache key. Without a stable prior, the resume would issue a genuinely
# novel request and a retry would legitimately repeat it.
_SEED_SUMMARY = b"pong"
_LOG_KEY = "log"
_LOG_ITEMS = tuple(f"entry-{index:02d}".encode() for index in range(12))
# Small enough that the 12 items cross it, large enough that a folded key
# (summary alone) does not.
_TRIGGER_BYTES = 100
_SUMMARIZE_INTENT_ID = intent_id_for(_ENTITY_KEY, _SEQ, 0)

#: Every post-fold `MemoryBlob`, recorded per attempt (module-global, so the
#: pickled summarizer copy inside the DoFn appends to this list in-process).
_FOLDED_BLOBS: list[bytes] = []
_SHARED_PROVIDER: list[FakeLLM] = [make_pong_provider()]


def shared_provider() -> FakeLLM:
    """Hand every DoFn setup the one provider instance the test can count."""
    return _SHARED_PROVIDER[0]


def summary_request(items: tuple[bytes, ...], prior_summary: bytes | None) -> LlmRequest:
    """Pure function of its inputs — the caller-side determinism obligation."""
    return LlmRequest(
        model_id="m",
        messages=["summarize", [item.decode() for item in items], (prior_summary or b"").decode()],
        tools_schema=None,
        sampling_params=None,
    )


def extract_summary(response: bytes) -> bytes:
    return response


class _RecordingSummarizer(SummarizeCompactor):
    """`SummarizeCompactor` that records the blob each fold produced.

    `_FOLDED_BLOBS` is a module global, so the pickled copy of this object
    living inside the DoFn appends to the list this test reads — the same
    in-process observation `test_longterm_retry_determinism` makes of its store.
    """

    async def compact(self, view: SummarizationView) -> None:
        await super().compact(view)
        _FOLDED_BLOBS.append(view.memory.to_blob().SerializeToString(deterministic=True))


async def summarizing_suspend_then_resume_agent(
    ctx: ActivationContext,
) -> Complete | Suspend:
    """Seed a stable summary, refill the log ring, suspend once, then complete.

    The seed write is what makes the two activations' summarization inputs
    identical; everything the agent stages is a pure function of committed
    state, so both attempts of the resume walk the same path.
    """
    ctx.memory.set(_SUMMARY_KEY, _SEED_SUMMARY)
    for item in _LOG_ITEMS:
        ctx.memory.append(_LOG_KEY, item, max_items=64)
    if not ctx.is_resume:
        ctx.act("http.post", '{"url":"x"}', ttl_ms=60_000)
        return Suspend(snapshot=b"waiting", adapter="test", timeout_ms=60_000)
    return Complete(output=b"resumed")


def _check_summarized_output(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert items == [b"resumed"], f"unexpected output: {items!r}"


def test_the_summarization_llm_call_replays_from_cache_on_bundle_retry() -> None:
    # Scenario: The summarization LLM call replays from cache on bundle retry.
    _FOLDED_BLOBS.clear()
    _SHARED_PROVIDER[0] = make_pong_provider()
    provider = _SHARED_PROVIDER[0]
    summarizer = _RecordingSummarizer(
        build_request=summary_request,
        extract_summary=extract_summary,
        source_keys=(_LOG_KEY,),
        summary_key=_SUMMARY_KEY,
        keep_recent=0,
        trigger_bytes=_TRIGGER_BYTES,
    )

    with fail_first_matching_commit(_is_resume_commit), _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(_ENTITY_KEY, b"go", 1000)])
            .add_elements([_tool_result(_ENTITY_KEY, _SUMMARIZE_INTENT_ID, 2000)])
            .advance_watermark_to_infinity()
        )
        out = keyed(p | stream) | RunAgent(
            summarizing_suspend_then_resume_agent,
            config=AgentConfig(provider_factory=shared_provider, summarizer=summarizer),
        )
        assert_that(out.output, _check_summarized_output, label="output")
        assert_that(out.errors, _check_no_errors, label="errors")

    # Three summarization passes ran — the suspend, the discarded resume
    # attempt, and Beam's retry of it — and exactly ONE of them reached the
    # provider. The two resume attempts were served from the replay cache the
    # suspend committed, which is the invariant-3 claim for tier-2 compaction.
    assert len(_FOLDED_BLOBS) >= 3, (
        f"expected the retry to re-run the summarizer, got {len(_FOLDED_BLOBS)} passes"
    )
    assert provider.call_count == 1, (
        f"the retried summarization re-hit the provider: {provider.call_count} calls, "
        f"requests={provider.requests!r}"
    )

    # ...and the discarded attempt and the retry folded to byte-identical
    # `MemoryBlob`s, so which attempt commits cannot change what is committed.
    assert _FOLDED_BLOBS[-1] == _FOLDED_BLOBS[-2], "the retry committed a different blob"


def test_assertion_helper_catches_a_broken_cache_first_path() -> None:
    # Negative-path check (task 2.6): a hand-built trace list simulating a
    # regression that re-calls the provider on resume (two "false" cache_hit
    # events instead of one "false" + one "true") must fail the assertion.
    broken_traces = [
        TraceEvent(
            event_type=TraceEvent.LLM_CALL,
            attributes={"beam_agents.cache_hit": "false"},
        ),
        TraceEvent(
            event_type=TraceEvent.LLM_CALL,
            attributes={"beam_agents.cache_hit": "false"},
        ),
    ]
    with pytest.raises(AssertionError):
        _assert_zero_additional_calls(broken_traces)


# --- Requirement: one activation per flush with unchanged replay accounting ---

# The batch scenarios buffer two events per key and flush on the size
# threshold, so no wall clock and no processing-time advance is involved: the
# retry behavior under test is the flush's, not the timer's.
_BATCH_SIZE = 2
# The flush's `call_model` consumes step_index=0, so its `act` mints at 1 --
# the same call-order derivation as the per-event scenario above.
_BATCH_INTENT_ID = intent_id_for(_ENTITY_KEY, _SEQ, _INTENT_STEP_INDEX)


def _adaptive_config() -> AgentConfig:
    return AgentConfig(
        provider_factory=make_pong_provider,
        batch_policy=BatchPolicy.ADAPTIVE,
        max_batch_size=_BATCH_SIZE,
        max_wait_ms=500,
    )


def _check_batch_output(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert items == [b"resumed:pong"], f"unexpected output: {items!r}"


def _check_committed_batch_intent(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert len(items) == 1, f"expected exactly one committed intent, got {items!r}"
    intent = items[0]
    assert intent.intent_id == _BATCH_INTENT_ID
    assert intent.seq == _SEQ
    assert intent.step_index == _INTENT_STEP_INDEX
    assert intent.SerializeToString(deterministic=True) == _expected_intent_bytes(intent)


def test_a_retried_flush_bundle_replays_deterministically() -> None:
    # Scenario: A retried flush bundle replays deterministically.
    #
    # The suspending activation here is a *flush* over a two-event buffer, and
    # the chaos-failed bundle is its resume -- the same pairing the per-event
    # gate above uses, and for the same reason: the replay cache is keyed by
    # `(entity_key, seq)`, so "zero additional provider calls" is only
    # observable across a suspend/resume pair sharing one seq. What batching
    # adds is that the seq, the cache scope, and the intent scope all belong to
    # the batch rather than to any one event -- and that a retried bundle
    # re-reads the same committed buffer. Both are asserted below.
    with fail_first_matching_commit(_is_resume_commit), _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(_ENTITY_KEY, b"a", 1000)])
            .add_elements([_event(_ENTITY_KEY, b"b", 2000)])  # size flush -> suspend
            .add_elements([_tool_result(_ENTITY_KEY, _BATCH_INTENT_ID, 3000)])
            .advance_watermark_to_infinity()
        )
        out = keyed(p | stream) | RunAgent(
            batch_suspend_then_recall_agent, config=_adaptive_config()
        )
        assert_that(out.output, _check_batch_output, label="output")
        assert_that(out.errors, _check_no_errors, label="errors")
        assert_that(out.intents, _check_committed_batch_intent, label="intents")
        assert_that(out.traces, _assert_zero_additional_calls, label="traces")
        assert_that(out.traces, _check_one_trace_spans_the_suspension, label="trace-id")


def _check_the_same_batch_was_reflushed(actual: object) -> None:
    """The retried flush read the same buffer, in the same order.

    A rolled-back bundle re-reads the committed bag plus its own replayed
    element, so the batch a retry sees is fixed by state, not by arrival
    timing -- and exactly one output reaches the sink however many attempts it
    took.
    """
    items = list(actual)  # type: ignore[call-overload]
    assert items == [b"a|b"], f"expected one flush over the same batch, got {items!r}"


def _check_one_deterministic_flush_intent(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert len(items) == 1, f"expected exactly one committed intent, got {items!r}"
    intent = items[0]
    # step_index 0: this agent's only step is its `act`.
    assert intent.intent_id == intent_id_for(_ENTITY_KEY, _SEQ, 0)
    assert intent.seq == _SEQ
    assert intent.step_index == 0


def test_a_retried_flush_commits_the_same_batch_and_the_same_intent() -> None:
    # The flush's own commit is the failure point here (not a resume's), so
    # this is the direct statement of "a retried flush bundle re-reads the same
    # buffer": Beam rolls the bundle's state back, the retry re-reads the
    # committed bag, and the second attempt commits one output and one
    # byte-identical intent -- never two, and never a partial batch.
    with fail_first_matching_commit(), _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(_ENTITY_KEY, b"a", 1000)])
            .add_elements([_event(_ENTITY_KEY, b"b", 2000)])  # size flush
            .advance_watermark_to_infinity()
        )
        out = keyed(p | stream) | RunAgent(batch_act_agent, config=_adaptive_config())
        assert_that(out.output, _check_the_same_batch_was_reflushed, label="output")
        assert_that(out.errors, _check_no_errors, label="errors")
        assert_that(out.intents, _check_one_deterministic_flush_intent, label="intents")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
