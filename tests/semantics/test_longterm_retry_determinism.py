"""Long-term flush determinism under a chaos-forced bundle retry.

Extends the retry-determinism gate to the sanctioned invariant-5 exception:
an activation whose long-term flush *succeeded* but whose bundle commit was
forced to fail must, on Beam's own retry, re-stage byte-identical upserts,
re-flush them through the equal-seq guard onto byte-identical rows, and emit
byte-identical intents — whether or not the retry's reads observe the first
attempt's flushed rows (the blind-upsert discipline).

Covers the memory-facade scenarios "A bundle retry across a completed flush
converges" and "Blind upserts keep replay path-stable". Offline: the store is
the in-memory reference implementation, shared across DoFn setups via a
patched factory so the test can observe every flush.
"""

from __future__ import annotations

import pytest
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.transforms.window import TimestampedValue

from beam_agents._protos import AgentEnvelope, ToolResult
from beam_agents.core.agent import Complete, Suspend, intent_id_for
from beam_agents.core.context import ActivationContext
from beam_agents.core.loop import ActivationResult
from beam_agents.core.transform import AgentConfig, RunAgent
from beam_agents.memory.stores import InMemoryMemoryStore, MemoryRecord, MemoryStore
from beam_agents.memory.stores.base import encode_envelope
from beam_agents.testing.chaos import fail_first_matching_commit
from tests.core._dofn_helpers import keyed, make_pong_provider
from tests.semantics._helpers import repeated_request

pytestmark = pytest.mark.semantics

_ENTITY_KEY = b"k"
_SEQ = 0
_TTL_MS = 60_000
# call_model() consumes step_index=0; act() consumes step_index=1.
_EXPECTED_INTENT_ID = intent_id_for(_ENTITY_KEY, _SEQ, 1)


class _RecordingStore(InMemoryMemoryStore):
    """Reference store that records the envelope bytes of every flush."""

    def __init__(self) -> None:
        super().__init__()
        self.flushed: list[tuple[str, bytes]] = []
        #: Applied rows, by key: the envelope bytes the store converged on.
        self.rows: dict[str, bytes] = {}

    async def _save(self, record: MemoryRecord) -> bool:
        envelope = encode_envelope(record)
        self.flushed.append((record.key, envelope))
        applied = await super()._save(record)
        if applied:
            self.rows[record.key] = envelope
        return applied


# One store shared across every DoFn setup in the pipeline run (the patched
# factory below hands out this same instance), so the test observes all
# flushes — including the discarded attempt's. A one-slot list rather than a
# rebound module global: the factory closes over the container, so a fresh
# store per test needs no `global`.
_STORE: list[_RecordingStore] = [_RecordingStore()]


def _shared_store(scheme: str, parts: tuple[str, ...]) -> MemoryStore:
    assert scheme == "memory"
    return _STORE[0]


async def longterm_recall_agent(ctx: ActivationContext) -> Complete | Suspend:
    """Suspend once; on resume, read then blindly upsert a long-term row.

    The save is computed from replay-stable inputs only (the resume payload);
    the preceding load's answer — which differs between the first attempt (row
    absent) and the chaos-forced retry (row flushed by the first attempt) —
    is deliberately not a decision input. That is the discipline under test.
    """
    resp = await ctx.call_model(repeated_request())
    if not ctx.is_resume:
        ctx.act("http.post", '{"url":"x"}', ttl_ms=_TTL_MS)
        return Suspend(snapshot=b"waiting", adapter="test", timeout_ms=_TTL_MS)
    assert ctx.resume_result is not None
    await ctx.memory.longterm.load("profile")  # point-in-time; not conditioned on
    ctx.memory.longterm.save("profile", b"seen:" + ctx.resume_result.payload)
    return Complete(output=b"resumed:" + resp.response)


def _streaming_pipeline() -> BeamTestPipeline:
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    return BeamTestPipeline(options=options)


def _event(t_ms: int) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=_ENTITY_KEY, event_time_ms=t_ms, external_event=b"go")
    return TimestampedValue(env, t_ms / 1000)


def _tool_result(t_ms: int) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=_ENTITY_KEY, event_time_ms=t_ms)
    env.tool_result.intent_id = _EXPECTED_INTENT_ID
    env.tool_result.entity_key = _ENTITY_KEY
    env.tool_result.payload = b"tool-done"
    env.tool_result.status = ToolResult.OK
    return TimestampedValue(env, t_ms / 1000)


def _is_resume_commit(result: ActivationResult) -> bool:
    # The resume — the activation whose flush precedes the failed commit — is
    # the only "completed" commit in this scenario.
    return result.status == "completed"


def _check_output(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert items == [b"resumed:pong"], f"unexpected output: {items!r}"


def _check_no_errors(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert items == [], f"unexpected errors: {items!r}"


def _check_committed_intent(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert len(items) == 1, f"expected exactly one committed intent, got {items!r}"
    assert items[0].intent_id == _EXPECTED_INTENT_ID


def test_a_bundle_retry_across_a_completed_flush_converges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Scenario: A bundle retry across a completed flush converges.
    # Scenario: Blind upserts keep replay path-stable.
    store = _RecordingStore()
    _STORE[0] = store
    monkeypatch.setattr("beam_agents.core.dofn.build_memory_store", _shared_store)

    with fail_first_matching_commit(_is_resume_commit), _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(1000)])
            .add_elements([_tool_result(2000)])
            .advance_watermark_to_infinity()
        )
        out = keyed(p | stream) | RunAgent(
            longterm_recall_agent,
            config=AgentConfig(provider_factory=make_pong_provider, longterm_memory="memory://"),
        )
        assert_that(out.output, _check_output, label="output")
        assert_that(out.errors, _check_no_errors, label="errors")
        assert_that(out.intents, _check_committed_intent, label="intents")

    # Both the discarded attempt and Beam's retry flushed — and flushed the
    # byte-identical upsert, so the store converged on one row.
    flushed = store.flushed
    assert len(flushed) >= 2, f"expected the retry to re-flush, got {flushed!r}"
    keys = {key for key, _ in flushed}
    assert keys == {"profile"}
    envelopes = {envelope for _, envelope in flushed}
    assert len(envelopes) == 1, "attempts staged different bytes for the same seq"

    # The converged row is byte-identical to what every attempt flushed.
    assert store.rows["profile"] == next(iter(envelopes))
