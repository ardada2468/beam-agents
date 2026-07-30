"""The continuous-evaluation pipeline from `docs/continuous_eval.md`, exercised.

`docs/traces.md` calls `.traces` "a `PCollection[TraceEvent]` you can consume
yourself"; this test holds that claim the way `test_failure_streak_alarm.py`
holds the errors output's: the documented pipeline stages below are copied
verbatim from the doc, fed trace bytes produced by the runtime's own encoder,
and driven fully offline — `TestPipeline`/`TestStream`, `FakeLLM` for the
judge, scripted watermark advances, no docker, no network.

Everything between the begin/end markers is from `docs/continuous_eval.md`.
Changing one without the other is a defect: the doc is the contract this test
holds the runtime to. Keep them in sync.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Aliased: a bare "TestPipeline" name would be mis-collected by pytest.
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import (
    BagStateSpec,
    ReadModifyWriteStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.utils.timestamp import Duration
from pydantic import BaseModel, Field, ValidationError

from beam_agents._protos import TraceEvent
from beam_agents.model import (
    FakeLLM,
    LLMClient,
    LlmRequest,
    LlmResponse,
    ProviderError,
    ServerError,
    TokenUsage,
    match_any,
    match_contains,
    raise_error,
    respond_with,
)
from beam_agents.observability import (
    CACHE_HIT,
    OPERATION_CHAT,
    OPERATION_NAME,
    REQUEST_MODEL,
    ActivationTrace,
    serialize_trace_event,
    trace_id_for,
    usage_attributes,
)

# --- begin docs/continuous_eval.md example (keep in sync) ---------------------

AGENT_ID = "fraud-triage"

JUDGE_PROMPT_VERSION = "triage-judge/v1"
JUDGE_PROMPT = (
    "You are grading one decision by the {agent_id} agent.\n"
    "Judge prompt version: {version}.\n"
    "The activation finished with status {activation_status} after spending\n"
    "{input_tokens} input and {output_tokens} output tokens.\n"
    "The business outcome that judges it: scenario={scenario}, "
    "label={outcome_label}.\n"
    'Reply with JSON only: {{"score": <integer 1-5>, "rationale": "<one sentence>"}}.'
)

TRACE_TAG = "trace"
OUTCOME_TAG = "outcome"
NO_OUTCOME_OUTPUT = "no_outcome"
ORPHANED_OUTCOMES_OUTPUT = "orphaned_outcomes"
JUDGE_ERRORS_OUTPUT = "judge_errors"


def parse_trace_event(payload: bytes) -> TraceEvent:
    """Decode one traces-topic value with the public proto bindings alone."""
    event = TraceEvent()
    event.ParseFromString(payload)
    return event


def activation_key(entity_key: bytes, seq: int) -> str:
    """Composite activation identity: the same pair `trace_id_for` hashes."""
    return f"{entity_key.hex()}|{seq}"


def summarize_activation(events: list[TraceEvent]) -> dict[str, Any]:
    """Fold one activation's trace events into a per-activation summary.

    Dedup by `(span_id, event_type)` comes first: trace delivery is
    at-least-once and identity is deterministic, so redelivered events collapse
    exactly and never change the sums below.
    """
    deduped: dict[tuple[bytes, int], TraceEvent] = {}
    for event in events:
        deduped.setdefault((event.span_id, event.event_type), event)
    first = next(iter(deduped.values()))
    end = next((e for e in deduped.values() if e.event_type == TraceEvent.ACTIVATION_END), None)
    if end is not None:
        status = end.attributes.get("beam_agents.activation.status", "unknown")
    elif any(e.event_type == TraceEvent.ERROR for e in deduped.values()):
        status = "error"
    else:
        status = "unknown"
    input_tokens = output_tokens = billed_llm_calls = 0
    for event in deduped.values():
        # Billed only: a replay-cache hit re-reports its already-paid-for
        # tokens with `beam_agents.billed=false`; summing those double-bills.
        if (
            event.event_type == TraceEvent.LLM_CALL
            and event.attributes.get("beam_agents.billed") == "true"
        ):
            billed_llm_calls += 1
            input_tokens += int(event.attributes.get("gen_ai.usage.input_tokens", "0"))
            output_tokens += int(event.attributes.get("gen_ai.usage.output_tokens", "0"))
    return {
        # Recomputed, never carried: trace identity is a pure function of
        # activation scope, so the summary needs no bytes from the events.
        "trace_id": trace_id_for(first.entity_key, first.seq).hex(),
        "entity_key": first.entity_key.hex(),
        "seq": first.seq,
        "activation_status": status,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "billed_llm_calls": billed_llm_calls,
    }


def keyed_trace(payload: bytes) -> tuple[str, tuple[str, bytes]]:
    """Key one traces-topic value by the activation identity it carries."""
    event = parse_trace_event(payload)
    return activation_key(event.entity_key, event.seq), (TRACE_TAG, payload)


def keyed_outcome(outcome: dict[str, Any]) -> tuple[str, tuple[str, bytes]]:
    """Key one outcome record by the activation identity it echoes back."""
    key = activation_key(bytes.fromhex(outcome["entity_key"]), outcome["seq"])
    return key, (OUTCOME_TAG, json.dumps(outcome).encode())


class TraceOutcomeJoin(beam.DoFn):
    """Deadline-bounded stateful join of activation traces with outcomes.

    Emission is outcome-triggered — latency is the outcome's own lag, not a
    window's worst case. The watermark-domain deadline timer does double duty:
    it emits an explicit `no_outcome` record for an activation nobody judged
    and garbage-collects the per-activation state. An outcome with no live
    activation state (already emitted, already collected, or never seen) routes
    to `orphaned_outcomes` — the downstream mirror of `orphaned_result`.
    """

    EVENTS = BagStateSpec("events", beam.coders.BytesCoder())
    DONE = ReadModifyWriteStateSpec("done", beam.coders.VarIntCoder())
    DEADLINE = TimerSpec("deadline", TimeDomain.WATERMARK)

    def __init__(self, evaluation_deadline_s: int) -> None:
        super().__init__()
        self._deadline_s = evaluation_deadline_s

    def process(
        self,
        element: tuple[str, tuple[str, bytes]],
        timestamp: Any = beam.DoFn.TimestampParam,
        events: Any = beam.DoFn.StateParam(EVENTS),
        done: Any = beam.DoFn.StateParam(DONE),
        deadline: Any = beam.DoFn.TimerParam(DEADLINE),
    ) -> Iterator[Any]:
        _key, (tag, payload) = element
        if tag == TRACE_TAG:
            if done.read():
                return  # redelivered after emission; the record is already out
            events.add(payload)
            # Event-time deadline: emission fallback and state GC in one timer.
            deadline.set(timestamp + Duration(seconds=self._deadline_s))
            return
        outcome = json.loads(payload)
        bagged = list(events.read())
        if done.read() or not bagged:
            yield beam.pvalue.TaggedOutput(ORPHANED_OUTCOMES_OUTPUT, outcome)
            return
        summary = summarize_activation([parse_trace_event(b) for b in bagged])
        done.write(1)
        events.clear()
        yield {
            **summary,
            "kind": "joined",
            "scenario": outcome["scenario"],
            "outcome_label": outcome["label"],
            "event_time_ms": outcome["event_time_ms"],
        }

    @on_timer(DEADLINE)
    def on_deadline(
        self,
        timestamp: Any = beam.DoFn.TimestampParam,
        events: Any = beam.DoFn.StateParam(EVENTS),
        done: Any = beam.DoFn.StateParam(DONE),
    ) -> Iterator[Any]:
        bagged = list(events.read())
        if not done.read() and bagged:
            # An activation nobody ever judged is a signal, not an absence:
            # dropping it silently would bias every rate this pipeline reports.
            yield beam.pvalue.TaggedOutput(
                NO_OUTCOME_OUTPUT,
                {
                    **summarize_activation([parse_trace_event(b) for b in bagged]),
                    "kind": "no_outcome",
                    "event_time_ms": timestamp.micros // 1000,
                },
            )
        events.clear()
        done.clear()


class Verdict(BaseModel):
    """The judge's constrained verdict: a bounded score, never coerced."""

    score: int = Field(ge=1, le=5)
    rationale: str


class JudgeScores(beam.DoFn):
    """LLM-as-judge through the model seam: any `LLMClient` substitutes."""

    def __init__(self, provider_factory: Callable[[], LLMClient], model_id: str) -> None:
        super().__init__()
        self._provider_factory = provider_factory
        self._model_id = model_id
        self._client: LLMClient | None = None

    def setup(self) -> None:
        self._client = self._provider_factory()

    def process(self, record: dict[str, Any]) -> Iterator[Any]:
        assert self._client is not None  # setup() ran
        prompt = JUDGE_PROMPT.format(
            agent_id=AGENT_ID,
            version=JUDGE_PROMPT_VERSION,
            activation_status=record["activation_status"],
            input_tokens=record["input_tokens"],
            output_tokens=record["output_tokens"],
            scenario=record["scenario"],
            outcome_label=record["outcome_label"],
        )
        request = LlmRequest(
            model_id=self._model_id,
            messages=[{"role": "user", "content": prompt}],
            tools_schema=None,
            sampling_params={"temperature": 0},
        )
        try:
            # Simplest correct async bridge; batch records per call if judge
            # throughput ever matters.
            response = asyncio.run(self._client.complete(request))
        except ProviderError as error:
            # No retry stack here: the input topic is lossless, so a failed
            # record is visible and re-drivable rather than silently retried.
            yield beam.pvalue.TaggedOutput(
                JUDGE_ERRORS_OUTPUT,
                {**record, "kind": "judge_error", "judge_error": type(error).__name__},
            )
            return
        try:
            verdict = Verdict.model_validate_json(response.response)
        except ValidationError:
            # Fail closed: never fabricate, default, or coerce a score — a
            # silently defaulted score corrupts every aggregate it feeds.
            yield beam.pvalue.TaggedOutput(
                JUDGE_ERRORS_OUTPUT,
                {**record, "kind": "judge_error", "judge_error": "invalid_verdict"},
            )
            return
        yield {
            **record,
            "kind": "verdict",
            "agent_id": AGENT_ID,
            "score": verdict.score,
            "rationale": verdict.rationale,
            "judge_prompt_version": JUDGE_PROMPT_VERSION,
            "judge_model": self._model_id,
        }


def aggregation_key(row: dict[str, Any]) -> tuple[str, str]:
    """Aggregate key: `(scenario, judge_prompt_version)`.

    Grouping by prompt version makes a prompt edit a visible discontinuity in
    the series instead of a silent shift that reads as an agent regression. A
    `no_outcome` row has no scenario (nobody judged it), and neither it nor a
    `judge_error` row carries a verdict, so they fall back to this pipeline's
    own version constant.
    """
    return (
        row.get("scenario", ""),
        row.get("judge_prompt_version", JUDGE_PROMPT_VERSION),
    )


class QualityAggregate(beam.CombineFn):
    """Per-window quality counters for one `(scenario, judge_prompt_version)`."""

    def create_accumulator(self) -> tuple[int, int, int, int]:
        return 0, 0, 0, 0  # judged, score_sum, no_outcome, judge_errors

    def add_input(
        self, accumulator: tuple[int, int, int, int], row: dict[str, Any]
    ) -> tuple[int, int, int, int]:
        judged, score_sum, no_outcome, judge_errors = accumulator
        if row["kind"] == "verdict":
            return judged + 1, score_sum + row["score"], no_outcome, judge_errors
        if row["kind"] == "no_outcome":
            return judged, score_sum, no_outcome + 1, judge_errors
        return judged, score_sum, no_outcome, judge_errors + 1

    def merge_accumulators(
        self, accumulators: Iterable[tuple[int, int, int, int]]
    ) -> tuple[int, int, int, int]:
        judged = score_sum = no_outcome = judge_errors = 0
        for part_judged, part_score_sum, part_no_outcome, part_judge_errors in accumulators:
            judged += part_judged
            score_sum += part_score_sum
            no_outcome += part_no_outcome
            judge_errors += part_judge_errors
        return judged, score_sum, no_outcome, judge_errors

    def extract_output(self, accumulator: tuple[int, int, int, int]) -> dict[str, Any]:
        judged, score_sum, no_outcome, judge_errors = accumulator
        return {
            "judged": judged,
            # None, not 0: a window with nothing judged has no mean score.
            "mean_score": score_sum / judged if judged else None,
            "no_outcome": no_outcome,
            "judge_errors": judge_errors,
        }


def aggregate_row(key: tuple[str, str], counters: dict[str, Any]) -> dict[str, Any]:
    """One BigQuery-shaped aggregate row per window and aggregate key."""
    scenario, judge_prompt_version = key
    return {
        "agent_id": AGENT_ID,
        "scenario": scenario,
        "judge_prompt_version": judge_prompt_version,
        **counters,
    }


@dataclass(frozen=True)
class EvalOutputs:
    """The evaluation pipeline's five output surfaces."""

    verdicts: beam.pvalue.PCollection
    aggregates: beam.pvalue.PCollection
    no_outcome: beam.pvalue.PCollection
    orphaned_outcomes: beam.pvalue.PCollection
    judge_errors: beam.pvalue.PCollection


def evaluation_outputs(
    traces: beam.pvalue.PCollection,
    outcomes: beam.pvalue.PCollection,
    provider_factory: Callable[[], LLMClient],
    judge_model: str,
    evaluation_deadline_s: int,
) -> EvalOutputs:
    """Assemble the pipeline: parse -> join -> judge -> verdicts + aggregates."""
    joined = (
        (
            traces | "keyed traces" >> beam.Map(keyed_trace),
            outcomes | "keyed outcomes" >> beam.Map(keyed_outcome),
        )
        | "one keyed stream" >> beam.Flatten()
        | "join"
        >> beam.ParDo(TraceOutcomeJoin(evaluation_deadline_s)).with_outputs(
            NO_OUTCOME_OUTPUT, ORPHANED_OUTCOMES_OUTPUT, main="joined"
        )
    )
    judged = joined.joined | "judge" >> beam.ParDo(
        JudgeScores(provider_factory, judge_model)
    ).with_outputs(JUDGE_ERRORS_OUTPUT, main="verdicts")
    aggregates = (
        (judged.verdicts, joined.no_outcome, judged.judge_errors)
        | "quality rows" >> beam.Flatten()
        | "hourly" >> beam.WindowInto(FixedWindows(3600))
        | "by scenario and prompt" >> beam.Map(lambda row: (aggregation_key(row), row))
        | "aggregate" >> beam.CombinePerKey(QualityAggregate())
        | "aggregate rows" >> beam.MapTuple(aggregate_row)
    )
    return EvalOutputs(
        verdicts=judged.verdicts,
        aggregates=aggregates,
        no_outcome=joined.no_outcome,
        orphaned_outcomes=joined.orphaned_outcomes,
        judge_errors=judged.judge_errors,
    )


# --- end docs/continuous_eval.md example --------------------------------------


# --- offline fixtures: encoder-produced trace bytes, lagged outcomes ----------

_KEY = b"card-42"
_SEQ = 7
# The activation clock (proto `start_ms`), unrelated to element timestamps.
_NOW_MS = 1_000_000_000


def _llm_call_event(
    entity_key: bytes, seq: int, *, step_index: int, usage: TokenUsage, billed: bool
) -> TraceEvent:
    """An LLM_CALL event shaped exactly as the model facade stages it."""
    return TraceEvent(
        entity_key=entity_key,
        seq=seq,
        step_index=step_index,
        event_type=TraceEvent.LLM_CALL,
        attributes={
            OPERATION_NAME: OPERATION_CHAT,
            REQUEST_MODEL: "agent-model",
            CACHE_HIT: "false" if billed else "true",
            **usage_attributes(usage, billed=billed),
        },
        start_ms=_NOW_MS,
        end_ms=_NOW_MS,
    )


def _activation_events(entity_key: bytes = _KEY, seq: int = _SEQ) -> list[TraceEvent]:
    """One completed activation: start, a billed call, a cache hit, end."""
    trace = ActivationTrace(entity_key=entity_key, seq=seq, now_ms=_NOW_MS)
    billed = _llm_call_event(
        entity_key, seq, step_index=0, usage=TokenUsage(200, 100, 300), billed=True
    )
    cached = _llm_call_event(
        entity_key, seq, step_index=1, usage=TokenUsage(50, 25, 75), billed=False
    )
    return [
        trace.activation_start(),
        trace.stamp(billed),
        trace.stamp(cached),
        trace.activation_end(status="completed", step_index=2),
    ]


def _payloads(events: list[TraceEvent]) -> list[bytes]:
    """Traces-topic values, produced by the runtime's own Kafka encoder."""
    return [serialize_trace_event(event)[1] for event in events]


def _outcome(
    entity_key: bytes = _KEY,
    seq: int = _SEQ,
    *,
    scenario: str = "chargeback",
    label: str = "confirmed_fraud",
    event_time_ms: int = 1_030_000,
) -> dict[str, Any]:
    return {
        "entity_key": entity_key.hex(),
        "seq": seq,
        "scenario": scenario,
        "label": label,
        "event_time_ms": event_time_ms,
    }


def _joined_record() -> dict[str, Any]:
    """The joined record for `_activation_events()` + `_outcome()`."""
    return {
        "trace_id": trace_id_for(_KEY, _SEQ).hex(),
        "entity_key": _KEY.hex(),
        "seq": _SEQ,
        "activation_status": "completed",
        "input_tokens": 200,
        "output_tokens": 100,
        "billed_llm_calls": 1,
        "kind": "joined",
        "scenario": "chargeback",
        "outcome_label": "confirmed_fraud",
        "event_time_ms": 1_030_000,
    }


_GOOD_VERDICT = b'{"score": 5, "rationale": "flagged the fraud"}'


def _scoring_provider() -> FakeLLM:
    """The scripted judge for pipeline runs: one verdict per known label."""
    return FakeLLM([(match_contains("label=confirmed_fraud"), respond_with(_GOOD_VERDICT))])


def _streaming_pipeline() -> BeamTestPipeline:
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    return BeamTestPipeline(options=options)


def _trace_elements(
    payloads: list[bytes], t_s: int
) -> list[TimestampedValue[tuple[str, tuple[str, bytes]]]]:
    return [TimestampedValue(keyed_trace(payload), t_s) for payload in payloads]


def _outcome_element(
    outcome: dict[str, Any], t_s: int
) -> TimestampedValue[tuple[str, tuple[str, bytes]]]:
    return TimestampedValue(keyed_outcome(outcome), t_s)


def _join_outputs(p: beam.Pipeline, stream: TestStream, deadline_s: int) -> Any:
    """The documented join stage over an already-keyed scripted stream."""
    return (
        p
        | stream
        # The stateful DoFn requires a KV element type, which the scripted
        # stream cannot declare itself.
        | "keyed" >> beam.Map(lambda kv: kv).with_output_types(tuple[str, tuple[str, bytes]])
        | beam.ParDo(TraceOutcomeJoin(deadline_s)).with_outputs(
            NO_OUTCOME_OUTPUT, ORPHANED_OUTCOMES_OUTPUT, main="joined"
        )
    )


# --- Requirement: the example consumes exported traces with public bindings ---


def test_exported_trace_bytes_decode_with_public_bindings() -> None:
    # Scenario: Exported trace bytes decode with public bindings. The parse
    # stage sees exactly what `serialize_trace_event` wrote and rebuilds the
    # event with nothing but `TraceEvent.ParseFromString`.
    for event in _activation_events():
        _key, payload = serialize_trace_event(event)
        decoded = parse_trace_event(payload)
        assert decoded.entity_key == _KEY
        assert decoded.seq == _SEQ
        assert decoded.event_type == event.event_type
        assert dict(decoded.attributes) == dict(event.attributes)
        assert decoded == event


def test_activation_identity_is_recomputed_not_carried() -> None:
    # Scenario: Activation identity is recomputed, not carried. The summary's
    # trace_id is a pure function of `(entity_key, seq)` and equals the ID
    # stamped on every consumed event.
    events = _activation_events()
    summary = summarize_activation(events)
    assert summary["trace_id"] == trace_id_for(_KEY, _SEQ).hex()
    assert all(event.trace_id == trace_id_for(_KEY, _SEQ) for event in events)


# --- Requirement: the trace-outcome join is deadline-bounded and honest -------


def test_a_lagging_outcome_joins_on_arrival() -> None:
    # Scenario: A lagging outcome joins on arrival — exactly one joined record,
    # emitted when the outcome lands, before the deadline.
    stream = (
        TestStream()
        .advance_watermark_to(1000)
        .add_elements(_trace_elements(_payloads(_activation_events()), 1000))
        .advance_watermark_to(1020)
        .add_elements([_outcome_element(_outcome(), 1030)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        outputs = _join_outputs(p, stream, deadline_s=60)
        assert_that(outputs.joined, equal_to([_joined_record()]), label="joined")
        assert_that(outputs.no_outcome, equal_to([]), label="no-outcome")
        assert_that(outputs.orphaned_outcomes, equal_to([]), label="orphans")


def test_the_deadline_emits_an_explicit_no_outcome_record() -> None:
    # Scenario: The deadline emits an explicit no-outcome record. The watermark
    # passes the deadline with no outcome; the activation is reported, not
    # silently dropped, and its state is cleared.
    stream = (
        TestStream()
        .advance_watermark_to(1000)
        .add_elements(_trace_elements(_payloads(_activation_events()), 1000))
        # Past the deadline (1060) while the stream is still live: the timer
        # fires on this advance, not on the jump to infinity.
        .advance_watermark_to(2000)
        .advance_watermark_to_infinity()
    )
    expected = {
        "trace_id": trace_id_for(_KEY, _SEQ).hex(),
        "entity_key": _KEY.hex(),
        "seq": _SEQ,
        "activation_status": "completed",
        "input_tokens": 200,
        "output_tokens": 100,
        "billed_llm_calls": 1,
        "kind": "no_outcome",
        "event_time_ms": 1_060_000,
    }
    with _streaming_pipeline() as p:
        outputs = _join_outputs(p, stream, deadline_s=60)
        assert_that(outputs.joined, equal_to([]), label="joined")
        assert_that(outputs.no_outcome, equal_to([expected]), label="no-outcome")
        assert_that(outputs.orphaned_outcomes, equal_to([]), label="orphans")


def test_an_outcome_past_the_deadline_is_orphaned_not_joined() -> None:
    # Scenario: An outcome past the deadline is orphaned, not joined. The
    # deadline fired and collected state; the late outcome routes to
    # `orphaned_outcomes` and no second record is emitted for the activation.
    late = _outcome(event_time_ms=1_090_000)
    stream = (
        TestStream()
        .advance_watermark_to(1000)
        .add_elements(_trace_elements(_payloads(_activation_events()), 1000))
        .advance_watermark_to(2000)
        .add_elements([_outcome_element(late, 1090)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        outputs = _join_outputs(p, stream, deadline_s=60)
        assert_that(outputs.joined, equal_to([]), label="joined")
        assert_that(outputs.orphaned_outcomes, equal_to([late]), label="orphans")


def test_duplicate_trace_events_do_not_change_the_joined_record() -> None:
    # Scenario: Duplicate trace events do not change the joined record. Every
    # event delivered twice, as at-least-once permits: the joined record equals
    # the single-delivery case byte for byte — token sums are not doubled.
    payloads = _payloads(_activation_events())
    stream = (
        TestStream()
        .advance_watermark_to(1000)
        .add_elements(_trace_elements(payloads, 1000))
        .add_elements(_trace_elements(payloads, 1001))
        .advance_watermark_to(1020)
        .add_elements([_outcome_element(_outcome(), 1030)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        outputs = _join_outputs(p, stream, deadline_s=60)
        assert_that(outputs.joined, equal_to([_joined_record()]), label="joined")
        assert_that(outputs.orphaned_outcomes, equal_to([]), label="orphans")


# --- Requirement: the judge scores through the model seam ---------------------


def _judge_with(client: LLMClient) -> JudgeScores:
    judge = JudgeScores(lambda: client, "judge-model")
    judge.setup()
    return judge


def test_verdict_rows_carry_the_judges_provenance() -> None:
    # Scenario: Verdict rows carry the judge's provenance — prompt version and
    # model ID on the row, and the version string in the request material the
    # provider actually received.
    fake = FakeLLM([(match_any(), respond_with(_GOOD_VERDICT))])
    rows = list(_judge_with(fake).process(_joined_record()))

    assert len(rows) == 1
    row = rows[0]
    assert row["judge_prompt_version"] == JUDGE_PROMPT_VERSION
    assert row["judge_model"] == "judge-model"
    assert row["score"] == 5
    assert fake.call_count == 1
    assert JUDGE_PROMPT_VERSION in repr(fake.requests[0].messages)


def test_an_unparseable_verdict_fails_closed() -> None:
    # Scenario: An unparseable verdict fails closed. Non-JSON prose, an
    # out-of-range score, and a provider failure all route to `judge_errors`
    # with a reason; no verdict row — no fabricated score — anywhere.
    fake = FakeLLM(
        [
            (match_contains("label=prose"), respond_with(b"I would say 4 out of 5.")),
            (
                match_contains("label=out_of_range"),
                respond_with(b'{"score": 99, "rationale": "x"}'),
            ),
            (match_contains("label=down"), raise_error(ServerError(503))),
        ]
    )
    judge = _judge_with(fake)
    outputs = [
        out
        for label in ("prose", "out_of_range", "down")
        for out in judge.process({**_joined_record(), "outcome_label": label})
    ]

    assert len(outputs) == 3
    assert all(isinstance(out, beam.pvalue.TaggedOutput) for out in outputs)
    assert all(out.tag == JUDGE_ERRORS_OUTPUT for out in outputs)
    reasons = [out.value["judge_error"] for out in outputs]
    assert reasons == ["invalid_verdict", "invalid_verdict", "ServerError"]
    assert all("score" not in out.value for out in outputs)


def test_the_seam_substitutes_structurally() -> None:
    # Scenario: The seam substitutes structurally. Any object with the
    # `LLMClient` shape — here a minimal local class that is not FakeLLM and
    # not a real provider — drives the stage identically; no code path
    # conditions on the concrete type.
    class StaticClient:
        async def complete(self, request: LlmRequest) -> LlmResponse:
            return LlmResponse(b'{"score": 2, "rationale": "static"}')

    rows = list(_judge_with(StaticClient()).process(_joined_record()))

    assert len(rows) == 1
    assert rows[0]["score"] == 2
    assert rows[0]["judge_prompt_version"] == JUDGE_PROMPT_VERSION


# --- Requirement: the example is a doc-contract pair that runs offline --------


def test_the_documented_pipeline_runs_offline_verbatim() -> None:
    # Scenario: The documented pipeline runs offline verbatim. The full
    # assembly — parse, join, judge, aggregate — over encoder-produced trace
    # bytes, a lagged outcome, an unjudged activation, and an orphan outcome,
    # with FakeLLM as the judge and no docker or network anywhere.
    judged_payloads = _payloads(_activation_events())
    unjudged_key = b"card-99"
    unjudged_payloads = _payloads(_activation_events(unjudged_key, 3))
    orphan = _outcome(b"card-00", 1, event_time_ms=1_030_000)
    stream = (
        TestStream()
        .advance_watermark_to(1000)
        .add_elements([TimestampedValue((TRACE_TAG, p), 1000) for p in judged_payloads])
        .add_elements([TimestampedValue((TRACE_TAG, p), 1000) for p in unjudged_payloads])
        .advance_watermark_to(1020)
        .add_elements(
            [
                TimestampedValue((OUTCOME_TAG, _outcome()), 1030),
                TimestampedValue((OUTCOME_TAG, orphan), 1030),
            ]
        )
        # Past the unjudged activation's deadline (1060) while the stream is
        # still live, so its no-outcome record precedes the final pane.
        .advance_watermark_to(2000)
        .advance_watermark_to_infinity()
    )
    expected_verdict = {
        **_joined_record(),
        "kind": "verdict",
        "agent_id": AGENT_ID,
        "score": 5,
        "rationale": "flagged the fraud",
        "judge_prompt_version": JUDGE_PROMPT_VERSION,
        "judge_model": "judge-model",
    }
    expected_no_outcome = {
        "trace_id": trace_id_for(unjudged_key, 3).hex(),
        "entity_key": unjudged_key.hex(),
        "seq": 3,
        "activation_status": "completed",
        "input_tokens": 200,
        "output_tokens": 100,
        "billed_llm_calls": 1,
        "kind": "no_outcome",
        "event_time_ms": 1_060_000,
    }
    expected_aggregates = [
        {
            "agent_id": AGENT_ID,
            "scenario": "chargeback",
            "judge_prompt_version": JUDGE_PROMPT_VERSION,
            "judged": 1,
            "mean_score": 5.0,
            "no_outcome": 0,
            "judge_errors": 0,
        },
        {
            "agent_id": AGENT_ID,
            "scenario": "",
            "judge_prompt_version": JUDGE_PROMPT_VERSION,
            "judged": 0,
            "mean_score": None,
            "no_outcome": 1,
            "judge_errors": 0,
        },
    ]
    with _streaming_pipeline() as p:
        tagged = p | stream
        traces = (
            tagged
            | "traces" >> beam.Filter(lambda kv: kv[0] == TRACE_TAG)
            | "trace payloads" >> beam.Map(lambda kv: kv[1])
        )
        outcomes = (
            tagged
            | "outcomes" >> beam.Filter(lambda kv: kv[0] == OUTCOME_TAG)
            | "outcome records" >> beam.Map(lambda kv: kv[1])
        )
        outputs = evaluation_outputs(
            traces,
            outcomes,
            provider_factory=_scoring_provider,
            judge_model="judge-model",
            evaluation_deadline_s=60,
        )
        assert_that(outputs.verdicts, equal_to([expected_verdict]), label="verdicts")
        assert_that(outputs.no_outcome, equal_to([expected_no_outcome]), label="no-outcome")
        assert_that(outputs.orphaned_outcomes, equal_to([orphan]), label="orphans")
        assert_that(outputs.judge_errors, equal_to([]), label="judge-errors")
        assert_that(outputs.aggregates, equal_to(expected_aggregates), label="aggregates")


def test_aggregates_are_grouped_by_scenario_and_prompt_version() -> None:
    # Scenario: Aggregates are grouped by scenario and prompt version. Rows
    # spanning two prompt versions are never averaged together — a version
    # change is a visible discontinuity, not a silent shift.
    rows: list[dict[str, Any]] = [
        {"kind": "verdict", "scenario": "chargeback", "judge_prompt_version": "j/v1", "score": 4},
        {"kind": "verdict", "scenario": "chargeback", "judge_prompt_version": "j/v1", "score": 2},
        {"kind": "verdict", "scenario": "chargeback", "judge_prompt_version": "j/v2", "score": 5},
        {"kind": "judge_error", "scenario": "chargeback", "judge_error": "invalid_verdict"},
        {"kind": "no_outcome"},
    ]
    expected = [
        {
            "agent_id": AGENT_ID,
            "scenario": "chargeback",
            "judge_prompt_version": "j/v1",
            "judged": 2,
            "mean_score": 3.0,
            "no_outcome": 0,
            "judge_errors": 0,
        },
        {
            "agent_id": AGENT_ID,
            "scenario": "chargeback",
            "judge_prompt_version": "j/v2",
            "judged": 1,
            "mean_score": 5.0,
            "no_outcome": 0,
            "judge_errors": 0,
        },
        {
            "agent_id": AGENT_ID,
            "scenario": "chargeback",
            "judge_prompt_version": JUDGE_PROMPT_VERSION,
            "judged": 0,
            "mean_score": None,
            "no_outcome": 0,
            "judge_errors": 1,
        },
        {
            "agent_id": AGENT_ID,
            "scenario": "",
            "judge_prompt_version": JUDGE_PROMPT_VERSION,
            "judged": 0,
            "mean_score": None,
            "no_outcome": 1,
            "judge_errors": 0,
        },
    ]
    with BeamTestPipeline() as p:
        aggregates = (
            p
            | beam.Create(rows)
            # Real event timestamps: rows land in one concrete hourly window.
            | "timestamped" >> beam.Map(lambda row: TimestampedValue(row, 1000))
            | "hourly" >> beam.WindowInto(FixedWindows(3600))
            | "by scenario and prompt" >> beam.Map(lambda row: (aggregation_key(row), row))
            | "aggregate" >> beam.CombinePerKey(QualityAggregate())
            | "aggregate rows" >> beam.MapTuple(aggregate_row)
        )
        assert_that(aggregates, equal_to(expected))
