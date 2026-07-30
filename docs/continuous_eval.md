# Example: continuous evaluation with an LLM-as-judge

The runtime tells you what an agent *did* — every activation is one trace on
`.traces`, deliverable losslessly to Kafka/Pub/Sub/BigQuery (see
[traces.md](traces.md)). It cannot tell you whether the agent decided *well*:
for the target workloads the quality signal is a business outcome — a
chargeback confirmed, an alert acknowledged as real — that arrives on a
separate stream, minutes to days after the activation it judges. Closing that
loop is a second, ordinary Beam pipeline:

```
traces topic ───► parse ─► key by (entity_key, seq) ─┐
                                                     ├─► stateful join ─► judge ─► verdict rows
outcome stream ─► key by (entity_key, seq) ──────────┘        │             │          │
                                                        no_outcome    judge_errors  hourly aggregates
                                                        orphaned_outcomes
```

No beam-agents runtime is involved: the pipeline consumes the traces topic
with the public proto bindings, and the judge calls a provider through the
same `LLMClient` seam `AgentConfig` takes. The code below is copied verbatim
into `tests/examples/test_continuous_eval.py`, which runs it offline against
trace bytes produced by the runtime's own encoder, with `FakeLLM` as the
judge; the two must stay in sync.

## The outcome record

The example expects outcome records shaped like this, from whatever system
observes your business reality:

```json
{"entity_key": "636172642d3432", "seq": 7, "scenario": "chargeback",
 "label": "confirmed_fraud", "event_time_ms": 1730000000000}
```

`(entity_key, seq)` is activation identity — the pair `trace_id_for` hashes
into the trace ID. Requiring it on the outcome is not an imposition invented
here: the runtime already exports activation identity into the world on every
`ToolIntent` (`trace_id` on the wire), so the system acting on an agent's
intent can thread it through to the eventual outcome event. `entity_key` is
lowercase hex, matching the BigQuery trace-table convention.

## Stage 1: parse and summarize

Decoding needs nothing but the public proto bindings. One activation is many
events, so the joined record is built from a per-activation *summary*: status
from `ACTIVATION_END` (or `ERROR`), token usage folded from `LLM_CALL`
events. Two rules keep the sums honest under the runtime's delivery contract:

- **Dedup first.** Trace delivery is at-least-once and identity is
  deterministic, so deduplicating by `(span_id, event_type)` collapses
  redelivered events exactly — a replayed bundle can never change a sum.
- **Billed only.** A replay-cache hit re-reports its tokens with
  `beam_agents.billed=false`; summing those would double-bill.

```python
import asyncio
import json
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import Any

import apache_beam as beam
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import (
    BagStateSpec,
    ReadModifyWriteStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import Duration
from pydantic import BaseModel, Field, ValidationError

from beam_agents._protos import TraceEvent
from beam_agents.model import LLMClient, LlmRequest, ProviderError
from beam_agents.observability import trace_id_for

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
```

## Stage 2: the deadline-bounded join

Why not `CoGroupByKey` over windows? A windowed CGBK emits only at window
close, so the window must be at least the worst credible outcome lag — a
chargeback window is 120 days — which delays *every* result by that worst
case; and outcomes beyond `allowed_lateness` are dropped inside the GBK where
no application code can observe or count them. The stateful join instead
emits **at outcome arrival** (latency = the outcome's own lag, irreducible),
bounds state with an explicit per-activation deadline, and turns both failure
modes into named outputs. These are the same moves `RunAgent`'s own stateful
DoFn makes — `BagState` blind appends, a watermark timer doing GC, orphan
routing — in user-land form:

```python
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
```

The join contract, spelled out:

- **A lagging outcome joins on arrival**, any time before the deadline.
- **The deadline emits an explicit `no_outcome` record** and clears state.
  Nothing vanishes silently — an activation nobody judged is itself a rate
  worth alerting on.
- **An outcome with no live activation state is orphaned**, never joined and
  never dropped: after emission (`DONE` set), after GC (state gone), or for an
  activation this pipeline never saw. Orphans are re-drivable — the traces
  topic is lossless.
- **Duplicates are harmless.** Redelivered trace events collapse in the
  summary dedup; a redelivered trace after emission is ignored; a redelivered
  outcome after emission routes to `orphaned_outcomes` rather than emitting a
  second record.
- **State is bounded** by the deadline timer (mandatory — there is no
  infinite-wait mode). Cost per in-flight activation: the activation's own
  event bytes plus one flag. Size the deadline generously (it caps how late an
  outcome can be judged) but finitely; it must also exceed your trace
  redelivery horizon, or a very late redelivery after GC re-arms state for an
  already-reported activation.
- **Watermark skew:** the deadline timer fires on the flattened stream's
  combined watermark, so a stalled outcome source *delays* `no_outcome`
  emission but never causes a wrong one. A processing-time deadline would make
  replays produce different join results — rejected for the same reason the
  runtime keeps its TTL GC on the watermark.

## Stage 3: the judge

The judge is a plain `DoFn` over the `LLMClient` seam — deliberately **not**
`RunAgent`. Scoring one joined record is a pure function of that record: no
keyed memory, no continuation, no intents — none of the machinery `RunAgent`
exists to provide. The seam is the reuse boundary: tests pass a factory
returning `FakeLLM`, a real deployment passes `AnthropicProvider` /
`OpenAICompatProvider` exactly as it would to `AgentConfig`, and any
structural implementation substitutes.

Two disciplines matter more than the rubric:

- **The prompt is a versioned artifact.** A judge-score time series is only
  meaningful within one (prompt, model) pair — an unversioned prompt edit
  shifts every score and reads as an agent regression. The version rides in
  every request and lands on every verdict row, so a change is a queryable
  discontinuity (`GROUP BY judge_prompt_version`), not an invisible one.
- **Verdicts fail closed.** The verdict is constrained JSON with a bounded
  score; anything that fails to parse or leaves the range routes to
  `judge_errors` with a reason. The example never coerces, defaults, or
  averages-in a fabricated score.

```python
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
```

Cost notes. Every joined record spends judge tokens; the first knob a real
deployment turns is deterministic sampling *before* the judge —
`beam.Filter(lambda r: int(r["trace_id"], 16) % 100 < 10)` keeps a
replay-stable 10%. The per-element `asyncio.run` is the simplest correct
bridge; throughput-sensitive deployments batch elements per call instead. The
runtime's own retry/breaker/replay-cache stack (`LlmFacade`) is deliberately
not rebuilt here — a `ProviderError` routes the record to `judge_errors`,
visible and re-drivable.

## Stage 4: verdict rows and aggregates

```python
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
```

## Wiring it to real sources

The contract-tested code above takes two ordinary PCollections; a deployment
supplies them from its sources and points the outputs at BigQuery:

```python
from beam_agents.model import AnthropicProvider, anthropic_decode  # or OpenAICompatProvider

outputs = evaluation_outputs(
    traces=(
        p
        | ReadFromKafka(
            consumer_config={"bootstrap.servers": "broker:9092"},
            topics=["agent-traces"],  # the topic a kafka:// traces_to sink writes
        )
        | beam.Map(lambda kv: kv[1])  # values: deterministic TraceEvent bytes
    ),
    outcomes=p | ReadOutcomes(...),  # your business-event source, as dicts
    provider_factory=lambda: AnthropicProvider(api_key=...),
    judge_model="my-judge-model",  # a real provider model ID
    evaluation_deadline_s=7 * 24 * 3600,
)
outputs.verdicts | WriteToBigQuery("proj:evals.verdicts", ...)
outputs.aggregates | WriteToBigQuery("proj:evals.quality_hourly", ...)
# no_outcome / orphaned_outcomes / judge_errors: at minimum, count and alert.
```

### Output layouts

Verdict rows (one per judged activation):

| Column | Type | Notes |
|---|---|---|
| `trace_id` | STRING | hex; joins back to the trace table, which is clustered on it |
| `entity_key` | STRING | hex |
| `seq` | INT64 | per-key activation counter |
| `agent_id` | STRING | pipeline-level constant: one deployment, one traces topic |
| `scenario` / `outcome_label` | STRING | from the outcome record |
| `activation_status` | STRING | `completed` / `suspended` / `error` / `unknown` |
| `score` | INT64 | the judge's bounded score |
| `rationale` | STRING | the judge's one-sentence reason |
| `judge_prompt_version` / `judge_model` | STRING | the provenance pair a score is only meaningful within |
| `input_tokens` / `output_tokens` / `billed_llm_calls` | INT64 | summed over deduped, `billed=true` events only |
| `event_time_ms` | INT64 | the outcome's event time |
| `kind` | STRING | `verdict` |

Aggregate rows (hourly, per `(scenario, judge_prompt_version)`): `agent_id`,
`scenario`, `judge_prompt_version`, `judged`, `mean_score`, `no_outcome`,
`judge_errors`. Deeper slicing belongs in SQL over the verdict rows, not in
the pipeline.

Verdict rows are at-least-once like every sink in the system, but they carry
deterministic identity, so downstream dedup is exact:

```sql
SELECT * EXCEPT (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY trace_id, judge_prompt_version ORDER BY event_time_ms
  ) AS rn
  FROM `proj.evals.verdicts`
) WHERE rn = 1
```

Drill-down from a bad score to the full trace — `trace_id` is the join key
into the trace table ([traces.md](traces.md)), which is clustered on it:

```sql
SELECT t.*
FROM `proj.evals.verdicts` AS v
JOIN `proj.my_dataset.traces` AS t USING (trace_id)
WHERE v.score <= 2 AND v.judge_prompt_version = 'triage-judge/v1'
ORDER BY t.seq, t.step_index
```

### The batch alternative: joining in BigQuery

Teams already landing traces in the published BigQuery table can run the same
join as a scheduled query instead — the batch alternative, with none of the
streaming pipeline's latency. The trap the streaming join solves explicitly
(the unbounded outcome lag) becomes the `WHERE` clause's problem here: the
query must re-scan a lag-bound's worth of history every run, and outcomes
beyond it are silently unjoined.

```sql
SELECT o.scenario, o.label, o.event_time_ms, s.*
FROM `proj.my_dataset.outcomes` AS o
JOIN (
  SELECT entity_key, seq, ANY_VALUE(trace_id) AS trace_id,
         SUM(CAST((SELECT value FROM UNNEST(attributes)
                   WHERE key = 'gen_ai.usage.input_tokens') AS INT64)) AS input_tokens
  FROM (SELECT DISTINCT * FROM `proj.my_dataset.traces`)  -- at-least-once dedup
  WHERE event_type = 'LLM_CALL'
    AND 'true' = (SELECT value FROM UNNEST(attributes) WHERE key = 'beam_agents.billed')
  GROUP BY entity_key, seq
) AS s USING (entity_key, seq)
WHERE o.event_time_ms > UNIX_MILLIS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))
```

## Keep in sync

`tests/examples/test_continuous_eval.py` runs the code above verbatim against
trace bytes produced by the runtime's own encoder, with `FakeLLM` as the
judge — fully offline. It is the proof of `docs/traces.md`'s claim that the
trace stream is consumable with nothing but public bindings. Changing the
code here without the test (or vice versa) is a defect.
