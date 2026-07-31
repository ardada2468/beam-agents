# The errors output and its sink

`RunAgent` routes every element-level failure to `.errors` rather than failing
the bundle. A dead letter means the activation committed **nothing** — no memory
write, no intent, no output — so the record is the only trace that the key was
touched at all.

```python
outputs = keyed_envelopes | RunAgent(agent, config=AgentConfig(...))
outputs.errors  # PCollection[ActivationError]
```

Consuming `.errors` directly gives you `ActivationError` dataclasses:

| Field | Meaning |
|---|---|
| `entity_key` | The key whose activation failed. |
| `reason` | One of the reasons below. |
| `detail` | Free-form context for the reason; empty when there is nothing truthful to say. |
| `event_time_ms` | The element's event time, or the timer's scheduled firing time. Never a wall clock. |

## Reasons

| Reason | Meaning |
|---|---|
| `activation_error` | The agent raised. `detail` leads with the original exception's `repr`, then the failure position. |
| `activation_timeout` | The activation exceeded `activation_timeout_s` and was cancelled. `detail` is empty: there is no exception to name. |
| `orphaned_result` | A tool result or approval arrived with no live continuation to admit it. `detail` is `<why>:<intent_id>` — one of `no_continuation`, `unknown_intent`, `deadline_passed`, `intent_expired`. |
| `hitl_timeout` | An approval never arrived and the policy's timeout route dropped it. |
| `ttl_wiped_suspension` | Working-memory GC reached a key still awaiting an answer; the suspension is unrecoverable. |
| `ttl_wiped_batch` | Working-memory GC reached a key with un-flushed buffered events (`docs/batching.md`). One record per wiped envelope; `detail` is `buffered=<n>,index=<i>`. |
| `batch_buffer_overflow` | An event arrived at a key whose batching buffer already held `max_buffered_events`. `detail` is `buffered=<n>,cap=<n>`. |
| `intent_dead_letter` | An intent could not be serialized for the outbox. `detail` is JSON: `{reason, intent_id, seq, tool_name}`. |

Two identities hold by construction and are worth alerting on if they break
(see [metrics.md](metrics.md)): `agent_errors + orphaned_results` equals the
element count on `.errors`, and `intents_emitted` equals the element count on
`.intents`.

## Configuring a sink

Set `errors_to` and the records are encoded and written for you:

```python
config = AgentConfig(
    provider_factory=make_client,
    intents_to="kafka://broker:9092/agent-intents",
    errors_to="kafka://broker:9092/agent-errors",
)
```

`.errors` stays exposed on `RunAgentOutputs` either way — attaching a sink adds
a branch, it does not consume the collection.

Intent dead letters (`WriteIntents`' serialization failures) are folded into the
same sink as `intent_dead_letter` records, so the errors topic carries exactly
one schema. They also remain available on `outputs.dead_letter` in their raw
`((entity_key, ToolIntent), reason)` form when no `errors_to` is configured.

### What gets written

**`kafka://` and `pubsub://`** receive `KV[bytes, bytes]`: the key is
`entity_key` (so one key's dead letters keep their order through a single
partition), and the value is a serialized `AgentEnvelope` whose
`external_event` holds a serialized `ActivationErrorRecord`:

```
AgentEnvelope
├── entity_key     = the failing key
├── event_time_ms  = the record's event time
└── external_event = ActivationErrorRecord{entity_key, reason, detail, event_time_ms}
```

The envelope wrapping is what makes the errors topic a valid **`RunAgent` input
stream**: key it by `entity_key` and it can feed another agent with no adapter.
It is a convention of this sink, not a constraint on `AgentEnvelope` — the
runtime imposes no schema on `external_event` bytes, and an ordinary Beam
pipeline (below) can read it just as easily.

Both encodings are deterministic, and `event_time_ms` is replay-deterministic by
construction, so a retried bundle republishes byte-identical records and
downstream dedup collapses them.

**`bigquery://`** receives a row instead, with `entity_key` as lowercase hex
(matching the trace rows, so a table can be clustered and joined on it without a
decode step):

```json
{"entity_key": "6b31", "reason": "activation_error", "detail": "...", "event_time_ms": 1700000000000}
```

## Example: a downstream failure-streak alarm

A single dead letter is noise; the same key failing five times in a row is a
page. Because the errors topic is a plain event stream, the alarm is a plain
Beam pipeline — no beam-agents runtime involved, only the published proto
bindings.

```python
from collections.abc import Iterator
from typing import Any

import apache_beam as beam
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec

from beam_agents._protos import ActivationErrorRecord, AgentEnvelope


def parse_error_record(payload: bytes) -> ActivationErrorRecord:
    """Decode one errors-topic value: an AgentEnvelope carrying the record."""
    envelope = AgentEnvelope()
    envelope.ParseFromString(payload)
    record = ActivationErrorRecord()
    record.ParseFromString(envelope.external_event)
    return record


class FailureStreak(beam.DoFn):
    """Alarms when one key accumulates `threshold` dead letters.

    Per-key state, so Beam serializes the counting for us — the same
    per-key-serialization property `RunAgent` itself relies on. The count
    resets on alarm: the streak is a fresh count of failures since the last
    page, not a running total that would re-alarm on every later error.
    """

    COUNT = ReadModifyWriteStateSpec("count", beam.coders.VarIntCoder())

    def __init__(self, threshold: int) -> None:
        super().__init__()
        self._threshold = threshold

    def process(
        self,
        element: tuple[bytes, ActivationErrorRecord],
        count: Any = beam.DoFn.StateParam(COUNT),
    ) -> Iterator[tuple[bytes, int]]:
        key, _record = element
        streak = (count.read() or 0) + 1
        if streak < self._threshold:
            count.write(streak)
            return
        count.clear()
        yield key, streak
```

Wire it to the topic the pipeline above writes to:

```python
alarms = (
    p
    | ReadFromKafka(
        consumer_config={"bootstrap.servers": "broker:9092"},
        topics=["agent-errors"],
    )
    | beam.Map(lambda kv: parse_error_record(kv[1]))
    | beam.WithKeys(lambda r: r.entity_key).with_output_types(
        tuple[bytes, ActivationErrorRecord]
    )
    | beam.ParDo(FailureStreak(threshold=5))
)
```

`alarms` carries `(entity_key, streak)` pairs — route them to a pager, a
notification topic, or a `RunAgent` triage agent of their own.

`tests/examples/test_failure_streak_alarm.py` runs this `FailureStreak`
verbatim against encoder-produced records; the two must stay in sync.

Variations worth knowing:

- **Rate, not streak:** window the records (`beam.WindowInto(FixedWindows(300))`)
  and count per window, so a key failing five times an hour apart does not page.
- **Filter by reason first:** `beam.Filter(lambda r: r.reason == "activation_error")`
  separates agent bugs from `orphaned_result`, which usually indicates a
  late-arriving effector result rather than a broken agent.
- **Reprocessing:** because a dead letter commits nothing, the original event can
  be replayed once the cause is fixed. Nothing in the runtime does this for you —
  the errors topic is where you would read the keys from.
