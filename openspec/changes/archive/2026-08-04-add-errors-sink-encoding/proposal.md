# Proposal: errors_to sink resolution with AgentEnvelope encoding

## Why

A configured `errors_to` sink is broken today: `.errors` carries `ActivationError` Python dataclasses, but `DefaultSinkResolver` returns a bare write transform for `errors_to` — Kafka/Pub/Sub writers require `KV[bytes, bytes]` and BigQuery requires row mappings, so the pipeline fails at runtime. This is the same encode gap design D9 already closed for `traces_to` (`_WriteTraces`), acknowledged as open in `tests/core/test_transform.py` ("`errors_to` carries `ActivationError` dataclasses, not `TraceEvent`s"). The intent dead-letter branch has the sibling defect: it pre-encodes ad-hoc JSON `KV[bytes, bytes]`, which breaks outright for a `bigquery://` errors sink and gives the errors topic two incompatible record shapes.

Beyond the fix, the errors stream is itself an event stream for the project's target workloads (ops automation, anomaly response). Encoding dead letters as `AgentEnvelope` — the single keyed input type — makes the errors topic directly consumable by a downstream Beam pipeline (including another `RunAgent`) with no adapter glue. A documented, test-backed failure-streak alarm example proves that loop end to end.

## What Changes

- Add a new top-level `ActivationErrorRecord` wire message to `protos/beam_agents.proto` (entity_key, reason, detail, event_time_ms), regenerated bindings committed.
- Extend the `ActivationError` dead-letter dataclass with `event_time_ms` (default `0`), populated at every emission site from the element's event time or the timer's scheduled firing time — never a wall-clock read, so replayed bundles encode byte-identical records.
- Encode errors for the sink: for `kafka://`/`pubsub://`, each `ActivationError` becomes `KV[entity_key, AgentEnvelope(entity_key, event_time_ms, external_event = serialized ActivationErrorRecord)]` with deterministic serialization; for `bigquery://`, a row mapping of the record fields.
- `DefaultSinkResolver.resolve("errors_to", ...)` returns an encoding writer (`_WriteErrors`, mirroring `_WriteTraces`) instead of a bare write transform.
- Unify the intent dead-letter branch: `WriteIntents` dead letters are mapped into `ActivationError` records (reason `intent_serialization_failed`, detail carrying the intent's identifying fields) and routed through the same errors encoder, so one schema covers the whole errors sink and the `bigquery://` scheme works for dead letters too.
- Add `docs/errors.md`: the errors-sink record schema and a downstream failure-streak alarm example — a small stateful DoFn consuming the errors topic keyed by `entity_key`, alarming after N dead letters for the same key — backed by a TestStream-driven test.

No breaking changes: the `AgentEnvelope` schema itself is untouched (`external_event` stays opaque bytes; the record schema inside it is a documented convention), the proto addition is additive, and `.errors` still exposes `ActivationError` objects to callers who consume the PCollection directly.

## Capabilities

### New Capabilities

- `errors-sink`: encoding of `.errors` dead letters and intent dead letters into `AgentEnvelope`-wrapped `ActivationErrorRecord` messages (keyed deterministic bytes for Kafka/Pub/Sub, rows for BigQuery), and the documented downstream failure-streak alarm consumption pattern.

### Modified Capabilities

- `wire-schemas`: add the `ActivationErrorRecord` top-level message; the importable-bindings requirement grows from seven to eight top-level message classes.
- `run-agent-transform`: `errors_to` now resolves to an encoding writer rather than a bare write transform, and the intent dead-letter → errors-sink route emits the unified error record encoding (works for all three schemes, not just Kafka/Pub/Sub).

## Impact

- `protos/beam_agents.proto` + committed regenerated `src/beam_agents/_protos/` (diff-clean regen gate applies).
- `src/beam_agents/core/dofn.py`: `ActivationError` gains `event_time_ms`; `_dead_letter`/`_error` chokepoints thread the timestamp through (metrics accounting unchanged).
- `src/beam_agents/core/transform.py`: `_WriteErrors` transform; `DefaultSinkResolver` special-cases `errors_to`; dead-letter branch re-routed through the shared encoder.
- `src/beam_agents/actions/write_intents.py`: `encode_intent_dead_letter` replaced by/refactored into a dead-letter → `ActivationError` mapping (JSON detail preserved).
- `docs/errors.md` (new) with a failure-streak alarm example; example code exercised by tests.
- Tests: wire-schema round-trips, encoder determinism, resolver behavior per scheme, dead-letter unification, TestStream-driven example test. All offline (no docker).
