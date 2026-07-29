# Design: errors_to sink resolution with AgentEnvelope encoding

## Context

`.errors` is a `PCollection[ActivationError]` — a frozen dataclass (`entity_key`, `reason`, `detail`) built at the single `_dead_letter` chokepoint in `core/dofn.py`. `DefaultSinkResolver` already special-cases `intents_to` (keyed `WriteIntents` outbox) and `traces_to` (`_WriteTraces` encode-then-write, design D9), but `errors_to` still returns the bare scheme writer, which cannot accept dataclasses: Kafka/Pub/Sub want `KV[bytes, bytes]`, BigQuery wants rows. The intent dead-letter branch in `RunAgent.expand` papers over this for Kafka only, via `encode_intent_dead_letter` producing ad-hoc JSON `KV[bytes, bytes]` — a second, incompatible record shape on the same topic, and broken for `bigquery://`.

The errors stream is also a first-class event stream for the project's target workloads: a dead letter for `entity_key` K is exactly the kind of system trigger (ops automation, anomaly response) beam-agents exists to serve. Wrapping error records in `AgentEnvelope` — the single keyed input type — lets a downstream pipeline (including another `RunAgent`) consume the errors topic directly.

Constraints in force: protobuf for all wire schemas; replay determinism (a retried bundle must re-emit byte-identical sink records); runtime-not-framework (no new authoring abstractions); offline unit tests.

## Goals / Non-Goals

**Goals:**
- A configured `errors_to` works for all three schemes, with schema'd, deterministic records.
- One record schema on the errors sink for both activation dead letters and intent dead letters.
- Error records carry a replay-deterministic event time for downstream triage/windowing.
- A documented, test-backed downstream failure-streak alarm example proving the "errors as events" loop.

**Non-Goals:**
- No change to the `.errors` PCollection's element type (`ActivationError` stays; direct consumers unaffected).
- No `AgentEnvelope` schema change (`external_event` stays opaque; the record inside is a convention).
- No shipped alarm transform in the public API — the example is documentation-level user code.
- No retroactive re-encoding concerns: the previous encodings were never released.

## Decisions

**D1 — Wrap sink records in `AgentEnvelope`, not bare `ActivationErrorRecord` bytes.** The envelope makes the errors topic a valid `RunAgent` input stream with zero adapter glue: key with `WithKeys(entity_key)` and feed it in, or consume it with any plain Beam DoFn that already speaks the project's one ingress type. Cost is ~20 bytes of duplication (`entity_key`, `event_time_ms` appear in both envelope and record); the record keeps its own copies so a consumer that strips the envelope still has a self-contained triage record. Alternative — publish bare record bytes — rejected: every consumer would need a second decode convention, and the failure-streak example would no longer demonstrate the canonical ingress path.

**D2 — New top-level `ActivationErrorRecord` proto message.** Project rule: protobuf for ALL wire schemas; the current dead-letter JSON is a violation the moment it crosses a topic. Fields mirror the dataclass plus `event_time_ms`: `entity_key` (bytes), `reason` (string), `detail` (string), `event_time_ms` (int64). The intent dead-letter identifying fields stay as JSON inside `detail` (matching the existing `detail` idiom for structured context, e.g. failure-context suffixes) rather than as first-class fields — reasons are open-ended and per-reason submessages would couple the wire schema to every future dead-letter source. Additive proto change; no state schema involved (this is wire-only), so no `state_schema_version` bump.

**D3 — `ActivationError` gains `event_time_ms: int = 0`, populated from deterministic time only.** Emission sites all have a deterministic timestamp in hand: the element path uses the envelope's event time (`now_ms` already threaded through `process`), timer callbacks use the timer's scheduled firing timestamp (the HITL deadline / TTL mark — deterministic because the runtime computed and armed it; never `time.time()`). Thread it through the `_dead_letter` chokepoint signature so no emission site can silently omit it; the metrics accounting there is untouched. Alternative — leave records timeless — rejected: it forfeits event-time windowing downstream and loses "when" from BigQuery triage rows, for the cost of one parameter.

**D4 — `_WriteErrors` mirrors `_WriteTraces`; pure encoders live in a new `core/error_records.py`.** Resolver shape: `resolve("errors_to", uri)` returns `_WriteErrors(scheme_writer, to_row=scheme == "bigquery")`, which maps `serialize_error_envelope` (→ `KV[bytes, bytes]`, envelope serialized with `deterministic=True`, keyed by `entity_key` for per-key ordering through one partition — same rationale as `WriteIntents`/`serialize_trace_event`) or `activation_error_to_row` (flat row, `entity_key` as lowercase hex matching `trace_event_to_row`; `reason`, `detail`, `event_time_ms` native). Encoders are pure module-level functions beside a re-export from `core`, not methods on the transform, exactly like `observability/exporters.py` — the sink wiring stays a one-line `beam.Map`, and the doc example can import the same encoder in its test to produce fixture bytes. Not in `dofn.py` (already large) and not in `observability/` (errors are core dead letters, not telemetry).

**D5 — Dead-letter unification replaces `encode_intent_dead_letter`.** `intent_dead_letter_to_error` maps `((key, intent), reason)` → `ActivationError(entity_key=key, reason="intent_dead_letter", detail=json{reason, intent_id, seq, tool_name}, event_time_ms=intent.created_at_ms)`. `created_at_ms` is the intent's own deterministic timestamp — already computed from element time — so replay identity holds. The reason constant `REASON_INTENT_DEAD_LETTER` joins the existing reason vocabulary in `dofn.py`. `encode_intent_dead_letter` is deleted (unreleased; its JSON payload shape survives inside `detail`, minus fields now first-class on the record).

Because both streams are now the same element type, `RunAgent.expand` **flattens** the mapped dead letters into `.errors` and attaches a *single* resolved `_WriteErrors` — rather than resolving `errors_to` twice and giving each branch its own writer, as the pre-change code did. One resolve, one sink, one schema: the two streams cannot drift into different shapes, and the previously Kafka-only dead-letter path inherits every scheme for free. `.errors` itself is unaffected (`Flatten` reads it, does not consume it), so it stays exposed on `RunAgentOutputs` exactly as before. This makes the three sink branches asymmetric, so `expand`'s uniform `_SINK_FIELDS` loop becomes three explicit branches with the same labels; `errors_to` is attached last, once the dead-letter branch exists.

**D6 — Failure-streak alarm example: docs-level user code, TestStream-backed.** `docs/errors.md` documents the wire format (envelope → record, per scheme) and a complete downstream pipeline: `ReadFromKafka` → parse `AgentEnvelope`/`ActivationErrorRecord` → `WithKeys(entity_key)` → a ~30-line stateful DoFn (`CombiningValueState` count; on reaching threshold `N`, emit one alarm `(entity_key, streak)` and clear). The test (`tests/examples/test_failure_streak_alarm.py`) copies the doc's DoFn verbatim, feeds encoder-produced envelope bytes through TestStream, and asserts the threshold/reset scenarios — proving the documented format is consumable with public parsing alone. The DoFn is NOT exported from `beam_agents`: shipping it would cross into framework territory; the point is that a plain Beam consumer suffices. Reset-on-alarm (count to N, emit once, clear) is chosen over sliding windows to keep the example state-minimal and deterministic; the doc notes windowed variants as an exercise.

## Risks / Trade-offs

- [Example code drift between `docs/errors.md` and its test] → the test embeds the doc's DoFn verbatim with a comment binding the two; a divergence is a review-visible edit to both files. Full literate extraction is overkill for one example.
- [Consumers mistaking `external_event` convention for schema] → `wire-schemas` delta and `docs/errors.md` state explicitly that the envelope wrapping is a documented convention of the errors sink, not an `AgentEnvelope` constraint.
- [`detail` JSON is schema-less inside a schema'd record] → accepted: reasons are open-ended and `detail` has always been free-form; the record-level fields (`reason`, `entity_key`, `event_time_ms`) carry everything the alarm path needs without parsing `detail`.
- [Threading `event_time_ms` touches every `_dead_letter` call site] → the chokepoint design makes this mechanical and mutation-test-visible; sites that lacked an obvious timestamp (none found — all have element or timer time) would surface at review as explicit `0`s.
- [Kafka writer expects `KV[bytes, bytes]` with exact coder] → `_WriteErrors` sets `.with_output_types(tuple[bytes, bytes])` on the encode `Map`, same as the traces and intents paths, so the cross-language expansion sees a concrete `KvCoder`.

## Migration Plan

Additive proto + regen (diff-clean gate), new module, resolver behavior change for a previously-broken path, dead-letter branch rewrite. Nothing released depends on the old encodings; no data migration. Rollback is a straight revert.

## Open Questions

None blocking. If a future change wants first-class structured detail (per-reason submessages), it layers an additive `oneof` onto `ActivationErrorRecord` without disturbing this encoding.
