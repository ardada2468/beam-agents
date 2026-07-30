## ADDED Requirements

### Requirement: The example consumes exported traces with public bindings only

The continuous-evaluation example SHALL read the traces topic written by a `kafka://` traces sink and decode each value with nothing but the public protobuf bindings — no runtime imports, no internal helpers. It SHALL treat `(entity_key, seq)` as activation identity, recomputing `trace_id` via `trace_id_for(entity_key, seq)` rather than carrying trace bytes through the join, and the recomputed ID MUST equal the `trace_id` stamped on the consumed events.

#### Scenario: Exported trace bytes decode with public bindings

- **WHEN** the example's parse stage is given values produced by the runtime's own Kafka trace encoder (`serialize_trace_event`)
- **THEN** each value decodes to a `TraceEvent` whose `entity_key`, `seq`, `event_type`, and attributes match the encoded event, using only public proto bindings

#### Scenario: Activation identity is recomputed, not carried

- **WHEN** a joined record is built for an activation identified by `(entity_key, seq)`
- **THEN** the record's `trace_id` equals `trace_id_for(entity_key, seq)` and equals the `trace_id` carried by every consumed event of that activation

### Requirement: The trace-outcome join is deadline-bounded, duplicate-tolerant, and honest about lateness

The example SHALL join per-activation trace summaries with outcome records in a stateful DoFn keyed by `(entity_key, seq)`, emitting the joined record when the outcome arrives rather than at the close of a lag-sized window. Per-activation state SHALL be garbage-collected by an event-time deadline timer. An activation whose deadline fires without an outcome SHALL emit an explicit `no_outcome` record; an outcome arriving for an already-emitted or already-collected activation SHALL route to an `orphaned_outcomes` output; neither case may be silently dropped. Duplicate trace events from at-least-once delivery SHALL be deduplicated by `(span_id, event_type)` before summarization, so replayed deliveries never alter the joined record.

#### Scenario: A lagging outcome joins on arrival

- **WHEN** an activation's trace events arrive, and its outcome record arrives later but before the evaluation deadline
- **THEN** exactly one joined record is emitted at outcome arrival, carrying the activation summary (status, token usage) and the outcome's scenario and label

#### Scenario: The deadline emits an explicit no-outcome record

- **WHEN** the watermark passes an activation's evaluation deadline with no outcome received
- **THEN** a `no_outcome` record for that activation is emitted, its per-activation state is cleared, and nothing is silently dropped

#### Scenario: An outcome past the deadline is orphaned, not joined

- **WHEN** an outcome record arrives after its activation's deadline has fired and state was collected
- **THEN** the outcome routes to the `orphaned_outcomes` output and no second joined record is emitted for the activation

#### Scenario: Duplicate trace events do not change the joined record

- **WHEN** the same trace events are delivered more than once before the outcome arrives, as at-least-once delivery permits
- **THEN** the joined record is identical to the single-delivery case — token usage is not double-counted and the summary reflects each `(span_id, event_type)` once

### Requirement: The judge stage scores through the model seam with versioned prompts and fail-closed verdicts

The LLM-as-judge stage SHALL be a plain DoFn calling a provider through the `LLMClient` protocol built from an injected provider factory — not `RunAgent` — so any structural implementation of the seam substitutes. The judge prompt SHALL be paired with an explicit version identifier included in every request, and every verdict row SHALL record that version and the judge model ID. Verdicts SHALL be parsed as constrained JSON with a bounded score range; a verdict that fails to parse or leaves the range SHALL route the record to a `judge_errors` output, and the example MUST NOT fabricate, default, or coerce a score.

#### Scenario: Verdict rows carry the judge's provenance

- **WHEN** a joined record is scored successfully
- **THEN** the emitted verdict row carries the judge prompt version and the judge model ID alongside the score, and the prompt version string appears in the request material sent to the provider

#### Scenario: An unparseable verdict fails closed

- **WHEN** the provider returns a payload that does not parse as the verdict schema or whose score is out of range
- **THEN** the record routes to `judge_errors` with the failure reason, no verdict row is emitted for it, and no aggregate counts a fabricated score

#### Scenario: The seam substitutes structurally

- **WHEN** the judge stage is constructed with a factory returning `FakeLLM` instead of a real provider
- **THEN** the stage behaves identically apart from the served payloads, with no code path conditioned on the provider's concrete type

### Requirement: The example is a doc-contract pair that runs offline and emits the documented rows

The example's pipeline code SHALL live in the documentation page and be copied verbatim into a doc-contract test between explicit keep-in-sync markers, following the established errors-example pattern. The test SHALL run fully offline — `TestPipeline`, encoder-produced trace bytes, `FakeLLM` for the judge, no docker, no network — with timer and lateness behavior driven by scripted watermark advances, never `sleep()`. Emitted verdict rows SHALL carry the documented fields (trace identity in hex, scenario, outcome label, score, judge provenance, deduplicated billed token usage), and per-scenario aggregates SHALL be grouped by scenario and judge prompt version so a prompt-version change is a visible discontinuity rather than a silent shift.

#### Scenario: The documented pipeline runs offline verbatim

- **WHEN** the doc-contract test runs the stages copied verbatim from the documentation against encoder-produced trace bytes, lagged outcomes, and a scripted `FakeLLM`, with no docker or network available
- **THEN** the pipeline completes and produces the joined, judged, and aggregated outputs the documentation describes

#### Scenario: Aggregates are grouped by scenario and prompt version

- **WHEN** verdict rows spanning two judge prompt versions are aggregated
- **THEN** the aggregate rows are keyed by `(scenario, judge_prompt_version)`, so scores from different prompt versions are never averaged together
