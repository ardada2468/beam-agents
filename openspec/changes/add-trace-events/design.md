## Context

`TraceEvent` was specified in `add-wire-schemas-and-coders` and has been produced in three places ever since, none of which fill its correlation fields:

| Producer | Events | `trace_id`/`span_id`/`parent_span_id` | Attributes |
|---|---|---|---|
| `core/loop.py:107,119` | `ACTIVATION_START`, `ACTIVATION_END` | all empty | none |
| `model/facade.py:_stage_trace` | `LLM_CALL` | all empty | 6, incl. usage forced to `0` when unknown |
| `core/context.py:_stage_llm_trace` | `LLM_CALL` | all empty | 2, no usage at all |

`TOOL_CALL`, `INTENT_EMITTED`, and `ERROR` are in the enum and have no producer. The DoFn's five failure routes (`activation_timeout`, `activation_error`, `orphaned_result`, `hitl_timeout`, `ttl_wiped_suspension`) emit only `.errors` records. `_escalate` mints a real `ToolIntent` from a timer callback and reports it nowhere. And `.traces` is plumbed to `AgentConfig.traces_to` through `DefaultSinkResolver`, which hands `TraceEvent` protos straight to `WriteToKafka`/`WriteToPubSub`/`WriteToBigQuery` — none of which accept a proto message, so the documented sink path has never been exercisable.

The constraints this design has to respect are the ones that make the runtime replayable:

- **Every non-determinism source is injected.** Contexts and the facade take `now_ms`, `rng`, and `sleep`; `model-facade`'s spec states outright that the facade MUST NOT read wall-clock time. Trace instrumentation must not be the exception that reintroduces one.
- **Correctness invariant 1**: staged effects apply only on activation success. Whatever the failure paths emit cannot come from a rolled-back context.
- **Correctness invariant 2**: `intent_id = uuid5(NS, key|seq|step_index)`, byte-identical on replay. Anything that perturbs the step counter perturbs intent ids, and the retry-determinism gate is watching.
- **Additive-only schema evolution** under `state_schema_version = 1`; `ToolIntent` is stored in `PENDING` BagState and guarded by golden blobs.

## Goals / Non-Goals

**Goals:**
- Every `TraceEvent` the runtime emits carries a `trace_id`, a `span_id`, and (for child events) a `parent_span_id`, derived deterministically from activation scope.
- One trace covers a whole logical invocation, including a suspend → effector → resume round trip, with no correlation state added to the wire beyond a single `ToolIntent.trace_id`.
- The event kinds the schema declares all have producers, and failures are traced, not just dead-lettered.
- Token counts on `.traces` are *true or absent* — never a placeholder zero — and a cache hit reports the stored response's real usage while remaining distinguishable from new provider spend.
- `.traces` can actually reach a configured sink.

**Non-Goals:**
- **Span durations.** All events take `start_ms`/`end_ms` from the injected activation clock, so spans have zero width. See D7.
- **An OTLP exporter, or a collector integration.** This change produces well-formed, correlated records on a PCollection and serializes them for the existing sink schemes. Shipping them to an OTel collector is a later change.
- **Beam metrics.** Counters/distributions (`observability/metrics.py` in the module map) are separate; the latency budget is measured by the benchmark harness, not by these events.
- **Sampling.** Every activation is traced. A sampling policy would have to be deterministic per key to survive replay; it is not needed until volume proves it is.
- **Changing what `.errors` carries.** The dead-letter records stay exactly as they are; `ERROR` trace events are additional, not a replacement.

## Decisions

### D1. Trace and span ids are `uuid5` of activation scope, exactly like `intent_id`

```
trace_id = uuid5(_TRACE_NAMESPACE, f"{key.hex()}|{seq}").bytes                       # 16 bytes
span_id  = uuid5(_TRACE_NAMESPACE, f"{key.hex()}|{seq}|{role}|{index}").bytes[:8]    #  8 bytes
```

`role` is one of `activation`, the event-type name (`LLM_CALL`, `TOOL_CALL`, `INTENT_EMITTED`, `SUSPENDED`, `ERROR`), or `timer`. Widths match the W3C trace-context / OTel wire formats, so an exporter can pass them through untranslated.

The alternative — random ids from `uuid4` or an OTel SDK `IdGenerator` — was rejected on two counts. It would make a replayed bundle emit *different* trace records for the same work, so the at-least-once duplicates a bundle retry produces would be indistinguishable from genuinely new activations. And with `trace_id` on `ToolIntent` (D6), a random id makes the intent bytes non-deterministic, which breaks correctness invariant 2 and the retry-determinism gate outright. Deriving ids the same way `intent_id_for` derives its own keeps one rule in the codebase: identity is a pure function of `(key, seq, step)`.

Including `role` in the span name is what makes collisions impossible without a global counter. `AgentContext` lets the agent choose the `step_index` it passes to `facade.complete`, and that number is drawn from the same space as intent step indices; without `role`, an `LLM_CALL` at step 2 and an `INTENT_EMITTED` at step 2 would share a span id.

### D2. One trace per `(key, seq)` — suspension is inside the trace, not across two of them

A resumed activation runs under the suspended activation's `seq` (that is what makes its `intent_id`s not collide), so `trace_id_for(key, seq)` recomputes to the same 16 bytes with nothing carried on the wire. `ToolResult` and `AgentEnvelope.Approval` therefore need **no** `trace_id` field: the resume path derives it.

Each activation *attempt* gets its own span: `activation` role with `index = ` the step index the activation entered at (`0` for the initial one, `Continuation.step_index` for a resume). The initial attempt's span is the trace root; every resume's span is parented to it. So a suspend → approve → resume cycle reads as one trace, root span plus one child span per resume, with the `SUSPENDED` and `INTENT_EMITTED` events hanging off the attempt that produced them.

The alternative of minting a fresh trace per activation and linking them with an OTel span *link* was rejected: links are an exporter-level concept this schema has no field for, and the natural join key — `(key, seq)` — is already stable across the boundary.

### D3. Correlation is stamped at the staging boundary

`stage_trace_event` (both contexts) owns an `ActivationTrace` and fills in any of `trace_id`, `span_id`, `parent_span_id` the incoming event left empty, using the event's own `event_type` and `step_index` for the span derivation. Producers emit uncorrelated events and stay ignorant of tracing:

- `model/facade.py` keeps its exact `complete` signature — no `trace_id`/`parent_span_id` parameters threaded through a call that already takes six.
- `tools/runner.py` is untouched; `AgentContext.run_tool` stages the `TOOL_CALL` because it is the layer that knows the activation.
- The loop driver builds the activation span once and hands it to the context.

The alternative — each producer computing its own ids — needs the activation's trace passed to every one of them and gives four places the chance to derive a span id slightly differently. Stamping centrally means the derivation exists once, and any future producer is correlated the moment it stages through the context. The cost is that a producer *cannot* override its parent (an event arriving with a non-empty `parent_span_id` is left alone, which is the escape hatch if one ever needs to).

### D4. Usage attributes are omitted when unknown, never zeroed; a cache hit reports stored usage and is marked unbilled

Three attributes carry the money:

| Attribute | Cache miss | Cache hit | Failure before a response |
|---|---|---|---|
| `gen_ai.usage.input_tokens` | decoded | decoded from stored bytes | *absent* |
| `gen_ai.usage.output_tokens` | decoded | decoded from stored bytes | *absent* |
| `beam_agents.billed` | `true` | `false` | `false` |

`ActivationContext` gains the same injected `Decode` callable the facade takes, so its `call_model` can decode the cached response instead of staging a usage-free stub. When no `decode` is configured (the parameter is optional so existing construction sites keep working), usage attributes are simply absent.

The rule is that **a present attribute is true**. The facade's current `str(usage.prompt_tokens if usage is not None else 0)` reports `0` for a call that never got a response, which a consumer summing `gen_ai.usage.input_tokens` cannot distinguish from a real zero-token call. Omission is the only honest encoding available in a `map<string, string>`.

`beam_agents.billed` (rather than dropping usage on hits) keeps both questions answerable from one stream: *what did this activation consume* sums everything, *what did we pay the provider for* filters `billed=true`. This matches `model-facade`'s existing accounting split, where a hit reports `TokenUsage` on its `FacadeResult` but does not touch the billed accumulator — the trace attributes now say the same thing in the same shape.

### D5. Failure traces are synthesized by the DoFn, not recovered from the failed activation

A failed or timed-out activation's staged traces are discarded exactly as they are today: invariant 1 is not weakened, and on the timeout path the context is unreachable anyway (it lives on a cancelled coroutine on the bridge thread).

Instead each failure route builds a self-contained `ERROR` event from what the DoFn already holds — `key`, `seq`, `now_ms`, the reason string, and `type(exc).__name__` — and yields it on `.traces` next to the `.errors` record it already yields. The event's `trace_id` is `trace_id_for(key, seq)`, so it lands in the same trace as the activation's other records *if any were committed*, and stands alone as a one-event trace if none were.

The rejected alternative — plumbing staged traces out of the failure path so a failed activation's `LLM_CALL` events survive — is more useful and considerably worse. It requires `run_activation` to catch, wrap, and re-raise with the partial context attached; it cannot work at all on the timeout path; and it puts the runtime in the business of partially committing staged effects, which is the exact distinction invariant 1 exists to keep sharp. The loss is real and is recorded in Risks: an activation that calls the provider and then fails leaves no `LLM_CALL` record. The `ERROR` event names the key, seq, and error type, which is what triage needs first.

`on_ttl` and `on_hitl` fire outside any activation and read `Continuation.seq` for the trace scope; their events use the `timer` span role so they cannot collide with an activation's steps.

### D6. `ToolIntent.trace_id` is the only wire addition

Field 11, `bytes`, populated at `_stage_intent` time from the staging context's trace. That single field lets the effector open its execution span under the pipeline's trace — the one hop where the trace would otherwise be severed, because the effector is a separate service reading intents off a topic.

`ToolResult` deliberately gets nothing: it already carries `entity_key` and `seq`, which is precisely the material `trace_id_for` needs, so a field there would be a redundant copy that could disagree with the derivation.

`TraceEvent.EventType` gains `SUSPENDED = 7`. A suspension is a first-class thing that happens to an activation — it has a deadline, an adapter, and a set of pending intent ids — and folding it into `ACTIVATION_END` attributes alone would make the most operationally interesting state (waiting, on what, until when) require attribute archaeology. Belt and braces for old readers: `ACTIVATION_END` *also* gains `beam_agents.activation.status = completed|suspended`, so a consumer that does not know enum value 7 still sees the outcome.

Both edits are additive: an old `ToolIntent` decodes with `trace_id = b""`, and a reader that predates `SUSPENDED` sees an unrecognized enum number rather than a parse failure. No `state_schema_version` bump.

### D7. Zero-width spans: `start_ms`/`end_ms` come from the activation clock

Every event sets both timestamps to the injected `now_ms` (the element's event time). Spans therefore have no duration, and this design accepts that rather than solving it here.

Measuring elapsed time requires a monotonic wall clock read inside the hot path — inside `LlmFacade.complete`, whose spec says in as many words that it MUST NOT read wall-clock time so a replayed bundle behaves identically. Two ways around it were considered and rejected: threading a second injected `monotonic_ms` seam through every facade and context (a real widening of four constructors to serve an observability field, and it makes trace records non-reproducible under replay, which costs the content-level dedup that D1 buys); or weakening the facade's no-wall-clock requirement (trading a load-bearing determinism rule for a nicer dashboard).

Latency has an owner already: Beam metrics surfaced to runner dashboards, which is what `observability/metrics.py` is for in the module map, and the p50/p99 activation-overhead budget is enforced by the benchmark harness, not by these records. What `.traces` answers here is *what happened, in what order, at what token cost*.

**Resolution (adopted during implementation):** rather than leave duration homeless, this change ships `observability/metrics.py` now — a `beam_agents/activation_ms` distribution plus per-outcome counters, measured by the DoFn *around* the bridge call with an injected monotonic clock (`_AgentDoFn(monotonic=...)`, defaulting to `time.monotonic`). This threads no clock into the activation, the facade, or any trace byte: metrics are not staged effects, so replayed bundles still emit byte-identical trace records and the no-wall-clock requirements stand untouched. The distribution holds only successful activations — a timeout's "latency" is the configured budget and would corrupt the p99 the distribution exists to watch; failures get a counter. Span *durations* remain deliberately zero-width; anyone reaching for latency now has a real home for it instead of a misleading span width.

### D8. Read-only tool calls do not advance the intent step counter

`AgentContext._step_index` seeds `intent_id_for`. If `run_tool` advanced it, an activation that runs a read-only tool before `act(...)` would mint a *different* `intent_id` than the same code minted before this change — deterministic still, but not the same bytes, which invalidates in-flight `Continuation`s across a pipeline `--update` and silently changes the effector's dedup keys.

`TOOL_CALL` events therefore carry their own monotonic `tool_index`, used only for span derivation, and the `role` component of D1 keeps their span ids disjoint from the intent/model steps. `TraceEvent.step_index` for a tool call records the intent-step cursor at the time of the call, which is what a reader needs to order it against the other events.

### D9. `.traces` serialization lives with the sink resolution, mirroring `intents_to`

`DefaultSinkResolver.resolve("traces_to", uri)` returns a composite transform that serializes before writing, exactly as it already returns `_KeyedWriteIntents` for `intents_to` on `kafka://`/`pubsub://`:

- `kafka://`/`pubsub://` — `(entity_key, SerializeToString(deterministic=True))` pairs, keyed by `entity_key` so one key's trace records keep their relative order through the partition.
- `bigquery://` — a flat row dict with hex-encoded ids, ms timestamps, the event-type name, and `attributes` as a repeated key/value record, which is the shape a BigQuery trace sink can cluster on `trace_id`.

The deterministic serializer matters for the same reason the coders use it: map fields (`attributes`) have no defined encoding order otherwise, and identical events would produce different bytes, defeating downstream dedup on content.

## Risks / Trade-offs

- **A failed activation leaves no record of the provider calls it made.** → D5's `ERROR` event names the key, seq, reason, and error type. The `LLM_CALL` detail is lost, but the provider spend is not silently lost overall: the replay cache means the retry re-serves the same response, and the retry's committed trace reports it with `billed=false`. Revisiting this needs the partial-commit machinery D5 declines to build.
- **Trace volume grows several-fold and every activation is traced.** → Records are small and go to a dedicated tagged output the caller can leave unconsumed (Beam drops it) or sink cheaply. Sampling is a Non-Goal until volume proves it necessary; when it comes it must be deterministic per key.
- **At-least-once traces: a retried bundle re-emits byte-identical events.** → Deliberate, and the reason ids are deterministic (D1): downstream dedup on `(trace_id, span_id, event_type)` collapses them exactly. Non-deterministic ids would have made these duplicates undetectable.
- **`ToolIntent` grows a field, and it is stored in `PENDING` BagState.** → Additive under `state_schema_version = 1`; old blobs decode with an empty `trace_id`. Guarded by keeping the existing golden blob parsing unchanged and adding a populated-`trace_id` golden beside it. Intents staged before an update resume without a trace id — they trace as a one-event `ERROR`/`INTENT_EMITTED` gap rather than failing.
- **Zero-width spans will read as "instantaneous" in any span viewer.** → Stated in Non-Goals and D7 rather than papered over. Anyone reaching for latency is directed at metrics.
- **Two context surfaces must agree.** `AgentContext` and `ActivationContext` both gain stamping, and only `ActivationContext` is currently driven by the DoFn. → The shared `ActivationTrace` helper is the single implementation; both contexts delegate to it, and the spec's scenarios are written against the behavior, not against one class.
- **New branches in `core/dofn.py` and `core/context.py` move the mutation-gate ceilings.** → Failure-route trace assertions go in the fake-handle unit tests (inside the mutmut selection) rather than only in the pipeline suite, to keep as much of the change mutation-covered as possible. **This turned out to have a much larger blast radius than "as much as possible" suggests**: a selected test that calls `process()` reclassifies every mutant in `process`/`_start`/`_resume`/`_commit` from "no tests" (ratcheted, allowed) to "survived" (always fatal), because until now only the deselected pipeline suites reached that code. 264 mutants moved; ~120 died to the new assertions and the rest needed `tests/core/test_dofn_commit.py`, a suite about commit and routing rather than about traces. The net is a real gain — `dofn.py`'s unreachable-by-mutation surface falls from 267 to 3 — but anyone adding a selected test that drives a previously pipeline-only entry point should expect the same bill.

## Migration Plan

1. Land the proto edit and regenerate `_pb2.py` (diff-clean in CI); add the new golden blob. Old readers are unaffected — a widened `ToolIntent` and an unknown enum number both decode.
2. Land the `observability` package and the stamping sink. At this point traces gain correlation with no wire dependency.
3. Land the producers (child events, failure routes, escalation) and the facade attribute change.
4. Land the sink serialization last, so a `traces_to` that has never worked starts working only once the records are worth shipping.

Rollback is per-step and needs no state migration: reverting the producers leaves `trace_id` populated on intents and harmlessly ignored, and reverting the proto edit is the only step that requires care (an intent written with `trace_id` and read by the reverted build decodes with the field as unknown-and-preserved, so a re-revert loses nothing).

## Open Questions

- ~~Should `observability/metrics.py` introduce the injected monotonic seam D7 declines to add?~~ **Resolved: yes, in this change** — see D7's resolution note. The seam lives on the DoFn (`monotonic=`), latency reports as Beam metrics, and trace bytes stay deterministic. The narrower question that remains open is whether `beam_agents.duration_ms` should ever be backfilled onto the events themselves; doing so would break byte-identical replay and content-level dedup, so the current answer is no.
- Should the effector, once it reads `ToolIntent.trace_id`, emit its own `TraceEvent`s to the results topic, or export directly to the collector from the service? The wire field is the same either way, so this can be decided in the effector's own change.
- Is `beam_agents.*` the right prefix for the runtime attributes, or should they follow OTel's `gen_ai.*` extension conventions more closely (e.g. `gen_ai.request.is_cached`)? The GenAI semantic conventions do not currently name a cache-hit attribute; `beam_agents.cache_hit` is the existing precedent in `model/facade.py` and this change keeps it rather than inventing a `gen_ai.*` key the spec does not define.
