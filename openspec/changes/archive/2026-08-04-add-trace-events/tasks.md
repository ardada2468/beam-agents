## 1. Wire schema (additive, lands first so everything else can compile against it)

- [x] 1.1 Add `bytes trace_id = 11` to `ToolIntent` and `SUSPENDED = 7` to `TraceEvent.EventType` in `protos/beam_agents.proto`, with comments matching the file's existing additive-evolution style (what an old reader sees, why no `state_schema_version` bump).
- [x] 1.2 Regenerate `_pb2.py` with the repo's generation target and confirm the regen is diff-clean under CI's check; never hand-edit the generated file.
- [x] 1.3 Write the schema-compat tests first (`tests/core/test_schema_compat.py`): the existing `tool_intent.bin` golden still decodes with `trace_id == b""` and every other field equal; a `TraceEvent` with `SUSPENDED` round-trips; 16/8-byte correlation ids round-trip at their widths. Confirm the `SUSPENDED` test fails before 1.1.
- [x] 1.4 Add a `tool_intent_traced.bin` golden (populated `trace_id`) via `tests/core/golden/generate.py`, leaving the existing goldens byte-unchanged.

## 2. Trace identity and the `observability` package

- [x] 2.1 Write `tests/observability/test_trace_ids.py` first: `trace_id_for` is 16 bytes and reproducible across processes; `span_id_for` is 8 bytes; equal `(key, seq)` with different `role`/`index` never collide; an `LLM_CALL` and an `INTENT_EMITTED` at the same `step_index` get different span ids; no clock/rng/module state is read (assert via a monkeypatched `time`/`random` that raises).
- [x] 2.2 Create `src/beam_agents/observability/__init__.py` and `traces.py`: `_TRACE_NAMESPACE`, `trace_id_for(entity_key, seq)`, `span_id_for(entity_key, seq, role, index)`, and the attribute-name constants (`gen_ai.operation.name`, `gen_ai.request.model`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`, `error.type`, `beam_agents.cache_hit`, `beam_agents.billed`, `beam_agents.attempts`, `beam_agents.circuit_state`, `beam_agents.reason`, `beam_agents.activation.status`, `beam_agents.activation.kind`, `beam_agents.intent_id`, `beam_agents.tool_name`, `beam_agents.intent_kind`, `beam_agents.expires_at_ms`, `beam_agents.deadline_ms`, `beam_agents.adapter`).
- [x] 2.3 Write tests for `ActivationTrace` (parent/child linkage, stamping semantics, `usage_attributes(None)` omitting keys), then implement it in `traces.py`: constructed from `(entity_key, seq, entry_step_index, is_resume)`, exposing the activation `span_id`, `stamp(event)` (fill only empty correlation fields, derive `span_id` from the event's own `event_type`/`step_index`), `activation_start()`, `activation_end(status)`, `suspended(...)`, `error(reason, error_type)`, and a `usage_attributes(usage | None, billed)` helper that omits rather than zeroes.
- [x] 2.4 Export the public names from `observability/__init__.py`; keep root `beam_agents/__init__.py` untouched (`tests/test_import.py` guards the public surface).

## 3. Staging-boundary correlation

- [x] 3.1 Write the stamping tests in `tests/core/test_context.py` first: an event staged with empty correlation comes back with the activation's `trace_id`, a derived `span_id`, and the activation span as `parent_span_id`; an event staged with a non-empty `parent_span_id` keeps it; `AgentContext` and `ActivationContext` stamp identically.
- [x] 3.2 Give both contexts an `ActivationTrace` (built from `entity_key`/`seq`, plus the entry `step_index` and resume flag for `ActivationContext`) and route `stage_trace_event`/`stage_trace` through `stamp`.
- [x] 3.3 Confirm `LlmFacade.complete`'s signature is unchanged and its staged events still arrive correlated — the facade must need no correlation parameters (spec scenario "The facade signature carries no correlation parameters").

## 4. Truthful usage on the LLM path

- [x] 4.1 Write the usage tests first: a cache hit reports the stored response's decoded input/output tokens with `beam_agents.billed = false`; a provider call reports `billed = true`; a call that fails before a response omits both usage keys entirely (assert key absence, not `== "0"`); summing billed usage across a retried bundle equals the first attempt's total. Cover both `LlmFacade` (`tests/model/`) and `ActivationContext.call_model` (`tests/core/test_context.py`).
- [x] 4.2 Rework `LlmFacade._stage_trace` to build attributes from `ActivationTrace.usage_attributes`: omit usage when `usage is None`, add `gen_ai.operation.name` and `beam_agents.billed`, keep `cache_hit`/`attempts`/`circuit_state`/`error.type`.
- [x] 4.3 Add an optional `decode: Decode | None` parameter to `ActivationContext` and use it in `call_model` to decode the cached (hit) or fresh (miss) response bytes; with no `decode` configured, stage the event with usage attributes absent.
- [x] 4.4 Thread the provider's `decode` from the loop driver / DoFn construction path so the runtime path actually gets truthful cache-hit counts rather than silently defaulting to "unknown".

## 5. Activation span and child events

- [x] 5.1 Write the activation-span tests first (`tests/core/test_loop.py`): start/end share the attempt's span id; `ACTIVATION_END` carries `beam_agents.activation.status`; a resume carries `activation.kind = resume`, shares the suspended attempt's `trace_id`, and is parented to the root activation span; `start_ms == end_ms == now_ms`.
- [x] 5.2 Rework `run_activation` to build the `ActivationTrace`, emit the correlated `ACTIVATION_START`/`ACTIVATION_END`, and emit a `SUSPENDED` event carrying `deadline_ms`, `adapter`, and the pending intent ids when the outcome is `Suspend`.
- [x] 5.3 Write then implement `INTENT_EMITTED` staging in both contexts' `_stage_intent` (attributes: `intent_id`, `tool_name`, `intent_kind`, `expires_at_ms`), asserting one event per staged intent.
- [x] 5.4 Write then implement `TOOL_CALL` staging in `AgentContext.run_tool`, with its own `tool_index` counter for span derivation. Assert explicitly that a preceding `run_tool` leaves the next `act(...)`'s `intent_id` byte-identical to the pre-change value (design D8) — this is the regression that would silently break in-flight continuations.

## 6. `trace_id` on emitted intents

- [x] 6.1 Write the propagation tests first: a staged intent's `trace_id` equals the activation's `ACTIVATION_START` `trace_id`; `intent_id` is unchanged by the new field; a replayed activation re-stages a byte-identical `ToolIntent` including `trace_id`.
- [x] 6.2 Populate `ToolIntent.trace_id` in both contexts' `_stage_intent` from the context's `ActivationTrace`.
- [x] 6.3 Extend the retry-determinism semantics gate (`tests/semantics/`) so its byte-identical-intent assertion now covers the populated `trace_id`, and confirm it still passes with zero extra FakeLLM calls.
- [x] 6.4 Confirm the effector path is unaffected: dedup keys on `intent_id` only, and `WriteIntents` serialization round-trips the widened message (`tests/actions/`, `tests/effector/`).

## 7. Failure-route ERROR events

- [x] 7.1 Write fake-handle unit tests first (in the mutmut selection, alongside `tests/core/test_dofn_hitl_timer.py` / `test_dofn_ttl.py` / `test_dofn_admission.py`) for all five reasons: `activation_timeout`, `activation_error` (with `error.type`), `orphaned_result`, `hitl_timeout`, `ttl_wiped_suspension` — each asserting the `.traces` `ERROR` event, the unchanged `.errors` record, and byte-for-byte unchanged state.
- [x] 7.2 Add a trace-event helper to `core/dofn.py` that synthesizes an `ERROR` event from `(key, seq, now_ms, reason, error_type)` and yield it alongside each `_error(...)` on the two activation routes and the resume-admission route.
- [x] 7.3 Emit the `ERROR` event from `on_hitl` (deny/drop) and `on_ttl` (live continuation), using the continuation's `seq` for the trace scope and the `timer` span role.
- [x] 7.4 Emit `INTENT_EMITTED` from `_escalate` for the approval intent it mints, in the suspended activation's trace, and populate that intent's `trace_id`.
- [x] 7.5 Add a test asserting the staged traces of a *failed* activation are not emitted — only the synthesized `ERROR` event — so correctness invariant 1 stays visibly intact.

## 8. Delivering `.traces` to a sink

- [x] 8.1 Write the sink tests first (`tests/core/test_transform.py`): `traces_to` on `kafka://`/`pubsub://` yields `(entity_key, deterministic bytes)` pairs; on `bigquery://` yields row dicts with hex ids, the event-type name, and key/value attributes; an unset `traces_to` leaves `.traces` a raw `TraceEvent` `PCollection`.
- [x] 8.2 Add `observability/exporters.py` with `serialize_trace_event` and `trace_event_to_row`, using `SerializeToString(deterministic=True)` so map-field ordering is stable.
- [x] 8.3 Special-case `traces_to` in `DefaultSinkResolver.resolve` to prepend the serialization step, mirroring the existing `intents_to` special case; leave `validate` unchanged.
- [x] 8.4 Add a `TestStream` end-to-end assertion that `.traces` carries the activation start/end plus one child event per model call and staged intent, all sharing one `trace_id`.

## 9. Docs and gates

- [x] 9.1 Update `openspec/project.md`'s module map line for `observability/` if the shipped module names differ from what it describes, and document the attribute vocabulary where the capability's reader will look for it.
- [x] 9.2 `make lint`, `make type` clean (`mypy --strict` on the new package).
- [x] 9.3 Full unit tier passes offline with no docker; the offline semantics gates (`semantics and not integration`) still pass.
- [x] 9.4 `make coverage-ratchet` at or above baseline; raise `coverage-baseline.toml` if it improves.
- [x] 9.5 `make mutation` passes; re-check `mutation-baseline.toml`'s `dofn.py` and `context.py` ceilings and document any move in the file's comment. (The fake-handle failure-route tests reach `process()` from inside the mutation selection, which moved 264 `dofn.py` mutants out of the "no tests" bucket; `tests/core/test_dofn_commit.py` was added to kill them. Ceilings: `dofn.py` 267 -> 3, `transform.py` 198 -> 240.)
- [x] 9.6 `uv run pre-commit run --all-files` clean.

## 10. Zero-width spans get their latency counterpart (resolution of D7's open question)

- [x] 10.1 Originally: ship a small activation-latency metrics module in this change. Superseded mid-implementation when `add-runtime-metrics` (C18) merged from `main` with the full capability — `activation_ms`, `overhead_ms`, `llm_ms`, per-outcome counters, tally-at-commit; this change's duplicate module, tests, and DoFn seam were dropped in its favor during the merge.
- [x] 10.2 Reconcile the two capabilities where they touch: `ActivationContext.run_tool` (theirs) now stages this change's `TOOL_CALL` trace event on its own `tool_index` counter; the failure routes emit this change's `ERROR` traces through their `_dead_letter` counting chokepoint; the escalation intent is counted (`intents_emitted`) and traced (`INTENT_EMITTED`) at the same site.
- [x] 10.3 Re-run every gate on the merged tree and re-measure both baselines (per the merge-resolution convention documented in each file).
