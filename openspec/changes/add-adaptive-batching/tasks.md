## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 `tests/core/test_batching.py`: `BatchPolicy` config surface — "NONE policy preserves existing semantics" (construction-level: defaults), "Misconfigured batch knobs fail at the construction site" (ADAPTIVE with non-positive `max_batch_size`/`max_wait_ms`/`max_buffered_events`, `max_buffered_events < max_batch_size`, NONE with an explicit knob), and the pure flush-decision helpers (threshold reached, deferral under a live continuation, overflow at the cap).
- [ ] 1.2 `tests/core/test_dofn_batching.py` (fake-handle unit tests over `tests/core/_dofn_fakes.py`, inside the mutation selection): "ADAPTIVE opt-in buffers instead of activating" (bag append, no `SEQ`, no emits, `events_buffered` counted); "Size-threshold flush runs one activation over the whole buffer" and "A size flush disarms the pending flush timer"; "A stale flush firing over an empty buffer is a no-op"; "The size trigger defers during a suspension" and "A timer firing during a suspension does not overwrite the continuation"; "A failed flush dead-letters every batched event and consumes the buffer"; "Overflow during deferral is explicit"; "Wiped buffered events are reported" (`ttl_wiped_batch` before the six-spec wipe).
- [ ] 1.3 `tests/core/test_context.py` + `tests/core/test_loop.py`: "The agent receives the batch as a list in arrival order", "A single-event flush is still a list", `ctx.is_batch`/`ctx.events` shapes under NONE / ADAPTIVE / resume, and "The batch clock is the latest buffered event time" (`now_ms = max(event_time_ms)` driving intent `expires_at_ms` and suspension deadlines).
- [ ] 1.4 `tests/core/test_dofn_pipeline.py` (`TestStream`, DirectRunner, no docker, no `sleep()`): "An undersized buffer flushes when max_wait elapses" and "The wait is measured from the first buffered event" via scripted `advance_processing_time`; "NONE policy preserves existing semantics" end-to-end (existing per-event pipeline assertions unchanged with the new topology declared); "A batch of N events consumes one seq"; "Batch metrics reconcile with batch behavior" and "A batch flush is one activation" via `result.metrics()`.
- [ ] 1.5 Suspension-interaction pipeline tests: "A batch suspension persists one continuation for the whole batch", "The batch resumes together", "HITL timeout fails the whole batch closed", and "Resolution flushes the deferred buffer promptly" (resume commits → re-armed `FLUSH_TIMER` flushes the deferred batch with its own `SEQ` increment).
- [ ] 1.6 Extend the retry-determinism semantics gate (`tests/semantics/test_retry_determinism.py`): "A retried flush bundle replays deterministically" — chaos-forced retry of a bundle containing a flush activation yields byte-identical `intent_id`s and zero additional FakeLLM calls.

## 2. BatchPolicy and configuration surface

- [ ] 2.1 Create `src/beam_agents/core/batching.py`: the `BatchPolicy` enum (`NONE`, `ADAPTIVE`), defaults, and pure flush-decision helpers (size trigger, deferral predicate, overflow predicate) with no Beam imports; import-side-effect-free.
- [ ] 2.2 `core/transform.py`: add `AgentConfig.batch_policy` (default `BatchPolicy.NONE`), `max_batch_size`, `max_wait_ms`, `max_buffered_events` (default `4 * max_batch_size`); validate in `__post_init__` per the spec; forward the knobs from `RunAgent.expand` into `_AgentDoFn`.

## 3. Topology — sixth state spec and third timer

- [ ] 3.1 `core/dofn.py`: declare `BATCH = BagStateSpec("batch", DeterministicProtoCoder(AgentEnvelope))` and `FLUSH_TIMER = TimerSpec("flush", TimeDomain.REAL_TIME)` alongside the existing five specs and two timers; no pickle fallback.
- [ ] 3.2 Add the injected wall-clock seam (`time_fn`, default `time.time`) used only for `FLUSH_TIMER` arming; document that the reading never influences staged effects, intent IDs, cache keys, or outputs.

## 4. Buffering and routing

- [ ] 4.1 `process()`: under `ADAPTIVE`, route `event` variants to the buffering branch — append to `BATCH`, re-arm `TTL_TIMER`, arm `FLUSH_TIMER` at `time_fn() + max_wait_ms` only on the empty→non-empty transition; no activation, no `SEQ`, no emits. `tool_result`/`approval` keep the existing resume path under both policies.
- [ ] 4.2 Enforce `max_buffered_events`: at the cap, dead-letter the incoming event through `_dead_letter` with reason `batch_buffer_overflow` instead of appending.

## 5. Flush activation

- [ ] 5.1 Inline size flush: on reaching `max_batch_size` with no live continuation, read the bag in arrival order, run one activation over the batch, clear `BATCH` and `FLUSH_TIMER` atomically with the commit tail.
- [ ] 5.2 `@on_timer(FLUSH_TIMER)` callback: guard the empty-buffer no-op and the live-continuation deferral; otherwise flush identically to 5.1.
- [ ] 5.3 `core/context.py` / `core/loop.py`: widen `ActivationContext.event` to `bytes | list[bytes]`, add `ctx.is_batch` and the uniform `ctx.events` tuple accessor, accept the batch entry in `run_activation`, and compute the batch clock `now_ms = max(event_time_ms)` in the DoFn flush path.
- [ ] 5.4 Batch failure route: on `ActivationTimeout`/`ActivationFailed`/other exception from a flush, emit one `ActivationError` per buffered envelope (detail carries batch size and trigger), clear `BATCH` and `FLUSH_TIMER`, leave all other specs and `SEQ` untouched.
- [ ] 5.5 Stamp `beam_agents.batch.size` and `beam_agents.batch.trigger` attributes on the flush activation's trace.

## 6. Suspension interaction (whole-batch)

- [ ] 6.1 Suspending flush commits one `Continuation` at the batch's `seq` through the existing commit tail (no `Continuation` proto change; `ctx.events` empty on resume, snapshot owns resume state).
- [ ] 6.2 Deferral: suppress both flush triggers while `CONTINUATION` is live (buffer keeps absorbing events past `max_batch_size`).
- [ ] 6.3 Resolution re-arm: after a resume activation commits, and on `on_hitl`'s `Deny`/`Drop` routes, re-arm `FLUSH_TIMER` to fire promptly when `BATCH` is non-empty; `Escalate` keeps deferring.

## 7. TTL GC and metrics

- [ ] 7.1 `on_ttl`: dead-letter each wiped buffered envelope as `ttl_wiped_batch`, then clear all six specs and `FLUSH_TIMER`; buffered elements re-arm `TTL_TIMER` like any processed element.
- [ ] 7.2 `observability/metrics.py`: add `events_buffered`, `batch_flushes_size`, `batch_flushes_timer` counters and the `batch_size` distribution; record buffering counts on the Beam thread at the buffering commit and flush counts/samples in `_record_commit`; update `testing/chaos.py` if the `_commit` signature it mirrors moves.

## 8. Documentation

- [ ] 8.1 Document adaptive batching in `docs/` (alongside `metrics.md`/`traces.md`): the policy knobs, the list-typed `ctx.event` contract, whole-batch suspension, the deferral/overflow/TTL rules, the new metric names, and the watermark/lateness caveat for timer-flushed outputs.

## 9. Gates

- [ ] 9.1 `make lint` and `make type` clean (`mypy --strict`; the `bytes | list[bytes]` union and new public knobs fully typed, no `Any`).
- [ ] 9.2 `make test-unit` passes offline with no docker.
- [ ] 9.3 `make test-semantics-offline` passes, including the extended retry-determinism assertion and the unchanged conformance DirectRunner leg under `BatchPolicy.NONE`.
- [ ] 9.4 `make coverage-ratchet` at or above baseline.
- [ ] 9.5 `make mutation` passes (core/ touched: `dofn.py`, `context.py`, `loop.py`, `transform.py`, new `batching.py`); document any `mutation-baseline.toml` ceiling moves in the file's comment, per precedent.
- [ ] 9.6 `uv run pre-commit run --all-files` clean.
- [ ] 9.7 `openspec validate add-adaptive-batching --strict` passes.
