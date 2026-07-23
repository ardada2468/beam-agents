## 1. Typed Runtime Errors

- [x] 1.1 Add failing wire-schema, import, deterministic-coder, and golden-compat tests for the additive `RuntimeError` message and all declared error types.
- [x] 1.2 Add `RuntimeError` to `protos/beam_agents.proto`, regenerate committed bindings, export the class, register its deterministic coder, and update golden fixtures until the new tests pass.

## 2. Activation Staging

- [ ] 2.1 Add failing unit tests for activation-scoped loading and staging of memory, replay cache, continuation, pending intents, sequence deltas, timers, tagged outputs, traces, and usage.
- [ ] 2.2 Implement the typed internal activation context and driver protocol with frozen activation time, new-event sequence allocation, continuation sequence reuse, state-size validation, and no Beam state handles.
- [ ] 2.3 Add failing unit tests for deterministic commit preparation, including stable pending-intent replacement order and preparation of every state/timer operation before output iteration.
- [ ] 2.4 Implement commit preparation and application in the specified `MEMORY`, `LLM_CACHE`, `CONTINUATION`, `PENDING`, `SEQ`, timers, then outputs order.

## 3. Async Bridge Lifecycle

- [ ] 3.1 Add failing tests proving one instance-owned asyncio loop is reused, loop-owned resources initialize and close on that loop, and teardown cancels outstanding work and joins the thread.
- [ ] 3.2 Implement the `_AgentDoFn` setup/teardown bridge thread and typed submission interface using `asyncio.run_coroutine_threadsafe`.
- [ ] 3.3 Add failing tests for activation timeout, cancellation/completion races, bounded cancellation observation, typed timeout output, and complete staged-effect discard.
- [ ] 3.4 Implement activation timeout waiting and fail-closed future cancellation without exposing Beam state handles to bridge tasks.

## 4. Keyed State and Element Routing

- [ ] 4.1 Add failing tests that inspect the exact `MEMORY`, `CONTINUATION`, `LLM_CACHE`, `PENDING`, and `SEQ` state specs and verify deterministic non-pickle coders.
- [ ] 4.2 Implement the five `_AgentDoFn` state declarations with protobuf coders, deterministic integer sum state, and typed Beam state parameters.
- [ ] 4.3 Add failing routing tests for external events, correlated tool results, approvals, mismatched KV/entity keys, absent payloads, busy keys, unknown results, and late results.
- [ ] 4.4 Implement discriminator-based routing, key validation, pending-intent correlation, continuation resume, busy-key rejection, and typed orphan/invalid errors.
- [ ] 4.5 Add failing stateful `TestPipeline` tests proving per-key isolation, monotonic new-event sequences, continuation sequence reuse, successful suspension, and deterministic pending-intent replacement.
- [ ] 4.6 Wire routing through activation execution and atomic commit until the stateful pipeline scenarios pass.

## 5. Timers

- [ ] 5.1 Add failing `TestStream` watermark tests for TTL scheduling, expiry cleanup without sequence changes, and preservation of the prior deadline after failed or timed-out activation.
- [ ] 5.2 Implement `TTL_TIMER` as a watermark timer derived only from successfully committed working-memory state.
- [ ] 5.3 Add failing deadline-decision and runner tests for earliest HITL scheduling, advancing or clearing after resolution, timeout fallback, fallback failure atomicity, and orphaning late results without `sleep()`.
- [ ] 5.4 Implement `HITL_TIMER` as a real-time timer that executes fallback through the same activation staging and commit path.

## 6. Correctness Gates

- [ ] 6.1 Add failure-injection tests proving agent exceptions, commit-validation failures, and timed-out activations preserve pre-activation state and discard staged intents, outputs, traces, usage, and cache inserts.
- [ ] 6.2 Add a semantics test that forces replay of a committed activation and asserts zero additional `FakeLLM` calls plus byte-identical deterministic intents.
- [ ] 6.3 Run the targeted core, memory, model replay-cache, schema-compat, and semantics tests; then run existing Ruff and strict MyPy checks for all touched modules and resolve every regression.
