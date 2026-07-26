## 1. Wire schemas

- [x] 1.1 Add `enum Kind { TOOL_KIND_UNSPECIFIED = 0; TOOL = 1; APPROVAL = 2; }` and `Kind kind = 10;` to `ToolIntent`, and `uint32 escalations = 9;` to `Continuation` in `protos/beam_agents.proto`, with comments stating both are additive under `state_schema_version = 1`.
- [x] 1.2 Run `make proto`; confirm the regenerated `_pb2.py`/`_pb2.pyi` diff contains only the additive change and that CI's diff-clean regen check passes.
- [x] 1.3 Write the wire-schema tests first (from the `wire-schemas` delta scenarios): full round-trip including `kind`; an `APPROVAL` intent is distinguishable without reading `tool_name`; a blob serialized without `kind` parses as `TOOL_KIND_UNSPECIFIED`; `escalations` round-trips and defaults to `0`.
- [x] 1.4 Extend `tests/core/test_schema_compat.py` and the `tests/core/golden/` blobs: an existing golden `ToolIntent`/`Continuation` blob still parses with the new fields at their defaults, and add golden blobs carrying the new fields.

## 2. `hitl.py`: policy, routes, and the effector guard

- [x] 2.1 Write the guard tests first (from "The effector guard refuses expired intents"): expired → reported expired plus a `ToolResult(status=EXPIRED)` correlated by `intent_id`/`entity_key`/`seq`; unexpired → live, no refusal result; `expires_at_ms <= 0` → expired.
- [x] 2.2 Create `src/beam_agents/hitl.py` with `intent_expired(intent, now_ms)` and `refuse_expired(intent, now_ms) -> ToolResult | None`. No I/O, no clock read, no Beam import — assert the no-Beam-import property in a test.
- [x] 2.3 Add the route types `Deny(output: bytes)`, `Drop(reason: str = "hitl_timeout")`, `Escalate(tool_name, args_json, timeout_ms)` and the `Route` union, all frozen dataclasses.
- [x] 2.4 Add the frozen `HitlPolicy` (`timeout_ms`, `intent_ttl_ms`, `approval_channel`, `max_escalations`, `on_timeout`) with a module-level default `deny` function returning `Deny(HITL_TIMEOUT_OUTPUT)`; document the purity contract (pure, synchronous, no clock, no model call) in the docstring.
- [x] 2.5 Test that a default-constructed `HitlPolicy` and its default `on_timeout` pickle cleanly (DirectRunner requirement).

## 3. Approval intents on both context surfaces

- [x] 3.1 Write the tests first (from "An activation can request a human approval" and "Every staged intent carries a positive expiry"): `request_approval` stages one `kind = APPROVAL` intent with the configured channel, canonical JSON, and a positive expiry; the same activation replayed re-mints byte-identical intents; no tool is looked up or executed; `AgentContext.act` stamps `expires_at_ms = now + intent_ttl_ms`.
- [x] 3.2 Add `request_approval(...)` to `ActivationContext`, sharing `_advance_step` and `intent_id_for` with `act`, setting `kind = APPROVAL` and a positive `expires_at_ms`.
- [x] 3.3 Add `request_approval(...)` to `AgentContext`, bypassing the tool registry (the approval channel is not a registered tool) but using the same canonical-JSON encoding and ID derivation.
- [x] 3.4 Set `kind = TOOL` explicitly on both `act` implementations, and make `AgentContext.act` stamp `expires_at_ms` from the configured intent TTL.
- [x] 3.5 Assert both surfaces mint identical intents for the same `(entity_key, seq, step_index)` and the same arguments.

## 4. Step-index continuity across suspend/resume

- [x] 4.1 Write the tests first (from "A resumed activation continues its suspended activation's step index"): an intent staged pre-suspend and one staged on resume within the same `seq` carry different `intent_id`s; an escalation intent collides with neither.
- [x] 4.2 Add a step-index seed to `ActivationContext` (defaulting to `0`) and have the DoFn's resume path seed it from `Continuation.step_index`.
- [x] 4.3 Re-check `Continuation.step_index` is written as the post-activation step count (so seeding is monotonic), and update any existing test asserting a literal resumed-activation `intent_id`.

## 5. Deadline reconciliation in the loop driver

- [x] 5.1 Write the tests first (from "A suspension's deadline is the earliest of its timeout and its intents' expiries"): a 60s intent expiry against a 24h timeout yields the 60s deadline; a suspension staging no intents uses its timeout; the existing `default_hitl_timeout_ms` behavior is preserved when it is the minimum.
- [x] 5.2 Change `run_activation` to compute `deadline_ms = min(now_ms + timeout, min(expires_at_ms of staged intents))`, and thread `HitlPolicy.timeout_ms` in as the default timeout.

## 6. Fail-closed resume admission (layer 1)

- [x] 6.1 Write the tests first (from "A resume is admitted only against a live, unexpired continuation"): timer-first then late approval → fallback once and the approval `orphaned_result`; an approval at/after the deadline with the timer not yet fired → `orphaned_result`, agent not resumed; a result whose `PENDING` intent expired → `orphaned_result`; an in-time approval resumes, commits, and clears `HITL_TIMER` so a later real-time advance past the old deadline emits no fallback; a re-injected `ToolResult(EXPIRED)` against a live continuation resumes normally.
- [x] 6.2 Extend `_resume` to read `PENDING` and apply the four admission conditions (continuation exists, `intent_id` matches, `now_ms < deadline_ms`, `now_ms < matching intent expires_at_ms`), treating non-positive deadlines/expiries as expired.
- [x] 6.3 Emit `orphaned_result` with a detail identifying the failed condition (`no_continuation`, `unknown_intent`, `deadline_passed`, `intent_expired`) and assert zero state mutation on every refusal path.

## 7. Timer dispatch, stale-handle guard, and routing

- [x] 7.1 Write the tests first (from "The HITL timer dispatches a pure policy fallback", "A stale HITL timer handle mutates nothing", and "HitlPolicy routes a timeout to deny, drop, or escalate"): policy receives `kind == "timer"` plus the expired handle; `SEQ` unchanged; a raising policy routes `hitl_timeout` to `.errors` without failing the bundle; a fire earlier than the live deadline is a no-op; a fire with no continuation is a no-op; a fire exactly at the deadline runs; `Deny`/`Drop`/`Escalate` each produce their documented effects; escalation is bounded; the default policy reproduces today's `HITL_TIMEOUT_OUTPUT` behavior.
- [x] 7.2 Extend `FallbackContext` in `core/agent.py` with `kind`, `deadline_ms`, `fired_at_ms`, and `pending_intent_ids`.
- [x] 7.3 Rewrite `on_hitl` to take `beam.DoFn.TimestampParam` (the fire timestamp) and `beam.DoFn.TimerParam(HITL_TIMER)`, apply the stale-handle guard (`continuation exists and fired_at_ms >= cont.deadline_ms`), and dispatch the policy.
- [x] 7.4 Implement the three route applications: `Deny` (bytes on main, clear state), `Drop` (typed record on `.errors`, clear state), `Escalate` (deterministic `APPROVAL` intent on `.intents`, rewritten continuation with later deadline and incremented `escalations`, re-armed timer) — none of them touching `SEQ`.
- [x] 7.5 Coerce `Escalate` to `Deny` once `escalations` has reached `max_escalations`, and wrap the policy call so a raise becomes a `Drop` to `.errors`.
- [x] 7.6 Add `REASON_HITL_TIMEOUT = "hitl_timeout"` alongside the existing reason constants.

## 8. Configuration and public API

- [x] 8.1 Write the tests first (from "HitlPolicy is validated at pipeline-construction time"): non-positive timeout/intent TTL, negative `max_escalations`, and empty approval channel each raise `ValueError` naming the field at `AgentConfig` construction.
- [x] 8.2 Add `AgentConfig.hitl_policy: HitlPolicy` (kw-only, defaulted) with `__post_init__` validation next to the existing knobs, and pass it through `RunAgent` into `_AgentDoFn`.
- [x] 8.3 Thread the policy to its three consumers: `run_activation`'s default timeout, both contexts' intent TTL, and `on_hitl`'s routing function.
- [x] 8.4 Re-export `HitlPolicy`, `Deny`, `Drop`, `Escalate`, and `FallbackContext` from `beam_agents/__init__.py`; confirm `HITL_TIMEOUT_OUTPUT` keeps its current value.
- [x] 8.5 Update `testing/chaos.py` so its `_commit` monkeypatch mirrors the real signature after any change to it. (No change needed: `_commit`'s signature is untouched by this change; verified by running the offline semantics gate, which drives the chaos wrapper.)

## 9. Semantics gate and CI

- [x] 9.1 Add an offline (`semantics and not integration`) gate: under a chaos-forced bundle retry around the HITL path, the fallback's effects are produced exactly once, an escalation re-mints a byte-identical intent, and no additional provider calls occur.
- [x] 9.2 Confirm every new streaming scenario uses `TestStream` processing-time/watermark advances and no `sleep()`, and that the whole unit tier passes offline with no docker.
- [x] 9.3 Run `ruff` (incl. ASYNC), `mypy --strict`, and the unit tier; fix any `Any` leaking into the new public signatures.
- [x] 9.4 Run `make mutation` for the touched `core/` files; re-check `mutation-baseline.toml` and only adjust a ceiling with the surviving mutants named and justified. (Gate passes: 621 killed, 7 equivalent survivors each named with a reason in `mutation-exclusions.toml`. `dofn.py`'s no-tests ceiling *dropped* 263 -> 261 -- the admission predicate and timer callback are pure functions tested inside the mutation selection; `transform.py` rose 196 -> 198 for `AgentConfig.hitl_policy` + its `validate()` call, which only the deselected pipeline suite reaches. Both movements are justified in the baseline file.)
- [x] 9.5 Verify the coverage ratchet does not regress and update `coverage-baseline.toml` if the tooling requires it. (Branch coverage rose 92.28% -> 92.70%; baseline raised to lock in the gain, as the ratchet instructs.)
