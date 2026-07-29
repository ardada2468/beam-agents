# Proposal: add-intent-info-for-tools

## Why

The effectively-once e2e gate (change `add-effectively-once-e2e-gate`, design finding F13) empirically demonstrated the inherent crash-window duplicate: a SIGKILL landing between a tool's side effect and the effector's `dedup.complete` leaves the intent claimed, the lease expires, and another worker re-executes the tool. For non-idempotent tools this is unavoidable — exactly-once *effects* over non-transactional downstreams cannot survive a crash inside the effect-to-durable-record window. But the runtime already mints the perfect idempotency key: the deterministic `intent_id` (uuid5 over `key + seq + step_index`), byte-identical across pipeline replays and effector redeliveries. Today a tool cannot see it — tools receive only their parsed arguments, and an agent cannot name `intent_id` at `ctx.act(...)` time because the context computes it after staging. `docs/effector.md` already calls passing it through "a natural follow-up"; this change is that follow-up.

## What Changes

- New frozen dataclass `IntentInfo` (`intent_id`, `entity_key`, `seq`, `step_index`, `attempt`) in `src/beam_agents/tools/`, exported from `beam_agents.tools`.
- The `@tool` registry inspects the wrapped callable's signature at registration: a tool declaring a keyword-only `intent: IntentInfo` parameter is marked as accepting intent identity, and that parameter is excluded from the Pydantic argument model and the provider-facing JSON schema (it is runtime-injected, never an LLM-visible argument). Tools that don't declare it are registered and called exactly as today — zero breakage.
- The effector's execute path (`execute_intent` / `EffectorToolRunner` in `src/beam_agents/effector/runner.py`) builds an `IntentInfo` from the `ToolIntent` and injects it as `intent=` when (and only when) the tool declares the parameter.
- Docs (`docs/effector.md`): replace the "derive your key from the arguments" workaround with the honest exactly-once contract — the runtime guarantees deterministic intent IDs plus at-most-one *completed* execution per `intent_id`; tools that key their downstream effect on `intent_id` (Stripe `Idempotency-Key`, Redis `SETNX`, keyed upsert) get true exactly-once effects; everything else is at-least-once across crash recovery.
- Follow-up wiring in the e2e gate (`tests/semantics/_e2e/agent.py`): the gate's `charge` tool becomes idempotent via first-writer-wins keyed on `intent_id`, with a separate always-increment attempt counter, and the gate asserts effective executions exactly 1 per intent while attempts stay within the crash-window bound — the strong-form assertion F13 deferred to this change.
- No wire-schema changes: `ToolIntent` already carries `intent_id`, `entity_key`, `seq`, `step_index`, and `attempt`.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `tool-registry`: registration recognizes an opt-in keyword-only `intent: IntentInfo` parameter — excluded from the argument model/schema, validated at registration (definition errors for malformed declarations), invisible to tools that don't opt in.
- `effector-execution`: the runner injects `IntentInfo` built from the executing `ToolIntent` into tools that declare it; tools that don't are invoked unchanged. (Delta against the `effector-execution` spec introduced by in-flight change `add-reference-effector`.)
- `effectively-once-e2e-gate`: the gate's charge tool becomes intent-keyed idempotent and the gate asserts the strong form — exactly one effective execution per `intent_id` even under induced kills, with raw attempts bounded by the crash-window formula. (Delta against the spec introduced by in-flight change `add-effectively-once-e2e-gate`.)

## Impact

- `src/beam_agents/tools/registry.py` (signature inspection, `IntentInfo`, schema exclusion), `src/beam_agents/tools/__init__.py` (export).
- `src/beam_agents/effector/runner.py` (`EffectorToolRunner.run` / `execute_intent` injection). The in-pipeline `ToolRunner` is untouched — read-only tools have no intent identity.
- `tests/semantics/_e2e/agent.py`, `tests/semantics/_e2e/ledger.py`, `tests/semantics/_e2e/assertions.py`, `tests/semantics/test_effectively_once_e2e.py` (idempotent charge tool + strong assertion).
- `docs/effector.md` (contract documentation).
- Depends on in-flight changes `add-reference-effector` (effector runner) and `add-effectively-once-e2e-gate` (the gate being wired); implementation of the gate tasks must land after that change's harness. No protobuf, coder, or state-schema changes; no public pipeline API changes beyond the additive `IntentInfo` export.
