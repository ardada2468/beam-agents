# Design: add-intent-info-for-tools

## Context

The e2e gate (change `add-effectively-once-e2e-gate`, finding F13) proved empirically what the effector docs already state: a SIGKILL between a tool's side effect and `dedup.complete` re-executes the tool after lease expiry, and no amount of effector engineering can close that window for non-idempotent tools — exactly-once effects over a non-transactional downstream require an idempotency key at the downstream. The runtime already mints the ideal key: `intent_id = uuid5(NAMESPACE, key + seq + step_index)` (correctness invariant 2), byte-identical across pipeline replays, sink duplicates, and effector redeliveries. Tools just can't see it: `EffectorToolRunner` invokes the callable with only the validated `args_json`, and an agent cannot put `intent_id` into `args_json` because the context computes it after `ctx.act(...)` stages the call. `docs/effector.md` explicitly names pass-through "a natural follow-up".

Current state, load-bearing details:

- `Tool` (`tools/registry.py`) builds a Pydantic argument model from the full signature; every parameter becomes a schema field, and un-annotated or variadic parameters are `ToolDefinitionError`s.
- `EffectorToolRunner.run` (`effector/runner.py`) validates against that model, then `_invoke` calls `t.unwrap()(**validated.model_dump())`. `execute_intent` is the only caller and holds the `ToolIntent`.
- The in-pipeline `ToolRunner` (`tools/runner.py`) executes read-only tools inline; there is no intent, hence no identity, on that path.
- `ToolIntent` already carries `intent_id`, `entity_key`, `seq`, `step_index`, `attempt` (set to `0` at staging, transported verbatim) — no wire change needed.
- The gate's `charge` tool (`tests/semantics/_e2e/agent.py`) counts executions by entity key via an unconditional Redis `INCR`; the gate's strong-form assertion was deferred to this change (F13 decision, user-approved).

## Goals / Non-Goals

**Goals:**

- Let a side-effecting tool opt in to receiving the executing intent's identity, keyed for downstream idempotency, with zero change for tools that don't opt in.
- State the honest exactly-once contract in the docs, replacing the "derive a key from your arguments" workaround.
- Upgrade the e2e gate to assert true exactly-once effective executions for an intent-keyed idempotent tool.

**Non-Goals:**

- No wire or state schema changes; no `state_schema_version` bump.
- No effector-side idempotency machinery (the effector cannot make a foreign downstream idempotent; only the tool can, with the key we hand it).
- No injection for read-only tools or the in-pipeline `ToolRunner` — there is no intent on the fast path.
- No exposure of `intent_id` to the agent at `ctx.act(...)` time, and no agent-facing API change.
- Not populating/incrementing `ToolIntent.attempt` beyond today's behavior — `IntentInfo.attempt` mirrors the wire field verbatim (currently always `0`; it is not an effector claim counter).

## Decisions

### D1: `IntentInfo` is a frozen stdlib dataclass in `tools/`, not a proto or Pydantic model

New module `src/beam_agents/tools/intent_info.py` defining `@dataclass(frozen=True, slots=True) class IntentInfo` with `intent_id: str`, `entity_key: bytes`, `seq: int`, `step_index: int`, `attempt: int`; exported from `beam_agents.tools`.

- *Why not the `ToolIntent` proto itself:* tool authors would take a dependency on generated `_pb2` types for ergonomics, and the proto carries fields (args_json, expiry, kind) a tool has no business reading — the injection surface should be exactly identity, nothing more. Wire stays protobuf per project convention; this is an in-process view.
- *Why not Pydantic:* nothing is validated or deserialized — the effector constructs it from trusted wire fields. Frozen + slots gives immutability, hashability (usable directly as a dedup key in tests and tool code), and no model overhead.
- *Why in `tools/` not `effector/`:* the declaring side is the tool author, who imports `beam_agents.tools` and must not need the effector package (import-boundary: `tools/` imports neither Beam nor effector).

### D2: Recognition rule is name + kind + annotation, with fail-fast near-miss errors

At decoration time, `@tool` recognizes the parameter iff it is **keyword-only**, named **`intent`**, and annotated **`IntentInfo`**. Annotations are resolved with `typing.get_type_hints`-style evaluation so `from __future__ import annotations` (string annotations — used pervasively in this repo) works.

Near-misses fail fast with `ToolDefinitionError` rather than silently becoming schema arguments:

- `IntentInfo` annotation on a positional-or-keyword or misnamed parameter → error (the author clearly wanted injection; a silent `IntentInfo`-typed LLM argument is nonsense).
- `intent: IntentInfo` on a `side_effect=False` tool → error (read-only tools run inline; identity would never arrive, and a silently-absent value is exactly the failure mode this repo's fail-closed convention forbids).
- `intent` with any *other* annotation stays an ordinary argument — zero breakage for existing tools that happen to use the name.

*Alternative considered:* a decorator flag (`@tool(side_effect=True, inject_intent=True)`). Rejected: the signature already states the fact; a flag can disagree with the signature and needs its own consistency validation. Signature inspection is the registry's established idiom (D1/D2 of `add-tool-registry`).

### D3: Exclusion happens in the argument model, so validation and schema need no special cases

`_build_argument_model` skips the recognized parameter; `Tool` gains `accepts_intent: bool`. Everything downstream is automatically correct: the JSON schema (built from the model) never shows `intent`; validation of `args_json` neither requires nor accepts an `intent` key (Pydantic's default `extra="forbid"`-equivalent behavior for `create_model` — an `"intent"` key in `args_json` of a declaring tool fails validation → `REJECTED`, never shadowed); `model_dump()` cannot collide with the injected keyword.

### D4: Injection point is `EffectorToolRunner`, threaded from `execute_intent`

`EffectorToolRunner.run` gains a keyword-only `intent_info: IntentInfo | None = None`; `_invoke` adds `intent=intent_info` to the call kwargs iff `t.accepts_intent`. `execute_intent` constructs `IntentInfo` from the `ToolIntent`'s five wire fields and passes it down.

- *Why runner-level, not a wrapper around the callable:* the runner is the single sanctioned execution seam (`unwrap()` discipline); injecting there keeps the sync-via-`to_thread` and async paths uniform for free.
- `execute_intent` passes `intent_info` **only when `tool.accepts_intent`**, so for non-declaring tools the `runner.run(...)` call is byte-identical to the pre-IntentInfo call. This is load-bearing, not cosmetic: test doubles (and any external code) subclass `EffectorToolRunner` and override `run` with the historical signature; an unconditional new keyword would `TypeError` through them (found by `test_main.py` / `test_service.py` shutdown tests during implementation). Omitting it for a declaring tool is a programming error surfaced by the callable's missing-argument `TypeError` → mapped to `ERROR` by `execute_intent`'s existing total status mapping. No new statuses, no new failure modes.

### D5: Docs state the honest contract as a two-sided guarantee

`docs/effector.md`'s "What is guaranteed, and what is not" section gains the contract: the runtime guarantees (a) deterministic, replay-stable `intent_id`s and (b) at-most-one *completed* execution per `intent_id`; therefore a tool that keys its downstream effect on `intent.intent_id` (Stripe `Idempotency-Key`, Redis `SETNX`, keyed upsert, `INSERT … ON CONFLICT DO NOTHING`) gets **exactly-once effects**, and a tool that doesn't gets **at-least-once across crash recovery** (duplicates only within the crash window). The existing "derive the key from your arguments" example is replaced by the `intent: IntentInfo` form; the "natural follow-up" paragraph is deleted.

### D6: The gate's charge tool becomes the reference idempotent consumer

`tests/semantics/_e2e/agent.py`'s `charge` declares `*, intent: IntentInfo` and records through a two-counter ledger (`tests/semantics/_e2e/ledger.py`):

- **attempts**: unconditional `INCR` keyed by `intent_id` — preserves the gate's existing "count at the side effect itself" measurement and its crash-window bound.
- **effective**: first-writer-wins `SETNX` keyed by `intent_id` — the modeled downstream effect; the return payload embeds whether this invocation won, keeping the tool a pure function of (args, intent, ledger state).

Assertions (`assertions.py` / `test_effectively_once_e2e.py`) move to the strong form: effective executions exactly `1` per minted tool `intent_id`; attempts keep the existing bounds (`≥ 1`; per-member `≤ 1 + kills`; duplicated members `≤ kills × max_concurrent_partitions`; exactly `1` when kills = 0). Counting flips from entity-key-keyed to `intent_id`-keyed — better than the old construction-based equivalence argument, since the tool now sees the id directly; the per-key/per-intent cross-check via the intents topic stays.

## Risks / Trade-offs

- **[Delta specs target in-flight changes]** The `effector-execution` and `effectively-once-e2e-gate` deltas modify specs that live in unarchived changes (`add-reference-effector`, `add-effectively-once-e2e-gate`). → Archive ordering is a hard dependency: this change archives only after both; the gate tasks in this change cannot be implemented until the gate harness lands.
- **[String-annotation resolution can fail]** `get_type_hints` raises on unresolvable forward references in the tool's module. → Fall back to comparing the literal string `"IntentInfo"` when evaluation fails; test both paths. This repo's own modules always resolve (top-level import).
- **[A declaring tool becomes uncallable without an intent]** Unit tests of the raw callable must now construct an `IntentInfo`. → Frozen dataclass with five primitive fields; one-line construction. Documented in the docs example.
- **[Pydantic extra-key behavior]** The "intent key in args_json is rejected" scenario relies on the generated model rejecting unknown keys. → Assert this in a registry unit test rather than trusting Pydantic defaults; if `create_model` defaults to ignoring extras, set `model_config = ConfigDict(extra="forbid")` on the generated model *for the declaring-tool case only* is wrong (it would change behavior for all tools) — instead forbid extras for all generated models only if the existing spec already implies it; otherwise reject the `intent` key explicitly during validation for declaring tools. Resolve at implementation against the existing tool-registry tests.
- **[Zero-breakage claim]** Signature inspection touches every registration. → The near-miss error surface is deliberately narrow (only `IntentInfo`-annotated parameters and `intent: IntentInfo` on read-only tools can newly raise); the full existing tool-registry and effector test suites must pass unmodified.

## Open Questions

None blocking. The one implementation-time check is the Pydantic extra-key default noted above (existing `test_tool_registry` behavior decides the mechanism, not the requirement — the requirement stands: an `"intent"` key in a declaring tool's `args_json` is `REJECTED`).
