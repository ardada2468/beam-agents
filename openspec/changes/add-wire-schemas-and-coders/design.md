## Context

The repo has scaffolding (uv, ruff, mypy strict, pytest tiers, proto drift hook) but zero runtime code: `protos/` contains only `.gitkeep` and `src/beam_agents/__init__.py` is empty. Every planned capability serializes through six message types — `MemoryBlob`, `ToolIntent`, `ToolResult`, `TraceEvent`, `AgentEnvelope`, `Continuation` — and the project's correctness invariants put hard constraints on how they are defined and encoded:

- State is protobuf, never pickle (invariant 7); pipeline `--update` compatibility demands additive-only schema evolution guarded by golden-blob tests.
- Deterministic intent IDs and the replay cache require that a replayed bundle re-encodes byte-identical payloads — so coders must be deterministic, not merely correct.
- Beam Python has no MapState: bounded maps live *inside* single-value proto blobs with explicit LRU eviction, which shapes `MemoryBlob`'s layout.
- Blobs ≤ 100 KiB; the schema must make size accounting cheap.
- Wire schemas must stay language-neutral (the effector is a separate service that may not be Python forever).

Existing tooling this change must fit: `scripts/gen_proto.sh` (grpcio-tools, globs `protos/*.proto`), the pre-commit regen-drift hook, and CI's diff-clean check.

## Goals / Non-Goals

**Goals:**

- One `.proto` source of truth for all six messages, with generated Python bindings importable from `beam_agents` code and committed diff-clean.
- Deterministic Beam coders for all six types: byte-identical re-encoding within a pipeline run, registered so no element or state value ever falls back to `PickleCoder`.
- Golden-blob fixtures + compat tests that establish the baseline for the additive-only evolution rule.
- Field layouts that already anticipate their consumers (DoFn state specs, effector dedup, OTel export, HITL) so the next changes don't need breaking edits.

**Non-Goals:**

- No DoFn, state specs, transform, context, or loop driver — schemas and coders only.
- No LRU eviction *logic*, size-cap *enforcement*, or compaction — `MemoryBlob` carries the metadata those need; the behavior lands with `core/dofn.py`.
- No effector implementation, no outbox sink, no OTLP exporter — only the message contracts they will consume.
- No cross-language codegen (Java/Go) — the `.proto` is language-neutral but only Python bindings are generated in v0.x.
- No `beam_agents/__init__.py` re-exports — everything here is private (`core/` internal).

## Decisions

### D1: Single proto file `protos/beam_agents.proto`, package `beam_agents.v1`

**Choice:** one file, `syntax = "proto3"`, `package beam_agents.v1`. All six top-level messages plus their nested enums/messages live in it.

**Alternatives considered:**
- One file per message: cleaner diffs, but the messages cross-reference (`AgentEnvelope` embeds `ToolResult`; `Continuation` references pending intents) and generated cross-file imports in Python protobuf are a well-known packaging headache.
- Versioned directory tree `protos/beam_agents/v1/…`: the conventional Buf layout, but `gen_proto.sh` globs `protos/*.proto` flat and the extra nesting buys nothing for a single file.

**Rationale:** the six messages form one cohesive wire contract that evolves together under one `state_schema_version` regime. The `v1` in the proto *package* (not the path) is what actually protects cross-language consumers.

### D2: Generated code lands in `src/beam_agents/_protos/`, not `protos/`

**Choice:** update `scripts/gen_proto.sh` to emit `--python_out`/`--pyi_out` into `src/beam_agents/_protos/`, a private package with an `__init__.py` that re-exports the message classes. `protos/` holds only `.proto` sources.

**Alternatives considered:**
- Keep emitting into `protos/` (current script behavior): the generated module would sit outside the installed package; importing it from `src/beam_agents` would require path manipulation or a build-time copy step — both fragile under `uv` builds and in Dataflow-submitted pipelines.
- Generate at build time (setuptools/hatch hook), don't commit: violates the project rule that generated `_pb2.py` is committed and regen must be diff-clean in CI.

**Rationale:** the package must be importable identically in local tests, CI, and staged Dataflow workers. Committing generated code inside the package is the only option that satisfies both "committed + diff-clean" and "importable everywhere". The underscore prefix marks it private per project conventions. The drift hook keeps working — it regenerates and diffs whatever paths the script writes.

### D3: Deterministic serialization via a custom `DeterministicProtoCoder`

**Choice:** `core/coders.py` defines one coder class parameterized by message type, calling `msg.SerializeToString(deterministic=True)` in `encode()` and advertising `is_deterministic() == True`. Beam's stock `ProtoCoder` is not used.

**Alternatives considered:**
- `beam.coders.ProtoCoder`: uses plain `SerializeToString()`, which does not guarantee map-field ordering, and reports itself non-deterministic — Beam would then reject these types as GBK keys and, worse, silently permit byte drift across bundle retries, breaking the replay-cache and intent-ID byte-identity argument.
- Canonical hand-rolled encoding (sorted JSON, custom TLV): maximal control, but reimplements protobuf badly and forfeits the language-neutral wire format.

**Rationale:** `deterministic=True` sorts map entries and gives repeatable output for a fixed protobuf library version. That is exactly the scope we need — byte-identity *within* a pipeline run and across bundle retries on the same workers. Cross-version drift is a real (documented) limitation handled by D6's golden-blob policy and by pinning `protobuf` in the lockfile.

### D4: Field-layout decisions for the six messages

**Choice (summary of the load-bearing points):**

- **Timestamps are `int64` unix epoch milliseconds** (`*_ms` suffix) everywhere — matches Beam's event-time representation and avoids well-known-type imports. `google.protobuf.Timestamp` was rejected as needless conversion overhead at every Beam boundary.
- **`MemoryBlob`**: `state_schema_version` (uint32), repeated `MemoryEntry { string key; bytes value; int64 last_access_ms; }`. Repeated-with-explicit-order rather than a proto `map<>` so LRU order is representable and encoding order is under our control, not the map serializer's. A `total_value_bytes` field caches size for cheap 100 KiB cap checks without a full re-serialize.
- **`ToolIntent`**: `intent_id` (string, uuid5 — computed by the caller, never by the coder), `entity_key` (bytes — keys are opaque), `seq` (int64), `step_index` (uint32), `tool_name`, `args_json` (string, canonical JSON — see below), `expires_at_ms`, `created_at_ms`, `attempt` (uint32). Args are canonical JSON rather than `google.protobuf.Struct` because the intent-ID determinism rule is already defined over canonical JSON, and `Struct` mangles ints into doubles.
- **`ToolResult`**: `intent_id`, `entity_key`, `seq`, `status` enum (`STATUS_UNSPECIFIED / OK / ERROR / EXPIRED / REJECTED`), `payload` (bytes), `error_message`, `completed_at_ms`. `EXPIRED` and `REJECTED` exist now so the HITL fail-closed path (invariant 6) needs no schema change.
- **`TraceEvent`**: `trace_id`/`span_id`/`parent_span_id` (bytes, OTel widths), `entity_key`, `seq`, `step_index`, `event_type` enum (`ACTIVATION_START/LLM_CALL/TOOL_CALL/INTENT_EMITTED/ACTIVATION_END/ERROR`), `attributes` map<string,string> keyed by OTel GenAI semantic-convention names (`gen_ai.request.model`, `gen_ai.usage.input_tokens`, …), `start_ms`/`end_ms`. The map is safe here because D3 sorts it deterministically.
- **`AgentEnvelope`**: `entity_key`, `event_time_ms`, `oneof payload { bytes external_event = …; ToolResult tool_result = …; Approval approval = …; }` where `Approval` is a nested message (`intent_id`, `approved` bool, `approver`, `decided_at_ms`). External events stay opaque bytes — the runtime does not impose an event schema (runtime, not framework). Approval is nested rather than a seventh top-level message because it only exists as an envelope payload.
- **`Continuation`**: `state_schema_version`, `seq`, `step_index`, repeated `pending_intent_ids`, `adapter` (string discriminator, e.g. `"langgraph"`), `snapshot` (bytes, framework-opaque), `suspended_at_ms`, `deadline_ms`. The snapshot is opaque on purpose: adapters own their checkpoint format; the runtime only guarantees durable storage and correlation.
- All enums have an `_UNSPECIFIED = 0` zero value; field numbers 1–15 are reserved for the hottest fields (single-byte tags); removals must use `reserved` statements.

**Rationale:** each layout answers a specific invariant or consumer identified in `project.md`; nothing speculative (no priority fields, no retry policies, no auth blocks) is included — those arrive additively when their capability lands.

### D5: Explicit `register_coders()` — no import-time registry mutation

**Choice:** `core/coders.py` exposes `register_coders()` which calls `beam.coders.registry.register_coder(MsgType, DeterministicProtoCoder)` for all six types. Nothing registers at import time. Future `RunAgent.expand()` calls it during pipeline construction; tests call it in fixtures.

**Alternatives considered:**
- Module-level registration on import: idiomatic in some Beam codebases, but it's global mutable state triggered by an import side effect — squarely against the project's "no global mutable state except documented worker-local singletons" rule, and it makes test isolation order-dependent.

**Rationale:** registration is idempotent, so calling it from every entry point is safe; explicitness keeps `import beam_agents` side-effect-free.

### D6: Golden blobs test decode + semantic equality, determinism tests assert byte equality

**Choice:** two distinct test families, because they guard different promises:

1. **Compat (golden blobs)**: `tests/core/golden/*.bin` are committed bytes produced once by a checked-in generator script (`tests/core/golden/generate.py`, run manually, never in CI). Tests decode each blob with the *current* bindings and assert field-level equality against expected values. They do **not** assert that re-encoding reproduces the committed bytes — protobuf library upgrades may legitimately change serialization details while remaining wire-compatible.
2. **Determinism**: hypothesis-generated messages (including map-heavy `TraceEvent`s with shuffled insertion order) are encoded twice via the coder; bytes must be identical. Round-trip tests assert `decode(encode(m)) == m` for all six types.

**Rationale:** conflating the two would make every protobuf upgrade a false alarm (if golden tests demanded byte equality) or would under-test retries (if determinism tests only checked round-trips). Byte-identity is a *same-process/same-version* promise; golden blobs are a *cross-version decode* promise. Both are needed; they are different tests.

## Risks / Trade-offs

- **[Protobuf `deterministic=True` is not canonical across library versions]** → determinism is only claimed within a pinned `protobuf` version; `uv.lock` pins it, the drift hook catches gencode changes on upgrade, and the semantics-tier retry tests (future change) exercise the real byte-identity guarantee end-to-end. Documented in `coders.py` docstring.
- **[Schema decisions freeze early]** → wrong field choices here cost a `state_schema_version` bump later. Mitigated by deriving every field from an already-specified consumer in `project.md` and refusing speculative fields; the additive-evolution rule plus reserved numbers keeps the escape hatch cheap.
- **[Opaque `bytes` for external events and snapshots shifts validation downstream]** → accepted deliberately: the runtime must not impose event or checkpoint schemas (runtime-not-framework). Adapters and user code own those bytes; `AgentEnvelope` only guarantees keying and event-time.
- **[Committed generated code churns diffs on protoc upgrades]** → contained by the drift hook (regen must be clean) and by treating gencode as build output in review (never hand-edited, per project rules).
- **[`TraceEvent.attributes` as map<string,string> stringifies numeric metrics]** → trade-off for OTel-attribute fidelity and schema stability; exporters parse known numeric keys. Revisit only if profiling shows it matters.

## Migration Plan

Greenfield — no existing state to migrate. This change *establishes* the migration baseline: golden blobs committed here are the v1 artifacts every future schema change must still decode. Rollback is `git revert`; nothing external consumes the schemas yet.

## Open Questions

- None blocking. Buf (`buf lint`/`buf breaking`) adoption for mechanical breaking-change detection is deferred to a future tooling change; until then the golden-blob tests are the compat gate.
