## Why

Every load-bearing invariant in `openspec/project.md` — atomic bundle commit, deterministic intent IDs, replay-cache keys, `--update`-safe state, golden-blob compat tests — presupposes protobuf messages that do not exist yet. `protos/` is an empty placeholder and `beam_agents` has no coders, so no `core/` capability (DoFn, context, loop driver) can land until the wire and state schemas are defined and encodable by Beam. This change delivers that foundation and nothing else.

## What Changes

- Add `protos/beam_agents.proto` (package `beam_agents.v1`) defining the six core messages:
  - `MemoryBlob` — per-key working memory: bounded string→bytes entries with LRU metadata (last-access ordering), `state_schema_version`, and byte-size accounting to enforce the 100 KiB blob cap.
  - `ToolIntent` — declarative side-effect request: deterministic `intent_id`, `entity_key`, `seq`, `step_index`, `tool_name`, canonical-JSON `args_json`, `expires_at`, attempt metadata for the effector.
  - `ToolResult` — effector outcome re-injected on the same key: `intent_id` correlation, status (OK / ERROR / EXPIRED / REJECTED), payload bytes, error detail.
  - `TraceEvent` — one observability record per activation step, aligned with OTel GenAI semantic-convention attribute names, carrying span/activation identity and token/latency metrics.
  - `AgentEnvelope` — the single keyed element type entering `RunAgent`: `entity_key`, event-time, and a `oneof` payload of external event / `ToolResult` / approval, so events, results, and approvals can be Flattened into one stream.
  - `Continuation` — persisted resume-state for a suspended activation: `state_schema_version`, `seq`, `step_index`, pending `intent_id`s, framework-opaque snapshot bytes plus adapter discriminator, and suspension deadline.
- Commit generated `beam_agents_pb2.py` / `.pyi` into a private `src/beam_agents/_protos/` package (so the bindings are importable in local tests, CI, and Dataflow workers alike); `scripts/gen_proto.sh` is updated to emit there. The pre-commit drift hook and CI regen check now have real inputs to guard.
- Add `src/beam_agents/core/coders.py`: a deterministic proto coder (serialization with `deterministic=True`) wrapped per message type, plus registration with Beam's coder registry so all six types round-trip through pipelines and keyed state without pickle.
- Add golden-blob fixtures (checked-in serialized bytes for each message) and compat tests that decode them, establishing the baseline the "additive-only proto changes" rule is enforced against.

No DoFn, transform, state specs, or runtime behavior lands here — schemas and coders only.

## Capabilities

### New Capabilities

- `wire-schemas`: the protobuf message definitions (`beam_agents.v1`) for all state and wire types, their versioning/compat rules, and the committed-generation workflow.
- `proto-coders`: deterministic Beam coders for the six message types, coder-registry integration, and round-trip/determinism guarantees.

### Modified Capabilities

<!-- None: repo-scaffolding requirements are unchanged; this change only exercises the proto drift hook it already specified. -->

## Impact

- **Files created**: `protos/beam_agents.proto`, committed `src/beam_agents/_protos/beam_agents_pb2.py` + `.pyi` (+ `__init__.py`), `src/beam_agents/core/__init__.py`, `src/beam_agents/core/coders.py`, `tests/core/test_coders.py`, `tests/core/test_schema_compat.py`, golden-blob fixtures under `tests/core/golden/`. Modified: `scripts/gen_proto.sh` output paths.
- **APIs**: nothing re-exported from `beam_agents/__init__.py`; coders and generated modules are internal (`core/` is private per project conventions).
- **Dependencies**: none new — `protobuf` and `grpcio-tools` are already declared; `apache-beam` provides the coder base classes.
- **Downstream**: unblocks the stateful DoFn change (state specs need `MemoryBlob`/`Continuation` coders), the outbox/effector contract (`ToolIntent`/`ToolResult`), observability (`TraceEvent`), and the `RunAgent` input contract (`AgentEnvelope`). Schema decisions made here are semi-frozen after first release: only additive changes without a `state_schema_version` bump.
