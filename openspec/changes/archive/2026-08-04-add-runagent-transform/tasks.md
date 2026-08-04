## 1. AgentConfig — construction-time validation

- [x] 1.1 Write failing tests for the "AgentConfig bundles runtime configuration and validates at construction" scenarios: valid config constructs and is immutable; a non-positive `activation_timeout_s`/`ttl_ms`/`cancel_grace_s` raises `ValueError` naming the knob; an unknown/malformed sink URI raises `ValueError` naming the field and URI.
- [x] 1.2 Add `AgentConfig` as a `@dataclass(frozen=True, slots=True)` carrying `provider_factory`, `activation_timeout_s=30.0`, `ttl_ms=3_600_000`, `cancel_grace_s=5.0`, and `intents_to`/`traces_to`/`errors_to` (default `None`) plus the sink resolver (default resolver).
- [x] 1.3 Implement `__post_init__` validation: reject non-positive knobs; ask the resolver to validate each configured sink URI's scheme (import-free) and reject unknown/malformed schemes with actionable `ValueError` messages.
- [x] 1.4 Make the tests pass; confirm the config is immutable (frozen) and validation runs before any pipeline object exists.

## 2. Sink resolver seam

- [x] 2.1 Write failing tests: a stub `SinkResolver` validates a scheme without importing IO; `DefaultSinkResolver` recognizes the documented schemes (`kafka://`, `pubsub://`, `bigquery://`) at validation and rejects an unknown scheme.
- [x] 2.2 Define the `SinkResolver` protocol/seam: `supports(uri) -> bool` (or a parse that raises) for eager scheme validation and `resolve(uri) -> beam.PTransform` for lazy write-transform construction at `expand`.
- [x] 2.3 Implement `DefaultSinkResolver` recognizing the documented scheme set for validation; defer heavy IO construction to `resolve()` (called only in `expand`).
- [x] 2.4 Make the tests pass; assert scheme validation performs no IO client import.

## 3. RunAgent — KV input, typed outputs, sink attachment

- [x] 3.1 Write failing tests for "RunAgent requires pre-keyed KV input": a `PCollection[KV[bytes, AgentEnvelope]]` flows through with no inserted keying step (`TestPipeline`); a bare `PCollection[AgentEnvelope]` raises `ValueError` at `expand` pointing at `WithKeys(entity_key)`.
- [x] 3.2 Write failing tests for "RunAgent exposes four named outputs as RunAgentOutputs": `.output`/`.intents`/`.traces`/`.errors` are addressable and correctly separated (`TestPipeline`).
- [x] 3.3 Write failing tests for "Configured sink URIs resolve and attach to their tagged outputs": a configured sink attaches to its tag and the tag stays exposed; each sink attaches only to its own tag; a stub resolver runs offline.
- [x] 3.4 Reshape `RunAgent.__init__` to `RunAgent(agent, *, config: AgentConfig)`; build `_AgentDoFn` from the agent plus the config's knobs.
- [x] 3.5 Remove the internal `beam.Map(_key_by_entity)`; consume `PCollection[KV[bytes, AgentEnvelope]]` directly and register the deterministic KV/element coder in `expand`.
- [x] 3.6 Implement KV-shape validation in `expand`: reject a positively-non-KV element type hint with an actionable `ValueError`; let absent/erased hints pass (rely on the DoFn's downstream KV requirement).
- [x] 3.7 Add `RunAgentOutputs` (frozen) wrapping the `DoOutputsTuple`, exposing `.output`/`.intents`/`.traces`/`.errors`; return it from `expand`.
- [x] 3.8 Attach resolved sinks as terminal branches: for each set `intents_to`/`traces_to`/`errors_to`, apply `resolver.resolve(uri)` to the matching tagged `PCollection` without removing it from `RunAgentOutputs`; never auto-sink `.output`.
- [x] 3.9 Make the tests pass.

## 4. Public surface and call-site migration

- [x] 4.1 Write a failing test that `RunAgent`, `AgentConfig`, and `RunAgentOutputs` import from the package root `beam_agents` with no import side effects.
- [x] 4.2 Re-export `RunAgent`, `AgentConfig`, `RunAgentOutputs` from `beam_agents/__init__.py`; delete/replace `tests/test_import.py::test_public_surface_is_empty`.
- [x] 4.3 Update all internal call sites and existing `RunAgent` tests to key upstream (`WithKeys(entity_key)` / `.with_output_types(tuple[bytes, AgentEnvelope])`) and pass an `AgentConfig` instead of loose keyword arguments.
- [x] 4.4 Make the tests pass.

## 5. Verification

- [x] 5.1 Run the full suite under `pytest` (no docker) plus `ruff` and `mypy --strict` on `src/`; confirm no pickle coder fallback on the KV path.
- [x] 5.2 Confirm each construction-time rejection (non-KV input, non-positive knob, unknown/malformed sink URI) fails before the pipeline runs with an actionable message, and that a configured sink attaches to the correct tag while keeping the tagged `PCollection` exposed.
