## Why

`_AgentDoFn` is the runtime engine, but the only public entry point the project promises — `RunAgent`/`AgentConfig` in `beam_agents/__init__.py` — is still a thin wrapper that keys elements itself, takes a loose bag of keyword arguments, exposes its four outputs as an untyped `DoOutputsTuple`, and knows nothing about where intents, traces, and errors are written. Users cannot express "run this agent, key by entity, and drain intents to my outbox topic" in one declarative object, and misconfiguration (non-KV input, a typo'd sink URI) fails deep inside the runner instead of at pipeline-construction time. This change makes `RunAgent` the real, validated public transform and anchors the root package surface.

## What Changes

- Introduce `AgentConfig`, a frozen, self-validating configuration object bundling the agent, the `provider_factory`, the runtime knobs (`activation_timeout_s`, `ttl_ms`, `cancel_grace_s`), and the optional sink URIs (`intents_to`, `traces_to`, `errors_to`). Invalid values (non-positive timeouts, unknown/malformed sink URI schemes) raise `ValueError` at `AgentConfig` construction with actionable messages.
- **BREAKING:** `RunAgent` now consumes a **pre-keyed** `PCollection[KV[bytes, AgentEnvelope]]` and no longer keys elements itself. Callers `Flatten` their event/result/approval streams and `WithKeys(entity_id)` upstream (matching the documented Dataflow shape). `RunAgent` validates the input is KV-shaped at `expand` (pipeline-construction) time and raises `ValueError` for non-KV input.
- `RunAgent.__init__` accepts an `AgentConfig` (plus the agent, if not carried on the config) instead of scattered keyword arguments.
- Formalize the four outputs behind a typed `RunAgentOutputs` result exposing `.output` (main), `.intents`, `.traces`, and `.errors` as named `PCollection` attributes, replacing positional `DoOutputsTuple` access. The four tag names remain `output`/`intents`/`traces`/`errors`.
- Resolve configured sink URIs to Beam write transforms via a pluggable `SinkResolver` and attach each resolved sink to its tagged output (`intents_to` → `.intents`, `traces_to` → `.traces`, `errors_to` → `.errors`). Unset sinks leave that tagged `PCollection` exposed on `RunAgentOutputs` for the caller to wire. Unknown schemes are rejected at construction, never silently dropped.
- Export `RunAgent`, `AgentConfig`, and `RunAgentOutputs` from the root `beam_agents/__init__.py`, retiring `test_public_surface_is_empty` and anchoring the public API surface named in `project.md`.

## Capabilities

### New Capabilities
- `run-agent-transform`: The public `RunAgent` PTransform and its `AgentConfig`: construction-time validation (non-positive knobs, non-KV input, unknown/malformed sink URIs), the KV-in / four-tagged-out (`output`/`intents`/`traces`/`errors`) contract exposed as a typed `RunAgentOutputs`, and sink-URI resolution that attaches `intents_to`/`traces_to`/`errors_to` write transforms to their tagged outputs.

### Modified Capabilities
<!-- The stateful-agent-runtime DoFn already consumes a keyed AgentEnvelope; this change moves the keying responsibility to the caller but does not change any DoFn-level requirement. No existing spec's requirements change. -->
(none — the `stateful-agent-runtime` DoFn contract is consumed unchanged)

## Impact

- **New/changed code:** `core/transform.py` (`RunAgent` reshaped around `AgentConfig`, KV-input validation, typed `RunAgentOutputs`, sink attachment), a new `AgentConfig` dataclass and `SinkResolver` seam (in `core/transform.py` or a small `core/config.py`/`core/sinks.py`), and `beam_agents/__init__.py` re-exports.
- **BREAKING API change:** `RunAgent(agent, provider_factory=...)` over `PCollection[AgentEnvelope]` becomes `RunAgent(agent, config=AgentConfig(...))` over `PCollection[KV[bytes, AgentEnvelope]]`, returning `RunAgentOutputs`. All call sites and tests that construct `RunAgent` or key inside it are updated.
- **Consumes (unchanged):** `stateful-agent-runtime` (`_AgentDoFn`, its state/timer topology and four outputs), `wire-schemas` (`AgentEnvelope`), `proto-coders` (the deterministic KV/element coder).
- **Dependencies:** `apache-beam` PTransform / typehints / `with_output_types` / `WithKeys`; the concrete Kafka/Pub/Sub/BigQuery write transforms behind resolved schemes are exercised in the integration tier, not the offline unit tier. No new third-party dependencies.
- **Verification surface:** unit tests for each construction-time rejection (non-KV input, bad timeout, unknown/malformed URI), a `TestPipeline` round-trip asserting KV input flows through and the four named outputs are populated, and a resolver test asserting each configured sink URI attaches its write transform to the correct tag while unset sinks stay exposed.
