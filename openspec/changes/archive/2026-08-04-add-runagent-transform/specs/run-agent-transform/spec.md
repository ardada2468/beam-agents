## ADDED Requirements

### Requirement: AgentConfig bundles runtime configuration and validates at construction
The system SHALL provide an `AgentConfig` object that bundles the `provider_factory`, the runtime knobs (`activation_timeout_s`, `ttl_ms`, `cancel_grace_s`), and the optional sink URIs (`intents_to`, `traces_to`, `errors_to`). `AgentConfig` SHALL validate its inputs at construction time and SHALL raise `ValueError` with an actionable message for any non-positive knob or any sink URI whose scheme is unknown or malformed. A valid `AgentConfig` SHALL be immutable.

#### Scenario: Valid config constructs
- **WHEN** an `AgentConfig` is constructed with a `provider_factory`, positive knobs, and either no sink URIs or sink URIs with recognized schemes
- **THEN** construction succeeds and the resulting config is immutable

#### Scenario: Non-positive runtime knob is rejected
- **WHEN** an `AgentConfig` is constructed with `activation_timeout_s`, `ttl_ms`, or `cancel_grace_s` that is zero or negative
- **THEN** construction raises `ValueError` naming the offending knob

#### Scenario: Unknown sink URI scheme is rejected at construction
- **WHEN** an `AgentConfig` is constructed with an `intents_to`, `traces_to`, or `errors_to` URI whose scheme is not recognized by the sink resolver, or that is malformed
- **THEN** construction raises `ValueError` naming the offending field and URI, and no pipeline is built

### Requirement: RunAgent requires pre-keyed KV input and validates it at pipeline construction
`RunAgent` SHALL consume a `PCollection[KV[bytes, AgentEnvelope]]` whose key is the `entity_key`, and SHALL NOT key elements itself. At pipeline-construction (`expand`) time `RunAgent` SHALL raise `ValueError` with an actionable message when the input is positively not KV-shaped (for example a bare `PCollection[AgentEnvelope]`), directing the caller to key upstream.

#### Scenario: Pre-keyed KV input flows through
- **WHEN** a `PCollection[KV[bytes, AgentEnvelope]]` is passed to `RunAgent`
- **THEN** each keyed envelope reaches the stateful DoFn under its `entity_key` and no additional keying step is inserted

#### Scenario: Non-KV input is rejected at construction
- **WHEN** a `PCollection[AgentEnvelope]` (not KV-shaped) is passed to `RunAgent`
- **THEN** `expand` raises `ValueError` explaining that KV input is required and pointing at `WithKeys(entity_key)`, before the pipeline runs

### Requirement: RunAgent exposes four named outputs as RunAgentOutputs
`RunAgent.expand` SHALL return a typed `RunAgentOutputs` exposing the main output as `.output` and the tagged outputs as `.intents`, `.traces`, and `.errors`, each a `PCollection`. The underlying tag names SHALL remain `output`, `intents`, `traces`, and `errors`.

#### Scenario: All four outputs are addressable by name
- **WHEN** a keyed envelope stream is passed through `RunAgent`
- **THEN** the returned `RunAgentOutputs` exposes `.output`, `.intents`, `.traces`, and `.errors` as `PCollection` attributes bound to the DoFn's main and tagged outputs

#### Scenario: Terminal outputs and intents/traces/errors are separable
- **WHEN** an activation emits an output and stages an intent, a trace, and (on a later element) an error
- **THEN** the terminal output appears only on `.output` and the intent, trace, and error appear only on `.intents`, `.traces`, and `.errors` respectively

### Requirement: Configured sink URIs resolve and attach to their tagged outputs
When `intents_to`, `traces_to`, or `errors_to` is set on the `AgentConfig`, `RunAgent.expand` SHALL resolve each URI to a Beam write transform via the config's sink resolver and attach it as a terminal branch to the matching tagged output (`intents_to` → `.intents`, `traces_to` → `.traces`, `errors_to` → `.errors`). An unset sink URI SHALL leave that tagged `PCollection` exposed on `RunAgentOutputs` with no write attached. Attaching a sink SHALL NOT remove or replace the tagged `PCollection` on `RunAgentOutputs`. The main `.output` SHALL never be auto-sunk.

#### Scenario: A configured sink is attached to its tag
- **WHEN** `RunAgent` runs with an `AgentConfig` whose `intents_to` resolves to a write transform
- **THEN** the resolved write is attached to the `.intents` output and `.intents` remains exposed on `RunAgentOutputs`

#### Scenario: Each sink attaches only to its own tag
- **WHEN** `traces_to` and `errors_to` are set but `intents_to` is not
- **THEN** the trace sink attaches only to `.traces`, the error sink attaches only to `.errors`, and `.intents` is exposed with no write attached

#### Scenario: Sink resolution is injectable for offline tests
- **WHEN** an `AgentConfig` is given a stub sink resolver that returns an in-memory write transform
- **THEN** `RunAgent` attaches the stub's transform without importing any external IO client, and the test runs offline

### Requirement: RunAgent, AgentConfig, and RunAgentOutputs are the public package surface
`RunAgent`, `AgentConfig`, and `RunAgentOutputs` SHALL be re-exported from `beam_agents/__init__.py` as the package's public API surface. Importing the package SHALL have no side effects.

#### Scenario: Public names are importable from the package root
- **WHEN** a user imports `beam_agents`
- **THEN** `RunAgent`, `AgentConfig`, and `RunAgentOutputs` are accessible from the package root and importing the package triggers no side effects
