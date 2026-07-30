## ADDED Requirements

### Requirement: A fully-qualified YAML-facing transform constructor wraps RunAgent

The system SHALL provide a transform constructor at the stable fully-qualified name `beam_agents.yaml.run_agent` that accepts only YAML-representable values (strings, numbers, booleans, and mappings thereof) as keyword arguments and returns a `beam.PTransform` wrapping the public `RunAgent`/`AgentConfig` surface unchanged. The `beam_agents.yaml` package SHALL NOT import `apache_beam.yaml`, and importing it SHALL have no side effects.

#### Scenario: Constructor is reachable by its fully-qualified name

- **WHEN** `beam_agents.yaml.run_agent` is resolved by fully-qualified name (as a Beam YAML Python provider would) and called with a valid YAML-shaped config
- **THEN** it returns a `beam.PTransform`, and no module under `apache_beam.yaml` has been imported by `beam_agents.yaml`

#### Scenario: The constructor is usable directly from Python

- **WHEN** the returned transform is applied to a schema'd row `PCollection` in an ordinary Python pipeline
- **THEN** it behaves identically to its YAML-invoked form, with no YAML parser involved

### Requirement: Agent and callable config values are module:object references resolved at construction time

The system SHALL accept agent, provider, decode, HITL route, and tool-registry values as `module:object` reference strings (importable module path, a colon, then a dotted attribute path), and SHALL resolve each reference by import when the transform constructor runs — at pipeline-construction time, never inside a bundle. A reference that is malformed, names an unimportable module, names a missing attribute, or resolves to an object unusable in its position SHALL raise `ValueError` identifying the offending reference, the failing step, and the expected form. The system SHALL NOT evaluate any code embedded in the YAML document itself.

#### Scenario: A valid agent reference resolves to the module-level agent

- **WHEN** the constructor is called with `agent="my_pkg.agents:fraud_agent"` and that module attribute is an agent callable
- **THEN** the wrapped `RunAgent` is constructed with exactly that object

#### Scenario: A malformed reference is rejected with the grammar

- **WHEN** the constructor is called with `agent="my_pkg.agents.fraud_agent"` (no colon) or `agent=":fraud_agent"` (empty module)
- **THEN** it raises `ValueError` quoting the reference and showing the `module:object` form

#### Scenario: An unimportable module is rejected naming the module

- **WHEN** the constructor is called with `agent="no_such_pkg.agents:fraud_agent"`
- **THEN** it raises `ValueError` naming `no_such_pkg.agents` and indicating the package must be installed in the launch environment, chained from the underlying `ImportError`

#### Scenario: A missing attribute is rejected naming both sides

- **WHEN** the constructor is called with `agent="my_pkg.agents:no_such_agent"` and the module imports cleanly
- **THEN** it raises `ValueError` naming the missing attribute and the module it was looked up on

#### Scenario: A resolved non-agent object is rejected

- **WHEN** the constructor is called with an `agent` reference that resolves to a non-callable object with no `activate` method (for example, a module or a string constant)
- **THEN** it raises `ValueError` stating what the reference resolved to and what an agent must be

### Requirement: YAML config maps totally onto AgentConfig and rejects unknown keys

The system SHALL map the YAML config surface onto `AgentConfig` as follows: `activation_timeout_s`, `ttl_ms`, and `cancel_grace_s` as scalars; `intents_to`, `traces_to`, and `errors_to` as verbatim URI strings validated by the existing sink-resolver grammar; `provider` as a reference with an optional `provider_config` mapping bound as keyword arguments into a picklable zero-argument `provider_factory`; `decode` as an optional reference; `hitl` as a nested mapping onto `HitlPolicy` fields with `on_timeout` as an optional reference; and `tool_registry` as an optional reference to a prebuilt registry. An unrecognized top-level key or an unrecognized key inside `hitl` SHALL raise `ValueError` listing the valid keys. Value-range and URI validation SHALL be delegated to `AgentConfig`, `HitlPolicy`, and the sink resolver rather than duplicated.

#### Scenario: A full YAML config round-trips onto AgentConfig

- **WHEN** the constructor is called with scalar knobs, sink URIs, a provider reference with `provider_config` kwargs, and a `hitl` mapping
- **THEN** the wrapped `AgentConfig` carries the same knob values, the same URI strings verbatim, an `HitlPolicy` with the mapped fields, and a `provider_factory` that pickles and, when called with no arguments, invokes the referenced callable with exactly the `provider_config` keyword arguments

#### Scenario: An unknown config key is rejected with the valid-key list

- **WHEN** the constructor is called with a misspelled key (for example `ttl` instead of `ttl_ms`, at top level or inside `hitl`)
- **THEN** it raises `ValueError` naming the unknown key and listing the accepted keys, and no pipeline is constructed

#### Scenario: Delegated validation still fires at the YAML boundary

- **WHEN** the constructor is called with a sink URI of an unknown scheme or a non-positive knob
- **THEN** the existing `AgentConfig`/sink-resolver `ValueError` propagates at construction time, before any pipeline runs

#### Scenario: A misspelled provider kwarg fails at construction when the signature is introspectable

- **WHEN** `provider_config` contains a keyword argument the referenced callable's signature does not accept
- **THEN** the constructor raises `ValueError` at construction naming the bad keyword, rather than deferring the failure to the first worker call

### Requirement: Input rows are keyed and enveloped; malformed rows dead-letter instead of crashing

The transform SHALL accept a `PCollection` of schema'd rows and construct `RunAgent`'s `KV[bytes, AgentEnvelope]` input itself: the configured `key_field` (default `key`) supplies the entity key (a `str` key is UTF-8 encoded; `bytes` pass through), the configured `payload_field` (default `payload`) supplies the envelope's opaque `external_event` bytes, and `event_time_ms` comes from the configured `event_time_field` when set, else the element timestamp. An input row missing a configured field SHALL be routed to the `errors` output as a malformed-input record naming the missing field, and SHALL NOT fail the bundle.

#### Scenario: Rows are keyed and enveloped by the configured fields

- **WHEN** rows with fields `key` and `payload` flow into the transform with default field configuration
- **THEN** the wrapped `RunAgent` receives `KV[bytes, AgentEnvelope]` elements whose `entity_key` is the encoded key and whose `external_event` is the payload bytes

#### Scenario: A row missing the key field dead-letters

- **WHEN** an input row lacks the configured `key_field`
- **THEN** a record naming the missing field appears on the `errors` output, the element produces no activation, and other elements in the bundle are processed normally

### Requirement: The four outputs are addressable by name from YAML

The transform SHALL expose its outputs under Beam YAML's multi-output convention with the names `output` (main), `intents`, `traces`, and `errors`, matching the `RunAgentOutputs` attribute names, so a downstream YAML transform can consume any of them by qualified name. Non-main outputs SHALL be schema'd rows: `traces` and `errors` via the existing `trace_event_to_row`/`activation_error_to_row` mappings, `intents` via an equivalent scalar-field mapping of `ToolIntent`, and `output` as rows carrying the entity key and the opaque output bytes. Configuring a sink URI SHALL NOT remove the corresponding named output.

#### Scenario: A downstream step consumes a non-main output by name

- **WHEN** a pipeline addresses the transform's `errors` output as the input of a downstream transform
- **THEN** the downstream transform receives the error rows, and the `output`, `intents`, and `traces` streams are independently addressable the same way

#### Scenario: Tagged streams surface as rows, not protos

- **WHEN** an activation stages an intent and emits traces
- **THEN** the `intents` output carries rows exposing the intent's scalar fields (including `intent_id`, `tool_name`, `args_json`) and the `traces` output carries rows in the existing trace-row shape, with no raw proto messages on any YAML-facing output

#### Scenario: A configured sink leaves the named output addressable

- **WHEN** `intents_to` is set to a valid sink URI
- **THEN** the resolved sink is attached inside the wrapped transform and the `intents` named output remains consumable downstream

### Requirement: An end-to-end YAML pipeline runs RunAgent offline with FakeLLM

The system SHALL run a complete Beam YAML pipeline document — declaring a provider mapping for `beam_agents.yaml.run_agent`, referencing a test agent by `module:object`, and configuring a `FakeLLM`-backed provider factory by reference — on DirectRunner with no docker, no network, and no real provider import, producing observable agent output. The repository SHALL include a documented example YAML pipeline and the provider declaration required to use it.

#### Scenario: A YAML document drives an agent activation end to end

- **WHEN** a YAML pipeline document whose transforms include `RunAgent` (mapped to `beam_agents.yaml.run_agent`, with `agent` referencing a test agent and `provider` referencing a module-level `FakeLLM` factory) is parsed and executed on DirectRunner
- **THEN** the pipeline completes offline, the agent's output is observed on the `output` stream, and the `FakeLLM` recorded the model calls the agent made

#### Scenario: The documented example matches the shipped surface

- **WHEN** the example pipeline and provider declaration in the docs are checked against the constructor's keyword surface and the packaged provider listing
- **THEN** every transform name, fully-qualified constructor path, and config key in the example is one the shipped code accepts
