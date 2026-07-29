# run-agent-transform Delta Specification

## MODIFIED Requirements

### Requirement: Configured sink URIs resolve and attach to their tagged outputs
When `intents_to`, `traces_to`, or `errors_to` is set on the `AgentConfig`, `RunAgent.expand` SHALL resolve each URI to a Beam write transform via the config's sink resolver and attach it as a terminal branch to the matching tagged output (`intents_to` → `.intents`, `traces_to` → `.traces`, `errors_to` → `.errors`). For `errors_to`, the default resolver's transform SHALL encode `ActivationError` elements for the resolved scheme (envelope-wrapped `ActivationErrorRecord` bytes for `kafka://`/`pubsub://`, row mappings for `bigquery://`) before writing; raw dataclasses SHALL never reach the scheme's writer. An unset sink URI SHALL leave that tagged `PCollection` exposed on `RunAgentOutputs` with no write attached. Attaching a sink SHALL NOT remove or replace the tagged `PCollection` on `RunAgentOutputs`. The main `.output` SHALL never be auto-sunk.

#### Scenario: A configured sink is attached to its tag
- **WHEN** `RunAgent` runs with an `AgentConfig` whose `intents_to` resolves to a write transform
- **THEN** the resolved write is attached to the `.intents` output and `.intents` remains exposed on `RunAgentOutputs`

#### Scenario: Each sink attaches only to its own tag
- **WHEN** `traces_to` and `errors_to` are set but `intents_to` is not
- **THEN** the trace sink attaches only to `.traces`, the error sink attaches only to `.errors`, and `.intents` is exposed with no write attached

#### Scenario: Sink resolution is injectable for offline tests
- **WHEN** an `AgentConfig` is given a stub sink resolver that returns an in-memory write transform
- **THEN** `RunAgent` attaches the stub's transform without importing any external IO client, and the test runs offline

#### Scenario: A configured errors sink receives encoded records, not dataclasses
- **WHEN** `RunAgent` runs with the default sink resolver and an `errors_to` URI, and an activation dead-letters
- **THEN** the scheme's writer receives the encoded form for that scheme and `.errors` still exposes `ActivationError` objects to direct consumers

## ADDED Requirements

### Requirement: Intent dead letters route to the errors sink as unified error records
When both `intents_to` (resolving to a `WriteIntents` outbox writer) and `errors_to` are configured, `RunAgent.expand` SHALL route the `WriteIntents` `.dead_letter` output to the errors sink by mapping each dead letter into an `ActivationError` (reason `intent_dead_letter`, JSON detail with the failure reason and the intent's `intent_id`, `seq`, `tool_name`) and applying the same errors encoding used for `.errors`. The `.dead_letter` `PCollection` SHALL remain exposed on `RunAgentOutputs` regardless of whether `errors_to` is set.

#### Scenario: Dead letters and activation errors share one sink schema
- **WHEN** an intent dead letter and an activation dead letter both reach a configured `kafka://` errors sink
- **THEN** both arrive as `KV[entity_key, AgentEnvelope bytes]` whose `external_event` parses as `ActivationErrorRecord`, distinguished only by `reason`

#### Scenario: dead_letter stays exposed without an errors sink
- **WHEN** `intents_to` resolves to a `WriteIntents` writer and `errors_to` is unset
- **THEN** `RunAgentOutputs.dead_letter` is exposed and no errors write is attached
