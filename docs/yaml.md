# Beam YAML provider

A system-triggered agent pipeline is mostly plumbing: read a topic, key by
entity, run the agent, drain the tagged outputs to sinks. [Beam
YAML](https://beam.apache.org/documentation/sdks/yaml/) expresses exactly that
shape declaratively, and `beam-agents` ships a provider so `RunAgent` is
reachable from a YAML document.

The whole integration surface is one fully-qualified constructor,
`beam_agents.yaml.run_agent`. There is no service to run and nothing to deploy
beyond installing the package where the pipeline launches.

!!! note "YAML names an agent; it does not author one"
    Nothing here templates prompts or describes agent logic declaratively. A
    document *names* an agent that lives in an importable Python package; the
    agent itself stays Python. That is the runtime-not-framework line.

## Declaring the provider

Add a `providers:` block to your pipeline document:

```yaml
providers:
  - type: python
    config:
      packages: ["beam-agents==1.0.0a1"]
    transforms:
      RunAgent: "beam_agents.yaml.run_agent"
```

`type: python` is Beam's own provider type for Python transforms. With a
`packages:` list Beam installs those distributions into a managed environment
and expands the transform there — which is what you want for a remote runner,
because the workers need `beam-agents` (and your agent's package) too. Drop
`config:` entirely to resolve the constructor in the launch environment instead:

```yaml
providers:
  - type: python
    transforms:
      RunAgent: "beam_agents.yaml.run_agent"
```

The package also ships that mapping as a standalone provider-listing file, so a
document can pull it in rather than copy it. `beam_agents.yaml.PROVIDER_LISTING`
is its path inside the installed wheel:

```yaml
providers:
  - include: "/path/to/site-packages/beam_agents/yaml/providers.yaml"
```

!!! warning "A YAML pipeline with this provider is code"
    Resolving a reference imports a module, and importing runs its top level.
    That is not an escalation — the provider's `packages:` list already installs
    arbitrary code onto workers — but it does mean a document carrying a
    `beam-agents` provider must be reviewed and provenance-tracked exactly like
    Python pipeline code. What the provider refuses is *dynamic code in the
    document*: references resolve only against installed modules. There is no
    `eval` arm, no inline source, and no file-path arm.

## References: `module:object`

Agents, provider factories, decoders, HITL routes, and tool registries are
Python objects, and YAML can only carry strings. Each is named with the
setuptools entry-point spelling — an importable module path, a colon, then a
dotted attribute path:

```yaml
agent: "my_pkg.agents:fraud_agent"
provider: "beam_agents.model.anthropic:AnthropicProvider"
```

Note the colon. It is what separates the *module* from the *attribute*, and it
is what distinguishes a config reference from the fully-qualified, all-dots
constructor path in the `providers:` block above.

Every reference is resolved by import when the transform is constructed — at
YAML expansion, before any runner is involved — so a mistake fails there, not
inside a bundle:

| Mistake | What you get |
| --- | --- |
| `my_pkg.agents.fraud_agent` (no colon) | `ValueError` quoting the reference and the `module:object` form |
| `no_such_pkg.agents:fraud_agent` | `ValueError` naming the module, pointing at the launch environment's install, chained from the `ImportError` |
| `my_pkg.agents:no_such_agent` | `ValueError` naming the missing attribute and the module it was looked up on |
| `my_pkg:agents` (a module, or any non-agent) | `ValueError` saying what it resolved to and what an agent must be |

Everything a reference resolves to must be **picklable** — module-level, never a
closure or a locally-defined function — because the runtime serializes it into
the runner. The provider factory is probed for this at construction, so a
closure fails at the document rather than deep in job submission.

## Config keys

```yaml
- type: RunAgent
  name: Triage
  input: Events
  config:
    agent: "my_pkg.agents:fraud_agent"
    provider: "beam_agents.model.anthropic:AnthropicProvider"
    provider_config:
      base_url: "https://api.anthropic.com"
    decode: "beam_agents.model.anthropic:anthropic_decode"
    tool_registry: "my_pkg.tools:REGISTRY"
    activation_timeout_s: 30
    ttl_ms: 3600000
    cancel_grace_s: 5
    intents_to: "kafka://broker:9092/agent-intents"
    traces_to: "otlp://collector:4318"
    errors_to: "kafka://broker:9092/agent-errors"
    key_field: "customer_id"
    payload_field: "event"
    event_time_field: "event_time_ms"
    hitl:
      timeout_ms: 900000
      intent_ttl_ms: 1800000
      approval_channel: "fraud-approvals"
      max_escalations: 1
      on_timeout: "my_pkg.policies:deny_and_alert"
```

| Key | Kind | Meaning |
| --- | --- | --- |
| `agent` | reference (**required**) | The agent to run. |
| `provider` | reference (**required**) | A callable returning an `LLMClient` — a provider class or a factory function. |
| `provider_config` | mapping | Keyword arguments bound onto `provider`, producing the zero-argument provider factory the runtime holds. |
| `decode` | reference | The provider's paired response decoder. Unset means LLM traces omit their token-usage attributes rather than report zeros. |
| `tool_registry` | reference | A prebuilt, module-level `ToolRegistry` of the read-only tools the agent may run inline. Unset means an empty registry: every inline call by name is refused. |
| `activation_timeout_s` | number | Wall-clock budget for one activation. |
| `ttl_ms` | integer | Working-memory time-to-live for a key. |
| `cancel_grace_s` | number | Grace period for cancelling a timed-out activation. |
| `intents_to` | URI | Sink for the `intents` stream — the outbox topic the effector consumes. |
| `traces_to` | URI | Sink for the `traces` stream. |
| `errors_to` | URI | Sink for the `errors` stream. |
| `hitl` | mapping | Human-in-the-loop policy; see below. |
| `key_field` | string | Input-row field carrying the entity key (`str` is UTF-8 encoded, `bytes` pass through). Default `key`. |
| `payload_field` | string | Input-row field carrying the opaque event payload. Default `payload`. |
| `event_time_field` | string | Input-row field carrying event time in epoch milliseconds. Unset uses the element's own timestamp. |

The `hitl` mapping takes `timeout_ms`, `intent_ttl_ms`, `approval_channel`,
`max_escalations`, and `on_timeout` — the last a reference to a pure,
module-level route function returning `Deny`, `Drop`, or `Escalate`.

Sink URIs follow the grammar the Python surface already uses
(`kafka://<servers>/<topic>`, `pubsub://<project>/<topic>`,
`bigquery://<project>/<dataset>/<table>`, and `otlp://<host>[:<port>]` for
`traces_to` only); see [Trace delivery](traces.md) and [Errors and dead
letters](errors.md).

An unrecognized key — at the top level or inside `hitl` — is an error, not a
default: writing `ttl` for `ttl_ms` raises a `ValueError` listing the accepted
keys. Value checks are not duplicated here; a non-positive knob or an unknown
sink scheme raises the same error the Python surface raises, at the same place
(document expansion).

!!! danger "Keep secrets out of the document"
    `provider_config` values land in the pipeline's serialized configuration.
    Do not write an API key literally. Template the document at launch time
    (Beam YAML supports Jinja-style variable substitution) or generate it, and
    keep the key in your deployment's secret store.

## Inputs and outputs

Beam YAML pipelines carry schema'd rows, so the transform does the keying and
enveloping itself: it reads `key_field` and `payload_field` off each row and
builds the `KV[bytes, AgentEnvelope]` input the runtime requires. A row missing a
configured field is a *data* error, not a configuration one — it dead-letters
onto `errors` naming the missing field, and the rest of the bundle is processed
normally.

Four named outputs come back, matching the Python surface's `RunAgentOutputs`:

| Output | Row shape |
| --- | --- |
| `output` | `output` — the agent's opaque output bytes. The runtime's main stream is **unkeyed**, so if you need the entity key downstream, have the agent emit it inside its own output bytes. |
| `intents` | `intent_id`, `entity_key`, `seq`, `step_index`, `tool_name`, `args_json`, `created_at_ms`, `expires_at_ms`, `attempt`, `kind`, `trace_id` |
| `traces` | The published trace-row shape (hex IDs, enum names, RFC 3339 `event_time`, key/value `attributes`) |
| `errors` | `entity_key`, `reason`, `detail`, `event_time_ms` |

Because the transform has more than one output, a downstream step must always
name the one it wants:

```yaml
- type: LogForTesting
  input: Triage.errors
```

Configuring a sink URI does not remove the corresponding named output: the
resolved writer is attached inside the transform and the stream stays consumable
in the DAG as well.

## A complete pipeline

```yaml
pipeline:
  transforms:
    - type: ReadFromKafka
      name: Events
      config:
        topic: "customer-events"
        bootstrap_servers: "broker:9092"
        format: "RAW"

    - type: MapToFields
      name: Shape
      input: Events
      config:
        language: python
        fields:
          key:
            callable: "lambda row: row.key"
          payload:
            callable: "lambda row: row.payload"

    - type: RunAgent
      name: Triage
      input: Shape
      config:
        agent: "my_pkg.agents:fraud_agent"
        provider: "beam_agents.model.anthropic:AnthropicProvider"
        provider_config:
          base_url: "https://api.anthropic.com"
        activation_timeout_s: 30
        intents_to: "kafka://broker:9092/agent-intents"
        traces_to: "otlp://collector:4318"
        errors_to: "kafka://broker:9092/agent-errors"

    - type: WriteToKafka
      name: Decisions
      input: Triage.output
      config:
        topic: "fraud-decisions"
        bootstrap_servers: "broker:9092"
        format: "RAW"

providers:
  - type: python
    config:
      packages: ["beam-agents==1.0.0a1"]
    transforms:
      RunAgent: "beam_agents.yaml.run_agent"
```

The `intents` topic is consumed by the [reference effector](effector.md), which
executes each side effect exactly once per `intent_id` and publishes the results
back onto the bus. Nothing about that changes because the pipeline was written in
YAML.

## Using the constructor from Python

`beam_agents.yaml.run_agent` is an ordinary transform constructor, so it is also
a perfectly good Python API when you have rows rather than pre-keyed envelopes:

```python
outputs = rows | run_agent(
    agent="my_pkg.agents:fraud_agent",
    provider="my_pkg.providers:make_client",
)
outputs["output"] | beam.Map(handle)
```

For pipelines already holding `KV[bytes, AgentEnvelope]`, use
[`RunAgent`](index.md) directly — the YAML constructor adds only the row
boundary and the reference resolution on top of it.
