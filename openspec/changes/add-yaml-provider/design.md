## Context

Beam YAML lets a pipeline be authored as a declarative document: sources, transforms, and sinks by name, with per-transform `config:` mappings, and external transforms pulled in through `providers:` entries. A Python-typed provider maps a YAML transform name to a fully-qualified Python transform constructor and passes the YAML `config:` mapping as keyword arguments. That is the consumption model this change targets; `beam_agents/yaml/` is the reserved module-map slot for it.

Four facts about the existing surface shape everything below:

1. **Agents are Python callables.** The runtime driver contract is the [`Agent` protocol](../../../src/beam_agents/core/agent.py:132) (an async callable over an `ActivationContext`); the authoring contract is [`StreamAgent`](../../../src/beam_agents/core/agent.py:56). Neither can be expressed in YAML — a YAML pipeline can only *name* an agent that lives in an importable package on the workers.
2. **`AgentConfig` is half string-friendly already.** The sink fields ([`intents_to`/`traces_to`/`errors_to`](../../../src/beam_agents/core/transform.py:417)) are URI strings with construction-time validation via the [`SinkResolver`](../../../src/beam_agents/core/transform.py:164) seam and the [`DefaultSinkResolver`](../../../src/beam_agents/core/transform.py:249) grammars, and the knobs are scalars. The callable-valued fields — [`provider_factory`](../../../src/beam_agents/core/transform.py:403), `decode`, `hitl_policy.on_timeout`, `tool_registry` — are the YAML gap.
3. **`RunAgent`'s element types are not YAML's.** Input is a pre-keyed `PCollection[KV[bytes, AgentEnvelope]]` ([proto](../../../protos/beam_agents.proto:130): `entity_key`, `event_time_ms`, and a payload oneof whose external-event arm is opaque bytes); outputs are bytes, `ToolIntent`, `TraceEvent`, and `ActivationError` streams on a typed [`RunAgentOutputs`](../../../src/beam_agents/core/transform.py:438). Beam YAML pipelines carry schema'd rows between steps.
4. **Everything the DoFn holds must pickle.** `RunAgent` serializes the agent, the provider factory, the policy, and the registry into the runner. A YAML-constructed pipeline adds nothing new here, but it does mean every object a reference resolves to must be picklable — module-level, not a closure — exactly as [`HitlPolicy`](../../../src/beam_agents/hitl.py:136) already documents for `on_timeout`.

Some of Beam YAML's own surface (the exact multi-output return contract for Python-provider transforms, provider discovery via entry points, whether the Python provider type runs constructors in-process or through an auto-managed expansion service) is not pinned down here; those go to Open Questions rather than being invented.

## Goals / Non-Goals

**Goals:**
- One stable fully-qualified constructor, `beam_agents.yaml.run_agent`, taking only YAML-representable values and returning a `beam.PTransform` that wraps the public `RunAgent` unchanged.
- A single `module:object` reference convention for every callable-valued config field, resolved at transform-construction time with `ValueError` failure modes that name the reference, the failing step, and the fix.
- A total, validated mapping from the YAML config surface onto `AgentConfig` — including the provider factory with YAML-supplied keyword arguments — that rejects unknown keys.
- Row-in/rows-out boundary adaptation so the transform composes with ordinary YAML steps on both sides, with the four outputs addressable by name.
- An end-to-end YAML pipeline test on DirectRunner with `FakeLLM` ([fake.py](../../../src/beam_agents/model/fake.py:128)), offline.
- A documented `providers:` block and example pipeline in `docs/yaml.md`.

**Non-Goals:**
- Agent authoring in YAML. Nothing here templates prompts or describes agent logic declaratively — a YAML document *names* an agent; the agent stays Python. Anything more violates the runtime-not-framework principle.
- A cross-language (Java/Go) surface. Wire schemas are language-neutral protobuf, but this provider is Python-to-Python; a portable expansion-service story is future work.
- New sink schemes or sink behavior. The URI grammar and resolution stay exactly `run-agent-transform`'s.
- Arbitrary Python evaluation from YAML (`eval`, inline lambdas, code blocks). References import installed code only.
- Publishing the package (C25's job) or hosting an expansion service.

## Decisions

### D1. The YAML surface is a plain fully-qualified transform constructor; we ship no service

`beam_agents.yaml.run_agent(**config) -> beam.PTransform` is the entire integration contract. Beam YAML's Python provider mechanism is pointed at that name and hands it the YAML `config:` mapping as keyword arguments; whatever machinery Beam uses to invoke it (in-process construction or an auto-managed Python expansion service — see Open Questions), our deliverable is a constructor plus documentation, never a running service. Consequences:

- `beam_agents/yaml` imports nothing from `apache_beam.yaml`. The dependency direction is Beam YAML → us. The package stays importable and testable with plain `apache-beam[gcp]`, and the offline unit tier needs `apache_beam.yaml` only in the one e2e suite that parses YAML documents.
- The constructor is also a perfectly usable Python API (`rows | run_agent(agent="pkg:agent", provider="pkg:factory")`), which is how the non-e2e tests drive it without a YAML parser in the loop.

*Alternative considered:* a bespoke expansion service exposing `RunAgent` as a cross-language external transform. Rejected for v0: both sides are Python, a service adds packaging/lifecycle surface with no capability gain, and Beam YAML's Python provider path exists precisely so Python packages do not need one.

### D2. Agent references are `module:object` strings, resolved by import at construction time, failing closed with actionable errors

The reference grammar is the setuptools entry-point convention: `my_pkg.agents:fraud_agent` — an importable module path, a colon, then an attribute path (dotted attribute access after the colon is allowed, e.g. `my_pkg.agents:triage.agent`, matching entry-point semantics). Resolution: `importlib.import_module` on the left, iterated `getattr` on the right. It runs when the constructor runs — YAML expansion, i.e. pipeline-construction time — so every failure surfaces before a runner is involved, matching the project rule that misconfiguration raises `ValueError` at construction with an actionable message.

Failure modes are enumerated, each a `ValueError` naming the offending reference:
- no colon (or empty side) → the grammar, with an example;
- module import fails → the module name and a "is the package installed in the launch environment?" pointer (chained from the original `ImportError`);
- attribute missing → the attribute and the module it was looked up on;
- the resolved object is not usable as an agent (not callable and not a `StreamAgent`) → what was found (its type) and what was expected.

The type gate is deliberately structural and shallow: `Agent` is a `runtime_checkable` protocol over `__call__`, so the check is "callable or has an async `activate`" — enough to catch `agent: "my_pkg.agents"` (a module) or a string constant, without pretending to verify async-ness or signature at import time. The DoFn remains the backstop for a plausible-but-wrong object.

**Trust boundary, stated not hand-waved:** resolving a reference imports code, and importing executes module top-levels. That is not an escalation — the YAML author already controls what runs, since the provider's `packages:` list installs arbitrary code onto workers — but the docs say it plainly: a Beam YAML pipeline with a `beam-agents` provider must be treated exactly like Python pipeline code for review/provenance purposes. What the design refuses is *dynamic code in the document itself*: references resolve only against installed modules; there is no eval arm, no inline source, no file-path arm.

*Alternative considered:* a decorator-populated agent registry (`@register_agent("fraud")`, YAML says `agent: fraud`). Rejected: it requires the registering module to be imported before resolution — which reintroduces the same import question one step removed, but implicitly — and it adds a registration API to the authoring surface, which the runtime-not-framework principle says we do not own.

### D3. The provider factory is a reference plus a YAML kwargs mapping, bound with `functools.partial` — no name registry

`AgentConfig.provider_factory` must be a picklable zero-arg callable returning an `LLMClient`. From YAML:

```yaml
provider: "beam_agents.model:AnthropicProvider"
provider_config:
  api_key: "{{ secrets.anthropic_key }}"
  base_url: "https://api.anthropic.com"
```

The constructor resolves the reference (D2 grammar, same failure modes), then builds `provider_factory = functools.partial(resolved, **provider_config)`. This works uniformly for the three shapes users actually have: a provider *class* whose keyword-only constructor takes scalars ([`AnthropicProvider.__init__`](../../../src/beam_agents/model/anthropic.py:75) — `api_key`, `base_url`, `anthropic_version`, `timeout_s` are all YAML-representable), a user-written factory function taking scalar kwargs, and a zero-arg factory with `provider_config` omitted. A `partial` over a module-level callable with plain-value kwargs pickles; the constructor verifies picklability eagerly (a `pickle.dumps` probe at construction) so a non-module-level callable fails at the YAML boundary with a message naming the reference, not deep in runner submission.

`provider_config` values pass through untyped — the resolved callable's own signature is the contract. But the factory is deliberately *not* invoked at construction (a factory may open network clients), which would leave a misspelled kwarg failing at the first worker call — too late. So kwarg names are validated against the callable's `inspect.signature` at construction where a signature is introspectable (downgrading to no check for C callables), and misspelled kwargs on ordinary providers fail at the YAML boundary.

`decode` follows the same convention when set (e.g. `decode: "beam_agents.model:anthropic_decode"`), keeping the trace-usage behavior of the core config available from YAML.

*Alternative considered:* a curated shorthand registry (`provider: anthropic`, `provider: fake`). Rejected for v0: it duplicates the reference mechanism, needs its own lookup-failure vocabulary, and hard-codes a provider list into the YAML layer that `model/` does not itself maintain. The full reference spelling is longer but uniform — one convention for agents, providers, decoders, policies, and registries. A shorthand table can be layered on later without breaking the long form (Open Questions).

### D4. The YAML config maps totally onto `AgentConfig`; unknown keys are errors

The constructor's keyword surface is a fixed, documented set with a one-to-one mapping:

| YAML key | Kind | Maps to |
|---|---|---|
| `agent` | reference (required) | `RunAgent(agent=...)` |
| `provider` / `provider_config` | reference + mapping | `AgentConfig.provider_factory` (D3) |
| `decode` | reference | `AgentConfig.decode` |
| `activation_timeout_s`, `ttl_ms`, `cancel_grace_s` | scalars | same-named knobs |
| `intents_to`, `traces_to`, `errors_to` | URI strings | same-named fields, verbatim |
| `hitl` | mapping | `HitlPolicy(...)` |
| `tool_registry` | reference | `AgentConfig.tool_registry` (a prebuilt `ToolRegistry` instance, [registry.py](../../../src/beam_agents/tools/registry.py:203)) |
| `key_field`, `payload_field`, `event_time_field` | strings | row-boundary adaptation (D5) |

`hitl` maps `timeout_ms`, `intent_ttl_ms`, `approval_channel`, `max_escalations` as scalars and `on_timeout` as an optional reference to a module-level route function — the picklability/purity constraints `HitlPolicy` already documents apply unchanged, and the reference machinery is exactly D2's. Nested unknown keys inside `hitl` are rejected the same way top-level ones are: `ValueError` listing the valid keys, so a typo (`ttl` for `ttl_ms`) is caught at the document, not defaulted over. `sink_resolver` is deliberately *not* exposed: it is an advanced Python seam, and a YAML pipeline that needs a custom resolver has crossed back into Python authoring.

Everything downstream of the mapping is delegated, not duplicated: scalar-range validation stays in `AgentConfig.__post_init__`/`HitlPolicy.validate`, and sink-URI validation stays in the resolver — the YAML layer adds *shape* validation (key set, reference grammar, mapping types) and forwards, so the two layers cannot drift apart on what a valid config is.

### D5. The transform adapts rows to envelopes on the way in and protos to rows on the way out

Input: the wrapped transform reads `key_field` (default `"key"`) and `payload_field` (default `"payload"`) off each row, encodes a `str` key as UTF-8 bytes (bytes pass through), and builds `AgentEnvelope(entity_key=..., event_time_ms=..., external_event=<payload bytes>)`, keying with the same `WithKeys(...).with_output_types(tuple[bytes, AgentEnvelope])` idiom the core transform documents — so `RunAgent`'s KV validation sees exactly the shape it requires. `event_time_ms` comes from `event_time_field` when configured, else the element timestamp. A row missing the configured fields is a data error, not a config error: it routes to the `errors` output as a malformed-input record rather than crashing the bundle, consistent with the project's route-element-failures-to-errors rule.

Output: YAML consumers need schema'd rows, and two of the four streams already have row mappers shipped for the BigQuery sinks — `trace_event_to_row` and `activation_error_to_row`, imported today by [`core/transform.py`](../../../src/beam_agents/core/transform.py:45). The YAML transform reuses those for `traces`/`errors`, adds the equivalent scalar-field mapping for `intents` (`intent_id`, `entity_key`, `seq`, `step_index`, `tool_name`, `args_json`, `created_at_ms`, `expires_at_ms`, `attempt`, `kind`), and emits the main output as `Row(key: bytes, output: bytes)` — the runtime imposes no schema on agent output bytes, so none is invented; a downstream YAML `MapToFields` can decode them. Sink URIs (`intents_to` etc.) remain the recommended drain path from YAML, in which case those tagged row streams simply go unconsumed; the row conversion exists so consuming them in-DAG is *possible*, not mandatory.

*Alternative considered:* requiring upstream YAML steps to hand-build `KV[bytes, AgentEnvelope]`. Rejected: proto-typed KVs are not expressible in ordinary YAML steps, so the requirement would make the provider unusable without a Python escape hatch — the exact thing this change removes.

### D6. Outputs are named `output`, `intents`, `traces`, `errors`, matching `RunAgentOutputs`

The YAML-facing transform exposes the wrapped `RunAgentOutputs` under Beam YAML's multi-output convention with the same four names the Python surface uses, `output` being the main stream. A YAML consumer addresses the non-main streams by qualified name (Beam YAML's `TransformName.output_name` input addressing). Keeping the names identical to the [`RunAgentOutputs`](../../../src/beam_agents/core/transform.py:438) attributes means docs, traces, and both surfaces agree on vocabulary. The conditional `dead_letter` stream (present only when `intents_to` resolves to the outbox writer) is exposed under that same name when it exists; whether a conditionally-present named output is idiomatic in Beam YAML is flagged in Open Questions, with the fallback being to expose it only when `errors_to` is unset (mirroring the core transform's merge behavior).

The exact mechanism by which a Python-provider transform declares multiple outputs (returning a `dict[str, PCollection]` from `expand`, a `DoOutputsTuple`, or a provider-level declaration) is a Beam-YAML surface detail verified during implementation — Open Questions — but the *requirement* (four addressable named outputs with these names) does not depend on which mechanism it is.

### D7. Provider packaging: a documented `providers:` block first, a packaged listing file second, discovery magic not at all (yet)

The primary, guaranteed-to-work path is an explicit block in the user's pipeline document, shaped per Beam's provider docs:

```yaml
providers:
  - type: python
    config:
      packages: ["beam-agents==0.1.0"]
    transforms:
      RunAgent: "beam_agents.yaml.run_agent"
```

(The exact `type:` spelling and config schema are confirmed against the pinned Beam version during implementation; the docs show whatever the verified form is.) This is why the change depends on C25: `packages:` must name something installable.

Secondarily, the package ships that same mapping as a static provider-listing file under `beam_agents/yaml/` so users who launch with Beam YAML's provider-inclusion mechanism can reference one canonical listing instead of copying the block. Automatic discovery (entry-point registration so `RunAgent` appears with no `providers:` block at all) is deliberately deferred: whether Beam YAML consults an entry-point group, and which one, is unverified (Open Questions), and an explicit block is the honest v0 — visible provenance for what D2 calls out as code-executing configuration.

## Risks / Trade-offs

- **Beam YAML's provider surface moves between Beam releases** → the blast radius is confined by D1: our deliverable is a constructor with a frozen keyword surface; only the e2e test and the docs' `providers:` block touch `apache_beam.yaml`. A Beam upgrade that changes provider declaration syntax is a docs-and-one-test change, not an API break.
- **Secrets in YAML documents** (`provider_config.api_key`) → the docs steer to environment-side templating (Beam YAML's jinja-style substitution where available, or generating the document) and never show a literal key. The runtime already keeps credentials out of request material (`LlmRequest` carries no API key); nothing here weakens that. A first-class secret-reference arm (`env:VAR`) is listed in Open Questions rather than invented.
- **Importing references executes code** → same trust boundary as the `packages:` list itself (D2), stated explicitly in docs; no dynamic-code arm in the grammar keeps the document reviewable.
- **A resolved object can be plausible but wrong** (a sync callable, a factory returning a non-`LLMClient`) → construction-time checks are structural and shallow by design; the DoFn's existing runtime error paths (routing to `errors`) are the backstop, and the e2e scenario pins the happy path so regressions in the wiring are caught offline.
- **Row-field conventions (`key_field`/`payload_field`) are a new micro-contract** → defaults match the obvious column names, misconfiguration fails per-element into `errors` with a reason naming the missing field, and the contract is spec'd, not implicit.
- **Two config-validation layers could drift** → D4 delegates all value validation downward and owns only shape; a test asserts that a config valid at the YAML layer constructs a valid `AgentConfig` for every mapped field (the round-trip scenario), pinning the seam.
- **The e2e suite needs `apache_beam.yaml`'s extras in the test env** → confined to one module with a clean skip if the import is unavailable would hide a broken gate; instead the dependency is declared in the test group (Impact) so the suite always runs in CI. If the extra proves heavy, the fallback is constructing the provider objects directly rather than via the YAML CLI path — still parsing a real YAML document.

## Migration Plan

Purely additive: a new subpackage, a new doc, no change to any existing module's behavior, no wire/state/proto movement, no public-surface change to `beam_agents/__init__.py` (the YAML constructor is addressed by its own fully-qualified name — that is the point of it). Rollout order: reference resolver → config mapping → wrapping transform → provider listing + docs, each behind its failing-first tests. Rollback is a revert with no data implications. Nothing ships to users until C25 publishes an installable package; if C25 slips, this change still merges and is exercised by its in-repo tests.

## Open Questions

- **Exact Python-provider declaration for the pinned Beam version.** Which provider `type:` value(s) apply (`python` vs. package-installing variants), the precise `config:` schema, and whether construction happens in-process or via an auto-managed expansion service — to be verified against `apache_beam.yaml`'s provider module for the `apache-beam[gcp]>=2.60` floor before the docs' block is written.
- **Multi-output declaration mechanism for Python-provider transforms.** Whether named outputs surface from a `dict[str, PCollection]` return, a `DoOutputsTuple`, or provider metadata — and the exact consumer-side addressing syntax to document (`RunAgent.errors` assumed).
- **Provider discovery.** Does Beam YAML support entry-point-based provider registration, and if so under which group? Deferred (D7); the listing file is designed so discovery can be added without changing the constructor.
- **Conditional `dead_letter` output.** Is a sometimes-present named output acceptable to Beam YAML consumers, or should it always exist (empty when no outbox writer resolved)?
- **Secret handling.** Whether to add an interpolating arm for provider config values (e.g. `env:ANTHROPIC_API_KEY` resolved at worker start) or rely wholly on document templating. Worker-side resolution interacts with `provider_factory` pickling and belongs to a follow-up if wanted.
- **Test-environment dependency.** Whether `apache_beam.yaml` imports cleanly under the existing `apache-beam[gcp]` pin or the test group must mirror Beam's `yaml` extra (and how heavy that is).
- **Shorthand provider names.** Whether a curated alias table (`anthropic`, `openai_compat`, `fake`) is worth adding on top of the uniform reference convention once real YAML usage exists (D3).
