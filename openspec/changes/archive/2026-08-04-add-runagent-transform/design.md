## Context

The runtime engine (`_AgentDoFn`, `stateful-agent-runtime`) is complete: it consumes a keyed `AgentEnvelope`, runs one activation per element on the async bridge, and emits four outputs (`output` main, plus `intents`/`traces`/`errors` tagged). What is missing is the *public* boundary the whole project promises in `project.md`: `RunAgent`/`AgentConfig` re-exported from `beam_agents/__init__.py`.

Today `RunAgent` (`core/transform.py`) is a placeholder: it takes loose keyword arguments, keys elements itself with an internal `beam.Map(_key_by_entity)`, and returns Beam's raw `DoOutputsTuple`. Three gaps make it unfit as the public surface:

1. **Keying is the caller's job, not the transform's.** The documented Dataflow shape `Kafka/PubSub events + tool-results + approvals ─► WithKeys(entity_id) ─► Flatten ─► RunAgent` means multiple upstream sources are flattened and keyed *before* `RunAgent`. A transform that re-keys a single already-normalized stream cannot express that topology, and it hides the hard SDK constraint (`project.md`: "stateful DoFn requires KV input").
2. **Misconfiguration fails late.** A non-KV input or a typo'd sink URI currently surfaces as an opaque runner error inside a bundle. `project.md` mandates: "raise `ValueError` at pipeline-construction time for misconfiguration (e.g., non-KV input) with actionable messages."
3. **Outputs are untyped and sink-unaware.** Callers index a `DoOutputsTuple` by tag string, and there is no declarative way to say "drain intents to my outbox topic" — the `intents ─► outbox topic`, `traces ─► OTLP/BigQuery`, `errors ─► dead-letter sink` wiring is left entirely manual.

**Constraints (load-bearing):**
- Beam Python **stateful DoFns require KV input**; the key coder must be the deterministic proto/bytes coder, never pickle.
- Unit tier **must pass offline with no docker**; concrete Kafka/Pub/Sub/BigQuery IO cannot be imported or exercised in unit tests, so sink resolution must be injectable/stubbable.
- Public API surface is exactly what `beam_agents/__init__.py` re-exports (`project.md`); this change is what finally anchors it, retiring `tests/test_import.py::test_public_surface_is_empty`.
- Construction-time errors must be `ValueError` with actionable messages (`project.md` code-style rule).

## Goals / Non-Goals

**Goals:**
- Introduce `AgentConfig`, a frozen self-validating config bundling `provider_factory`, runtime knobs (`activation_timeout_s`, `ttl_ms`, `cancel_grace_s`), and optional sink URIs (`intents_to`, `traces_to`, `errors_to`).
- Reshape `RunAgent` to consume a pre-keyed `PCollection[KV[bytes, AgentEnvelope]]`, validating the KV shape at pipeline-construction (`expand`) time.
- Expose the four outputs as a typed `RunAgentOutputs` (`.output`/`.intents`/`.traces`/`.errors`), keeping the tag names `output`/`intents`/`traces`/`errors`.
- Resolve configured sink URIs to Beam write transforms via a pluggable `SinkResolver` and attach each to its tagged output.
- Reject bad config early: non-positive knobs and unknown/malformed sink URI schemes at `AgentConfig` construction; non-KV input at `RunAgent.expand`.
- Re-export `RunAgent`, `AgentConfig`, `RunAgentOutputs` from `beam_agents/__init__.py`.

**Non-Goals:**
- The `_AgentDoFn` internals, its state/timer topology, and its four-output behavior (consumed unchanged from `stateful-agent-runtime`).
- Production IO for the resolved schemes end-to-end (KafkaIO/PubsubIO/BigQueryIO wiring is exercised in the integration tier; this change delivers the resolver seam and the default scheme→transform mapping, not integration-verified sinks).
- Upstream normalization/Flatten/`WithKeys` helpers (the caller owns keying; we only validate its result).
- The effector, outbox semantics, and OTel/BigQuery trace exporters (separate capabilities).
- Retry/backoff, replay-cache, and memory-facade behavior (consumed as-is).

## Decisions

### D1. `RunAgent` consumes pre-keyed `KV[bytes, AgentEnvelope]`; keying is the caller's responsibility
`RunAgent` drops the internal `beam.Map(_key_by_entity)`. Its input is `PCollection[KV[bytes, AgentEnvelope]]`, where `bytes` is the `entity_key`. Callers `Flatten` their event/result/approval streams and `WithKeys(entity_id)` upstream, exactly as the `project.md` Dataflow diagram shows. `expand` inspects the input's element type hint and raises `ValueError` when it is not a two-tuple/`KV` of `(bytes, AgentEnvelope)`, with a message naming the expected shape and pointing at `WithKeys`.

*Alternative considered:* keep re-keying a single `PCollection[AgentEnvelope]` inside `RunAgent`. Rejected — it cannot express the multi-source Flatten (events + tool-results + approvals arrive on different topics), it silently swallows the KV requirement instead of enforcing it, and it forecloses letting callers choose their own key extraction.

### D2. `AgentConfig` is a frozen dataclass that validates in `__post_init__`
`AgentConfig(provider_factory, *, activation_timeout_s=30.0, ttl_ms=3_600_000, cancel_grace_s=5.0, intents_to=None, traces_to=None, errors_to=None)` is `@dataclass(frozen=True, slots=True)`. `__post_init__` raises `ValueError` for non-positive `activation_timeout_s`/`ttl_ms`/`cancel_grace_s` and for any sink URI whose scheme the resolver does not recognize or that is malformed. Validating at *config construction* puts the error at the site of the typo (in user setup code), the earliest possible point — before a pipeline object even exists.

*Alternative considered:* validate everything inside `RunAgent.expand`. Rejected for the knobs and sink URIs — the config is constructed before the transform, so `__post_init__` catches errors strictly earlier and closer to the offending literal. (KV-input validation *must* stay in `expand` per D1, since it needs the upstream `PCollection`.)

### D3. Sink URIs resolve through a pluggable `SinkResolver`; schemes are validated eagerly, transforms built lazily
A `SinkResolver` maps a URI to a `beam.PTransform` write. `AgentConfig` holds a resolver (default: `DefaultSinkResolver`) and, at construction, asks it only to *validate* each configured scheme (`supports(uri) -> bool` / parse) — cheap, import-free, offline. The heavy `beam.PTransform` is built in `RunAgent.expand` (`resolve(uri) -> PTransform`), so no Kafka/Pub/Sub/BigQuery client is imported at config time. The default resolver recognizes a documented scheme set (`kafka://`, `pubsub://`, `bigquery://`); tests inject a stub resolver that returns an in-memory sink. Unknown scheme → `ValueError` at construction (D2).

*Alternative considered:* hardcode `KafkaIO`/`PubsubIO`/`BigQueryIO` construction directly in `RunAgent`. Rejected — it couples the public transform to heavy optional IO dependencies, makes the unit tier un-runnable offline, and gives no seam for alternate sinks. A resolver keeps the public surface thin and the scheme set extensible.

### D4. The four outputs are a typed `RunAgentOutputs`, not a raw `DoOutputsTuple`
`RunAgent.expand` returns `RunAgentOutputs`, a small frozen wrapper exposing `.output` (the main `PCollection`), `.intents`, `.traces`, `.errors` as named `PCollection` attributes. Tag names stay `output`/`intents`/`traces`/`errors` on the underlying `with_outputs(...)`. This replaces stringly-typed `tuple["intents"]` access with attribute access that type-checkers and readers can follow.

*Alternative considered:* return the bare `DoOutputsTuple`. Rejected — positional/string access is error-prone, it has no natural place to hang sink attachment, and it does not read as a public API.

### D5. A resolved sink is a terminal side-branch; the tagged `PCollection` stays observable
When `intents_to`/`traces_to`/`errors_to` is set, `expand` applies the resolved write transform to that tagged `PCollection` as a **terminal branch** (the write returns `PDone`/`None`), while `RunAgentOutputs` still exposes the original tagged `PCollection`. So a sink being wired never hides the stream from a downstream step or a `TestPipeline` assertion; an unset sink simply leaves the tagged `PCollection` exposed for the caller to wire manually. Each sink attaches only to its own tag: `intents_to`→`.intents`, `traces_to`→`.traces`, `errors_to`→`.errors`; `.output` is never auto-sunk (terminal outputs are the caller's to route).

*Alternative considered:* have `RunAgentOutputs.intents` become the sink's `PDone` when a sink is configured. Rejected — it makes the type of an attribute depend on config and breaks tests/consumers that read the stream.

### D6. `RunAgent(agent, *, config)`; the agent is the subject, the config is the wiring
`RunAgent.__init__(self, agent, *, config: AgentConfig)`. The agent is the positional subject of the transform; `AgentConfig` carries provider + knobs + sinks. `expand` builds `_AgentDoFn` from the agent plus the config's knobs, unchanged from how it is constructed today. This keeps `AgentConfig` reusable across agents and keeps the call site readable.

*Alternative considered:* fold the agent into `AgentConfig`. Rejected — the agent is what the transform *runs*, not configuration; separating them lets one config wire many agents and reads naturally as `events | RunAgent(agent, config=cfg)`.

## Risks / Trade-offs

- **[KV-shape detection is best-effort — Beam type hints can be absent or erased]** → Validate against the declared element type hint when present and, failing that, require the upstream to carry a KV/tuple coder; the message tells the caller to `WithKeys(...)`/`.with_output_types(tuple[bytes, AgentEnvelope])`. We reject the clearly-non-KV case (single `AgentEnvelope`) rather than guaranteeing detection of every erased hint; the stateful DoFn still fails closed downstream if an untyped input slips through.
- **[Sink resolution imports heavy IO / breaks offline unit tier]** → Scheme validation at config time is import-free; transform construction is deferred to `expand` and only runs when a sink is configured. Unit tests inject a stub `SinkResolver`; real IO is integration-tier only.
- **[BREAKING change strands existing call sites]** → This is the first release to expose `RunAgent` publicly (root package was intentionally empty), so blast radius is internal call sites and tests only; all are updated in this change. No external consumers exist yet.
- **[A configured sink silently swallows a stream needed downstream]** → D5 keeps the tagged `PCollection` observable; the sink is an additional terminal branch, never a replacement for the exposed output.
- **[Scheme set drifts from what the effector/exporters actually consume]** → The resolver's supported-scheme set is documented and centralized; adding a scheme is a resolver change, not a `RunAgent` change, and unknown schemes fail loudly at construction rather than being dropped.

## Migration Plan

New public surface; no runtime state migration. Rollout: (1) add `AgentConfig`, `RunAgentOutputs`, and `SinkResolver`; (2) reshape `RunAgent` to KV-in / typed-out with construction-time validation; (3) update internal call sites and tests to key upstream (`WithKeys`) and pass an `AgentConfig`; (4) re-export the three names from `beam_agents/__init__.py` and delete `test_public_surface_is_empty`. Rollback = revert; no persisted state or wire schema changes. The BREAKING input-shape change is safe because `RunAgent` had no external consumers prior to this release.

## Open Questions

- Exact supported scheme set for the default resolver at v1 (`kafka://`, `pubsub://`, `bigquery://` are the documented targets; whether `file://`/`text://` dead-letter and an OTLP scheme land in this change or a follow-up). **Leaning:** ship scheme *validation* + the seam here with the three documented schemes; land concrete integration-verified IO per-scheme in follow-ups.
- Whether `AgentConfig` should also carry the deterministic-coder registration toggle or leave `register_coders()` implicit in `expand` (current behavior). **Leaning:** keep coder registration implicit in `expand`; it is not user-tunable.
- Whether KV-shape validation should hard-fail on an erased/absent element type hint or only warn. **Leaning:** hard-fail only on a positively-non-KV hint; absent hints pass construction and rely on the DoFn's downstream KV requirement.
