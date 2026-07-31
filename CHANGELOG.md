# Changelog

All notable changes to `beam-agents` are recorded here. Entries are written at
change time as [`changelog.d/`](changelog.d/README.md) fragments and assembled
into a dated section by `make changelog` when a release is cut.

The project is pre-1.0 and versioned `0.MINOR.PATCH`: a MINOR release may add
features and may break the documented compatibility surface (every break gets a
**Breaking changes** entry naming the migration); a PATCH release contains only
fixes and documentation. See [`docs/releasing.md`](docs/releasing.md) for the
compatibility surface and the release procedure.

<!-- towncrier release notes start -->

## 0.1.0 - 2026-07-30

First public release. This section is hand-curated: it summarizes the
capability set built before changelog fragments existed (the nine changes in
[`openspec/changes/archive/`](openspec/changes/archive/) plus the runtime,
adapter, effector, and observability work merged on top of them). Mechanical
assembly from `changelog.d/` applies from 0.2.0 onwards.

### Added

- **`RunAgent`, the core transform.** `events | RunAgent(my_agent)` turns an
  agent into a keyed, stateful Beam step with four named outputs — `.output`,
  `.intents`, `.traces`, `.errors`. `AgentConfig` bundles the model-provider
  factory, runtime knobs, and sink URIs; misconfiguration (including non-KV
  input) raises at pipeline-construction time with an actionable message.
- **Stateful DoFn runtime with durable keyed memory.** Protobuf state (never
  pickle) across working memory, continuations, the replay cache, pending
  intents, and a per-key activation counter, with watermark-driven TTL
  collection. Every effect an activation produces is staged and committed
  atomically with the Beam bundle: a failed or timed-out activation mutates
  nothing.
- **Effectively-once side effects via intents.** Side-effecting tools never run
  inside the pipeline. `ctx.act(...)` emits a `ToolIntent` with a deterministic
  `intent_id`, so a replayed bundle that walks the same path produces
  byte-identical intents and the effector deduplicates on them.
- **Reference effector service** (`beam-agents-effector`, `effector` extra):
  consumes intents from Kafka or Pub/Sub, deduplicates against Redis or
  Bigtable, executes the tool, and publishes results back onto the bus for
  re-injection on the same key.
- **Human-in-the-loop suspension.** A `HitlPolicy` on the config sets the
  suspension timeout and intent TTL, names the approval channel, and decides
  what a timed-out suspension does via a pure routing function returning
  `Deny`, `Drop`, or `Escalate`. Timeouts fail closed at both layers: the timer
  fires *and* the effector refuses expired intents.
- **LLM replay cache.** Every model call is keyed by model, canonicalized
  messages, tool schemas, sampling parameters, key, and `seq`, and memoized in
  keyed state (bounded LRU with a TTL and a blob cap). Bundle retries incur
  zero additional provider calls on the cached path.
- **Async `LLMClient` facade with real providers.** One provider-neutral entry
  point owning replay-cache short-circuiting, typed retry with
  `Retry-After`-honoring jittered backoff, per-endpoint circuit breaking, usage
  accounting, constrained JSON outputs, and trace points — with Anthropic,
  OpenAI-compatible, and vLLM (endpoint or GPU-worker sidecar, `vllm` extra)
  providers behind it.
- **`FakeLLM`,** the scripted, request-recording model used by every test tier
  and by the runnable examples, so the whole suite runs offline.
- **`@tool` registry.** Machine-readable schemas for provider tool-calling,
  argument validation, and a hard, enforced line between read-only tools that
  run inline and `side_effect=True` tools whose direct invocation raises.
- **Memory facade and long-term stores** over Bigtable, Redis, Firestore, and
  SQLAlchemy, with a soft working-memory cap and a compaction hook.
- **LangGraph adapter** (`langgraph` extra): `LangGraphAgent`, a Beam-state
  checkpoint saver, and `interrupt` → intent translation, so a LangGraph graph
  runs under the same durability and re-injection rules as a native agent.
  `import beam_agents` never imports the framework.
- **Observability.** OpenTelemetry GenAI-shaped trace events, runtime metrics
  surfaced to runner dashboards, a schema'd BigQuery trace sink, and a batched
  non-blocking OTLP/HTTP exporter (`otlp` extra).
- **Typed error routing.** Element failures never crash the pipeline: they land
  on the `errors` output as typed protobuf error records, with orphaned
  re-injected results distinguished from activation failures.
- **Protobuf wire and state schemas** with Beam coders, generated bindings
  committed and regeneration checked for drift in CI, and additive-only
  evolution guarded by `state_schema_version` and golden-blob compatibility
  tests.
- **The type marker.** The wheel ships `py.typed`; the package is fully
  annotated and checked under `mypy --strict`.

### Documentation

- The documentation site at <https://ardada2468.github.io/beam-agents/>, built
  strictly from `docs/` with three runnable, offline, FakeLLM-driven examples
  rendered verbatim from `examples/`.

### Notes on supported versions

- Python 3.11 and 3.12 (`requires-python = ">=3.11,<3.13"`). Python 3.10 was
  dropped before the first release: `asyncio.TimeoutError` and the builtin
  `TimeoutError` are distinct classes there, and `apache-beam[gcp]`'s own
  dependency chain was already sunsetting it.
- Runners: DirectRunner, Dataflow, and Flink are supported; Spark is
  best-effort.
