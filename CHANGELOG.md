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

## 0.3.0 - 2026-07-31

The **M2 milestone release**. It closes out the nine M2 changes, records how
design-partner feedback was triaged, and publishes the first benchmark
comparison against Apache Flink Agents.

The **Added** and **Documentation** subsections below are assembled
mechanically from `changelog.d/`. The three subsections after them — the M2
batch, the release gate, and the feedback dispositions — are the milestone
record the `release-0-3` capability requires, and are hand-curated.

### The M2 batch

The nine changes this milestone is defined as closing out. Several landed
before the `0.1.0` section was curated and are described there as part of the
first public release; they are enumerated here regardless, because a milestone
whose contents a reader has to reconstruct from two sections is not a release
note.

| Roadmap | Change | What it delivered |
| ------- | ------ | ----------------- |
| C26 | `add-vllm-provider` | vLLM as a first-class provider in both shapes — an unauthenticated, base-URL-mandatory endpoint client and an in-process GPU-worker sidecar sharing one engine per worker via `beam.utils.shared.Shared` (`vllm` extra). |
| C27 | `add-adaptive-batching` | `BatchPolicy.ADAPTIVE`: per-key event bursts buffered behind a size/`FLUSH_TIMER` trigger and run as one activation that suspends and resumes as a unit. |
| C28 | `add-token-budgets` | `max_tokens_per_activation` with fail-fast `BudgetExceeded`, charged across replay-cache hits so a retried bundle makes the identical decision. |
| C29 | `add-longterm-memory-stores` | Long-term `MemoryStore` backends over Bigtable, Redis, Firestore, and SQLAlchemy, idempotent on `(key, seq)` (`memory-stores` extra). |
| C30 | `add-compaction-strategies` | Real compaction in the memory facade's long-dead seam, so a key that keeps appending compacts instead of marching into permanent `MemoryOverflow`. |
| C31 | `add-adk-adapter` | Google ADK agents under the same durability and re-injection rules as a native agent, with the import boundary preserved (`adk` extra). |
| C32 | `add-state-schema-migration` | `state_schema_version` finally *read*: lazy per-message migrations, a refusal path for future versions, and a per-version golden corpus. The gate any future breaking proto change must pass through. |
| C33 | `add-benchmark-harness` | The pyperf suite (`make bench`) and `scripts/bench_gate.py` — where the p50 < 15 ms / p99 < 60 ms overhead budget is actually rendered, plus the committed-median ratchet. |
| C34 | `add-hot-key-sharding-guidance` | `shard_key`, `unshard_key`, and `ShardKeys` for fan-out across a hot entity, with the memory-free-only safety contract stated in the module, the docs, and a test that demonstrates the failure mode. |

### Added

- Adaptive batching: set `AgentConfig(batch_policy=BatchPolicy.ADAPTIVE, max_batch_size=..., max_wait_ms=...)` and `RunAgent` buffers each key's event burst and runs it as one activation, with `ctx.event` presented as a `list[bytes]` (and a uniform `ctx.events` accessor). A batch suspends and resumes as a unit, buffered events are bounded by `max_buffered_events` and reported on `.errors` if dropped, and four new `beam_agents.runtime` metrics (`events_buffered`, `batch_flushes_size`, `batch_flushes_timer`, `batch_size`) make the batching ratio visible. The default `BatchPolicy.NONE` keeps today's per-event behavior unchanged. See `docs/batching.md`. (add-adaptive-batching)
- The fraud-triage example now ships as a Dataflow Flex Template
  (`examples/fraud_triage_dataflow/`): one `gcloud dataflow flex-template run`
  puts it on Dataflow with topics, provider reference and human-approval deadline
  supplied as parameters, all in the same URI and `module:object` grammars the
  Python and YAML surfaces use. Provider API keys are supplied as Secret Manager
  version resource names and resolved on the worker — never as launch parameters. (add-dataflow-flex-template)
- You can now get one entity's runtime state out of a running pipeline and re-run
  its activation offline. Publish an `AgentEnvelope` carrying the new
  `export_request` payload to the events topic and `RunAgent` answers with a
  `StateSnapshot` on its new `.snapshots` output (routed by `AgentConfig
  .snapshots_to`, exactly like `traces_to`), without running an activation or
  mutating a single state cell. The new `beam-agents-replay` console script then
  reconstructs that activation from the snapshot, its trace stream, and the
  triggering envelope, and re-runs it locally against a provider that holds no
  transport: every model call is served from the snapshot's replay cache, a miss
  fails loudly naming the cache key instead of reaching a network, and the re-run
  is diffed against the traced record with scriptable exit codes (0 reproduced,
  1 diverged, 2 usage or version refusal, 3 irreproducible). Snapshots from older
  schema versions migrate on load through the same migrations the pipeline
  applies; newer ones are refused. See [docs/replay.md](https://github.com/ardada2468/beam-agents/blob/main/docs/replay.md). (add-replay-cli)
- Token budgets: set `AgentConfig(max_tokens_per_activation=..., decode=...)` and an activation that crosses its bound fails fast with `BudgetExceeded` (importable from `beam_agents.model`), dead-lettering to `.errors` with the new reason `budget_exceeded` and committing nothing. The budget bounds one activation attempt, charges every response the agent consumes — replay-cache hits included, so a retried bundle makes the identical decision — and a swallowed trip can never spend again. Two new `beam_agents.runtime` distributions, `prompt_tokens` and `completion_tokens`, publish the billed input/output split that provider price sheets are quoted in. Unset (the default) is unlimited and unchanged. See `docs/errors.md` and `docs/metrics.md`. (add-token-budgets)
- Pipelines can now be written in [Beam YAML](https://github.com/ardada2468/beam-agents/blob/main/docs/yaml.md): declare a `python` provider mapping `RunAgent` to `beam_agents.yaml.run_agent`, name your agent and provider factory with `module:object` references, and the transform keys and envelopes your rows for you and returns `output`/`intents`/`traces`/`errors` as addressable named row streams. References resolve at document-expansion time, so a typo'd module, a missing attribute, or an unknown config key fails before the pipeline is submitted rather than inside a bundle. (add-yaml-provider)
- `beam-agents` is now published to PyPI: `pip install beam-agents` (with the `effector`, `langgraph`, `otlp`, and `vllm` extras). Releases are cut by pushing a `vX.Y.Z` tag, which builds, verifies, and publishes the distributions via PyPI trusted publishing; [`docs/releasing.md`](https://github.com/ardada2468/beam-agents/blob/main/docs/releasing.md) documents the pre-1.0 versioning policy and the compatibility surface a `0.MINOR` bump is allowed to break. (add-0-1-0-release)

### Documentation

- New [state-compatibility policy](https://ardada2468.github.io/beam-agents/state-compat/): what beam-agents promises about keyed state across releases (state written by release N is readable by N+1, and Dataflow `--update` between adjacent releases is supported), what it explicitly does not promise (skip-level updates, downgrades, cross-version byte-identity, Flink savepoints), and a table classifying every schema, coder and graph-shape change an author can make. A nightly Dataflow `--update` gate now proves the promise on a real job carrying a live suspension and populated working memory across the version hop, and a red gate blocks cutting a release. (add-state-guarantees)
- New upstreaming artifacts under `docs/design/`: a Beam-community design document proposing an `apache_beam.ml.agents` package — the runtime-not-framework principle, the seven correctness invariants, the two execution paths, the keyed-state and timer layout under the Beam Python SDK's real constraints, the outbox/effector effectively-once model with its honest duplicate window, the adapter conformance matrix as the compatibility story, and a module-by-module record of what would move upstream and what would stay external — plus a dev@beam.apache.org thread plan pairing the announcement draft with an objections register. The design document's evidence section carries a thread-ready checklist rather than figures: no number appears without an artifact behind it, and the announcement is blocked on that checklist. (add-upstream-design-doc)

### Benchmarks

- **First published comparison against Apache Flink Agents**:
  [`docs/benchmarks/0.3.0-vs-flink-agents.md`](docs/benchmarks/0.3.0-vs-flink-agents.md),
  versioned with this release and frozen at publication — later performance
  changes appear in a later release's report, never by editing this one. It
  pairs the C33 harness's gated `overhead_50ms` scenario with its nearest
  idiomatic Flink Agents equivalent, runs a scripted fake model of equal cost on
  both legs so the figures are runtime overhead rather than provider latency,
  and enumerates every dimension on which the two systems are not like-for-like
  with a statement of which side each favors.

  Its **measurement tables are published empty, marked `pending (CI hardware)`.**
  `benchmark-baseline.toml`'s `[medians_ms]` is deliberately unseeded and
  `docs/benchmarks.md` forbids seeding from developer hardware; a competitive
  comparison naming another Apache project is the last place to relax that rule.
  The methodology, the pairing, the version pins, and the honesty rules are
  final at 0.3.0; only the numbers wait on a CI-hardware run.

### Release gate

Evaluated as a whole at the release-candidate commit and recorded here with its
evidence, per this milestone's design decision D5. **Status: not yet fully
green — `v0.3.0` is not tagged.** The gate does not bend: partial shipping
("tag now, fix the red cell in 0.3.1") is not available, so an unmet condition
slips the release rather than shrinking it.

| Gate condition | Evidence | Verdict |
| -------------- | -------- | ------- |
| All nine M2 changes archived | All nine are implemented, gated, and merged; their change folders are still live under `openspec/changes/` awaiting the archive step | pending (archival) |
| C33 benchmark regression gates green — overhead p50 < 15 ms and p99 < 60 ms per activation, excluding LLM and tool time | Requires the nightly `bench` job on a GitHub-hosted runner; `benchmark-baseline.toml`'s `[medians_ms]` is unseeded and developer-hardware figures are inadmissible | pending (CI hardware) |
| Adapter conformance matrix green on both legs, no cell newly skipped | DirectRunner leg rides the required offline `ci` semantics selection; Flink leg is `make test-conformance-flink` in the `integration` workflow — both need a run at the candidate commit | pending (CI run) |
| Every release-blocking feedback fix archived | Intake list is empty, so the blocking bucket is empty — see the dispositions below | pass |
| Offline gate roster green at the candidate commit | `make lint`, `make type`, `make test-unit`, `make test-semantics-offline`, `make coverage-ratchet` | pass |

### Design-partner feedback

Triaged through this milestone's rubric (design decision D2), which has exactly
two buckets:

- **Release-blocking fix** — *if and only if* the item evidences a violation of
  a correctness invariant documented in `openspec/project.md`, loss or
  corruption of user data or state, a break in pipeline-`--update` state
  compatibility, or a security defect. Each gets its own OpenSpec change folder
  and must be archived before the release gate can pass.
- **Follow-up OpenSpec change** — everything else: feature requests,
  ergonomics, documentation gaps, and performance short of the stated budget.
  Captured as a proposed change or roadmap entry targeting a post-0.3.0
  milestone, so the request is durable without holding the release hostage.

The bar is deliberately anchored to that invariant list rather than to severity
adjectives, so "release-blocking" cannot mean "a partner wants it".

| Item | Bucket | Rationale | Disposition |
| ---- | ------ | --------- | ----------- |
| *No design-partner feedback items were received during the 0.1.x cycle.* | — | The 0.1.0 publish step is still blocked on the one-time PyPI project registration and trusted-publisher binding (`add-0-1-0-release`, tasks 5.1/5.2/5.4), so no design partner has yet run a released build. | Intake list empty; no item to bucket. |

An empty table is a disposition. An absent one would be a process failure.

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
