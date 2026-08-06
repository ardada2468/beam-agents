# Changelog

All notable changes to `beam-agents` are recorded here. Entries are written at
change time as [`changelog.d/`](changelog.d/README.md) fragments and assembled
into a dated section by `make changelog` when a release is cut.

Every `0.MINOR.PATCH` section below was written under the pre-1.0 policy: a
MINOR release could add features and break the documented compatibility surface
(every break gets a **Breaking changes** entry naming the migration), and a
PATCH release contained only fixes and documentation. From `1.0.0` that
latitude is gone — see [what the 1.0.0 number promises](#what-the-100-number-promises).
[`docs/releasing.md`](docs/releasing.md) carries the compatibility surface and
the release procedure.

<!-- towncrier release notes start -->

## 1.0.0 - 2026-07-31

The **M4 milestone release**, and the first release whose version number is a
promise rather than a snapshot. It closes out the four M4 hardening changes,
which together are the *stability surface*: what the public API is and how it is
allowed to change (`add-1-0-api-freeze`), what keyed state survives a version
hop (`add-state-guarantees`), who is authorized to make the effector act
(`add-effector-security`), and where Spark stands (`promote-spark-runner`).

The **Breaking changes** subsection below is assembled mechanically from
`changelog.d/`. The three subsections around it — the M4 batch, what the number
promises, and the release gate — are the milestone record the `release-1-0`
capability requires, and are hand-curated.

### The M4 hardening batch

The four changes this milestone is defined as closing out. Only one of them
(`add-1-0-api-freeze`) wrote a changelog fragment that assembly rendered below;
`add-effector-security`'s fragment was consumed into the `0.5.0` section, and
`add-state-guarantees`' into `0.3.0`, while `promote-spark-runner` is test and
CI infrastructure that landed with no fragment at all. Mechanical assembly
therefore could never have produced a complete M4 section, which is why this
table is not optional.

| Roadmap | Change | What it delivered |
| ------- | ------ | ----------------- |
| C44 | `add-effector-security` | Application-level authenticity for the effects loop: `ToolIntent` gains an additive HMAC-SHA256 signature envelope, `WriteIntents` signs at the outbox writer, and the effector verifies **before** every other phase — ahead of even expiry refusal, so an unauthenticated message drives no behavior at all. Failures dead-letter and never publish a `ToolResult`. Rollout is a dial (`off` → `permissive` → `require`), broker transport security (SASL/TLS) is configurable by credential *reference* rather than value, and [`docs/security.md`](docs/security.md) states the hardening baseline, the least-privilege role matrices, and the rule that secrets never travel in tool arguments. |
| C45 | `add-1-0-api-freeze` | The public surface audited, frozen into the generated `public-surface.toml` snapshot, and enumerated one contract-line per name in [`docs/api.md`](docs/api.md). `tests/test_public_surface.py` fails on any unreviewed addition *or* removal, ruff `D1` requires a docstring on every public name, and `src/beam_agents/_deprecation.py` is the executable half of the deprecation window CONTRIBUTING.md defines. This is what makes 1.0 a freeze rather than an announcement. |
| C46 | `add-state-guarantees` | The cross-release state contract written down in [`docs/state-compat.md`](docs/state-compat.md) — state written by release N is readable by N+1, Dataflow `--update` is supported between adjacent releases, and a table classifies every schema/coder/graph-shape change an author can make — plus the nightly Dataflow `--update` gate (`tests/dataflow/test_update_compat.py`) that carries live suspended state and populated working memory across a real version hop, with a red gate blocking a release. |
| C47 | `promote-spark-runner` | The Spark portable-runner **promotion process**, not a promotion: a third `spark` conformance leg with per-scenario `Run`/`Skip(reason)` declarations, the `docker/compose.spark.yaml` overlay, a weekly-never-per-PR `.github/workflows/spark-weekly.yml`, and `scripts/spark_weekly_status.py` computing the four-consecutive-green-scheduled-weeks streak and the two-red-weeks demotion verdict mechanically. The support statement itself is untouched by design — flipping it is what the streak authorizes. |

### Breaking changes

- The public API surface is now audited, frozen, and documented. `beam_agents` re-exports two more names the docs always promised — `tool` and `StreamAgent` — and every public module now declares an `__all__` naming exactly its contract, enumerated with one line each in the new [API reference](https://beam-agents.readthedocs.io/api/). Names that were only ever internal machinery are now underscore-prefixed and are no longer importable under their old spelling: the adapter transport helpers (`find_async_client`, `install_transport`, `warn_fallback` in each `adapters/*/transport.py`), the provider `decode` functions (still exported as `beam_agents.model.anthropic_decode` / `openai_compat_decode`), the memory-store envelope/seq codecs, the ADK and Pydantic AI session/history internals, and effector wiring (`build_service`, `load_registry`, `serve`, `execute_intent`, `encode_payload`, the dedup lease codecs). This is the last such sweep: after 1.0, removing or renaming any frozen name requires a deprecation window of at least one minor release with a `DeprecationWarning` naming the replacement. (add-1-0-api-freeze)

### What the 1.0.0 number promises

1.0.0 is a regime change, not a feature release. Its content is the pair of
standing policies it activates, and both are artifacts you can point at rather
than sentiments:

- **The public API is frozen by `public-surface.toml`.** From `v1.0.0`, every
  change to the public surface is governed by the deprecation policy in
  [`CONTRIBUTING.md`](CONTRIBUTING.md): removing or renaming a frozen name needs
  a window of at least one minor release during which the old spelling keeps
  working and emits a `DeprecationWarning` naming its replacement and the first
  release that may take it away. The snapshot is generated, never hand-edited,
  and `tests/test_public_surface.py` compares it against the tree by exact
  equality in both directions — so the review artifact for an API change is the
  diff to that file. Two tiers are frozen: the sixteen names
  `beam_agents/__init__.py` re-exports, and the module-tier names each public
  module declares in its `__all__`, enumerated in [`docs/api.md`](docs/api.md).
- **Wire and state changes are governed by `docs/state-compat.md`.** From
  `v1.0.0`, every change to a wire or state proto complies with the documented
  migration guarantees — additive, or `state_schema_version`-bumped with a lazy
  migration and golden-blob coverage — and Dataflow `--update` between adjacent
  releases stays supported. A post-1.0 proposal that breaks either policy
  without following it is rejected or re-scoped before implementation lands.

**In-flight deprecations at 1.0.0: none.** The C45 freeze renamed its internal
machinery outright rather than deprecating it — that sweep is the release's
**Breaking changes** entry above and is the last one — so no name in
`public-surface.toml` carries a `DeprecationWarning` or a removal horizon, and
`src/beam_agents/_deprecation.py` ships with no call sites. The first
deprecation will be the policy's first exercise, not its backlog.

**Spark: deferred, not promoted.** `promote-spark-runner` landed the weekly
Spark conformance leg and the mechanical promotion gate, and that gate requires
**four consecutive green scheduled weekly runs with no skip added during the
window**. Zero such runs exist at this commit — the leg has never run against a
Spark job server, three of its seven scenarios are declared structural skips
with named constraints, and the other four are provisional `Run()` declarations
whose evidence is the first weekly run. Promotion is therefore **deferred**:
`openspec/project.md`'s support statement is unchanged and still reads
"Supported runners v1.0: DirectRunner, Dataflow, Flink. Spark is best-effort",
which is what 1.0 has scoped all along. This is a recorded outcome, not an
omission — the gate is indifferent between promoting and deferring and strict
about writing the decision down, because an unrecorded decision is what makes a
release announcement unable to state Spark's status truthfully. The window,
the promotion checklist, and the symmetric two-red-weeks demotion path are in
[`docs/ci.md`](docs/ci.md); a future release promotes Spark when the streak is
real.

### Release gate

Evaluated as a whole at the release-candidate commit and recorded here with its
evidence. **Status: not yet fully green — `v1.0.0` is not tagged.** The gate
does not bend: an unmet condition is resolved in the change that owns it and
never waived here, and it slips the release date rather than shrinking the
release.

| Gate condition | Evidence | Verdict |
| -------------- | -------- | ------- |
| All four M4 hardening changes archived | All four are archived and their delta specs are landed in the main specs: `openspec/changes/archive/2026-08-04-add-1-0-api-freeze/`, `openspec/changes/archive/2026-08-04-add-effector-security/`, `openspec/changes/archive/2026-08-04-add-state-guarantees/`, and `openspec/changes/archive/2026-08-04-promote-spark-runner/` (2026-08-03, discharging gate condition 2.1). Archival — not merge — is the gating state, because archival is when a change's delta lands in the main specs, and each landing was `openspec validate --strict` clean | pass |
| The public-surface freeze snapshot test is green | `tests/test_public_surface.py` runs in the required offline `ci` tier and is green at this commit; `public-surface.toml` matches the tree by exact equality in both directions, and the ruff `D1` docstring gate is part of `make lint` | pass |
| State guarantees documented, and the nightly Dataflow `--update` compat test green on the latest scheduled run | [`docs/state-compat.md`](docs/state-compat.md) ships with the full change-class table, and its offline companions (`tests/dataflow/test_update_compat_harness.py`, `tests/core/test_state_compat_doc.py`) are green here. The gate itself, `tests/dataflow/test_update_compat.py`, is `dataflow`-marked and nightly-only: it needs a GCP project, a region, and a temp bucket, and no scheduled run exists at this commit to be green *on* | pending (CI run) |
| Effector intent signing shipped with its rollout complete — verification *enforced*, not merely available | Signing ships end to end: the additive proto envelope, `IntentSigner` on `WriteIntents`, verification as the effector's first phase, dead-lettering that never publishes a `ToolResult`, and an offline semantics gate (`tests/semantics/test_effector_signing.py`) proving a mixed genuine/tampered/forged stream under `require`. But `EffectorConfig.verify_intents` still defaults to `off`, and moving a deployment to `require` is an operator step this repository cannot observe or evidence. "Available" is what shipped; "enforced" is not yet a fact anyone here can assert | pending (rollout) |
| The Spark promotion decision is recorded either way | Recorded above as **deferred**, with the reason (zero of four qualifying weekly runs) and the unchanged best-effort support statement in `openspec/project.md`. The gate requires the decision to exist, not to be "promote" | pass |
| Standing release blockers re-verified: semantics gates green and unskipped; no open latency-budget benchmark regression | The offline semantics selection is green here — `make test-semantics-offline`: 79 passed, 5 skipped, every skip declared and pre-existing — and no gate is marked flaky or xfail. The latency-budget half is now met with an admissible figure: `benchmark-baseline.toml`'s `[medians_ms]` was seeded 2026-08-03 from the scheduled nightly `bench` job (run 30806138398, `ubuntu-latest`, `main` @ `e5cf356`), whose absolute budget read p50 0.8517 ms (< 15 ms) / p99 0.9767 ms (< 60 ms) on 1000 pooled samples, and `scripts/bench_gate.py` over that run's artifact exits 0 against the seeded baseline — no open regression. What remains is the docker-backed `semantics and integration` leg at the release commit, which runs in CI's `flink-minicluster` job on the release PR | pending (CI run) |
| Offline gate roster green at the candidate commit | `make lint`, `make type`, `make test-unit` (1941 passed, 4 skipped, total coverage 95.68%), `make test-semantics-offline`, `make coverage-ratchet` (at baseline) | pass |
| Version and changelog agree | `pyproject.toml` and `uv.lock` both read `1.0.0`, `docs/yaml.md`'s two provider pins follow, and this section names all four M4 changes and both policy artifacts — pinned by `tests/release/test_release_1_0_0.py` | pass |

The archival row and the Spark row are both checked against the repository
rather than asserted. `test_recorded_archival_verdict_matches_the_repository`
fails if this table records `pending (archival)` once every M4 change *is*
archived, and fails just as loudly if the table drops the marker while any of
them is not; `test_changelog_section_records_the_spark_promotion_decision`
fails if no decision is recorded at all, and fails if this section claims
promotion while `openspec/project.md` still scopes Spark as best-effort.

## 0.5.0 - 2026-07-31

The **M3 milestone release**. It closes out the seven M3 changes, which
together are the *adoption surface*: the ways an agent reaches the runtime
(Beam YAML, the Pydantic AI adapter), the ways a pipeline reaches production
(the Dataflow Flex Template, the Slack approval surface, the continuous-eval
pipeline), the way an activation is debugged after the fact (the replay CLI),
and the way the design is put to the Beam community (the upstream design doc).

The **Added** subsection below is assembled mechanically from `changelog.d/`.
The two subsections around it — the M3 batch and the release gate — are the
milestone record the `release-0-5` capability requires, and are hand-curated.

### The M3 batch

The seven changes this milestone is defined as closing out. Four of them
(`add-yaml-provider`, `add-dataflow-flex-template`, `add-replay-cli`,
`add-upstream-design-doc`) wrote changelog fragments that assembly consumed
into the `0.3.0` section, and three (`add-pydantic-ai-adapter`,
`add-slack-approval-example`, `add-eval-pipeline-example`) landed with no
fragment at all. Both facts are why this table is not optional: a milestone
whose contents a reader has to reconstruct from a previous section plus the
git log is not a release note.

| Roadmap | Change | What it delivered |
| ------- | ------ | ----------------- |
| C36 | `add-yaml-provider` | `RunAgent` reachable from a [Beam YAML](docs/yaml.md) document through one fully-qualified constructor, `beam_agents.yaml.run_agent`, with `module:object` references resolved at document-expansion time so a typo fails before submission rather than inside a bundle. |
| C37 | `add-dataflow-flex-template` | The fraud-triage example as a Dataflow Flex Template (`examples/fraud_triage_dataflow/`): one `gcloud dataflow flex-template run`, topics and provider and approval deadline as parameters, provider keys as Secret Manager resource names resolved on the worker — never as launch parameters. |
| C38 | `add-replay-cli` | `export_request` → `StateSnapshot` on `RunAgent`'s new `.snapshots` output, and the `beam-agents-replay` console script that re-runs that activation offline against a transport-free provider, diffing it against the traced record with scriptable exit codes. |
| C39 | `add-pydantic-ai-adapter` | `PydanticAIAgent` (`pydantic-ai` extra): an existing Pydantic AI agent under the same durability and re-injection rules as a native agent — history persisted latest-only in working memory, `side_effect=True` tools declared external and staged as intents, recognized httpx-backed models served through the replay-cache path. Registered on the conformance matrix, so it is covered on both legs. |
| C40 | `add-slack-approval-example` | A worked approval surface (`examples/slack_approval/`, [docs](docs/examples/slack-approval.md)): consume `kind == APPROVAL` intents through the effector's `IntentSource` seam, post one Block Kit message each, publish the decision back as an `Approval` on the same key — with the Slack transport behind a `SlackGateway` seam so the whole loop tests offline. |
| C41 | `add-eval-pipeline-example` | A [continuous-evaluation pipeline](docs/continuous_eval.md): a second, ordinary Beam job that joins the traces topic to a late-arriving business-outcome stream on `(entity_key, seq)` and scores the pair with an LLM-as-judge, with `no_outcome`, `orphaned_outcomes`, and `judge_errors` as first-class outputs. |
| C42 | `add-upstream-design-doc` | The `docs/design/` upstreaming artifacts: a Beam-community design document proposing an `apache_beam.ml.agents` package, plus a dev@beam.apache.org thread plan pairing the announcement draft with an objections register. |

### Added

- `ToolIntent`s can now be signed at the outbox writer and verified by the effector before anything else runs, so write access to the intents topic is no longer the authority to execute any side-effecting tool: pass `WriteIntents(..., signer=IntentSigner(key_id, "env:MY_KEYS"))` and roll the effector's `--verify-intents` dial from `off` through `permissive` to `require`, with failures preserved on `--dead-letters-to` and never published as a `ToolResult`. Kafka SASL/TLS is configurable on every client the library constructs, with credentials supplied as `env:`/`file:` references rather than values, and credentialed URIs are now redacted in configuration errors, `repr(EffectorConfig)`, and the CLI's startup output. See [`docs/security.md`](https://github.com/ardada2468/beam-agents/blob/main/docs/security.md) for the rollout, the least-privilege role matrices, and the rule that secrets must never travel in tool arguments. (add-effector-security)

### Release gate

Evaluated as a whole at the release-candidate commit and recorded here with its
evidence. **Status: not yet fully green — `v0.5.0` is not tagged.** The gate
does not bend: under this milestone's design decision D1 an unmet condition
slips the release date, it does not shrink the release — no subset of the M3
batch ships as 0.5.0 and no straggler trails in a 0.5.x.

| Gate condition | Evidence | Verdict |
| -------------- | -------- | ------- |
| All seven M3 changes archived | All seven are implemented, gated, and merged; their change folders are still live under `openspec/changes/` awaiting the archive step, and `openspec/changes/archive/` still holds only the nine pre-0.1.0 changes. Archival — not merge — is the gating state (design D2) | pending (archival) |
| Adapter conformance matrix green on the release commit, both legs, every registered adapter | The registry now carries four adapters (`reference`, `langgraph`, `pydantic_ai`, `adk`), so "matrix green" means "green including Pydantic AI" with no extra wiring (design D3). The DirectRunner leg is green here — `make test-semantics-offline`: 79 passed, 5 skipped, all declared — but the Flink leg is `make test-conformance-flink` in the `integration` workflow and needs a run at the candidate commit | pending (CI run) |
| Benchmark regression gate green — overhead p50 < 15 ms and p99 < 60 ms per activation, excluding LLM and tool time | Requires the nightly `bench` job on a GitHub-hosted runner; `benchmark-baseline.toml`'s `[medians_ms]` is still unseeded and `docs/benchmarks.md` forbids seeding it from developer hardware | pending (CI hardware) |
| `ci`, `integration`, and `quality` green on the release commit | The docker-backed halves of `integration` and the `quality` mutation job have not run at this commit; D3 requires the runs to be on the commit the tag will point at, not an earlier green one | pending (CI run) |
| Offline gate roster green at the candidate commit | `make lint`, `make type`, `make test-unit`, `make test-semantics-offline`, `make coverage-ratchet` | pass |
| Version and changelog agree | `pyproject.toml` and `uv.lock` both read `0.5.0`, `docs/yaml.md`'s provider pin follows, and this section names all seven M3 changes — pinned by `tests/release/test_release_0_5_0.py` | pass |

The archival row is checked against the repository rather than asserted:
`test_recorded_archival_verdict_matches_the_repository` fails if this table
records `pending (archival)` once every M3 change *is* archived, and fails just
as loudly if the table drops the marker while any of them is not.

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
