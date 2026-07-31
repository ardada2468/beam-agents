# Project Context: beam-agents

## Purpose

`beam-agents` makes AI agents first-class citizens of Apache Beam streaming pipelines. An agent becomes a keyed, stateful, fault-tolerant transform: `events | RunAgent(my_agent)`. Target workloads are **system-triggered agents** (fraud triage, anomaly response, personalization, IoT reaction, ops automation) where events invoke the agent and decisions must be durable, replayable, and horizontally scalable. It is NOT for sub-second interactive chat.

**Governing principle: beam-agents is an agent RUNTIME, not an agent FRAMEWORK.** Agent authoring belongs to LangGraph, Google ADK, Pydantic AI, or a plain async-function protocol, integrated via adapters. We own what those frameworks lack: durable keyed memory, event/processing-time semantics, effectively-once side effects, backpressure-aware scale-out, and runner portability (DirectRunner, Dataflow, Flink, Spark). Any proposal that adds prompt templating, an orchestration DSL, or agent-authoring abstractions violates this principle and must be rejected or re-scoped.

Direct competitor: Apache Flink Agents. Our differentiation: Dataflow managed service, runner portability, bring-your-own-framework adapters, and outbox-based effectively-once side effects (they use inline durable-execution instead).

## Tech stack

- Python ≥ 3.11; `apache-beam[gcp] >= 2.60`
- `uv` for env/deps; `pyproject.toml` dependency groups; lockfile committed
- Protobuf for ALL wire and state schemas (`protos/` → generated `_pb2.py` committed; regen must be diff-clean in CI)
- `httpx[http2]` async clients; `pydantic` v2 for tool schemas and constrained JSON outputs
- Tests: `pytest` + `pytest-asyncio` (auto mode) + `pytest-timeout` + `hypothesis`; mutation testing with `mutmut` on `core/`
- Lint/type: `ruff` (incl. ASYNC rules), `mypy --strict` (Beam modules: `ignore_missing_imports`)
- Local services: docker compose with Redpanda (Kafka API), Redis, Flink 1.19; unit tests MUST pass with no docker
- CI: GitHub Actions (`ci`, `integration`, `quality`, `nightly` with Dataflow via Workload Identity Federation)
- Node-based OpenSpec CLI manages specs; spec folders live in `openspec/`

## Architecture (read carefully; these are load-bearing)

### Dataflow shape

```
Kafka/PubSub events ──┐
tool-results topic ───┼─► WithKeys(entity_id) ─► Flatten ─► [enrichment] ─► RunAgent
approvals topic ──────┘                                                       │
outputs: .output (main) · .intents ─► outbox topic ─► effector ─► results topic (re-injected)
         .traces ─► OTLP/BigQuery · .errors ─► dead-letter sink
```

### Two execution paths through RunAgent

1. **Fast path:** agent runs to completion inside one `process()` call. Pure/read-only tools execute inline; LLM calls go through the async model client.
2. **Re-injection path:** for side-effectful tools or human approval, the agent emits a `ToolIntent`, persists a `Continuation` in keyed state, and yields. The `ToolResult`/`Approval` re-enters the pipeline on the same key and the agent resumes. Iterative loops with external effects cycle through the message bus, never through the DAG (Beam DAGs are acyclic; do not propose DAG cycles or loop unrolling).

### Correctness invariants (violating any of these is a blocking defect)

1. **Atomic commit:** state mutations and emitted outputs commit atomically with the Beam bundle. All agent effects (memory writes, cache inserts, intents, traces, outputs) are STAGED in the activation context and applied only on success. A failed/timed-out activation mutates nothing.
2. **Deterministic intent IDs:** `intent_id = uuid5(NAMESPACE, key + seq + step_index)`. A replayed bundle that walks the same path produces byte-identical intents; the effector dedups on `intent_id`. This is the entire effectively-once argument.
3. **Replay cache:** every LLM call is keyed by `sha256(model_id, canonical_json(messages), tools_schema, sampling_params, key, seq)` and cached in keyed state (LRU, max 64 entries, 6h TTL, 100 KiB blob cap). Bundle retries must incur ZERO additional provider calls on the cached path.
4. **Per-key serialization:** Beam stateful DoFns process one element at a time per key. Memory is race-free by construction. Cross-key parallelism comes from the runner. Never introduce cross-key shared mutable state.
5. **Side effects only via intents:** calling a `side_effect=True` tool directly raises. `ctx.act(...)` is the only effect path. External writes never execute inside the pipeline (exception: documented idempotent upserts to the long-term MemoryStore keyed by `(key, seq)`).
6. **Timeouts fail closed at both layers:** HITL timer fires → fallback path AND effector refuses expired intents (`expires_at`). Late results are dropped as `orphaned_result` to the errors output.
7. **State is protobuf, never pickle.** Pipeline `--update` compatibility: additive proto changes only; breaking changes require `state_schema_version` bump + lazy migration + golden-blob compat test.

### Stateful DoFn layout (`core/dofn.py`)

State specs: `MEMORY` (ReadModifyWriteState, MemoryBlob), `CONTINUATION` (ReadModifyWriteState), `LLM_CACHE` (ReadModifyWriteState, bounded blob), `PENDING` (BagState of ToolIntent), `SEQ` (CombiningValueState, sum).
Timers: `TTL_TIMER` (WATERMARK — memory GC), `HITL_TIMER` (REAL_TIME — approval/result timeout), `FLUSH_TIMER` (REAL_TIME — adaptive batching only).
Python SDK has no MapState: bounded maps live inside single-value proto blobs with explicit LRU eviction. Every blob ≤ 100 KiB; working memory soft cap 1 MiB with compaction hook.

Async bridge: `setup()` starts one background thread per DoFn instance with a dedicated asyncio loop and shared httpx pools; `process()` submits the activation coroutine and blocks with `activation_timeout`; on timeout, cancel and route to errors with no state mutation.

### Module map

`core/` transform, dofn, context, loop driver, coders · `model/` LLMClient + providers (anthropic, openai_compat, vertex, vllm) + replay cache · `tools/` @tool registry (side_effect flag), read-only MCP · `actions/` intents + outbox sinks · `memory/` facade, stores (Bigtable/Redis/Firestore/SQL), compaction · `adapters/` protocol, langgraph (BeamCheckpointSaver + interrupt→intent), adk, pydantic_ai · `observability/` traces (OTel GenAI conventions), metrics, exporters (BigQuery schema'd writer, batched non-blocking OTLP/HTTP) · `hitl.py` · `yaml/` provider · `effector/` (separate reference service: consume intents → dedup → execute → publish results).

## Conventions

### Spec-driven + TDD workflow (mandatory)

- Every change starts as an OpenSpec change folder; no `src/` commits without a referenced change. Small, focused changes — one capability each.
- Spec requirements are Given/When/Then behavioral scenarios. Tests are derived from scenarios, named after them, written FIRST, and must fail for the right reason before implementation. Scenario → test → code is the traceability chain; do not write tests from the implementation.
- Never weaken a test to make an implementation pass. If the spec is wrong, update the spec first, get it reviewed, then the test, then the code.
- Generated protobuf files are never hand-edited.

### Testing tiers

- `pytest` (default): pure unit + TestPipeline/TestStream; no docker; must pass offline.
- `-m integration`: Redpanda + Redis + Flink mini-cluster (testcontainers).
- `-m semantics`: correctness gates — retry determinism (chaos wrapper forcing bundle retries: zero extra FakeLLM calls, byte-identical intents), effectively-once end-to-end (real Kafka/Redis/Flink mini-cluster, SIGKILLed effector workers, a killed TaskManager, duplicate sink writes, full pipeline replay over 10k events: zero lost effects, duplicates bounded to the SIGKILL crash window between a tool's effect and its durable completion record (strict exactly-once with zero kills; true exactly-once requires tools idempotent on intent_id), replay adds zero executions, zero lost approvals — `tests/semantics/test_effectively_once_e2e.py`), state compat (golden blobs). These gate every release and never get skipped or marked flaky. Split further by infra need: `semantics and not integration` is offline (no docker) and runs as a required `ci` check on every PR; docker-backed semantics gates additionally carry the `integration` marker and run in the `integration` workflow via `make test-semantics` (`-m "semantics and integration"`). `scripts/check_semantics_partition.py` (a required `ci` step) fails the build if any semantics test escapes both selections or either selection goes empty.
- Adapter conformance matrix (`tests/conformance/`): seven lifecycle scenarios × every registered adapter (reference protocol agent + LangGraph today; an importable adapter subpackage without a registration fails collection) × two runners. The DirectRunner leg carries `semantics` only and rides the required offline `ci` semantics selection; the Flink leg carries `semantics + integration` and runs in the `integration` workflow as its own step, `make test-conformance-flink` (kept separate from `make test-semantics` so an e2e-gate timeout and a conformance failure are distinguishable). A meta-test audits registry × scenario × leg against collected cells, counting declared per-leg skips, so the matrix cannot silently shrink.
- `-m dataflow`: nightly only, real Dataflow, FakeLLM-over-HTTP.
- `-m smoke`: nightly only, real Anthropic/OpenAI-compatible endpoints; excluded from `test-unit`, skips locally/in PRs without provider credentials (`ANTHROPIC_API_KEY`/`OPENAI_API_KEY`). Taxonomy mapping and decode behavior for these providers are still verified offline via `httpx.MockTransport`, not gated behind this marker.
- Timer/watermark behavior is tested with TestStream scripted watermark/processing-time advances — never with `sleep()`.
- FakeLLM (scripted matcher responses, records all requests) is the default model in all tests; real providers only in nightly smoke.

### Code style

- `ruff` clean, `mypy --strict` clean on `src/`. Full type hints. No `Any` in public signatures.
- Async-first internals; never block the bridge event loop (ruff ASYNC rules enforce).
- Public API is two tiers, frozen by `public-surface.toml` (generated; `tests/test_public_surface.py` fails on any unreviewed addition *or* removal) and enumerated with one line of contract each in `docs/api.md`. **Root tier** — `beam_agents/__init__.py` re-exports exactly sixteen names: `RunAgent`, `AgentConfig`, `RunAgentOutputs`, `StreamAgent`, `tool`, `HitlPolicy`, `FallbackContext`, `Deny`, `Drop`, `Escalate`, `ShardKeys`, `shard_key`, `unshard_key`, and the lazily-resolved adapter classes `LangGraphAgent`, `AdkAgent`, `PydanticAIAgent`. **Module tier** — names that are contract at their dotted path; every public module declares an `__all__` naming exactly its contract, and every public name it declares must be in that `__all__`. Everything else is private: underscore-prefix it, or put it in an underscore-prefixed module or package. Every public name carries a docstring (ruff `D1`, in `make lint`); after 1.0, removing or renaming one needs the deprecation window in CONTRIBUTING.md.
- Errors: never swallow exceptions; route element failures to the `errors` output with typed error protos; raise `ValueError` at pipeline-construction time for misconfiguration (e.g., non-KV input) with actionable messages.
- No global mutable state except documented worker-local singletons (circuit breakers, vLLM sidecar via `beam.utils.shared.Shared`).

### Git/PR

- Squash-merge; one merged commit = one archived OpenSpec change. Commit messages reference the change folder.
- Required checks: ci (lint, type, unit matrix 3.11–3.12), integration, quality (mutation on touched core files, coverage ratchet — coverage may never decrease).
- PR description links the spec scenarios each new test implements.

## Domain glossary

- **Activation:** one execution of the agent for one element inside `process()`.
- **Continuation:** persisted resume-state for a suspended activation awaiting a ToolResult/Approval.
- **Intent / ToolIntent:** a declarative request to perform a side effect, executed by the effector, deduped by deterministic `intent_id`.
- **Effector:** external reference service consuming intents, executing tools exactly-once per intent_id, publishing ToolResults.
- **Re-injection:** results/approvals re-entering the pipeline as new elements on the same key.
- **Replay cache:** keyed-state memoization of LLM calls making bundle retries cheap and path-stable.
- **seq:** per-key monotonic activation counter; scopes cache keys and intent IDs.
- **Fast path:** activation that completes in a single element with no suspension.

## Constraints and non-goals

- Python-only for v0.x; wire schemas stay language-neutral (protobuf) to preserve cross-language future.
- No agent-authoring DSL, no prompt templating, no UI, no hosted effector.
- Latency budget: runtime overhead p50 < 15 ms / p99 < 60 ms per activation (excluding LLM/tool time); benchmark regressions on this are release blockers.
- Working-memory hard cap 1 MiB per key; blobs ≤ 100 KiB; state growth is bounded by TTL_TIMER GC.
- Supported runners v1.0: DirectRunner, Dataflow, Flink. Spark is best-effort.
- Beam Python SDK realities to respect: no MapState/OrderedListState in user state, no portable async DoFn (hence the bridge thread), stateful DoFn requires KV input.

## External dependencies

- LLM providers: Anthropic, OpenAI-compatible, Vertex (remote); vLLM (endpoint or GPU-worker sidecar).
- Messaging: Kafka (Redpanda in tests) and Pub/Sub for events, intents, results, approvals.
- Stores: Redis/Bigtable for effector dedup; Bigtable/Redis/Firestore/SQLAlchemy for long-term MemoryStore.
- Observability: OTLP exporter, BigQuery trace sink; Beam metrics surface to runner dashboards.
