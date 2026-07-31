## Why

The module map reserves an `adapters/adk` slot next to `adapters/langgraph`, and
"bring-your-own-framework adapters" is a named differentiator — but today an agent
authored with Google ADK (`google-adk`: `LlmAgent` trees, `Runner`, `SessionService`,
function tools) cannot run on the runtime at all. It would have to be rewritten by hand
against `ActivationContext` to get durable keyed memory, the outbox side-effect path,
HITL approvals, or the replay cache. This change adds the second framework adapter,
modeled on the proven seams of the LangGraph adapter
([agent.py:73](../../../src/beam_agents/adapters/langgraph/agent.py:73)): each ADK
persistence/tool/model seam is expressed through the runtime seam it corresponds to, so
an **existing** ADK agent adopts the runtime's correctness guarantees by re-tagging its
side-effectful tools — not restructuring the agent tree. Unlike the LangGraph adapter,
which landed before the conformance matrix existed, this adapter lands *into* the
matrix: its registration extends the seven-scenario × two-runner suite, and the adapter
ships only when every cell is green.

## What Changes

- New `beam_agents.adapters.adk` package with **`AdkAgent`**, which wraps an ADK agent
  (an `LlmAgent`/`BaseAgent` tree) as a runtime `Agent`: per activation it builds an ADK
  `Runner` over the adapter's session service, drives `run_async` on the bridge event
  loop under a per-key session (`session_id`/`user_id` derived from the entity key), and
  maps the run's terminal state to `Complete` or its pending long-running tool calls to
  a single `Suspend`.
- **`BeamSessionService`**: an ADK `BaseSessionService` that persists the per-key
  session (state dict plus event history) through the activation's `Memory` facade under
  a reserved `__adk__/` key namespace — the same construction as `BeamCheckpointSaver`'s
  `__langgraph__/` namespace
  ([checkpoint.py:54](../../../src/beam_agents/adapters/langgraph/checkpoint.py:54)).
  Session mutations are therefore staged in the activation context and commit atomically
  with the Beam bundle (correctness invariant 1): a failed/timed-out activation leaves
  no partial session, and a worker failover resumes from the last committed session on
  the same key. Retention is one session per key.
- **Tool tagging shim**: wrappers that turn runtime `@tool` objects into ADK function
  tools. Read-only tools execute inline with validated arguments (unchanged ADK
  semantics). `side_effect=True` tools are declared as long-running function calls that
  never execute in-pipeline: the shim collects the call, the adapter stages one
  `ToolIntent` per call (deterministic `intent_id`, invariant 2) plus an approval-shim
  path through `request_approval`, and the activation suspends (invariant 5). Re-injected
  `ToolResult`s/`Approval`s resume the run as function-response content on the committed
  session. Adoption cost: re-tag tools with the runtime `@tool` decorator and wrap them
  with the shim — no agent-tree changes.
- **Event stream teed to traces**: the ADK events consumed from `run_async` are
  projected into the activation's trace surface
  ([traces.py:129](../../../src/beam_agents/observability/traces.py:129)) via
  `ctx.stage_trace_event`
  ([context.py:624](../../../src/beam_agents/core/context.py:624)) — inline tool
  executions become `TOOL_CALL` events (closing, for this adapter, the observability gap
  the conformance change surfaced for `BeamToolNode`), model turns surface as `LLM_CALL`
  through the model path, and every adapter-staged event carries the
  `beam_agents.adapter` attribute. Only deterministic projections are staged (activation
  clock, per-activation counters — never ADK's own event ids/timestamps), so replayed
  bundles emit byte-identical traces.
- **Model routing**: the LangGraph adapter's httpx transport hook
  ([transport.py:59](../../../src/beam_agents/adapters/langgraph/transport.py:59)) is
  hoisted to a shared framework-free module (`adapters/_transport.py`, depending only on
  core `httpx` + `LlmRequest`) and taught the `google-genai` client layout, so the ADK
  agent's Gemini calls are served through the activation's cache-first `call_model` path
  — zero provider calls on bundle retries (invariant 3). Unrecognized model clients warn
  once per agent instance and fall back, exactly the existing
  `warn_fallback` degradation
  ([transport.py:113](../../../src/beam_agents/adapters/langgraph/transport.py:113)).
- New optional dependency extra (`beam-agents[adk]`, pinned `google-adk>=1.0,<2`); the
  core package keeps zero ADK imports at module scope, and `AdkAgent` is re-exported
  lazily from `beam_agents/__init__.py` following the existing `__getattr__` pattern
  ([\_\_init\_\_.py:42](../../../src/beam_agents/__init__.py:42)).
- **Conformance registration**: a `ConformanceAdapter` entry for `adk` in
  [tests/conformance/_registry.py:58](../../../tests/conformance/_registry.py:58) with a
  factory translating every `ScenarioSpec` into an ADK agent, extending the matrix to
  all seven scenarios × three adapters × both legs. The registry guard
  ([_registry.py:189](../../../tests/conformance/_registry.py:189)) makes shipping the
  subpackage without this registration a collection error, so the adapter cannot land
  unregistered.

## Capabilities

### New Capabilities

- `adk-adapter`: running a Google ADK agent as a beam-agents activation — `Runner`
  execution inside the activation, session persistence in working memory with
  bundle-atomic commit and failover resume, the tool tagging shim mapping
  `@tool(side_effect=...)` semantics onto long-running function calls and the intent
  system, the event-stream tee into `ActivationTrace`, and replay-cached model routing
  with warning fallback — verified by the full adapter conformance matrix.

### Modified Capabilities

- `adapter-conformance-matrix`: the mandated adapter axis grows from {reference,
  LangGraph} to {reference, LangGraph, ADK}; the scenario bodies, legs, per-leg skip
  declarations, and meta-test accounting are unchanged and simply gain the new
  adapter's cells.

## Impact

- **Depends on**: `add-adapter-conformance-matrix` (C22) — the conformance harness,
  `ScenarioSpec` vocabulary, registry guard, and Flink leg this adapter must plug into;
  the new adapter MUST pass the full conformance suite (all seven scenarios, both legs)
  before it ships. Also consumed unchanged: the stateful `_AgentDoFn` runtime, the
  `Memory` facade, the `@tool` registry, `ActivationTrace`, and the LangGraph adapter's
  transport module (hoisted, not rewritten).
- **New code:** `src/beam_agents/adapters/adk/` (`__init__.py`, `agent.py` — `AdkAgent`,
  `session.py` — `BeamSessionService`, `tools.py` — the tagging shim, `events.py` — the
  trace tee, `transport.py` — the adapter-local httpx hook carrying the google-genai
  recognition table), `tests/adapters/adk/` unit suite,
  `tests/conformance/_adapters/adk.py` conformance factory,
  `tests/conformance/test_adk_registration.py`.
- **Modified code:** `src/beam_agents/__init__.py` (lazy `AdkAgent` export),
  `tests/conformance/_registry.py` (the `adk` `ConformanceAdapter` entry),
  `tests/conformance/_spec.py` + `_cells.py` (the per-adapter skip declaration),
  `tests/conformance/_flink/pipeline.py` (one additional per-adapter job, same
  multiplexing pattern), `tests/conformance/test_harness_unit.py` and
  `tests/test_import.py` (adapter-count/public-surface assertions),
  `docker/sdk-harness.Dockerfile` (ADK importable worker-side).
  **Not modified:** `src/beam_agents/adapters/langgraph/transport.py` — hoisting the
  shared `_ReplayTransport` is a separate, parallel change (see tasks 2.3 / 1.7); this
  adapter ships its own local seam meanwhile.
- **CI/build:** new `adk` extra in `pyproject.toml` next to the `langgraph` extra
  ([pyproject.toml:30](../../../pyproject.toml:30)); the unit/integration lanes install
  it so the adapter and conformance cells actually run; no new workflow steps — the
  DirectRunner cells ride the required offline semantics selection and the Flink cells
  ride the existing `make test-conformance-flink` target
  ([Makefile:54](../../../Makefile:54)). Lockfile updated via `uv sync`.
- **Gates:** `make lint`, `make type` (mypy --strict on the new package),
  `make test-unit` (offline; ADK cells skip cleanly where the extra is absent),
  `make test-conformance-flink`, coverage ratchet. No state-schema change: `__adk__/`
  lives inside the existing `MemoryBlob`, no proto edits, no `--update` concerns.
