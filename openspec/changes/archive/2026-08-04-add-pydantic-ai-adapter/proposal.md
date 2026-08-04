## Why

beam-agents is an agent *runtime*, not an authoring framework: Pydantic AI is one of the
three frameworks the constitution names as adapter targets (`adapters/ … pydantic_ai` in
the module map), and today only LangGraph has an adapter. A Pydantic AI agent cannot use
durable keyed memory, the outbox side-effect path, HITL approvals, or the replay cache
without being rewritten against `ActivationContext` by hand. This change adds the second
framework adapter, `PydanticAIAgent`, modeled on the proven LangGraph seams — and,
unlike the LangGraph change (which predated the conformance matrix), it lands *through*
the adapter conformance suite: the registry guard
([_registry.py:189](../../../tests/conformance/_registry.py:189)) makes an importable
`beam_agents.adapters.pydantic_ai` subpackage without a conformance registration a
collection error, so the adapter and its full seven-scenario × two-runner conformance
coverage ship as one unit.

## What Changes

- New `beam_agents.adapters.pydantic_ai` package with **`PydanticAIAgent`**, wrapping a
  user-authored `pydantic_ai.Agent` as a runtime `Agent`
  ([agent.py:132](../../../src/beam_agents/core/agent.py:132)): an activation loads the
  conversation history, runs `Agent.run(...)` on the bridge event loop, and maps the
  run's end state to `Complete` or `Suspend`.
- **Message-history persistence via the `Memory` facade**: the run's model messages are
  serialized and stored as a scalar under a reserved `__pydantic_ai__/` working-memory
  namespace (the same pattern as `BeamCheckpointSaver`'s `__langgraph__/` namespace,
  [checkpoint.py:54](../../../src/beam_agents/adapters/langgraph/checkpoint.py:54)).
  History is therefore staged in the activation context and commits atomically with the
  Beam bundle (correctness invariant 1); it carries the conversation across activations
  on the same key, is bounded by the blob/working-memory caps, and is GC'd by the
  memory TTL like any other working memory.
- **Side-effect tools suspend via deferred tool calls**: `BeamToolset` presents runtime
  `@tool` objects to Pydantic AI. `side_effect=True` tools are declared as *deferred* —
  the framework ends the run at the call boundary instead of executing anything; the
  adapter stages one `ToolIntent` per deferred call through `ctx.act(...)`
  ([context.py:547](../../../src/beam_agents/core/context.py:547), deterministic
  `intent_id`, invariant 2/5) and returns `Suspend`. Re-injected `ToolResult`s
  accumulate until all pending calls are answered, then the adapter re-runs the agent
  with the persisted history plus the deferred results. Approval-required tools take the
  same shape through `ctx.request_approval(...)`
  ([context.py:559](../../../src/beam_agents/core/context.py:559)), resuming as an
  approved/denied decision.
- **Read-only tools execute inline through `ctx.run_tool`**
  ([context.py:514](../../../src/beam_agents/core/context.py:514)): unlike
  `BeamToolNode` (which executes read-only tools directly and stages no trace — a
  surfaced finding in the conformance change's design), Pydantic AI tool functions run
  inside the activation with the context in reach, so inline tools get validated
  arguments, `SideEffectToolError` protection, and `TOOL_CALL` trace events for free.
- **Model calls route through the replay-cached model path**: the framework-neutral core
  of the LangGraph transport hook
  ([transport.py:59](../../../src/beam_agents/adapters/langgraph/transport.py:59)) —
  `_ReplayTransport`, the activation contextvar, and the warn-once fallback — is hoisted
  into a shared `beam_agents.adapters._transport` module (move-only; the LangGraph
  module keeps re-exports). The Pydantic AI adapter adds its own client-recognition
  probing: Pydantic AI's Anthropic/OpenAI model classes wrap the official provider SDKs,
  whose async clients are httpx-based (the same stack as the runtime's own provider
  clients), so a recognized model's HTTP calls are served by
  `ctx.call_model` ([context.py:469](../../../src/beam_agents/core/context.py:469)) —
  cache-first, zero provider calls on bundle retries (invariant 3). Unrecognized models
  warn once per agent instance, increment the existing
  `beam_agents.adapters/transport_fallback` counter, and run untouched.
- **Usage accounting**: the run's reported token usage is folded into the activation
  tally via `ctx.accumulate_usage`
  ([context.py:631](../../../src/beam_agents/core/context.py:631)), so Pydantic AI
  activations report `total_tokens`/`usage_observed` like facade-driven agents do.
- **Packaging**: new optional extra `pydantic-ai`. `pydantic>=2` is already a core
  runtime dependency ([pyproject.toml:11](../../../pyproject.toml:11)) — the extra adds
  only the Pydantic AI *framework* distribution. `beam_agents.PydanticAIAgent` is
  exported lazily via the existing `__getattr__` pattern
  ([__init__.py:42](../../../src/beam_agents/__init__.py:42)) with an actionable
  `ImportError` naming `beam-agents[pydantic-ai]`; core keeps zero framework imports.
- **Conformance registration**: a `pydantic_ai` `ConformanceAdapter`
  ([_registry.py:58](../../../tests/conformance/_registry.py:58)) with factories
  translating every `ScenarioSpec`
  ([_spec.py:306](../../../tests/conformance/_spec.py:306)) into a Pydantic AI agent —
  all seven scenarios (single_shot, multi_tool_inline, suspension_resume,
  approval_timeout_fallback, restart_mid_suspension, bundle_retry_cache, ttl_expiry) on
  both the DirectRunner and Flink legs, honoring the matrix's per-leg declared skips.

## Capabilities

### New Capabilities

- `pydantic-ai-adapter`: running a Pydantic AI agent as a beam-agents activation —
  message-history persistence in working memory with bundle-atomic commit, deferred-tool
  mapping of side effects and approvals onto intents/suspension, inline read-only tools
  through the runtime tool path, the transport hook routing model calls through the
  runtime `LLMClient` with warn-once fallback, run-usage accounting, and full
  conformance-matrix membership.

### Modified Capabilities

- `adapter-conformance-matrix`: the required adapter axis grows — the conformance suite
  SHALL contain the Pydantic AI adapter alongside the reference protocol agent and the
  LangGraph adapter. No scenario, leg, or accounting behavior changes; the registry and
  meta-test machinery absorb the new axis entry by construction.

## Impact

- **Depends on:** `add-adapter-conformance-matrix` (C22) — the adapter MUST pass the
  full conformance suite: its registration joins `ADAPTERS`
  ([_registry.py:58](../../../tests/conformance/_registry.py:58)), and the collection
  guard would otherwise fail the build for an importable-but-unregistered
  `pydantic_ai` subpackage. Also builds on `add-langgraph-adapter` (the transport-hook
  and reserved-namespace patterns this change generalizes).
- **New code:** `src/beam_agents/adapters/_transport.py` (framework-neutral replay
  transport, hoisted), `src/beam_agents/adapters/pydantic_ai/` (`agent.py`,
  `history.py`, `toolset.py`, `transport.py`), `tests/adapters/pydantic_ai/`, and
  `tests/conformance/_adapters/pydantic_ai.py` (the conformance factory, mirroring
  [langgraph.py:115](../../../tests/conformance/_adapters/langgraph.py:115)).
- **Modified code:** `src/beam_agents/adapters/langgraph/transport.py` (move-only
  extraction of `_ReplayTransport`/contextvar/fallback helpers into the shared module,
  public names re-exported unchanged), `src/beam_agents/__init__.py` (lazy
  `PydanticAIAgent` export), `tests/conformance/_registry.py` (the `pydantic_ai`
  registration). No changes to `core/`, `model/`, `memory/`, or the conformance
  scenario specs.
- **CI/build:** new `pydantic-ai` extra in `pyproject.toml` mirrored into the test
  dependency group (the same pattern as the `langgraph` extra,
  [pyproject.toml:30](../../../pyproject.toml:30)); the unit matrix installs it so
  adapter tests run; the conformance legs need no wiring changes — the DirectRunner
  cells ride the required offline semantics selection and the Flink cells ride the
  existing `make test-conformance-flink` step, both picking up the new axis entry
  automatically.
- **Gates:** `make lint`, `make type` (mypy --strict on the new package), `make
  test-unit` offline (cells skip cleanly where the extra is absent), the offline
  semantics selection with the enlarged matrix under its wall-clock budget,
  `make test-conformance-flink`, and the coverage ratchet.
