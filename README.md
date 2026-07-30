# beam-agents

`beam-agents` makes AI agents first-class citizens of Apache Beam streaming
pipelines — a keyed, stateful, fault-tolerant runtime (`events |
RunAgent(my_agent)`), not an agent-authoring framework. See
[`openspec/project.md`](openspec/project.md) for the full architecture and
governing principles.

**Documentation:** <https://ardada2468.github.io/beam-agents/> — the rendered
[`docs/`](docs/) tree plus three runnable, offline, FakeLLM-driven examples
([`examples/`](examples/)), built strictly by the `docs` workflow. Build it
locally with `make docs` or browse it live with `make docs-serve`.

## Bootstrap

```sh
uv sync --all-groups
uv run pre-commit install
```

or equivalently:

```sh
make bootstrap
```

Requires Python `>=3.11,<3.13` (this repo pins `3.11` via `.python-version`)
and [`uv`](https://docs.astral.sh/uv/).

## Running tests

Four testing tiers, mirrored 1:1 by CI (see [`docs/ci.md`](docs/ci.md)):

```sh
make test-unit         # offline, no docker — required for every change
make compose-up        # start Redpanda, Redis, Flink locally
make test-integration   # requires compose-up
make test-semantics     # requires compose-up, correctness/determinism gates
make compose-down       # tear the stack down
make mutation           # mutmut against core/ (quality gate)
```

`pytest` markers are a closed registry (`integration`, `semantics`,
`dataflow`, `slow`) — see [`pyproject.toml`](pyproject.toml).

## Other useful targets

```sh
make lint   # ruff check + format --check
make type   # mypy --strict
make fmt    # ruff check --fix + format
make proto  # regenerate protobuf bindings from protos/*.proto
```

Run `make help` for the full list.

## Running a LangGraph graph

An existing LangGraph graph adopts the runtime's guarantees (durable keyed
checkpoints, outbox side effects, HITL approvals, replay-cached model calls)
with three changes — no topology edits:

1. Re-declare side-effectful tools with the runtime decorator:
   `@tool(side_effect=True)`.
2. Swap LangGraph's prebuilt `ToolNode` for
   `beam_agents.adapters.langgraph.BeamToolNode(tools)`.
3. Wrap the graph: `RunAgent(LangGraphAgent(graph, chat_models=[model]))`.

```sh
uv pip install 'beam-agents[langgraph]'
```

Checkpoints persist latest-only inside working memory (the 1 MiB cap applies —
trim or summarize message history on the LangGraph side). `interrupt(...)`
suspends the activation as an approval intent and resumes via
`Command(resume=...)`; on resume the interrupted node re-runs from its start
(LangGraph's own semantics), so keep pre-interrupt node code idempotent.
Recognized httpx-backed chat models are served through the runtime's
`LLMClient` replay-cache path; unrecognized ones fall back to direct calls
with a one-time warning and a `transport_fallback` metric. See the module
docstrings under `src/beam_agents/adapters/langgraph/` for the details.

## Running a Pydantic AI agent

An existing Pydantic AI agent adopts the same guarantees with two changes — no
restructuring of instructions, output types, or control flow:

1. Re-declare side-effectful tools with the runtime decorator:
   `@tool(side_effect=True)`; name any read-only tool you want gated on a human
   in `approval_required`.
2. Wrap the agent: `RunAgent(PydanticAIAgent(agent, tools=tools))`.

```sh
uv pip install 'beam-agents[pydantic-ai]'
```

The conversation's message history persists latest-only in working memory
under a reserved `__pydantic_ai__/` namespace and commits atomically with the
Beam bundle (the 1 MiB cap applies — trim or summarize with a Pydantic AI
history processor). A model call on a `side_effect=True` tool never executes
in-pipeline: the tool is declared *external*, the run ends cleanly at the call,
the adapter stages one `ToolIntent` per pending call, and the activation
suspends; the re-injected result resumes it as a fresh run seeded with the
committed history plus the deferred results. Approval-gated tools take the same
shape through the approval channel. Read-only tools run inline through the
runtime tool path, so they get validated arguments, side-effect protection, and
`TOOL_CALL` trace events. Recognized httpx-backed models (the Anthropic/OpenAI
model classes, whose SDK client is httpx-based) are served through the
runtime's `LLMClient` replay-cache path; unrecognized ones fall back to direct
calls with a one-time warning and a `transport_fallback` metric. See the module
docstrings under `src/beam_agents/adapters/pydantic_ai/` for the details.

## Running the effector

Side effects execute outside the pipeline, in the reference effector service:
`intents → dedup → execute → results → re-injection`. See
[`docs/effector.md`](docs/effector.md) for deployment preconditions, the
lease/TTL budgets, and what is (and is not) guaranteed.

```sh
uv pip install 'beam-agents[effector]'
beam-agents-effector --registry myapp.agent:TOOLS ...
```

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the OpenSpec workflow this
repository requires before any change under `src/`.
