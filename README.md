# beam-agents

`beam-agents` makes AI agents first-class citizens of Apache Beam streaming
pipelines — a keyed, stateful, fault-tolerant runtime (`events |
RunAgent(my_agent)`), not an agent-authoring framework. See
[`openspec/project.md`](openspec/project.md) for the full architecture and
governing principles.

**Documentation:** <https://ardada2468.github.io/beam-agents/> — the rendered
[`docs/`](docs/) tree plus seven runnable example programs
([`examples/`](examples/)), most of them offline and FakeLLM-driven, built
strictly by the `docs` workflow. Build it
locally with `make docs` or browse it live with `make docs-serve`.

Documentation site: [`website/`](website/) — run it with `make site-dev`. Its
content is verified against this repository on every change; see
[`website/README.md`](website/README.md).

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

## See it run: the console

The runtime records a lot — deterministic traces, errors over a closed reason
vocabulary, state snapshots — and until now looking at any of it meant
provisioning a collector or a BigQuery dataset first. The console is a local
viewer over exactly those records: one process, one SQLite file, no broker and
no cloud project.

```sh
make console-up       # build and start; http://localhost:8787
make console-logs     # follow the console and the demo pipeline
make console-down     # stop, keeping the database volume
```

That stack starts the console **and** a demo pipeline that keeps feeding it, so
you land on a populated console with traffic still arriving rather than an empty
one. The demo runs on `DirectRunner` over the fake provider: no API key, no
broker, no network. It drives the awkward cases on purpose — suspensions,
approvals and denials, tool errors, budget exhaustion, TTL wipes, dead-lettered
intents — because those are what the error views and the approval queue exist
to show.

Pointing your own pipeline at it is one constructor argument, or zero if you are
already exporting to OTLP, Kafka, or BigQuery. See
[`docs/console.md`](docs/console.md) for the five ingest paths, the CLI
reference, and the honest list of what the console deliberately does not do
(no auth, trusted networks only, not an APM, not long-horizon storage).

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
`dataflow`, `smoke`, `slow`, `spark`) — see [`pyproject.toml`](pyproject.toml).

### Runner verification

The runtime targets DirectRunner, Dataflow, and Flink; **Spark is
best-effort**. Best-effort is verified, not assumed: the adapter conformance
matrix has a third `spark` leg that runs against a Beam Spark job server in
the weekly `spark-weekly` workflow — never on a pull request, and never as a
required check. Promotion of Spark to supported requires four consecutive
green scheduled weekly runs with no conformance skip added in that window; the
process, including the demotion path, is in [`docs/ci.md`](docs/ci.md).

```sh
make compose-up-spark          # base stack + the Spark job-server overlay
make test-conformance-spark    # the weekly spark leg, locally
make compose-down-spark
```

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

The adapter lives behind the `langgraph` extra. From a source checkout:

```sh
uv sync --extra langgraph
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
uv sync --extra effector
uv run beam-agents-effector --registry myapp.agent:TOOLS ...
```

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the OpenSpec workflow this
repository requires before any change under `src/`.
