# beam-agents

`beam-agents` makes AI agents first-class citizens of Apache Beam streaming
pipelines — a keyed, stateful, fault-tolerant runtime (`events |
RunAgent(my_agent)`), not an agent-authoring framework. See
[`openspec/project.md`](openspec/project.md) for the full architecture and
governing principles.

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
