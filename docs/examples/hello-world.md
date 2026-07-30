# Hello, world

The smallest complete `RunAgent` pipeline: one `AgentEnvelope` created
in-process, keyed by `entity_key`, processed by an agent that makes a single
model call and completes. This is the **fast path** — the activation runs to
completion inside one `process()` call, with no suspension, no side-effect
intents, and no state carried between elements.

The model is a scripted [`FakeLLM`], so the pipeline runs offline with no API
keys: one `match_any()` rule answers every request with the greeting. Swapping
`make_provider` for a factory returning a real provider client is the only
change a production pipeline needs.

[`FakeLLM`]: https://github.com/ardada2468/beam-agents/blob/main/src/beam_agents/model/fake.py

## Run it

```sh
uv run python -m examples.hello_world
```

It prints the single terminal output and exits:

```
b'Hello from the beam-agents runtime!'
```

## The whole program

The code below is included verbatim from
[`examples/hello_world.py`](https://github.com/ardada2468/beam-agents/blob/main/examples/hello_world.py)
— the same file
[`tests/examples/test_hello_world.py`](https://github.com/ardada2468/beam-agents/blob/main/tests/examples/test_hello_world.py)
executes in CI, asserting exactly what this page claims.

```python
--8<-- "examples/hello_world.py"
```

## What the output means

`RunAgent` exposes four named outputs. Here only the main one carries anything:

- `.output` — exactly one element: the scripted response the agent returned
  via `Complete(output=...)`.
- `.intents` — empty: the agent staged no side effects.
- `.errors` — empty: the activation committed successfully.
- `.traces` — the activation's trace events (one `LLM_CALL` among them); see
  [trace delivery](../traces.md).

The one subtlety worth taking away: `ctx.call_model` is **cache-first**. The
response is staged in the keyed replay cache and committed atomically with the
bundle, so if the runner retries this bundle the activation replays from cache
with zero additional provider calls — the property the retry-determinism gate
pins for every pipeline, demonstrated here at its smallest.
