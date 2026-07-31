# Console demo

The [console](../console.md) renders what the runtime already records:
activations, span trees, errors grouped by the closed `reason` vocabulary,
per-model token spend and cache-hit ratio, the HITL approval queue, and
per-entity-key timelines. Every one of those views is empty until something
produces the record it renders — which is why a demo that only emits happy-path
completions leaves most of the screen looking broken.

This example produces the whole vocabulary in one command, offline: a scripted
[`FakeLLM`] serves every model call, so there is no API key, no broker, and no
network anywhere in it.

[`FakeLLM`]: https://github.com/ardada2468/beam-agents/blob/main/src/beam_agents/model/fake.py

## Run it

With nothing else started, it prints a summary of everything the round produced:

```sh
uv run python -m examples.console_demo
```

Against a console started with `beam-agents-console`:

```sh
uv run python -m examples.console_demo --console console://localhost:8787
```

Or keep producing, so the console shows traffic arriving rather than a static
snapshot — this is what `docker compose` runs:

```sh
uv run python -m examples.console_demo --console console://localhost:8787 --loop
```

## The twelve scenarios

Each one exists because some part of the UI is unreachable without it.

| Scenario | What the pipeline does | What the console gets |
|---|---|---|
| `completion` | one model call, then `Complete` | a `completed` activation, an `LLM_CALL` with token counts, and a `StateSnapshot` from an in-band `export_request` |
| `multi_tool` | a model call, then two inline `ctx.run_tool` calls | one `TOOL_CALL` span per tool, carrying `beam_agents.tool_name` — the per-tool volume panel |
| `cache_hit` | the same request twice in one activation | `beam_agents.cache_hit=false, billed=true` then `true`/`false`, both with real token counts — the cache-hit ratio |
| `suspension_approved` | suspend for approval, resume on an approval that arrives in time | one trace with two attempts, `kind=start` then `kind=resume` |
| `suspension_denied` | the same, resumed by a **denied** approval | a `completed` resume with nothing on `.errors`: a denial is an answer, not a failure |
| `suspension_timeout` | suspend, and never answer | `reason=hitl_timeout` on `.errors` — the timed-out approval view |
| `tool_error` | an inline tool raises | `reason=activation_error` with `error.type=ToolError` |
| `activation_error` | the agent raises after a model call | `reason=activation_error` with `error.type=RuntimeError` and the `beam_agents.failure.*` position scalars |
| `budget_exceeded` | call the model past `max_tokens_per_activation` | `reason=budget_exceeded`, its own group: a cost question, not a stack trace |
| `orphaned_result` | deliver a `ToolResult` for a key with no live continuation | `reason=orphaned_result`, `detail=no_continuation:<intent_id>` |
| `intent_dead_letter` | stage an intent the outbox refuses to route | `reason=intent_dead_letter`, built by the runtime's own `intent_dead_letter_to_error` off `RunAgent`'s dead-letter branch |
| `batch_overflow` | under `BatchPolicy.ADAPTIVE`, suspend and then keep the burst coming | `reason=batch_buffer_overflow`, plus `ttl_wiped_batch` and `ttl_wiped_suspension` when working-memory GC reaches the still-suspended key |

Two of those are worth a second look:

- **`tool_error` and `activation_error` share a `reason`.** An inline tool's
  `TOOL_CALL` span is staged only *after* the call returns, so a tool that raises
  leaves no tool span at all — the activation failure is the whole record, and
  `error.type` is what tells the two apart. The console groups by `reason` and
  breaks down by `error.type` for exactly this case.
- **`batch_overflow` needs a live suspension.** The size trigger flushes a buffer
  before it can reach its cap, so overflow is reachable only while a continuation
  defers flushing. The demo's batching branch suspends its first flushed batch
  and then keeps sending.

## What it prints

```
69 records over 12 scenarios: 48 trace events, 13 errors, 1 snapshots, 7 outputs (11 committed activations)

completion
  keys      completion|000000|000
  status    completed
  events    ACTIVATION_START > LLM_CALL > ACTIVATION_END
  reasons   (none)
  tokens    in=11 out=7 cache_hits=0

multi_tool
  keys      multi_tool|000000|001
  status    completed
  events    ACTIVATION_START > LLM_CALL > TOOL_CALL > TOOL_CALL > ACTIVATION_END
  reasons   (none)
  tokens    in=11 out=7 cache_hits=0

cache_hit
  keys      cache_hit|000000|002
  status    completed
  events    ACTIVATION_START > LLM_CALL > LLM_CALL > ACTIVATION_END
  reasons   (none)
  tokens    in=22 out=14 cache_hits=1

suspension_approved
  keys      suspension_approved|000000|003
  status    suspended, completed
  events    ACTIVATION_START > LLM_CALL > INTENT_EMITTED > SUSPENDED > ACTIVATION_END > ACTIVATION_START > ACTIVATION_END
  reasons   (none)
  tokens    in=11 out=7 cache_hits=0

suspension_denied
  keys      suspension_denied|000000|004
  status    suspended, completed
  events    ACTIVATION_START > LLM_CALL > INTENT_EMITTED > SUSPENDED > ACTIVATION_END > ACTIVATION_START > ACTIVATION_END
  reasons   (none)
  tokens    in=11 out=7 cache_hits=0

suspension_timeout
  keys      suspension_timeout|000000|005
  status    suspended
  events    ACTIVATION_START > LLM_CALL > INTENT_EMITTED > SUSPENDED > ACTIVATION_END > ERROR
  reasons   hitl_timeout
  tokens    in=11 out=7 cache_hits=0

tool_error
  keys      tool_error|000000|006
  status    (none committed)
  events    ERROR
  reasons   activation_error
  tokens    in=0 out=0 cache_hits=0

activation_error
  keys      activation_error|000000|007
  status    (none committed)
  events    ERROR
  reasons   activation_error
  tokens    in=0 out=0 cache_hits=0

budget_exceeded
  keys      budget_exceeded|000000|008
  status    (none committed)
  events    ERROR
  reasons   budget_exceeded
  tokens    in=0 out=0 cache_hits=0

orphaned_result
  keys      orphaned_result|000000|009
  status    completed
  events    ACTIVATION_START > LLM_CALL > ACTIVATION_END > ERROR
  reasons   orphaned_result
  tokens    in=11 out=7 cache_hits=0

intent_dead_letter
  keys      intent_dead_letter|000000|010
  status    completed
  events    ACTIVATION_START > LLM_CALL > INTENT_EMITTED > ACTIVATION_END
  reasons   intent_dead_letter
  tokens    in=11 out=7 cache_hits=0

batch_overflow
  keys      batch_overflow|000000|011
  status    suspended
  events    ACTIVATION_START > INTENT_EMITTED > SUSPENDED > ACTIVATION_END > ERROR
  reasons   batch_buffer_overflow, batch_buffer_overflow, ttl_wiped_batch, ttl_wiped_batch, ttl_wiped_batch, ttl_wiped_batch, ttl_wiped_suspension
  tokens    in=0 out=0 cache_hits=0

11 activations produced
```

Run it twice with the same `--seed` and you get that output byte for byte. That
is not a coincidence and it is not a fixture: trace identity is
`uuid5(entity_key, seq)`, span identity is `uuid5(entity_key, seq, role, index)`,
intent identity is `uuid5(entity_key, seq, step_index)`, and the fake provider
replays a script — so nothing in a round reads a clock or a randomness source.
It is what makes the screenshots on the console page reproducible.

The `--seed` rides in every entity key (`<scenario>|<seed>|<index>`), so a
different seed moves every trace id with it. That matters under `--loop`,
because the store deduplicates on `(trace_id, span_id, event_type)` — the key
`docs/traces.md` publishes — and a round that reused its predecessor's ids would
be silently collapsed onto it instead of showing up as new traffic.

## Pointing your own pipeline at the console

One `AgentConfig`, four keyword arguments:

```python
--8<-- "examples/console_demo/pipeline.py"
```

`ConsoleSinkResolver` *wraps* the runtime's `DefaultSinkResolver` instead of
replacing it, so every other scheme a pipeline already uses is untouched and
`core/transform.py` is not modified. Removing those four arguments returns the
pipeline to byte-for-byte what it was.

## What the demo deliberately does not do

- **It does not invent records.** Every field the console shows comes from a
  `TraceEvent` attribute, an `ActivationErrorRecord` field, or a `StateSnapshot`
  field. The demo drives the runtime; it never hand-writes a record.
- **It does not fabricate durations.** Spans are zero-width by design
  (`start_ms == end_ms`), because measuring elapsed time would need a wall-clock
  read in the hot path. The summary above reports token counts and event
  sequences, never a span width.
- **It does not stand up infrastructure.** `DirectRunner`, a scripted
  `TestStream` for both clocks, and a `FakeLLM`. No Kafka, no BigQuery, no
  collector, no credentials.
