# IoT reaction

Keyed rolling memory on a stream: react to a *trend*, not to every reading.
Per-device temperature readings flow through one agent. Each activation
appends its reading to a bounded ring in working memory
(`ctx.memory.append(..., max_items=...)`) and reads the rolled window back
with `ctx.memory.ring(...)`. Beam's per-key serialization makes this race-free
by construction — one element at a time per device, with cross-device
parallelism supplied by the runner.

Two devices are scripted:

- **sensor-1** stays quiet. Its three readings accumulate in the window
  (`ok:sensor-1:window=1`, `window=2`, `window=3`) and the agent completes
  each activation **without a model call** — the runtime does not charge
  tokens for uninteresting events.
- **sensor-2** trends hot. When its window average crosses the threshold, the
  agent makes exactly one scripted model call, emits
  `b"reaction:sensor-2:throttle"`, and resets the window so one sustained
  excursion produces one reaction — the same reset idiom as the
  [failure-streak alarm](../errors.md#example-a-downstream-failure-streak-alarm).

The zero-model-calls-on-quiet-readings claim is a **tested property, not
prose**:
[`tests/examples/test_iot_reaction.py`](https://github.com/ardada2468/beam-agents/blob/main/tests/examples/test_iot_reaction.py)
runs the quiet readings alone and asserts the `FakeLLM` recorded zero calls,
then runs the full script and asserts exactly one.

!!! note "Memory is bounded, and it expires"

    The ring holds at most `WINDOW_ITEMS` readings per device — `max_items`
    trims on every append. Beyond the example: working memory is capped at
    1 MiB per key (individual blobs at 100 KiB), and a per-key TTL timer
    garbage-collects state for devices that go silent, so state growth stays
    bounded by construction. See the state bounds in
    [`openspec/project.md`](https://github.com/ardada2468/beam-agents/blob/main/openspec/project.md).

## Run it

```sh
uv run python -m examples.iot_reaction
```

```text
b'ok:sensor-1:window=1'
b'ok:sensor-2:window=1'
b'ok:sensor-1:window=2'
b'reaction:sensor-2:throttle'
b'ok:sensor-1:window=3'
b'ok:sensor-2:window=1'
```

(The final `ok:sensor-2:window=1` is the reading after the reaction: the reset
window is growing again.)

## The whole program

The code below is included verbatim from
[`examples/iot_reaction.py`](https://github.com/ardada2468/beam-agents/blob/main/examples/iot_reaction.py)
— the same file the CI test executes.

```python title="examples/iot_reaction.py"
--8<-- "examples/iot_reaction.py"
```
