## Why

Verification of the `tool-registry` capability found that `ToolRunner.run` never actually executes an `async def` tool: it calls the wrapped callable and returns whatever comes back, which for an async function is an unawaited coroutine object, not the tool's result. Python even raises `RuntimeWarning: coroutine was never awaited`. The tool silently never runs. `beam_agents` is async-first internally and the loop driver will call tools from an async activation context, so the fast path must support async tools, not just sync ones.

## What Changes

- **BREAKING**: `ToolRunner.run` becomes an `async def` method. Existing callers must `await` it.
- `ToolRunner.run` awaits the callable's result when it is awaitable (covers `async def` tools and any sync tool that happens to return an awaitable), and returns the tool's actual result either way.
- `Tool.__call__` is unchanged: calling a tool directly still returns whatever calling the original function would return (a coroutine for an async function), preserving "transparent callable with the original function's semantics."

## Capabilities

### New Capabilities
<!-- None. -->

### Modified Capabilities
- `tool-registry`: The "ToolRunner executes read-only tools inline with argument validation" requirement changes — `ToolRunner.run` is now async and correctly runs both sync and async `side_effect=False` tools, returning the actual result rather than an unawaited coroutine for async tools.

## Impact

- `src/beam_agents/tools/runner.py`: `ToolRunner.run` signature changes from `def run(...)` to `async def run(...)`.
- `tests/tools/test_runner.py`, `tests/tools/test_side_effect_guard.py`: update to `async def` tests that `await runner.run(...)`.
- No change to `Tool`, `ToolRegistry`, `@tool`, or the error taxonomy.
- Any future caller of `ToolRunner.run` (loop driver, adapters) must call it from an async context, which is already how the rest of the runtime's per-activation execution works.
