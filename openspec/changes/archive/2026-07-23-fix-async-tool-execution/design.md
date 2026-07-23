## Context

`ToolRunner.run` was implemented as a plain sync method that validates arguments and calls `Tool.__call__`. For a sync tool this works; for an `async def` tool, `Tool.__call__` returns the coroutine object (calling an async function doesn't run it), and the sync `run` method just handed that coroutine back to its caller as if it were the result — silently discarding the tool's actual execution. Caught by an end-to-end verification pass, not by the original test suite, because no test exercised an async-def tool.

## Goals / Non-Goals

**Goals:**
- `ToolRunner.run` actually executes async tools and returns their real result.
- Sync tools keep working exactly as before, with no added overhead of note.
- `Tool.__call__`'s semantics (transparent proxy to the original callable) stay unchanged, so direct calls to an async tool still behave like calling the original async function.

**Non-Goals:**
- Timeouts, cancellation, or activation-timeout semantics around awaiting a tool — that belongs to the loop driver's activation-timeout bridge (`core/dofn.py`), not this fast-path runner.
- Detecting async-ness at `@tool` decoration time or restricting it — any callable is accepted; `ToolRunner` adapts to what calling it returns.

## Decisions

**Detect awaitability by inspecting the call result (`inspect.isawaitable`), not the function (`inspect.iscoroutinefunction`).**
Checking the actual return value is one dynamic check per call, is naturally correct for any awaitable-returning callable, and keeps `Tool` itself unchanged (no new `is_async` field to keep in sync with the wrapped callable). *Alternative:* tag `Tool` with `is_async` at construction via `inspect.iscoroutinefunction`. Rejected — adds a field only `ToolRunner` needs, and doesn't generalize to sync functions that happen to return an awaitable (e.g. via `functools.partial` over an async function, or a sync wrapper returning a `Future`).

**`ToolRunner.run` becomes `async def`; there is no sync variant.**
The runtime is async-first internally and the loop driver already calls tools from within an async activation. A dual sync/async API would double the surface for one fast-path runner used from exactly one kind of caller. *Alternative:* keep `run` sync and add `run_async`. Rejected — unnecessary surface area; every real caller is already async.

## Risks / Trade-offs

- **Breaking change to `ToolRunner.run`'s signature** (sync → async). → Scoped: `ToolRunner` has no other in-tree callers yet (the loop driver that will call it doesn't exist), so the only affected code is this change's own tests, updated alongside the fix.
- **`inspect.isawaitable` misses generator-based coroutines from `@asyncio.coroutine`** (removed in Python 3.11, the project's minimum version) — not a real risk on this codebase's supported Python range.
