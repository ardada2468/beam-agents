## 1. Tests first (scenario → test)

- [x] 1.1 Add a failing test in `tests/tools/test_runner.py` for an `async def` tool: `ToolRunner.run` must return the tool's actual result, not a coroutine
- [x] 1.2 Convert existing `ToolRunner.run` call sites in `tests/tools/test_runner.py` and `tests/tools/test_side_effect_guard.py` to `async def` tests that `await runner.run(...)`
- [x] 1.3 Confirm the new async-tool test fails for the right reason (returns/asserts against a coroutine, or a `RuntimeWarning`) before implementing

## 2. Fix ToolRunner

- [x] 2.1 Change `ToolRunner.run` to `async def run(...)` in `src/beam_agents/tools/runner.py`
- [x] 2.2 After calling the tool, `await` the result when `inspect.isawaitable(result)` is true; otherwise return it directly

## 3. Quality gates

- [x] 3.1 `ruff` clean, `mypy --strict` clean on `src/`; full test suite green; coverage does not decrease
- [x] 3.2 Manually re-verify: sync tool runs inline, async tool runs inline and returns its real result (not a coroutine), side-effect tool called directly still raises with its actionable message
