## 1. Regression tests (written first, must fail for the right reason)

- [x] 1.1 Add a `TestStream` scenario with `ttl_ms` smaller than the HITL window (the configuration no existing test exercises — every current pipeline test pins `_BIG_TTL_MS`): suspend, advance the watermark past the TTL mark and processing time past the deadline, assert the timeout route's output is emitted. Confirm it fails today with an empty main output, not with an assertion about timer internals.
- [x] 1.2 Add the mirror scenario with the two clock advances swapped, asserting the same output, so the fix is shown to make the outcome ordering-independent.
- [x] 1.3 Add an escalation scenario: the escalated deadline extends past the mark the original suspension armed, the watermark crosses that original mark, and an answer to the escalation still resumes.
- [x] 1.4 Add fake-handle unit tests (in the mutmut test selection, alongside `test_dofn_hitl_timer.py`) for `on_ttl`: a live continuation emits `ttl_wiped_suspension` on `.errors` and clears every spec; no continuation emits nothing and clears every spec.
- [x] 1.5 Confirm the existing TTL scenarios (`test_ttl_fire_wipes_state_and_resets_seq`, `test_new_element_rearms_ttl_and_supersedes_old_mark`) still pass unchanged — they use completing agents, so the suspension branch must not touch them.

## 2. Implementation

- [x] 2.1 `_commit`: derive the TTL mark from the suspension deadline — `max(now_ms, result.hitl_deadline_ms) + ttl_ms` when suspending, `now_ms + ttl_ms` otherwise — keeping timers last in the documented commit order.
- [x] 2.2 `on_hitl`: add `beam.DoFn.TimerParam(TTL_TIMER)` and thread it to `_escalate`.
- [x] 2.3 `_escalate`: re-arm `TTL_TIMER` at `deadline_ms + ttl_ms` alongside the existing `HITL_TIMER` re-arm.
- [x] 2.4 `on_ttl`: add `beam.DoFn.KeyParam`, read `CONTINUATION` before clearing, and emit `ActivationError(reason=REASON_TTL_WIPED_SUSPENSION)` when one is live; keep the wipe unconditional.
- [x] 2.5 Add `REASON_TTL_WIPED_SUSPENSION = "ttl_wiped_suspension"` alongside the existing reason constants and export it where the others are exported.
- [x] 2.6 Update `testing/chaos.py` if either monkeypatched signature changed (`on_hitl` gains a parameter — the chaos wrapper mirrors it exactly, defaults included, or Beam injects nothing).

## 3. Gates

- [x] 3.1 `make lint`, `make type` clean.
- [x] 3.2 Full unit tier passes offline with no docker; the two offline HITL semantics gates still pass.
- [x] 3.3 `make coverage-ratchet` at or above baseline; raise `coverage-baseline.toml` if it improves.
- [x] 3.4 `make mutation` passes; re-check `mutation-baseline.toml`'s `dofn.py` ceiling and document any move in the file's comment.
- [x] 3.5 `uv run pre-commit run --all-files` clean.
