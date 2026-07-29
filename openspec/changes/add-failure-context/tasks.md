## 1. The wrapper and its context (tests first, per repo convention)

- [x] 1.1 Write loop-level tests first (`tests/core/test_loop.py`): an agent that makes one model call, stages one intent, then raises produces `ActivationFailed` with `__cause__` being the original exception and `FailureContext(step_index=2, last_event="INTENT_EMITTED", staged_intents=1, llm_calls=1)`; a cache-hit call does not move `llm_calls`; `CancelledError` raised inside the agent propagates unwrapped; an agent raising before any staging reports `last_event="ACTIVATION_START"`. Confirm each fails today for the right reason (bare exception, no wrapper).
- [x] 1.2 Add `FailureContext` (frozen, slots) and `ActivationFailed` to `core/loop.py`, and wrap the agent invocation in `run_activation` with `except Exception as exc: raise ActivationFailed(context=...) from exc` — catching `Exception` only, positioned so context construction stays outside the wrap. Update the docstring's "raises whatever the agent raises" to name the wrapper.

## 2. The DoFn surfaces it

- [x] 2.1 Write fake-handle DoFn tests first (inside the mutation selection, alongside `tests/core/test_dofn_failure_traces.py`): on the raise route, both `_start` and `_resume` emit an `ERROR` event carrying all four `beam_agents.failure.*` attributes and `error.type` naming the *original* exception class, and a dead letter whose detail is `repr(cause)` + ` failed_at_step=N after=<EVENT>`; on the timeout route no `beam_agents.failure.*` key is present; state byte-for-byte untouched on both; two identical failing runs synthesize byte-identical enriched events.
- [x] 2.2 Add the four attribute-name constants (`beam_agents.failure.step`, `.last_event`, `.staged_intents`, `.llm_calls`) to `observability/traces.py` beside the existing vocabulary, and let `ActivationTrace.error` (or `_error_trace`) accept the optional `FailureContext`.
- [x] 2.3 Catch `ActivationFailed` in `_start` and `_resume` ahead of the generic `Exception` fallback; build detail and trace attributes from one helper so the two records cannot disagree; keep the generic fallback's shape byte-identical to today for non-wrapped failures.
- [x] 2.4 Keep `_dead_letter` the single counting chokepoint — the enriched route still counts `agent_errors` exactly once.
- [x] 2.5 Update the existing raise-route assertions in `tests/core/test_dofn_failure_traces.py` and `tests/core/test_dofn_metrics.py` that pin the old detail string (`detail == "RuntimeError('agent blew up')"`) to the new suffixed format.

## 3. Gates

- [x] 3.1 `make lint`, `make type` clean.
- [x] 3.2 Full unit tier offline; offline semantics gates (`semantics and not integration`) still pass — the retry-determinism gate must be unaffected (failure paths commit nothing).
- [x] 3.3 `make coverage-ratchet` at or above baseline; raise if improved.
- [x] 3.4 `make mutation` passes; the new `loop.py`/`dofn.py` branches are reachable from the selection and must be killed, not ceilinged; re-check `mutation-baseline.toml` and renumber any shifted `mutation-exclusions.toml` entries for the edited functions.
- [x] 3.5 `uv run pre-commit run --all-files` clean.

## 4. Sequencing note

- [ ] 4.1 Archive after `add-trace-events`: this change's delta modifies the `trace-events` requirement that `add-trace-events` introduces, so its spec sync must land on top of that capability's main spec.
