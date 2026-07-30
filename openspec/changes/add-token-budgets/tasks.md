## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 `tests/model/test_budget.py`: the `TokenBudget` meter itself — charges accumulate; the trip is strictly-greater (exactly-at-limit does not raise, per "Exactly at the limit is within budget"); the first charge past the limit raises `BudgetExceeded` carrying `limit` and `consumed`; the latch is sticky (every later `charge` and entry check raises); `repr` is a pure function of limit and consumed; `BudgetExceeded` is not a `ProviderError`.
- [ ] 1.2 `tests/model/test_facade.py`: facade enforcement — a call whose decoded total crosses the budget raises `BudgetExceeded` from `_finish` after billed usage accumulates, staging an `LLM_CALL` trace with `error.type = BudgetExceeded` and usage attributes, and does not return a `FacadeResult` ("The crossing call fails fast"); a cache hit charges the stored response's decoded total while accumulating no billed usage ("A cache hit charges the same as the miss that stored it"); a post-trip call raises at entry with no cache read, no breaker consultation, and no provider call ("A swallowed trip cannot spend again"); the retry loop never retries a trip — no sleep, no second provider attempt ("A tripped budget is never retried as a transport failure"); `budget=None` leaves every existing facade test byte-identical.
- [ ] 1.3 `tests/core/test_context.py`: raw-path enforcement — `ActivationContext.call_model` with a `decode` and a budget charges the decoded total on both the hit and miss branches, decodes each response exactly once, and trips identically on either ("Enforcement holds on both surfaces"); constructing `ActivationContext` with a budget and no `decode` raises `ValueError`; `accumulate_usage` on both surfaces now also sums `prompt_tokens`/`completion_tokens` into the tally.
- [ ] 1.4 `tests/core/test_transform.py`: `AgentConfig` validation — `max_tokens_per_activation=0` and `=-1` raise `ValueError` naming the field ("A non-positive budget fails at construction"); set-with-`decode=None` raises with the decoder explanation ("A budget without a decoder fails at construction"); unset validates fine and `RunAgent.expand` passes the field through to `_AgentDoFn`.
- [ ] 1.5 `tests/core/test_loop.py`: an agent that trips its budget propagates `ActivationFailed` whose `__cause__` is the `BudgetExceeded` and whose `FailureContext` reflects the calls and intents staged before the trip; a `run_activation` call without a budget is unchanged.
- [ ] 1.6 `tests/core/test_dofn_budget.py` (fake-handle unit tests, inside the mutmut selection, over `tests/core/_dofn_fakes.py`): the budget-kill route on both `_start` and `_resume` — one `.errors` record with reason `budget_exceeded` whose detail leads with the `BudgetExceeded` repr and carries the `failed_at_step=`/`after=` suffix, one `ERROR` trace with `beam_agents.reason = budget_exceeded`, `error.type = BudgetExceeded`, and the four `beam_agents.failure.*` attributes ("The budget kill produces both enriched records"); all five state specs byte-for-byte unchanged and no intent emitted ("Nothing staged escapes a budget kill"); `agent_errors` incremented once through the chokepoint and no commit-path metric moved ("A budget kill is an agent error, not a committed activation"); a non-budget raise still routes as `activation_error`; two identical failing runs produce byte-identical records ("A replayed budget kill produces byte-identical records").
- [ ] 1.7 `tests/core/test_dofn_metrics.py` + `tests/core/test_dofn_pipeline.py`: cost metrics — a committed activation with decoded usage records one `prompt_tokens` and one `completion_tokens` sample matching the summed split, alongside `tokens` ("Decoded usage is sampled"); no sample in any of the three without decoded usage ("An activation with no decoded usage contributes no sample"); an all-cache-hit activation records none ("A replayed walk bills nothing"); the DirectRunner pipeline query returns both new names under `beam_agents.runtime` ("Every declared metric is queryable after a pipeline run").
- [ ] 1.8 Suspension/resume scope: an activation that consumes under budget, suspends, and resumes gets a fresh meter and completes ("A resume starts a fresh budget"); the persisted `Continuation` is byte-identical with and without a budget configured ("The continuation is unchanged by budgeting").
- [ ] 1.9 Extend the retry-determinism semantics gate (`tests/semantics/test_retry_determinism.py`): a chaos-forced bundle retry of a budgeted activation that committed within budget re-walks via cache hits, charges the same totals, re-mints byte-identical intents, and makes zero additional FakeLLM calls ("A chaos-forced bundle retry makes the identical budget decision").

## 2. The budget core (`model/facade.py`)

- [ ] 2.1 Add `BudgetExceeded` (not a `ProviderError`; carries `limit` and `consumed`, deterministic `repr`) and `TokenBudget` (`charge(total_tokens)`, strictly-greater trip, sticky `exhausted` latch, `check()` entry guard) beside `TokenUsage` and the existing non-retryable exceptions.
- [ ] 2.2 `LlmFacade`: optional kw-only `budget: TokenBudget | None = None` constructor parameter; entry `check()` at the top of `complete`, before the cache lookup and breaker; charge in `_finish` immediately after `accumulate_usage`, on both the hit and provider paths; on trip, stage the call's `LLM_CALL` trace with `error.type` (mirroring `OutputSchemaError`) and raise.
- [ ] 2.3 Export `BudgetExceeded` (and `TokenBudget` for construction sites) from `model/__init__.py`; nothing added to the package root.

## 3. The context surfaces (`core/context.py`, `core/loop.py`)

- [ ] 3.1 `ActivationContext`: accept kw-only `max_tokens_per_activation: int | None = None`; reject set-without-`decode` at construction; build the per-attempt `TokenBudget`; in `call_model`, entry-check before the cache lookup, refactor the hit/miss branches to decode once, charge, and pass the decoded usage into `_stage_llm_trace` (no second decode on the hot path); on trip, stage the `LLM_CALL` trace then raise.
- [ ] 3.2 `AgentContext`: accept the same kw-only knob, build the budget, and hand it to the `LlmFacade` it constructs — the facade is this surface's only model path, so no further wiring.
- [ ] 3.3 Both surfaces' `accumulate_usage`: also sum `prompt_tokens` and `completion_tokens` into the tally (billed-only rule unchanged — the facade calls it only on provider-reached decodes).
- [ ] 3.4 `run_activation`: thread `max_tokens_per_activation` through to the `ActivationContext` constructor, defaulted `None` so every existing call site still builds.

## 4. Config and DoFn (`core/transform.py`, `core/dofn.py`, `observability/metrics.py`)

- [ ] 4.1 `AgentConfig.max_tokens_per_activation` (kw-only, default `None`): `_require_positive` when set, the set-without-`decode` `ValueError`, and the `RunAgent.expand` pass-through into `_AgentDoFn`.
- [ ] 4.2 `observability/metrics.py`: `DISTRIBUTION_PROMPT_TOKENS = "prompt_tokens"` and `DISTRIBUTION_COMPLETION_TOKENS = "completion_tokens"` added to `DISTRIBUTIONS` in the documented surface order; `ActivationTally` gains `prompt_tokens`/`completion_tokens` int fields.
- [ ] 4.3 `core/dofn.py`: `REASON_BUDGET_EXCEEDED = "budget_exceeded"` beside the existing reason constants and in `__all__`; `_failed_activation` selects it via `isinstance(cause, BudgetExceeded)` and otherwise keeps `REASON_ERROR` byte-identical; the dead letter still flows through the `_dead_letter` chokepoint; `_AgentDoFn.__init__` accepts and forwards `max_tokens_per_activation` into `_activate`'s `run_activation` call.
- [ ] 4.4 `_record_commit`: observe `prompt_tokens`/`completion_tokens` under the existing `usage_observed` guard, beside `tokens`.
- [ ] 4.5 `testing/chaos.py`: confirm the `_commit` wrapper's mirrored signature is unmoved (no `_commit` parameter changed here) and the chaos-forced retry path exercises the budget charge on cache hits.

## 5. Documentation

- [ ] 5.1 `docs/metrics.md`: the two new distributions, their billed-only sampling rule, and the explicit contrast with the budget meter's consumed-tokens rule.
- [ ] 5.2 `docs/errors.md`: the `budget_exceeded` reason, its detail format (`BudgetExceeded(...)` repr + position suffix), and the catch-and-wrap-up caveat (a swallowing agent commits but cannot spend further).

## 6. Gates

- [ ] 6.1 `make lint` and `make type` clean (`mypy --strict`, no `Any` in public signatures).
- [ ] 6.2 `make test-unit` passes offline with no docker.
- [ ] 6.3 `make test-semantics-offline` passes, including the extended retry-determinism assertion.
- [ ] 6.4 Coverage ratchet (`make coverage-ratchet`) at or above baseline; raise `coverage-baseline.toml` if improved.
- [ ] 6.5 `make mutation` passes — `dofn.py`, `context.py`, `loop.py`, and `transform.py` are all under the gate; new branches are reachable from the fake-handle and unit suites and must be killed, not ceilinged; re-check `mutation-baseline.toml` and renumber any shifted `mutation-exclusions.toml` entries per the established procedure.
- [ ] 6.6 `uv run pre-commit run --all-files` clean.
- [ ] 6.7 `openspec validate add-token-budgets --strict` passes.

## 7. Sequencing note

- [ ] 7.1 Archive after `add-runtime-metrics`: this change's delta modifies three `runtime-metrics` requirements that change introduces, so its spec sync must land on top of that capability's main spec. When the deltas reconcile, the `trace-events` failure-route list gains `budget_exceeded` alongside the routing requirement stated in `token-budgets`.
