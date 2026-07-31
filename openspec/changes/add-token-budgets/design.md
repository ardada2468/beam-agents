## Context

Two model-call paths exist, and any budget has to hold on both:

- **The facade path** (`AgentContext` → `LlmFacade.complete`): cache-first, breaker-guarded, retried; *always* decodes — `decode` is a required constructor argument — and reports billed usage through the `StagingSink` seam (`_finish` calls `accumulate_usage` only when `cache_hit` is false).
- **The raw path** (`ActivationContext.call_model`): awaits the provider directly, decodes only when the optional `decode` is configured, and uses the decoded usage solely for trace attributes (`_decoded_usage` returns `None` → attributes omitted, per the "absent, not defaulted" rule).

Three prior decisions constrain the design:

**Decisions must be replay-deterministic.** The retry-determinism gate forces bundle retries and asserts byte-identical intents with zero extra provider calls: the retried walk serves its calls from the replay cache. Whatever feeds the budget branch must read identically on the miss walk and the hit walk. `add-runtime-metrics` D7 drew the line as "measurement is not decision"; a budget is decision, so its inputs are held to the standard clocks were exempted from.

**Failures already have a typed exit.** `add-failure-context` gave the agent-raise path a wrapper (`ActivationFailed` + `FailureContext`) and the DoFn a dedicated handler (`_failed_activation`) that builds both failure records from one context. A budget trip is an agent-path raise; it should ride that machinery, not grow a parallel one.

**The metric surface is a closed contract.** `runtime-metrics` declares "exactly seven counters and exactly six distributions", and defines `agent_errors` over an explicit reason list. New cost distributions and a new reason are spec modifications, not silent additions.

## Goals / Non-Goals

**Goals:**
- A per-activation token bound configured on `AgentConfig`, enforced on both call surfaces, that stops further model spend fast when crossed.
- A charging rule that is a pure function of the deterministic activation walk, so the budget decision is byte-stable under the retry-determinism gate.
- `budget_exceeded` as a first-class dead-letter reason with the failure-position enrichment triage already gets for `activation_error`.
- The billed input/output token split published as runtime metrics, closing the cost-visibility gap the `tokens` total leaves.

**Non-Goals:**
- **A cross-resume (per-`seq`) budget.** Requires persisting consumed tokens in `Continuation` (proto + `state_schema_version` implications) and resolving double-charging when a resume replays suspended-attempt calls from the cache. Left as an open question with the design sketched.
- **A dollar-denominated budget or price sheet.** Prices change out-of-band; the runtime bounds tokens and publishes the split, and cost-in-currency stays a dashboard multiplication.
- **Bounding tool executions or intent counts.** Different resources, different knobs; `iterations` already measures step consumption.
- **Making `BudgetExceeded` uncatchable.** Only `BaseException` inheritance would do that, and the bridge's cancellation semantics forbid new `BaseException`s on the agent path (the `run_activation` wrap catches `Exception` only, deliberately).
- **Enforcing on the no-`decode` raw path.** Unknown usage must not be silently free; it is a configuration error instead (D1).

## Decisions

### D1. The knob lives on `AgentConfig`, and it requires `decode`

`max_tokens_per_activation: int | None = None`, kw-only, on the frozen validated dataclass — the same shape as `tool_registry` and `decode`, threaded through the same `RunAgent → _AgentDoFn → run_activation → ActivationContext` chain. `__post_init__` rejects non-positive values with the field-naming `ValueError` the class already uses (`_require_positive`).

It also rejects `max_tokens_per_activation` set while `decode` is `None`. The raw path's token counts are genuinely unknown without a decoder — `_decoded_usage` returns `None` and the traces omit usage — and a budget over unknown consumption has two bad readings: unknown-is-free (the budget silently does nothing, discovered on an invoice) or unknown-is-fatal (every undecoded call trips it, making the knob unusable). Both are worse than failing at the construction site, which is where this codebase puts every misconfiguration. `ActivationContext` re-checks the same pair defensively at its own construction, since it is buildable without `AgentConfig`.

### D2. Charge consumed responses — cache hits included — never billed ones

`TokenBudget.charge` is fed the decoded `total_tokens` of **every model-call response the agent receives**, whether it came from the provider or the replay cache. This is the load-bearing decision.

The alternative — charging only provider-reached calls, i.e. reusing the `accumulate_usage` billing rule — reads naturally ("budget what you pay for") and is wrong here. Provider-reached-ness is exactly the property a bundle retry does not preserve: the chaos wrapper forces a retry after commit, the retried walk hits the cache where the original missed, and a billed-only meter charges 0 where the original charged N. The budget *branches* on the meter, branches decide which intents get minted, and the gate asserts byte-identical intents. A decision input must be a pure function of the walk. Response bytes are (the cache stores them; `compute_cache_key` pins them to the call sequence), and decoded token counts are a pure function of the bytes — `Decode` is deterministic by the same contract that lets `_decoded_usage` run on stored cache entries. So the charging rule is: decode once, charge `total_tokens`, identically on the hit and miss branches.

`total_tokens` rather than the completion count alone because input tokens are where runaway context growth and most of the spend live; an output-only budget would let the context balloon unbounded. The billed/unbilled split (`usage_attributes(usage, billed=...)`) is untouched and `accumulate_usage` stays provider-reached-only: cost accounting keeps measuring spend, the budget meters consumption, and the two are allowed to disagree — a replayed activation consumes N tokens and bills zero, which is precisely the replay-cache invariant working.

### D3. Post-decode charge at both surfaces, with a sticky entry check

You cannot know a call's cost before its response exists, so a pure pre-check cannot bound anything — the enforcement site has to be after decode. In `LlmFacade.complete` that is `_finish`, where the cache-hit and provider paths already converge and the response is decoded exactly once; the charge lands right after `accumulate_usage`, before the output-schema parse (a call that busted the budget fails as a budget failure, not as whatever its JSON looked like). In `ActivationContext.call_model`, both branches currently decode inside `_stage_llm_trace`; the branches are refactored to decode once, charge, and pass the usage down to the trace builder — no second decode on the hot path.

The trip is one-way: `charge` latches `exhausted` when the running total exceeds the limit (strictly greater — an activation that lands exactly on its budget is within it), and both surfaces check the latch **at entry, before the cache lookup**. The entry check is what makes fail-fast robust rather than advisory: an agent whose `except Exception:` swallows the trip cannot spend again — every subsequent call raises before contacting cache or provider — and the pre-cache placement means a spent budget serves nothing, not even free hits, because served context is still consumption. On the facade path the entry check also precedes the breaker, mirroring the cache-first ordering rationale: budget state, like cache state, must not depend on endpoint health.

When the facade trips it stages the call's `LLM_CALL` trace first (usage attributes present, `error.type=BudgetExceeded`), exactly as `OutputSchemaError` does. On the normal fail-fast path that staged event is discarded with everything else; it becomes visible only if the agent swallows the trip and completes — in which case the committed trace shows precisely where the budget died, which is the one record that scenario needs.

### D4. `BudgetExceeded` is model-layer vocabulary and must not be retryable

It lives in `model/facade.py` beside `TokenUsage` and the existing deliberately-not-`ProviderError` exceptions, for the same reason they are deliberately not: `_call_with_retry` classifies retryability **by class** (`except ProviderError`), and re-calling the provider because the budget tripped would be the exact opposite of the feature. It carries `limit` and `consumed` as attributes and in its message; both are replay-stable (D2), so `repr(exc)` — which the dead-letter detail leads with — is byte-identical under replay, keeping the errors-sink encoding deterministic.

Placement in `model/` rather than `core/` because both enforcement sites are model-call sites, `core/context.py` already imports the facade's vocabulary (`TokenUsage`, `Decode`, `RetryPolicy`), and the reverse import would cycle.

### D5. Routing: cause-typed dispatch in `_failed_activation`, a new reason, no new machinery

`BudgetExceeded` propagates out of the agent as an ordinary raise, so `run_activation`'s existing wrap already delivers it to the DoFn as `ActivationFailed` with a `FailureContext` — position metadata (step cursor, last staged event, staged-intent count, provider-reached calls) comes for free and is exactly what triage wants for "how far did it get before the money ran out". The only new logic is in `_failed_activation`: `isinstance(failed.__cause__, BudgetExceeded)` selects `REASON_BUDGET_EXCEEDED = "budget_exceeded"` over `REASON_ERROR` for both records. The dead letter still flows through `_dead_letter`, the single counting chokepoint, so it lands in `agent_errors` with no new counter wiring; the `ERROR` trace event carries the new reason, `error.type`, and the `beam_agents.failure.*` attributes unchanged in shape.

A dedicated exception catch in `_start`/`_resume` was rejected: the wrap already funnels every agent raise through one handler, and a second catch clause would be a second place for the two records to drift apart — the exact disease `_failed_activation` was built to cure. The reason constant is new vocabulary stated in the `token-budgets` spec (following `add-runtime-metrics`' precedent of stating cross-cutting requirements in the owning capability); the `trace-events` route list is noted as gaining the reason when the deltas reconcile at archive time, rather than carrying a third capability delta that would collide with `add-failure-context`'s in-flight modification of the same requirement.

### D6. The budget bounds one attempt; a resume starts a fresh meter

The glossary is explicit — an activation is "one execution of the agent for one element inside `process()`" — and the runtime already scopes its bounds that way: `activation_timeout_s` restarts on resume, and `iterations` samples "only its own steps, not its predecessor's" by seeding against the continuation's cursor. `max_tokens_per_activation` follows the same reading: the `TokenBudget` is built fresh in the context constructor, per attempt, and nothing about it is persisted.

Spanning resumes was seriously considered and deferred: it needs a `consumed_tokens` field on `Continuation` (additive proto change, golden-blob movement), and it interacts badly with D2 — a resume that re-issues a suspended attempt's request gets a cache hit, which D2 must charge, double-charging the logical activation across attempts unless the persisted meter and the charging rule grow reconciliation logic. Per-attempt has no such interaction, no state change, and an honest name. The trade-off — an agent that suspends every N tokens is unbounded per `seq` — is real but bounded by the HITL deadline/TTL machinery that already limits suspension cycles, and is recorded as an open question.

### D7. Cost metrics: two billed distributions, recorded at commit like every other

`prompt_tokens` and `completion_tokens` join the surface as distributions (matching `tokens`, whose sum/count already serve rate dashboards; Beam counters would lose the per-activation shape and add nothing a distribution's sum lacks). They are **billed**: fed by `accumulate_usage` — which both surfaces call only for provider-reached decodes — into two new `ActivationTally` fields, sampled in `_record_commit` under the same `usage_observed` guard as `tokens`, so their sample counts stay "activations with known usage" and their means are not deflated by zero-padding. They ride the existing staging: filled on the bridge thread, recorded on the Beam thread at commit, absent for failed activations — which means a budget-killed activation contributes *no* cost sample, consistent with `overhead_ms`'s rule that a tally which never escapes is not guessed at.

A `budget_exceeded` counter was considered and rejected: `agent_errors + orphaned_results` is spec-defined to partition `.errors` exactly, an overlapping sub-count would muddy the one closure property the counters are built on, and per-reason counts remain the documented cheap follow-up if operations demand them (`add-runtime-metrics` risks, verbatim). The kill count is on every dead letter and trace today.

## Risks / Trade-offs

- **A catching agent can complete under a tripped budget** → the latch guarantees no further model spend, but an agent that swallows `BudgetExceeded` and returns `Complete` commits its staged effects. Deliberate: the runtime cannot forbid `except Exception:` in user code without `BaseException` (ruled out by the cancellation contract), and graceful wrap-up under a budget is a legitimate authoring pattern. The committed trace carries the trip's `LLM_CALL` event, so the behavior is observable, and the spec pins that no post-trip call reaches cache or provider.
- **The crossing call is paid for** → unavoidable: a call's cost is unknowable until its response exists. The budget's real guarantee is `consumed < limit + one call's worth`; stated in the spec rather than implied.
- **Uncached failure replays are only as deterministic as the provider** → a budget-killed activation commits nothing, so a bundle retry of its element re-calls the provider, and a *real* provider could return a different-sized response and pass. Pre-existing property of every failure path (failed activations never cache); with FakeLLM — every test tier below nightly smoke — responses are scripted and the decision is exact. The determinism claim is scoped accordingly: byte-stable under the retry-determinism gate's cached-path replay.
- **Per-attempt scope is gameable via suspension** → an agent suspending every N tokens spends N per resume, unbounded per `seq`. Accepted for now (D6); each attempt stays bounded and suspension cycles are already bounded by HITL deadlines and intent TTLs. The per-`seq` design is sketched in Open Questions for when demand shows up.
- **`decode` becomes load-bearing for correctness, not just traces** → a decoder that miscounts tokens now mis-budgets. The provider decoders are already conformance-tested offline via `httpx.MockTransport`; the budget tests add exact-charge assertions over scripted responses.
- **Mutation-gate movement** → new branches in `dofn.py` (`_failed_activation` dispatch, two observes), `context.py` (entry check, charge, single-decode refactor), `loop.py` (pass-through), `transform.py` (validation). All reachable from the fake-handle and unit suites, so they must be killed, not ceilinged; baseline and exclusion renumbering re-checked per the established procedure.

## Migration Plan

No wire, state, or default-behavior change: `max_tokens_per_activation` defaults to `None`, an unset pipeline is byte-identical, and there is no proto edit, so pipeline `--update` is unaffected and no golden blob moves. Adopters set the field (and a `decode`) and gain the bound; rollback is unsetting it or reverting the code, with no data implications. The two new metric names appear on dashboards only when queried; the new `budget_exceeded` reason reaches `.errors` consumers only from pipelines that opted in.

## Open Questions

- Should a per-`seq` budget spanning resumes ship later — `Continuation.consumed_tokens` (additive proto field), charged from the same meter, with cache-hit charges on a resume reconciled against the persisted total to avoid double-charging the logical activation?
- Should `BudgetExceeded` be re-exported from the package root once adapter authors need it routinely, alongside a documented "catch and wrap up" pattern? For now it lives in `beam_agents.model` with the rest of the facade vocabulary.
- Should the LangGraph adapter surface the budget to graph nodes (e.g. remaining tokens on its config), so authored graphs can degrade before the trip instead of dying on it?
- Should there be a per-call `max_tokens_per_call` sibling for the single-huge-call case, which a per-activation post-decode budget can only catch after paying for it once?
