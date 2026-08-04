## ADDED Requirements

### Requirement: `max_tokens_per_activation` is a validated `AgentConfig` field that requires a decoder

`AgentConfig` SHALL expose a kw-only `max_tokens_per_activation: int | None` field defaulting to `None`, where `None` means unlimited and preserves today's behavior exactly. Construction SHALL raise `ValueError` at the construction site for a non-positive value, and SHALL raise `ValueError` when `max_tokens_per_activation` is set while `decode` is `None`: without a decoder the raw path's token counts are unknown, and an unenforceable budget MUST fail at the site of the misconfiguration rather than silently meter nothing. The value SHALL be threaded `RunAgent → _AgentDoFn → run_activation → ActivationContext`, and `ActivationContext` SHALL apply the same set-without-decode rejection at its own construction.

#### Scenario: Unset means unlimited

- **WHEN** a pipeline runs with `max_tokens_per_activation` unset
- **THEN** no budget is enforced, no `BudgetExceeded` can be raised, and the transform's outputs and committed state are byte-identical to the previous release's

#### Scenario: A non-positive budget fails at construction

- **WHEN** `AgentConfig` is constructed with `max_tokens_per_activation=0`
- **THEN** `ValueError` naming the field is raised at the construction site, before any pipeline exists

#### Scenario: A budget without a decoder fails at construction

- **WHEN** `AgentConfig` is constructed with `max_tokens_per_activation` set and `decode=None`
- **THEN** `ValueError` is raised explaining that budget enforcement requires the provider's response decoder

### Requirement: The budget charges the decoded tokens of every consumed model response, cache hits included

The budget meter SHALL be charged the decoded `total_tokens` of every model-call response the agent receives, on both call surfaces (`LlmFacade.complete` and `ActivationContext.call_model`), and SHALL charge a replay-cache hit exactly as it charges the provider-reached call that produced the same bytes. The charge SHALL be a pure function of the response bytes and the call sequence — never of provider-reached-ness, elapsed time, or any other value a bundle retry does not hold fixed — so a replayed activation whose calls are served from the replay cache makes byte-identical budget decisions and mints byte-identical intents.

Billed-usage accounting SHALL be unchanged: `accumulate_usage` remains provider-reached-only, and a cache hit still contributes no billed usage. The budget meters consumption; cost accounting meters spend; the two MAY disagree on a replayed walk.

#### Scenario: A cache hit charges the same as the miss that stored it

- **WHEN** an activation issues a model call that the replay cache serves from a live entry storing a response of N total tokens
- **THEN** the budget meter is charged N — the same charge the original provider-reached call incurred — while billed usage accumulates nothing for the hit

#### Scenario: A chaos-forced bundle retry makes the identical budget decision

- **WHEN** the retry-determinism gate forces a bundle retry of an activation that committed within budget, so the retried walk serves its calls from the replay cache
- **THEN** the retried walk charges the same totals, takes the same branches, re-mints byte-identical intents, and makes zero additional provider calls

#### Scenario: Enforcement holds on both surfaces

- **WHEN** one agent consumes tokens through `LlmFacade.complete` and another through `ActivationContext.call_model`, each crossing the configured budget
- **THEN** both raise `BudgetExceeded`, with the same charging rule and the same running-total semantics

### Requirement: The crossing call trips the budget and the trip is a sticky latch

The budget SHALL trip when the running total strictly exceeds `max_tokens_per_activation`: the check runs after each response is decoded and charged, so the crossing call itself raises `BudgetExceeded` and its response is not returned to the agent. An activation whose consumption lands exactly on the limit SHALL NOT trip. Once tripped, the budget SHALL be exhausted for the remainder of the attempt: every subsequent model call on either surface SHALL raise `BudgetExceeded` at entry, before the replay-cache lookup and before any provider or breaker interaction, so an agent that catches the trip cannot spend again — not even from the cache. `BudgetExceeded` SHALL NOT be a `ProviderError` subclass, so the facade's transport retry loop can never retry a tripped budget against the provider, and its `repr` SHALL be a pure function of the limit and the charged total so failure records built from it are byte-stable under replay.

#### Scenario: The crossing call fails fast

- **WHEN** an activation with a budget of 600 makes calls decoding to 250, 250, and 250 tokens
- **THEN** the third call raises `BudgetExceeded` after its response is decoded and charged, and its response is not returned to the agent

#### Scenario: Exactly at the limit is within budget

- **WHEN** an activation's charged total lands exactly on `max_tokens_per_activation`
- **THEN** no `BudgetExceeded` is raised and the activation may complete normally

#### Scenario: A swallowed trip cannot spend again

- **WHEN** an agent catches `BudgetExceeded` and issues another model call — including one the replay cache could serve
- **THEN** the call raises `BudgetExceeded` at entry, with no cache read, no breaker consultation, and no provider call

#### Scenario: A tripped budget is never retried as a transport failure

- **WHEN** `BudgetExceeded` is raised inside `LlmFacade.complete`
- **THEN** the facade's retry loop does not catch it, no backoff sleep occurs, and no additional provider attempt is made

### Requirement: A budget-exceeded activation is dead-lettered with reason `budget_exceeded` and commits nothing

An uncaught `BudgetExceeded` SHALL propagate through `run_activation`'s existing failure wrap as `ActivationFailed` and be routed by the DoFn to `.errors` with a new reason constant `budget_exceeded`, declared beside `activation_timeout`/`activation_error` and distinct from both. The dead letter's detail SHALL lead with the `BudgetExceeded` `repr` followed by the established ` failed_at_step=<step> after=<last_event>` position suffix, and the synthesized `ERROR` trace event SHALL carry `beam_agents.reason = budget_exceeded`, `error.type = BudgetExceeded`, and the `beam_agents.failure.*` position attributes. The record SHALL be counted as `agent_errors` through the DoFn's single counting chokepoint.

The activation SHALL commit nothing: staged intents, memory writes, replay-cache inserts, traces, and outputs are discarded whole, all five state specs are byte-for-byte unchanged, and `SEQ` does not advance — the atomic-commit invariant applies to a budget kill exactly as to any other activation failure.

#### Scenario: The budget kill produces both enriched records

- **WHEN** an agent makes two in-budget model calls, stages one intent, and trips the budget on its third call
- **THEN** `.errors` carries one record with reason `budget_exceeded` whose detail leads with the `BudgetExceeded` repr and names the failure position, and `.traces` carries one `ERROR` event with `beam_agents.reason = budget_exceeded`, `error.type = BudgetExceeded`, and failure-position attributes reflecting the two calls and one staged intent

#### Scenario: Nothing staged escapes a budget kill

- **WHEN** an activation stages memory writes and intents and then trips the budget
- **THEN** no intent is emitted, no memory or cache blob is written, `SEQ` is unchanged, and the element's only outputs are the dead letter and the synthesized `ERROR` event

#### Scenario: A replayed budget kill produces byte-identical records

- **WHEN** a bundle containing a budget-killed element is retried and the activation walks the same path to the same trip
- **THEN** the dead letter and the `ERROR` event are byte-for-byte identical to the first attempt's

### Requirement: The budget bounds one activation attempt

The budget meter SHALL be constructed fresh for each activation attempt — a resume starts a new meter at zero, exactly as `activation_timeout_s` and the `iterations` sample are already per-attempt — and SHALL NOT be persisted: no `Continuation` field, no wire message, and no state blob carries a token total, so this change has no `state_schema_version` implication and moves no golden blob.

#### Scenario: A resume starts a fresh budget

- **WHEN** an activation consumes most of its budget, suspends, and is later resumed by a tool result
- **THEN** the resumed attempt's meter starts at zero and is bounded by the full `max_tokens_per_activation` for its own consumption

#### Scenario: The continuation is unchanged by budgeting

- **WHEN** a budgeted activation suspends
- **THEN** the persisted `Continuation` is byte-identical to the one the same activation would persist with no budget configured
