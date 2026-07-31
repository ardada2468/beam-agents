# runtime-metrics Delta Specification

## MODIFIED Requirements

### Requirement: The runtime publishes a fixed metric surface under one namespace

`RunAgent` SHALL publish Beam user metrics under the namespace `beam_agents.runtime`, consisting of exactly eleven counters — `activations`, `llm_calls`, `tool_calls`, `intents_emitted`, `agent_errors`, `suspensions`, `orphaned_results`, `longterm_upserts`, `events_buffered`, `batch_flushes_size`, `batch_flushes_timer` — and exactly nine integer distributions — `activation_ms`, `overhead_ms`, `llm_ms`, `tokens`, `prompt_tokens`, `completion_tokens`, `memory_bytes`, `iterations`, `batch_size`. Names and namespace are part of the observable contract: renaming one breaks every dashboard and alert built on it, so a rename SHALL be treated as a breaking change. The namespace SHALL be distinct from `beam_agents.memory`, which keeps its existing `soft_cap_warnings` counter unchanged.

The four batch metrics SHALL be recorded as follows: `events_buffered` once per `event` element appended to the `BATCH` buffer; `batch_flushes_size` and `batch_flushes_timer` once per *committed* flush activation, by the trigger that fired it; `batch_size` sampled once per committed flush with the number of batched envelopes. Under `BatchPolicy.NONE` all four SHALL read zero.

Metrics SHALL be published unconditionally. There SHALL be no configuration knob to disable them and no `AgentConfig` field controlling them. (`AgentConfig.tool_registry` exists to supply the tools `run_tool` executes, the `AgentConfig` batch knobs configure batching behavior, and `AgentConfig.max_tokens_per_activation` bounds an activation's token consumption; each configures runtime behavior, not metrics, which are recorded identically whether or not any of them is set.)

#### Scenario: Every declared metric is queryable after a pipeline run

- **WHEN** a pipeline that activates an agent, emits an intent, suspends, and dead-letters an orphaned result runs to completion on the DirectRunner
- **THEN** querying the pipeline result for the `beam_agents.runtime` namespace returns the declared counters and distributions under exactly those names, the cost pair `prompt_tokens`/`completion_tokens` included

#### Scenario: The memory namespace is untouched

- **WHEN** working memory crosses its soft cap during an activation
- **THEN** `beam_agents.memory/soft_cap_warnings` is incremented as before, and no runtime-namespace counter is affected by it

#### Scenario: Batch metrics reconcile with batch behavior

- **WHEN** an `ADAPTIVE` pipeline buffers five events for a key, flushes three on the size threshold, and flushes two on a `FLUSH_TIMER` firing
- **THEN** `events_buffered` reads 5, `batch_flushes_size` reads 1, `batch_flushes_timer` reads 1, and `batch_size` holds two samples (3 and 2)

### Requirement: Counters close over the transform's outputs

The counters SHALL be defined so that they account for the transform's outputs exactly, and this SHALL be asserted end-to-end rather than assumed:

- `intents_emitted` SHALL equal the number of elements emitted on `.intents`, including intents minted by a HITL escalation inside the timer callback.
- `agent_errors` plus `orphaned_results` SHALL equal the number of elements emitted on `.errors`, where `orphaned_results` counts exactly those records whose reason is `orphaned_result` and `agent_errors` counts every other reason (`activation_timeout`, `activation_error`, `budget_exceeded`, `hitl_timeout`, `ttl_wiped_suspension`, `ttl_wiped_batch`, `batch_buffer_overflow`).
- `activations` SHALL equal the number of activations that reached the commit path, which is the number of `SEQ` increments; a committed batch flush counts as one activation regardless of batch size.
- `suspensions` SHALL equal the number of committed activations whose outcome was `Suspend`, and SHALL NOT exceed `activations`.
- `batch_flushes_size` plus `batch_flushes_timer` SHALL equal the number of committed flush activations, and SHALL equal the number of `batch_size` samples.

#### Scenario: Intent count matches the intents output

- **WHEN** a pipeline run stages intents across several activations and one HITL escalation
- **THEN** `intents_emitted` equals the number of elements on `.intents`, escalation intent included

#### Scenario: Error counts partition the errors output

- **WHEN** a pipeline run produces a mixture of orphaned results, activation failures, a batch-buffer overflow, a budget-exceeded activation, and a HITL timeout drop
- **THEN** `orphaned_results` equals the number of `orphaned_result` records, `agent_errors` equals the count of all other `.errors` records — the `budget_exceeded` record among them — and their sum equals the total number of elements on `.errors`

#### Scenario: A budget kill is an agent error, not a committed activation

- **WHEN** an activation trips its token budget and is dead-lettered with reason `budget_exceeded`
- **THEN** `agent_errors` is incremented by one, and `activations`, `suspensions`, and every commit-path distribution are unchanged, because the activation never reached the commit path

#### Scenario: Suspensions are a subset of activations

- **WHEN** a pipeline run mixes completing and suspending activations
- **THEN** `suspensions` counts only the suspending ones and `activations` counts both, so `suspensions <= activations`

#### Scenario: A batch flush is one activation

- **WHEN** a committed flush activation runs over four buffered events
- **THEN** `activations` increases by one — not four — matching the single `SEQ` increment, and the flush contributes one `batch_size` sample of 4

### Requirement: Commit-path metrics are recorded inside the commit, after state writes

The DoFn's fixed commit order SHALL become `MEMORY, LLM_CACHE, CONTINUATION, PENDING, SEQ, timers, metrics, emits`. `activations`, `suspensions`, `intents_emitted`, `llm_calls`, `tool_calls`, and the `memory_bytes`, `iterations`, `tokens`, `prompt_tokens`, `completion_tokens`, `llm_ms`, and `overhead_ms` distributions SHALL be recorded at that point, so an activation that failed, timed out, or was refused admission contributes none of them — the same all-or-nothing rule the state mutations obey.

`memory_bytes` SHALL sample the committed working-memory size for that activation. `iterations` SHALL sample the number of agent steps the activation consumed — the advance of the step cursor that mints intent IDs — so a resumed activation samples only its own steps, not its predecessor's.

#### Scenario: A failed activation records no commit-path metric

- **WHEN** an activation raises after staging intents and memory writes
- **THEN** `activations`, `suspensions`, and `intents_emitted` are unchanged, no `memory_bytes`, `iterations`, `tokens`, `prompt_tokens`, or `completion_tokens` sample is recorded, and the element is dead-lettered

#### Scenario: A refused resume records no commit-path metric

- **WHEN** a tool result arrives for a key with no live continuation
- **THEN** only `orphaned_results` moves; `activations` is unchanged and no distribution is sampled

#### Scenario: A resumed activation samples only its own steps

- **WHEN** an activation that suspended after three steps is resumed and takes two more before completing
- **THEN** the resume records an `iterations` sample of two, not five

### Requirement: `tokens` is sampled only from decoded provider usage

`tokens`, `prompt_tokens`, and `completion_tokens` SHALL each sample once per committed activation from the token counts the model layer decodes from provider responses and reports through the activation context's staging sink: `tokens` the activation's summed total, `prompt_tokens` its summed input tokens, and `completion_tokens` its summed output tokens — the input/output split provider price sheets are quoted in. An activation that decoded no usage SHALL contribute no sample to any of the three, rather than a zero sample, so each distribution's count means "activations with known usage" and its mean is not deflated by activations whose usage was never decoded.

Cache-served responses SHALL NOT contribute usage, matching the model facade's existing behavior of accumulating usage only on a provider-reached call. The token-budget meter is deliberately not this accounting: it charges cache hits for replay determinism, while these distributions remain billed-only, so a replayed activation may consume tokens here while billing zero.

#### Scenario: Decoded usage is sampled

- **WHEN** an activation makes provider-reached model calls whose responses decode to known token counts
- **THEN** one `tokens` sample equal to the activation's summed total tokens is recorded, alongside one `prompt_tokens` sample of its summed input tokens and one `completion_tokens` sample of its summed output tokens

#### Scenario: An activation with no decoded usage contributes no sample

- **WHEN** an activation completes without any decoded token usage
- **THEN** no `tokens`, `prompt_tokens`, or `completion_tokens` sample is recorded, and each distribution's sample count is unchanged

#### Scenario: A replayed walk bills nothing

- **WHEN** an activation whose model calls are all served from the replay cache commits
- **THEN** no `tokens`, `prompt_tokens`, or `completion_tokens` sample is recorded for it, even though the budget meter charged its cached responses
