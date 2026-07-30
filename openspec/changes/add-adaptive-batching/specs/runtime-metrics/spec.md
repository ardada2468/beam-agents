# runtime-metrics Delta Specification

## MODIFIED Requirements

### Requirement: The runtime publishes a fixed metric surface under one namespace

`RunAgent` SHALL publish Beam user metrics under the namespace `beam_agents.runtime`, consisting of exactly ten counters — `activations`, `llm_calls`, `tool_calls`, `intents_emitted`, `agent_errors`, `suspensions`, `orphaned_results`, `events_buffered`, `batch_flushes_size`, `batch_flushes_timer` — and exactly seven integer distributions — `activation_ms`, `overhead_ms`, `llm_ms`, `tokens`, `memory_bytes`, `iterations`, `batch_size`. Names and namespace are part of the observable contract: renaming one breaks every dashboard and alert built on it, so a rename SHALL be treated as a breaking change. The namespace SHALL be distinct from `beam_agents.memory`, which keeps its existing `soft_cap_warnings` counter unchanged.

The four batch metrics SHALL be recorded as follows: `events_buffered` once per `event` element appended to the `BATCH` buffer; `batch_flushes_size` and `batch_flushes_timer` once per *committed* flush activation, by the trigger that fired it; `batch_size` sampled once per committed flush with the number of batched envelopes. Under `BatchPolicy.NONE` all four SHALL read zero.

Metrics SHALL be published unconditionally. There SHALL be no configuration knob to disable them and no `AgentConfig` field controlling them. (`AgentConfig.tool_registry` exists to supply the tools `run_tool` executes, and the `AgentConfig` batch knobs configure batching behavior; neither configures metrics.)

#### Scenario: Every declared metric is queryable after a pipeline run

- **WHEN** a pipeline that activates an agent, emits an intent, suspends, and dead-letters an orphaned result runs to completion on the DirectRunner
- **THEN** querying the pipeline result for the `beam_agents.runtime` namespace returns the declared counters and distributions under exactly those names

#### Scenario: The memory namespace is untouched

- **WHEN** working memory crosses its soft cap during an activation
- **THEN** `beam_agents.memory/soft_cap_warnings` is incremented as before, and no runtime-namespace counter is affected by it

#### Scenario: Batch metrics reconcile with batch behavior

- **WHEN** an `ADAPTIVE` pipeline buffers five events for a key, flushes three on the size threshold, and flushes two on a `FLUSH_TIMER` firing
- **THEN** `events_buffered` reads 5, `batch_flushes_size` reads 1, `batch_flushes_timer` reads 1, and `batch_size` holds two samples (3 and 2)

### Requirement: Counters close over the transform's outputs

The counters SHALL be defined so that they account for the transform's outputs exactly, and this SHALL be asserted end-to-end rather than assumed:

- `intents_emitted` SHALL equal the number of elements emitted on `.intents`, including intents minted by a HITL escalation inside the timer callback.
- `agent_errors` plus `orphaned_results` SHALL equal the number of elements emitted on `.errors`, where `orphaned_results` counts exactly those records whose reason is `orphaned_result` and `agent_errors` counts every other reason (`activation_timeout`, `activation_error`, `hitl_timeout`, `ttl_wiped_suspension`, `ttl_wiped_batch`, `batch_buffer_overflow`).
- `activations` SHALL equal the number of activations that reached the commit path, which is the number of `SEQ` increments; a committed batch flush counts as one activation regardless of batch size.
- `suspensions` SHALL equal the number of committed activations whose outcome was `Suspend`, and SHALL NOT exceed `activations`.
- `batch_flushes_size` plus `batch_flushes_timer` SHALL equal the number of committed flush activations, and SHALL equal the number of `batch_size` samples.

#### Scenario: Intent count matches the intents output

- **WHEN** a pipeline run stages intents across several activations and one HITL escalation
- **THEN** `intents_emitted` equals the number of elements on `.intents`, escalation intent included

#### Scenario: Error counts partition the errors output

- **WHEN** a pipeline run produces a mixture of orphaned results, activation failures, a batch-buffer overflow, and a HITL timeout drop
- **THEN** `orphaned_results` equals the number of `orphaned_result` records, `agent_errors` equals the count of all other `.errors` records, and their sum equals the total number of elements on `.errors`

#### Scenario: Suspensions are a subset of activations

- **WHEN** a pipeline run mixes completing and suspending activations
- **THEN** `suspensions` counts only the suspending ones and `activations` counts both, so `suspensions <= activations`

#### Scenario: A batch flush is one activation

- **WHEN** a committed flush activation runs over four buffered events
- **THEN** `activations` increases by one — not four — matching the single `SEQ` increment, and the flush contributes one `batch_size` sample of 4
