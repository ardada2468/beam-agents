## ADDED Requirements

### Requirement: The runtime publishes a fixed metric surface under one namespace

`RunAgent` SHALL publish Beam user metrics under the namespace `beam_agents.runtime`, consisting of exactly seven counters — `activations`, `llm_calls`, `tool_calls`, `intents_emitted`, `agent_errors`, `suspensions`, `orphaned_results` — and exactly six integer distributions — `activation_ms`, `overhead_ms`, `llm_ms`, `tokens`, `memory_bytes`, `iterations`. Names and namespace are part of the observable contract: renaming one breaks every dashboard and alert built on it, so a rename SHALL be treated as a breaking change. The namespace SHALL be distinct from `beam_agents.memory`, which keeps its existing `soft_cap_warnings` counter unchanged.

Metrics SHALL be published unconditionally. There SHALL be no configuration knob to disable them and no `AgentConfig` field controlling them. (`AgentConfig.tool_registry` exists to supply the tools `run_tool` executes; it configures tool execution, not metrics.)

#### Scenario: Every declared metric is queryable after a pipeline run

- **WHEN** a pipeline that activates an agent, emits an intent, suspends, and dead-letters an orphaned result runs to completion on the DirectRunner
- **THEN** querying the pipeline result for the `beam_agents.runtime` namespace returns the declared counters and distributions under exactly those names

#### Scenario: The memory namespace is untouched

- **WHEN** working memory crosses its soft cap during an activation
- **THEN** `beam_agents.memory/soft_cap_warnings` is incremented as before, and no runtime-namespace counter is affected by it

### Requirement: Counters close over the transform's outputs

The counters SHALL be defined so that they account for the transform's outputs exactly, and this SHALL be asserted end-to-end rather than assumed:

- `intents_emitted` SHALL equal the number of elements emitted on `.intents`, including intents minted by a HITL escalation inside the timer callback.
- `agent_errors` plus `orphaned_results` SHALL equal the number of elements emitted on `.errors`, where `orphaned_results` counts exactly those records whose reason is `orphaned_result` and `agent_errors` counts every other reason (`activation_timeout`, `activation_error`, `hitl_timeout`, `ttl_wiped_suspension`).
- `activations` SHALL equal the number of activations that reached the commit path, which is the number of `SEQ` increments.
- `suspensions` SHALL equal the number of committed activations whose outcome was `Suspend`, and SHALL NOT exceed `activations`.

#### Scenario: Intent count matches the intents output

- **WHEN** a pipeline run stages intents across several activations and one HITL escalation
- **THEN** `intents_emitted` equals the number of elements on `.intents`, escalation intent included

#### Scenario: Error counts partition the errors output

- **WHEN** a pipeline run produces a mixture of orphaned results, activation failures, and a HITL timeout drop
- **THEN** `orphaned_results` equals the number of `orphaned_result` records, `agent_errors` equals the count of all other `.errors` records, and their sum equals the total number of elements on `.errors`

#### Scenario: Suspensions are a subset of activations

- **WHEN** a pipeline run mixes completing and suspending activations
- **THEN** `suspensions` counts only the suspending ones and `activations` counts both, so `suspensions <= activations`

### Requirement: Metrics are recorded on the Beam worker thread, never the bridge thread

A Beam metric update resolves its cell through a thread-local state sampler; off the Beam worker thread there is no tracker and the update is discarded with no error. The runtime SHALL therefore never call a Beam metric from the async bridge thread. Counts and durations produced during an activation SHALL be accumulated in a plain in-process tally on the activation context, carried out on the activation result, and recorded by the DoFn on the Beam thread.

The tally SHALL be worker-local: it SHALL NOT be written to `MemoryBlob`, `Continuation`, or any other keyed state, and SHALL NOT appear in any wire message.

#### Scenario: Activation-internal work is counted despite running off-thread

- **WHEN** an activation performs model calls on the async bridge thread and the pipeline completes
- **THEN** `llm_calls` reflects those calls, rather than reading zero because the updates were made from a thread with no metrics container

#### Scenario: The tally never reaches keyed state

- **WHEN** an activation that accumulated counts and durations commits
- **THEN** the persisted `MemoryBlob` and `Continuation` are byte-identical to what the same activation would persist with no metrics recorded

### Requirement: Commit-path metrics are recorded inside the commit, after state writes

The DoFn's fixed commit order SHALL become `MEMORY, LLM_CACHE, CONTINUATION, PENDING, SEQ, timers, metrics, emits`. `activations`, `suspensions`, `intents_emitted`, `llm_calls`, `tool_calls`, and the `memory_bytes`, `iterations`, `tokens`, `llm_ms`, and `overhead_ms` distributions SHALL be recorded at that point, so an activation that failed, timed out, or was refused admission contributes none of them — the same all-or-nothing rule the state mutations obey.

`memory_bytes` SHALL sample the committed working-memory size for that activation. `iterations` SHALL sample the number of agent steps the activation consumed — the advance of the step cursor that mints intent IDs — so a resumed activation samples only its own steps, not its predecessor's.

#### Scenario: A failed activation records no commit-path metric

- **WHEN** an activation raises after staging intents and memory writes
- **THEN** `activations`, `suspensions`, and `intents_emitted` are unchanged, no `memory_bytes`, `iterations`, or `tokens` sample is recorded, and the element is dead-lettered

#### Scenario: A refused resume records no commit-path metric

- **WHEN** a tool result arrives for a key with no live continuation
- **THEN** only `orphaned_results` moves; `activations` is unchanged and no distribution is sampled

#### Scenario: A resumed activation samples only its own steps

- **WHEN** an activation that suspended after three steps is resumed and takes two more before completing
- **THEN** the resume records an `iterations` sample of two, not five

### Requirement: Error and orphan counters are recorded wherever a dead-letter record is emitted

`agent_errors` and `orphaned_results` SHALL be incremented at every site that emits an `ActivationError`, including the two timer callbacks — the HITL fallback's `Drop` route and the `ttl_wiped_suspension` record — and not only on the element path. A dead-letter record that no counter accounts for SHALL be treated as a defect.

#### Scenario: A timer-emitted dead letter is counted

- **WHEN** `TTL_TIMER` fires over a live continuation and emits a `ttl_wiped_suspension` record
- **THEN** `agent_errors` is incremented by one

#### Scenario: A HITL timeout drop is counted

- **WHEN** the HITL policy's timeout route drops the suspension to `.errors`
- **THEN** `agent_errors` is incremented by one, and `activations` is unchanged because a timer fire is not a committed activation

### Requirement: `llm_calls` and `llm_ms` count provider-reached calls only

A replay-cache hit performs no provider call, so it SHALL NOT increment `llm_calls` and SHALL NOT contribute an `llm_ms` sample. Every model call that reaches the provider SHALL increment `llm_calls` exactly once and contribute exactly one `llm_ms` sample measuring the wall time of the provider await. The count of `llm_ms` samples SHALL therefore equal `llm_calls`.

This makes the replay-cache invariant observable: replaying an activation whose calls are all cached adds provider-reached calls of zero.

#### Scenario: A cache hit is not a call

- **WHEN** an activation issues a model call that the replay cache serves from a live entry
- **THEN** `llm_calls` is unchanged and no `llm_ms` sample is recorded

#### Scenario: A provider-reached call is timed once

- **WHEN** an activation issues a model call that misses the cache and reaches the provider
- **THEN** `llm_calls` increments by one and exactly one `llm_ms` sample is recorded for it

### Requirement: Inline read-only tools execute on the runtime surface and are counted

`ActivationContext` SHALL expose `run_tool(tool_name, arguments)`, executing a `side_effect=False` tool inline against the `ToolRegistry` configured on `AgentConfig` (defaulting to an empty registry) — the documented fast-path behavior that previously existed only on the authoring surface. A `side_effect=True` tool SHALL be refused with `SideEffectToolError` before executing, preserving correctness invariant 5, and a refused or failing tool SHALL NOT be counted.

Each successful inline execution SHALL add one `tool_calls` to the activation's tally and one wall-time duration measured with the injected monotonic clock, so `overhead_ms` can exclude tool time. `run_tool` SHALL NOT advance the step cursor: it mints no intent and makes no model call, so it must not perturb `intent_id` derivation or `iterations`.

#### Scenario: A read-only tool runs inline on the runtime surface and is counted

- **WHEN** an agent driven by the stateful DoFn calls `ctx.run_tool(...)` for a registered `side_effect=False` tool and the activation commits
- **THEN** the tool's return value is available to the agent, and `tool_calls` reflects the execution after the pipeline completes

#### Scenario: A side-effecting tool is refused and not counted

- **WHEN** an agent calls `run_tool` naming a `side_effect=True` tool
- **THEN** `SideEffectToolError` is raised before the tool executes, and `tool_calls` is unchanged

#### Scenario: Inline tool execution does not advance the step cursor

- **WHEN** an activation runs an inline tool between two `act(...)` calls
- **THEN** the two intents' `intent_id`s are the same as they would be with no tool call, and `iterations` counts only the two intent steps

### Requirement: `overhead_ms` isolates the runtime's own cost per committed activation

The release-gating latency budget excludes LLM and tool time; `activation_ms` includes both, so it cannot instrument that budget. `overhead_ms` SHALL sample, once per committed activation, the activation's wall time minus the summed wall time of its provider-reached model calls and its inline tool executions, clamped at zero (an agent that awaits calls concurrently can make the subtrahend exceed the wall time). Its sample count SHALL equal `activations`. A failed or timed-out activation contributes an `activation_ms` sample but no `overhead_ms` sample — its tally does not escape the failed activation, so the subtraction is not computable, and a wrong number is worse than none.

#### Scenario: Overhead subtracts model and tool time from the activation

- **WHEN** an activation whose wall time is 10 ms spends 3 ms in a provider-reached model call and 2 ms in an inline tool, and commits
- **THEN** one `overhead_ms` sample of 5 is recorded

#### Scenario: Concurrent calls clamp overhead at zero

- **WHEN** an activation's summed model and tool durations exceed its wall time because calls were awaited concurrently
- **THEN** the `overhead_ms` sample is 0, not negative

#### Scenario: A failed activation contributes no overhead sample

- **WHEN** an activation raises or times out
- **THEN** an `activation_ms` sample is recorded and no `overhead_ms` sample is

### Requirement: `tokens` is sampled only from decoded provider usage

`tokens` SHALL sample the activation's total token usage, accumulated from the token counts the model layer decodes from provider responses and reports through the activation context's staging sink. An activation that decoded no usage SHALL contribute no sample, rather than a zero sample, so the distribution's count means "activations with known usage" and its mean is not deflated by activations whose usage was never decoded.

Cache-served responses SHALL NOT contribute usage, matching the model facade's existing behavior of accumulating usage only on a provider-reached call.

#### Scenario: Decoded usage is sampled

- **WHEN** an activation makes provider-reached model calls whose responses decode to known token counts
- **THEN** one `tokens` sample equal to the activation's summed total tokens is recorded

#### Scenario: An activation with no decoded usage contributes no sample

- **WHEN** an activation completes without any decoded token usage
- **THEN** no `tokens` sample is recorded, and the distribution's sample count is unchanged

### Requirement: `activation_ms` measures every agent run, including failures

`activation_ms` SHALL sample the wall-clock duration of running the agent for one element, measured on the Beam thread around the bounded bridge submission, and SHALL be recorded for failed and timed-out activations as well as successful ones. Its sample count SHALL therefore be at least `activations`. A resume refused at admission never runs the agent and SHALL NOT be sampled.

#### Scenario: A timed-out activation is still timed

- **WHEN** an activation exceeds `activation_timeout_s` and is routed to `.errors`
- **THEN** an `activation_ms` sample covering the elapsed time is recorded, alongside the `agent_errors` increment

#### Scenario: An orphaned result is not timed

- **WHEN** a resume is refused at admission and dead-lettered
- **THEN** no `activation_ms` sample is recorded, because no agent ran

### Requirement: Durations come from an injected monotonic clock and never influence behavior

Durations SHALL be measured with a monotonic clock supplied by injection (defaulting to the process monotonic clock), never from the element's event time and never from a wall clock read inside the agent's own code path. No staged effect, cache key, `intent_id`, deadline, timer mark, or branch SHALL depend on a duration reading: measurement SHALL NOT be a decision input.

The runtime SHALL never read a metric value back. Metrics are output only.

#### Scenario: Replay determinism is unaffected by measurement

- **WHEN** an activation is replayed under the retry-determinism gate with different measured durations
- **THEN** the re-minted intents are byte-identical, the provider is not called again for cached calls, and the committed state matches — the timings differ and nothing else does

#### Scenario: Timings are injectable in tests

- **WHEN** a test supplies a scripted monotonic clock
- **THEN** the recorded `activation_ms` and `llm_ms` samples are exactly the scripted durations, with no reliance on real elapsed time or `sleep`

### Requirement: Metrics are best-effort telemetry, not an exactly-once ledger

Beam reports attempted metric values on most runners, so a bundle that is retried after a failure SHALL be expected to re-apply its increments even though its state and outputs roll back. The runtime SHALL NOT present these counters as an exactly-once accounting of effects, and SHALL NOT use a metric value in any correctness path. The authoritative record of what happened remains `.traces`, `.intents`, and `.errors`; the effector's `intent_id` dedup remains the effectively-once mechanism.

#### Scenario: A retried bundle may double-count without affecting correctness

- **WHEN** a bundle fails after activation and Beam retries it, and the retry commits
- **THEN** the counters may reflect both attempts, while the committed state, the emitted intents, and the effector's exactly-once behavior are unchanged

### Requirement: The recorder is an injectable seam with a no-op implementation

The metric surface SHALL be reachable through a small sink protocol so that unit tests can assert recorded values without a running pipeline, and so a component constructed outside a Beam context records nothing rather than failing. A no-op implementation SHALL be available for that case. The seam SHALL remain private: nothing is added to the public API surface re-exported from the package root, and the DoFn's injection point is a private constructor parameter for tests, not a user-facing configuration knob.

#### Scenario: A fake sink records values in a unit test

- **WHEN** a unit test constructs the DoFn with a recording fake sink and drives an activation with fake state handles
- **THEN** the fake observes the expected counter increments and distribution samples, with no pipeline and no Beam metrics container

#### Scenario: Recording outside a Beam context is harmless

- **WHEN** the Beam-backed recorder is used with no metrics container present
- **THEN** the updates are discarded by Beam and the caller proceeds normally, with no exception
