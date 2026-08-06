## ADDED Requirements

### Requirement: Chaos helper fails exactly one targeted commit, once
`beam_agents.testing.chaos` SHALL provide a context manager that, for its duration, causes the first `_AgentDoFn` commit whose `ActivationResult` satisfies a caller-supplied predicate to raise, and lets every commit thereafter — including Beam's own retry of that same failed bundle — proceed unmodified through the original commit logic. It SHALL be test-only infrastructure living under `beam_agents.testing` and SHALL NOT be re-exported from the root `beam_agents` API, and it SHALL require no change to `_AgentDoFn` or any other production code (fault injection is applied and removed from the test module only, for the context manager's duration).

#### Scenario: Only the first matching commit fails

- **WHEN** the chaos context manager is active with a predicate matching a given activation's `ActivationResult`, and that activation's commit is attempted twice (once failed, once retried)
- **THEN** the first attempt raises and the second attempt (Beam's retry) commits normally, and no other activation's commit is affected

#### Scenario: A non-matching commit is never failed

- **WHEN** the chaos context manager is active with a predicate that does not match a given activation's `ActivationResult`
- **THEN** that activation's commit proceeds normally on its first attempt

### Requirement: A forced retry of a resumed activation adds zero provider calls
For an activation that suspends after issuing an LLM call and later resumes and re-issues the identical request (same `entity_key`, same `seq`, same request material) before completing, forcing the resume's own first commit attempt to fail SHALL NOT increase the number of real provider invocations: the request SHALL be served from the `LLM_CACHE` state committed at suspend time on both the discarded failed attempt and the successful retry, so the total count of real provider calls for that request across the whole activation (suspend through resumed completion, chaos-forced retry included) equals the count from a run with no forced retry.

#### Scenario: Suspend-time cache serves the resume's repeated call under a forced retry

- **WHEN** an activation calls the model (cache miss), stages an intent, and suspends; a matching `tool_result` resumes it; the resume calls the model again with the identical request before completing; and the resume's first commit attempt is chaos-forced to fail
- **THEN** the committed `.traces` contain exactly one `LLM_CALL` event with `attributes["beam_agents.cache_hit"] == "false"` (the original call) and exactly one with `attributes["beam_agents.cache_hit"] == "true"` (the resume), regardless of the forced extra attempt

#### Scenario: A broken cache-first path is caught

- **WHEN** the resume's repeated call would incorrectly re-invoke the provider (a hypothetical regression bypassing the cache)
- **THEN** the committed `.traces` show a second `cache_hit == "false"` event instead of a `cache_hit == "true"` one, and an assertion built on the expected trace shape fails rather than passing silently

### Requirement: A forced retry commits the deterministically-expected intent
Under a chaos-forced commit failure and Beam's subsequent retry, the `ToolIntent` ultimately committed on `.intents` SHALL match the value predicted by `intent_id_for(entity_key, seq, step_index)` for the known call sequence — i.e., the retry does not alter `seq` or `step_index` derivation, and exactly one intent is committed (the discarded failed attempt commits nothing, per Beam's own bundle rollback).

#### Scenario: Committed intent matches the deterministic formula under a forced retry

- **WHEN** an activation that stages one `ToolIntent` has its commit chaos-forced to fail once and then succeeds on Beam's retry
- **THEN** `.intents` contains exactly one `ToolIntent` whose `intent_id` equals `intent_id_for(entity_key, seq, step_index)` computed from the activation's own known key/seq/step sequence

### Requirement: The retry-determinism gate is a required, offline CI check
The retry-determinism semantics test SHALL be marked `-m semantics`, SHALL run offline (`FakeLLM` + a streaming `TestPipeline` on the classic DirectRunner, no docker), and SHALL be selected by an offline semantics selection (`-m "semantics and not integration"`) wired into the required `ci` workflow. That selection SHALL NOT tolerate an empty test collection: if no semantics test is collected, the check SHALL fail rather than report success. Docker-backed semantics tests SHALL remain in the integration workflow.

#### Scenario: Gate runs on every PR without docker

- **WHEN** the required `ci` workflow runs for a pull request
- **THEN** the offline semantics selection executes the retry-determinism test using `FakeLLM` and the DirectRunner with no docker services started

#### Scenario: An empty semantics selection fails the check

- **WHEN** the offline semantics selection collects no tests (e.g. the marker or test is removed)
- **THEN** the CI step exits non-zero instead of passing on a "no tests collected" result
