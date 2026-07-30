## Purpose

A single parameterized conformance suite proving that every agent-framework adapter, driven through the same runtime, exhibits identical lifecycle semantics — fast path, inline tools, suspension/resume, fail-closed timeouts, restart survival, bundle-retry determinism, and TTL GC — on both DirectRunner and Flink, so adapters cannot silently diverge from the runtime's correctness invariants.

## ADDED Requirements

### Requirement: Scenarios are written once and parameterized over an adapter seam
The conformance suite SHALL define each lifecycle scenario exactly once, against runtime-observable behavior only (the transform's `.output`/`.intents`/`.traces`/`.errors` collections and committed keyed state), and SHALL obtain the agent under test from a per-adapter conformance factory. Each registered adapter's factory SHALL build a behaviorally equivalent agent for that framework: the same scripted FakeLLM conversation, the same read-only and side-effect tools, and the same suspend/approval behavior, so any assertion difference between matrix cells is attributable to the adapter, not to the fixture. The initial adapter axis SHALL contain at least the reference protocol agent (a plain async activation function, serving as the baseline) and the LangGraph adapter.

#### Scenario: Same scenario body runs for every registered adapter

- **WHEN** the conformance suite is collected with N adapters registered
- **THEN** every scenario produces one test cell per registered adapter, and each cell drives the runtime through that adapter's factory with the shared scenario script

#### Scenario: A missing optional framework skips its cells cleanly

- **WHEN** the suite is collected in an environment where a registered adapter's framework package (e.g. `langgraph`) is not installed
- **THEN** that adapter's cells are reported as skipped with the missing-package reason, and all other adapters' cells still run

### Requirement: An unregistered installed adapter fails collection
If an adapter package that ships in `beam_agents.adapters` is importable in the test environment but has no conformance registration, the suite SHALL fail (error, not skip) at collection time, naming the unregistered adapter. The matrix SHALL never silently shrink because someone added an adapter without adding its conformance factory.

#### Scenario: New adapter without a conformance factory breaks the build

- **WHEN** a new adapter subpackage is importable but absent from the conformance registry
- **THEN** conformance collection raises an error identifying the adapter, and the semantics selection fails rather than passing with fewer cells

### Requirement: Single-shot fast-path conformance
For every registered adapter, an activation whose scripted conversation requires no tools and no suspension SHALL complete in a single activation: exactly one terminal output on `.output`, zero elements on `.intents`, no persisted continuation after commit, and exactly one provider call recorded in `.traces` with `cache_hit == "false"`.

#### Scenario: One event in, one output out, no side-effect machinery touched

- **WHEN** a single event is processed by an adapter-built agent whose FakeLLM script answers immediately
- **THEN** `.output` carries the expected terminal output for that key, `.intents` and `.errors` are empty, and the committed state holds no continuation

### Requirement: Multi-tool inline conformance
For every registered adapter, an activation whose scripted conversation invokes multiple read-only tools SHALL execute all of them inline within the same activation (fast path): every tool's result SHALL be observable in the terminal output (proving execution and ordering uniformly across adapters), the activation SHALL still produce exactly one terminal output with zero `.intents`, and the number of provider calls SHALL match the scripted conversation's turn count exactly (observable as `.traces` `LLM_CALL` events). Tool executions are additionally traced as `TOOL_CALL` events only where the adapter routes inline tools through the runtime's `ctx.run_tool` path — a surfaced finding, not a conformance requirement: `BeamToolNode` executes read-only tools directly and stages no `TOOL_CALL` trace (see design.md, Findings).

#### Scenario: Two read-only tools execute inline in one activation

- **WHEN** the scripted conversation requests two distinct read-only tools before answering
- **THEN** the terminal output on `.output` embeds both tool results in invocation order, all scripted model turns appear in `.traces` for one activation, and `.intents` is empty

### Requirement: Suspension/resume conformance with deterministic intents
For every registered adapter, an activation whose scripted conversation invokes a side-effect tool SHALL stage exactly one `ToolIntent` whose `intent_id` equals the deterministic formula value for the activation's known `(entity_key, seq, step_index)`, suspend with a persisted continuation, and — when the matching `ToolResult` re-enters on the same key — resume to a terminal output that reflects the injected result. The side-effect tool's body SHALL NOT execute inside the pipeline for any adapter.

#### Scenario: Side-effect tool suspends, re-injected result resumes

- **WHEN** an adapter-built agent's conversation calls a side-effect tool and the harness re-injects the matching `ToolResult` on the same key
- **THEN** `.intents` carries exactly one intent with the deterministically-expected `intent_id`, the terminal output on `.output` incorporates the injected result, and the side-effect tool's in-pipeline execution count is zero

### Requirement: Approval timeout fallback conformance
For every registered adapter, an approval-kind suspension whose HITL deadline elapses without a decision SHALL take the fail-closed fallback path: the fallback terminal output SHALL be emitted, the continuation SHALL be cleared, and a decision arriving after the timer has fired SHALL NOT resume the activation — it SHALL surface on `.errors` as an `orphaned_result`.

#### Scenario: Deadline elapses before the decision

- **WHEN** an adapter-built agent suspends awaiting approval and the runtime clock passes the suspension's deadline with no decision delivered
- **THEN** the fallback terminal output appears on `.output` and the continuation is cleared

#### Scenario: Late decision is orphaned, not resumed

- **WHEN** a decision for that suspension arrives after the fallback has run
- **THEN** no second terminal output is produced and the late decision is routed to `.errors` as `orphaned_result`

### Requirement: Restart mid-suspension conformance
For every registered adapter, a resume delivered after the runtime instance that performed the suspension has been discarded and replaced SHALL succeed using committed protobuf state alone (continuation, snapshot, LLM cache, seq). No adapter may depend on in-process memory surviving between suspend and resume: the resumed activation SHALL produce the same terminal output and the same intent set as an uninterrupted suspend/resume run.

#### Scenario: Resume on a fresh runtime instance

- **WHEN** the suspension is committed, the processing runtime is torn down and recreated, and the matching result is then delivered on the same key
- **THEN** the activation resumes from committed state and produces the terminal output and trace shape identical to the no-restart run

### Requirement: Bundle-retry replay-cache conformance
For every registered adapter, a chaos-forced failure of the resume's first commit attempt SHALL NOT change externally visible behavior: the total count of real provider invocations across the whole activation SHALL equal the no-retry count (the retried attempt is served from the suspend-committed replay cache), and the committed `.intents` SHALL be byte-identical to the no-retry run's intents.

#### Scenario: Forced retry adds zero provider calls and preserves intent bytes

- **WHEN** an adapter-built agent suspends after a model call, its resume's first commit attempt is chaos-forced to fail, and Beam retries the bundle
- **THEN** `.traces` show no additional `cache_hit == "false"` provider call attributable to the retry, and the single committed intent's serialized bytes equal the deterministic expectation

### Requirement: TTL expiry conformance
For every registered adapter, working memory written by an activation SHALL be garbage-collected when the watermark passes the configured memory TTL: a later activation on the same key, after TTL expiry, SHALL observe empty working memory, while an activation before expiry SHALL observe the previously written memory.

#### Scenario: Memory visible before TTL, gone after

- **WHEN** an activation writes working memory, a second event arrives before the TTL and a third arrives after the watermark passes the TTL boundary
- **THEN** the second activation observes the written memory and the third observes empty memory

### Requirement: The matrix runs on DirectRunner and Flink
The conformance matrix SHALL execute on two runners. The DirectRunner leg SHALL be offline (FakeLLM, scripted watermark/processing-time advances, no docker), marked so it is selected by the required offline semantics CI selection. The Flink leg SHALL run the suite against the local Flink mini-cluster through the portable job server, marked so it is selected by the docker-backed semantics selection in the integration workflow, and SHALL exercise restart-mid-suspension via a real TaskManager restart. Any scenario not runnable on a leg SHALL be declared per-scenario with a recorded reason (an explicit skip naming the constraint), never dropped silently.

#### Scenario: Offline leg needs no docker

- **WHEN** the offline semantics selection runs in an environment with no docker services
- **THEN** every DirectRunner conformance cell executes (or skips only for a missing optional framework package) and none requires Kafka, Redis, or Flink

#### Scenario: Flink leg survives a TaskManager restart mid-suspension

- **WHEN** the Flink leg runs the restart-mid-suspension scenario and the TaskManager is restarted between the suspend commit and the result delivery
- **THEN** the resumed activation's terminal output is observed on the output topic after recovery, with the deterministically-expected intent observed exactly once by the assertion consumer

#### Scenario: A leg-inexpressible scenario is an explicit skip

- **WHEN** a scenario is declared not runnable on one leg
- **THEN** that cell reports as skipped with the declared reason, and the meta-test's expected cell accounting includes it as a declared skip rather than a missing cell

### Requirement: The matrix cannot silently lose cells
A meta-test SHALL compute the expected cell count from the registered adapters, the scenario list, and the per-scenario runner declarations, and SHALL fail if collection produces a different number of cells than expected. The conformance tests SHALL carry the semantics marker (and the integration marker on the Flink leg only) so the existing semantics-partition check covers them.

#### Scenario: Wiring regression is caught by the meta-test

- **WHEN** a refactor accidentally de-parameterizes a scenario or drops an adapter from the registry without touching the expected-cell declaration
- **THEN** the meta-test fails with the expected-versus-collected cell difference
