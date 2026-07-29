## ADDED Requirements

### Requirement: The gate runs the closed loop on real infrastructure
The effectively-once end-to-end gate SHALL exercise the full loop on live services — events on a real Kafka broker (Redpanda), `RunAgent` submitted to the Flink mini-cluster through a Beam job server with checkpointing enabled, intents on a real Kafka topic, a pool of real `beam-agents-effector` worker *processes*, and a real Redis dedup store — with no in-memory transport, no in-memory dedup store, and no simulated runner. The model SHALL remain `FakeLLM` (no provider network calls). Because cross-language Kafka IO is unavailable on this stack (no Java SDK environment on the TaskManager), the pipeline SHALL read its Kafka topics through a durable, replayable spool: a harness drainer tails the events and re-injection topics into immutable append-only segment files, and a pure-Python Splittable DoFn replays them with its position held in restriction state — so a checkpoint restore re-reads byte-identical input. This substitution SHALL be documented alongside the outbox-producer gap note. Each run SHALL provision its own uniquely-named topics, consumer group, dedup namespace, ledger namespace, and spool directory, so concurrent or repeated runs never observe one another's state.

#### Scenario: Every hop is a live service

- **WHEN** the gate runs
- **THEN** events, intents, results, and approvals each traverse a real Kafka topic; the pipeline executes on the Flink mini-cluster via the job server; dedup claims land in Redis; and the effector runs as separate OS processes rather than in the test process

#### Scenario: Runs are isolated from one another

- **WHEN** the gate is run twice in a row against the same stack
- **THEN** the second run provisions fresh topics, consumer group, dedup namespace, and ledger namespace, and its assertions are unaffected by any state the first run left behind

### Requirement: Effectively-once execution under induced worker kills
Across a run of 10,000 events with worker processes killed at randomized points throughout, execution SHALL be counted at the side effect itself — a Redis ledger the test tool increments — not inferred from message counts, since duplicate messages are expected by design. The gate SHALL assert: **zero lost effects** (every minted tool `intent_id` has a ledger count of at least one), and **duplicates only within the crash window** — a `SIGKILL` landing between a tool's effect and the effector's durable completion record unavoidably re-executes after lease expiry (exactly-once side effects over non-idempotent, non-transactional effects cannot survive that window; this is the executor's documented at-least-once residue, empirically demonstrated by this gate). Duplicates SHALL be bounded: no member's count may exceed `1 + kills`, and the number of duplicated members may not exceed `kills × max_concurrent_partitions`. In a run with **no** kills, the bound collapses to strict exactly-once. True exactly-once for tools that key their downstream effect on `intent_id` is deferred to the follow-up change that passes intent identity to side-effect tools.

#### Scenario: A killed effector worker never loses an effect, and duplicates stay inside the crash-window bound

- **WHEN** effector worker processes are `SIGKILL`ed at randomized points while 10,000 events' intents flow through them, and the killed workers' claims are recovered by lease expiry and consumer-group rebalance
- **THEN** the ledger records at least one execution for every minted tool `intent_id` (no lost effect), every count exceeding one is attributable to the crash window (bounded by the number of kills and the in-flight limit), and no other source of duplication exists

#### Scenario: A killed TaskManager restores checkpointed state without duplicating pre-kill work

- **WHEN** the Flink TaskManager is killed mid-run and the job restores from its last completed checkpoint
- **THEN** every output committed before the kill appears without duplication after the restore, and the restore itself adds no ledger executions

#### Scenario: A wiped pipeline replayed from zero does not double-mint an effect

- **WHEN** the job is cancelled after a kill and the identical pipeline is resubmitted against the same immutable spool with completely fresh pipeline state, replaying every event from the beginning (in-place source resumption after restore is unavailable on this runner — a documented limitation)
- **THEN** every replayed activation re-mints a byte-identical `intent_id`, the effector collapses every redelivered intent, the ledger counts are byte-for-byte unchanged from before the replay (the replay adds zero executions), and every event is accounted for after recovery

### Requirement: Duplicate sink writes never produce a second execution or a divergent outcome
The gate SHALL deliberately manufacture duplicate deliveries: the outbox producer publishes a configurable fraction of intents to the intents topic twice, and the effector's own publish-then-commit ordering republishes results and approvals after a kill. Duplicate deliveries of one `intent_id` SHALL produce exactly one execution and exactly one terminal outcome: every published `ToolResult` for a given `intent_id` SHALL serialize to identical bytes, and every published approval message for a given `intent_id` SHALL likewise agree.

#### Scenario: A doubly-published intent executes once

- **WHEN** the harness publishes the same `intent_id` to the intents topic twice
- **THEN** the ledger records exactly one execution for it, and both deliveries resolve to the same stored terminal result

#### Scenario: Republished results agree

- **WHEN** a worker is killed after storing a result but before committing its offset, so the recovered worker republishes that intent's result
- **THEN** every `ToolResult` observed on the results topic for that `intent_id` serializes to identical bytes — no consumer ever sees two different outcomes for one `intent_id`

### Requirement: Zero lost approvals
Every `APPROVAL` intent minted during the run SHALL reach the approvals topic at least once, and every approval decision fed back into the pipeline SHALL resume its key and produce exactly one terminal decision on `.output`. No approval-bearing key SHALL be left stranded: at the end of the run, the count of distinct approval-bearing entity keys that produced a terminal decision (an approved/denied outcome, or a fail-closed HITL timeout fallback) SHALL equal the count of approval-bearing keys admitted. An approval whose result arrives after its deadline SHALL appear on `.errors` as an `orphaned_result` rather than vanishing.

#### Scenario: Every approval intent reaches the approvals topic

- **WHEN** the run mints `APPROVAL` intents and workers are killed throughout
- **THEN** every minted approval `intent_id` appears at least once on the approvals topic, and none is absent

#### Scenario: Every approval-bearing key reaches a terminal decision

- **WHEN** approval decisions are fed back onto the approvals topic and re-injected on the same key
- **THEN** each approval-bearing entity key emits exactly one terminal decision on `.output`, and the number of keys reaching a terminal decision equals the number of approval-bearing keys admitted

#### Scenario: A late approval is surfaced, not dropped

- **WHEN** an approval decision arrives after its intent's deadline has passed and the HITL timer has already fired the fallback path
- **THEN** the key's fallback decision stands as its single terminal decision and the late approval is emitted on `.errors` as an `orphaned_result` — it is never silently discarded and never produces a second decision

### Requirement: Intent IDs stay deterministic across pipeline recovery
Every `intent_id` observed on the intents topic SHALL equal `intent_id_for(entity_key, seq, step_index)` for that activation's key, sequence, and step. This SHALL hold for intents minted before a kill, for intents re-minted when a wiped pipeline replays the full spool from zero, and for the pair of any duplicate delivery — a replayed activation SHALL NOT drift its `seq` or `step_index` derivation.

#### Scenario: Replayed activations mint identical intent IDs

- **WHEN** the pipeline is killed, cancelled, and resubmitted with fresh state against the same immutable spool, re-running activations that had already minted intents
- **THEN** every re-minted intent's `intent_id` is byte-identical to the one minted before the kill, matching `intent_id_for(entity_key, seq, step_index)` for the same key/seq/step

### Requirement: All admitted events are accounted for
At the end of the run, all 10,000 events SHALL be accounted for with no silent loss: every event SHALL have produced either a terminal outcome (an execution recorded in the ledger with its result published, or an explicit refusal — `EXPIRED` or a typed error on `.errors`) or a documented, asserted-upon terminal state. The gate SHALL assert the total, not merely a sample, and SHALL fail on any unaccounted event rather than reporting a percentage.

#### Scenario: The full population balances

- **WHEN** the 10,000-event run completes after all injected kills
- **THEN** the sum of executions, explicit refusals, and typed errors equals 10,000, and the gate fails if any event is unaccounted for

### Requirement: The gate is release-blocking and never skipped
The end-to-end gate SHALL be marked `semantics`, `integration`, and `slow`, SHALL be selected by the docker semantics selection (`-m "semantics and integration"`) in the integration workflow, and SHALL NOT tolerate an empty test collection. It SHALL NOT be marked `xfail`, `skipif`, or otherwise made tolerant of failure, and its event volume SHALL be pinned at 10,000 in CI (tunable downward only for local iteration, via an environment variable that CI does not set). The two semantics selections — offline (`semantics and not integration`) and docker-backed (`semantics and integration`) — SHALL together cover every `semantics`-marked test, so no gate can escape both.

#### Scenario: The gate runs in the integration lane

- **WHEN** the integration workflow runs
- **THEN** the docker semantics selection executes the end-to-end gate against the compose stack, and a non-zero exit fails the workflow

#### Scenario: An empty docker semantics selection fails

- **WHEN** the docker semantics selection collects no tests (e.g. the marker or test is removed)
- **THEN** the CI step exits non-zero instead of passing on a "no tests collected" result

#### Scenario: The two selections partition the tier

- **WHEN** every `semantics`-marked test in the repo is enumerated
- **THEN** each one is selected by exactly one of `semantics and not integration` or `semantics and integration`, with none selected by neither

### Requirement: The chaos harness is test-only and injects faults without production hooks
The end-to-end harness — stack provisioning, effector worker supervisor, kill injectors, at-least-once outbox producer, and Redis execution ledger — SHALL live under `tests/` and SHALL require no change to any module under `src/beam_agents/`. Kills SHALL be real process termination (`SIGKILL` to an effector worker or Beam SDK worker process, or a container kill for the TaskManager), not an in-process exception. The supervisor SHALL guarantee cleanup: no effector process, topic, or consumer group outlives the test, whether it passes or fails.

#### Scenario: A kill is a real signal to a real process

- **WHEN** the harness injects a worker kill
- **THEN** an actual OS process receives `SIGKILL` (or the container is killed) and terminates without running shutdown handlers, rather than an exception being raised inside the test process

#### Scenario: No production module is modified

- **WHEN** the gate is implemented
- **THEN** `src/beam_agents/` is unchanged: fault injection, ledgering, and duplicate publication are entirely contained in the test harness

#### Scenario: The harness cleans up after a failure

- **WHEN** the gate fails an assertion or times out mid-run
- **THEN** every effector worker process it launched is terminated and every topic and consumer group it provisioned is deleted before the test session ends
