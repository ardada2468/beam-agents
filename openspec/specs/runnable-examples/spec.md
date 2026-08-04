# runnable-examples Specification

## Purpose
TBD - created by archiving change add-docs-site. Update Purpose after archive.
## Requirements
### Requirement: Examples are self-contained, offline, runnable modules

Each example SHALL be a single module under `examples/` (`hello_world.py`, `fraud_triage.py`, `iot_reaction.py`) that is executable offline as `uv run python -m examples.<name>` on the DirectRunner, completing with no API keys, no docker services, and no network access. Every model interaction SHALL go through a scripted `FakeLLM`, and no example SHALL call `sleep()` or read a wall clock for timer or watermark behavior — scripted `TestStream` advances only. Examples SHALL import only `apache_beam`, the `beam_agents` runtime surface, and the public proto bindings — never anything under `tests/` — and every function a pipeline references SHALL be module-level so DirectRunner pickling by reference works. The `examples/` directory SHALL be lint- and type-checked to the repository standard (`ruff`, `mypy --strict` with only the established per-module Beam-untyped-API relaxations) and SHALL NOT be included in the built wheel.

#### Scenario: An example runs to completion with nothing but the repo checkout

- **WHEN** `uv run python -m examples.hello_world` is executed in an offline environment with no provider credentials and no docker
- **THEN** the pipeline runs on the DirectRunner, the scripted FakeLLM serves every model call, and the process exits zero after printing the documented output

#### Scenario: An example importing test helpers fails the unit lane

- **WHEN** an example module gains an import from `tests/`
- **THEN** the offline self-containment check over `examples/` fails naming the module, because a user copying the example out of the repository could not run it

### Requirement: The hello-world example demonstrates the minimal fast path

`examples/hello_world.py` SHALL build the smallest complete `RunAgent` pipeline: a single `AgentEnvelope` created in-process, keyed upstream by `entity_key` exactly as `RunAgent`'s KV-input contract requires, processed by an agent that awaits one `ctx.call_model(...)` and returns `Complete`, with the terminal output observable on `.output`. The example SHALL produce zero elements on `.intents` and `.errors`.

#### Scenario: One event in, one output out

- **WHEN** the hello-world pipeline runs its single scripted event
- **THEN** `.output` carries exactly one terminal output containing the FakeLLM-scripted response, and `.intents` and `.errors` are empty

### Requirement: The fraud-triage example demonstrates suspension, approval resume, and the fail-closed timeout

`examples/fraud_triage.py` SHALL run a streaming pipeline over transaction events for two accounts in which the agent triages each transaction via a scripted model call, requests human approval with `ctx.request_approval(...)`, and returns `Suspend` with an explicit `timeout_ms`. For the first account, a scripted approval SHALL re-enter the pipeline on the same key (a `TestStream` branch standing in for the approvals topic) and the resumed activation SHALL emit the documented freeze decision. For the second account, no decision SHALL arrive: a scripted processing-time advance SHALL elapse the HITL deadline and the deny route SHALL emit its deterministic fallback output — the fail-closed path, never a silent drop. The example SHALL derive the pending approval's `intent_id` with the runtime's deterministic formula, and its docs page SHALL state that production approvals arrive from the effector already carrying this id.

#### Scenario: Approved account resumes to a freeze decision

- **WHEN** the first account's suspicious transaction suspends awaiting approval and the scripted approval arrives on the same key before the deadline
- **THEN** the resumed activation emits the documented freeze output on `.output`, and exactly one approval intent for that account appears on `.intents`

#### Scenario: Unanswered account fails closed at the deadline

- **WHEN** the second account's suspension receives no decision and the scripted processing-time advance passes its `timeout_ms`
- **THEN** the deny route's deterministic fallback output appears on `.output` for that account and no freeze decision is ever emitted for it

### Requirement: The IoT-reaction example demonstrates keyed rolling memory on a stream

`examples/iot_reaction.py` SHALL run a streaming pipeline over a `TestStream` of per-device sensor readings in which the agent appends each reading to bounded per-key working memory (`ctx.memory.append(...)`/`ring(...)`), completes without any model call while the rolling window stays below the documented threshold, and calls the model for a reaction decision only when the window crosses it, emitting the documented reaction output. The zero-model-calls-on-quiet-readings property SHALL be observable via the FakeLLM's recorded call count.

#### Scenario: Quiet readings accumulate memory without model calls

- **WHEN** a device's readings stay below the threshold
- **THEN** each activation completes, the rolling window in working memory grows per reading, and the FakeLLM records zero calls

#### Scenario: A threshold breach triggers exactly one reaction

- **WHEN** a device's rolling window crosses the documented threshold
- **THEN** the agent makes exactly one scripted model call for the breaching activation and emits the documented reaction output on `.output` for that device's key

### Requirement: Every example is verified by a test that executes the documented module

For each example there SHALL be a unit-tier test module under `tests/examples/` that imports the example module the docs page renders and drives its pipeline under `TestPipeline`/`TestStream`, asserting the outputs the docs page documents — the doc-example-verified-by-test pattern already established for `docs/errors.md`. These tests SHALL run offline in `make test-unit` with no new pytest markers, and a behavioral change to an example that contradicts its docs page's documented outputs SHALL fail them.

#### Scenario: Example behavior and documentation cannot diverge silently

- **WHEN** an example module's behavior is changed such that its documented outputs no longer hold
- **THEN** the example's test fails in the offline unit lane before the site can publish the stale documentation

#### Scenario: The examples run in the required unit lane

- **WHEN** `make test-unit` runs in an environment with no docker and no credentials
- **THEN** all example tests execute (none skipped for missing services) and pass against the committed examples
