## ADDED Requirements

### Requirement: The repository's reference deployment enforces verification

The repository's own reference deployment — the docker compose stack together with the pipeline and `beam-agents-effector` worker pool the effectively-once end-to-end gate runs against it — SHALL operate at the terminal step of the rollout sequence: the pipeline's outbox SHALL sign every `ToolIntent` with a key minted per run and resolved worker-side from a `file:` reference, and every effector worker SHALL run with `verify_intents=require`, the same keyring by reference, and a run-scoped dead-letter channel. The gate SHALL prove enforcement on live infrastructure by injecting exactly one unsigned intent that is otherwise well-formed (a registered tool, canonical arguments, an `intent_id` minted by the runtime's deterministic formula, an unexpired TTL) and asserting that it is dead-lettered verbatim exactly once, that no `ToolResult` of any status is published for it, that it reaches no execution record, and that every genuine intent on the wire verifies under the run's key. The gate SHALL fail if the effector pool runs in any mode other than `require`. `EffectorConfig.verify_intents` SHALL remain `off` by default; enforcement is a property of the deployment, not of the default, and the signing key SHALL NOT be committed to the repository.

#### Scenario: Genuine intents are signed end to end across the real boundary

- **WHEN** the effectively-once gate runs against the compose stack
- **THEN** every intent the pipeline publishes carries a signature that verifies under the run's key, and every one of them executes exactly once as the gate's effectively-once assertions already require

#### Scenario: An injected unsigned intent is dead-lettered, not executed

- **GIVEN** the effector pool running `require` against the run's keyring and dead-letter topic
- **WHEN** the gate publishes one unsigned, otherwise well-formed intent naming a registered side-effect tool directly to the intents topic
- **THEN** the dead-letter topic holds exactly that delivery's raw key and raw payload once, the results topic carries no `ToolResult` for its `intent_id`, and the execution ledger records no attempt and no effective execution for it in either phase

#### Scenario: Turning the dial back fails the gate

- **WHEN** the effector pool is launched with `verify_intents` at `off` or `permissive`
- **THEN** the injected unsigned intent executes, its `intent_id` appears in the execution ledger as an unknown member, the dead-letter topic stays empty, and the gate reports the failure naming the verification mode

#### Scenario: Removing the signer fails the gate

- **WHEN** the pipeline publishes unsigned intents while the pool runs `require`
- **THEN** genuine intents are dead-lettered rather than executed and the gate fails on lost effects

#### Scenario: Key material stays out of the repository and the pipeline graph

- **WHEN** the gate provisions a run
- **THEN** the signing key is freshly generated, written only under the gitignored spool directory, named in configuration and on the effector's command line only as a `file:` reference, and removed at teardown
