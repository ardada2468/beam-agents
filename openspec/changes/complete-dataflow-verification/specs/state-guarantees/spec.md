## ADDED Requirements

### Requirement: The `--update` gate provisions inside the project's own managed environment

The nightly Dataflow `--update` compatibility gate SHALL provision both legs using tooling that is
present in the environment the project's own lockfile produces. It SHALL NOT depend on a package
manager or module that the project's documented install does not provide. A provisioning step that
cannot run is a gate failure, not a skip: the gate SHALL fail loudly rather than silently degrade.

#### Scenario: Provisioning works in the environment CI actually creates

- **WHEN** the gate runs in an environment provisioned by `uv sync --locked`, which does not install
  `pip`
- **THEN** every provisioning step — building head's wheel, obtaining the previous release, creating
  its environment, and capturing both legs' resolutions — completes without requiring `pip` to be
  importable

#### Scenario: A provisioning failure is not mistaken for a compatibility verdict

- **WHEN** a provisioning step fails before any Dataflow job is launched
- **THEN** the failure is reported as an environment/provisioning failure, and the run does not report
  a verdict about update compatibility

### Requirement: A self-update run is recorded distinctly from cross-version evidence

When no previously released version is resolvable, the gate SHALL run its documented bootstrap
head → head leg and SHALL label the result as a self-update. A bootstrap run SHALL NOT be recorded as
evidence that a released version can be updated to head. The published state-compatibility promise
SHALL be treated as unevidenced until a cross-version run has passed.

#### Scenario: The bootstrap leg announces what it does and does not prove

- **WHEN** the gate runs with no previous release available on the index
- **THEN** the run is labelled a self-update, states that it proves the harness and the update
  mechanics but is not cross-version evidence, and any report citing it repeats that limitation

#### Scenario: Cross-version is selected once a release exists

- **WHEN** a previously released version is resolvable
- **THEN** the gate provisions that version as the launch leg, head as the update leg, records both
  legs' full dependency resolutions, and asserts the continuation nonce, the memory marker and a
  fresh key all survive the update
