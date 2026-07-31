# Delta Spec: adapter-conformance-matrix

## MODIFIED Requirements

### Requirement: Scenarios are written once and parameterized over an adapter seam
The conformance suite SHALL define each lifecycle scenario exactly once, against runtime-observable behavior only (the transform's `.output`/`.intents`/`.traces`/`.errors` collections and committed keyed state), and SHALL obtain the agent under test from a per-adapter conformance factory. Each registered adapter's factory SHALL build a behaviorally equivalent agent for that framework: the same scripted FakeLLM conversation, the same read-only and side-effect tools, and the same suspend/approval behavior, so any assertion difference between matrix cells is attributable to the adapter, not to the fixture. The adapter axis SHALL contain at least the reference protocol agent (a plain async activation function, serving as the baseline), the LangGraph adapter, and the ADK adapter.

#### Scenario: Same scenario body runs for every registered adapter

- **WHEN** the conformance suite is collected with N adapters registered
- **THEN** every scenario produces one test cell per registered adapter, and each cell drives the runtime through that adapter's factory with the shared scenario script

#### Scenario: A missing optional framework skips its cells cleanly

- **WHEN** the suite is collected in an environment where a registered adapter's framework package (e.g. `langgraph` or `google.adk`) is not installed
- **THEN** that adapter's cells are reported as skipped with the missing-package reason, and all other adapters' cells still run

### Requirement: A scenario may declare a per-adapter skip for an inexpressible construction
A `ScenarioSpec` SHALL be able to declare, per adapter, that the scenario's *construction* is not expressible in that framework's semantics, carrying the reason. This is reserved for a premise the framework makes unreachable — never for an adapter that merely fails the scenario, and never as a way to weaken an assertion. A declared per-adapter skip SHALL remain a collected, counted matrix cell reported as a skip carrying its reason, exactly like the existing per-leg skip declarations, so the meta-test's registry × scenario × leg accounting is unchanged and the matrix cannot silently shrink.

#### Scenario: A declared per-adapter skip is still a counted cell

- **WHEN** a scenario declares a per-adapter skip for one registered adapter and the suite is collected
- **THEN** that cell is collected and reported as a skip carrying the declared reason, the meta-test's expected-cell count is unchanged, and every other adapter's cell for that scenario still runs
