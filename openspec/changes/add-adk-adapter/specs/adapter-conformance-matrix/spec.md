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
