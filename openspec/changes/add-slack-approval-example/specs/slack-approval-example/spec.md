## ADDED Requirements

### Requirement: The surface turns each approval-kind intent into exactly one interactive Slack message

The example surface SHALL consume serialized `ToolIntent`s through the effector's `IntentSource` protocol and, for each delivered intent with `kind == APPROVAL` that is not yet expired, SHALL post one Slack message containing the approval request (channel name, rendered `args_json`, expiry) and Block Kit approve/deny actions whose action value carries the `intent_id`, the hex-encoded `entity_key`, and `expires_at_ms`. The surface SHALL commit the delivery only after the post has succeeded, so a crash before posting re-delivers rather than loses. A delivered intent whose `kind` is not `APPROVAL` (including `TOOL_KIND_UNSPECIFIED`) SHALL be skipped and committed without posting and without publishing anything. Within one process lifetime the surface SHALL NOT post a second interactive message for an `intent_id` it has already posted; across restarts a redelivered intent MAY post again, and the documentation SHALL state this residual and why it is harmless (both messages resolve to the same `intent_id`; the pipeline admits at most one verdict).

#### Scenario: An approval intent becomes a Block Kit message and is then committed

- **WHEN** the surface consumes a live intent with `kind == APPROVAL` staged by a FakeLLM-driven activation's `request_approval`
- **THEN** exactly one message is posted through the Slack gateway carrying approve and deny actions whose action value includes that intent's `intent_id`, hex `entity_key`, and `expires_at_ms`, and the delivery is committed only after the gateway post returns

#### Scenario: A crash before posting loses nothing

- **WHEN** the gateway post fails and the surface stops before committing the delivery
- **THEN** the delivery is not committed, so the transport re-delivers the intent to the next surface instance

#### Scenario: Non-approval intents are skipped

- **WHEN** the surface is pointed at a topic carrying a `kind == TOOL` intent and a `kind == TOOL_KIND_UNSPECIFIED` intent
- **THEN** no message is posted and no envelope is published for either, and both deliveries are committed

#### Scenario: A redelivered intent does not double-post within a process

- **WHEN** the same `intent_id` is delivered twice to one running surface (the effector publishes approval notifications before marking them terminal)
- **THEN** only one interactive message is posted, and the second delivery is committed without a second post

### Requirement: An approve decision publishes an Approval envelope that resumes the suspended activation

When the Slack gateway reports an approve interaction for a pending, unexpired intent, the surface SHALL publish exactly one `AgentEnvelope` whose `payload` is `approval` with `intent_id` from the action value, `approved = true`, `approver` set to the Slack user id from the interaction, and `decided_at_ms` from the interaction timestamp. The envelope SHALL be serialized deterministically and published through the `MessageSink` under the raw `entity_key` bytes, so it lands on the suspended key's partition/ordering key. After publishing, the surface SHALL edit the message to show the verdict and remove the actions, and SHALL NOT publish a further envelope for later interactions on that `intent_id`, answering them as already decided. The surface SHALL NOT attempt global at-most-one-verdict enforcement: the pipeline's `_resume` admission is the arbiter, and duplicate or late envelopes are orphaned there.

#### Scenario: Approve click publishes a keyed approval envelope

- **WHEN** an approve interaction arrives for a pending, unexpired intent
- **THEN** exactly one deterministically serialized `AgentEnvelope` with `approval.intent_id` equal to the intent's id, `approval.approved == true`, and the clicking user as `approval.approver` is published under the intent's raw `entity_key`, and the message is then edited to show the approval with its buttons removed

#### Scenario: The published envelope resumes the suspended activation

- **WHEN** the envelope published by the surface is re-injected into the pipeline on the same key while the continuation is live and unexpired
- **THEN** the suspended activation resumes and the pipeline's main output carries the approved decision, with no `orphaned_result` on `.errors`

#### Scenario: A second click on a decided intent publishes nothing

- **WHEN** a second interaction arrives for an `intent_id` the surface has already published a verdict for
- **THEN** no further envelope is published and the interaction is answered as already decided

### Requirement: A deny decision publishes a fail-closed denial the agent can act on

A deny interaction for a pending, unexpired intent SHALL publish the same envelope shape with `approved = false`, keyed and serialized identically, and SHALL edit the message to show the denial. The example's demo agent SHALL demonstrate the denied path: resumed with `approved == false`, it emits its documented denied outcome rather than performing the guarded action.

#### Scenario: Deny click publishes approved=false and the agent takes the denied path

- **WHEN** a deny interaction arrives for a pending, unexpired intent and the published envelope is re-injected on the same key
- **THEN** the envelope carries `approval.approved == false`, the suspended activation resumes, and the demo agent's main output is its denied outcome

### Requirement: An expired intent is never actionable at the surface

The surface SHALL evaluate expiry with `hitl.intent_expired` against an injectable clock, and SHALL apply it at three points: (1) an intent already expired at consume time SHALL be surfaced as a non-interactive expired notice — no actions — and committed; (2) a periodic sweep SHALL edit any pending message whose `expires_at_ms` has passed to an expired state with its actions removed; (3) every interaction SHALL be re-checked against the `expires_at_ms` carried in its action value before an envelope is built, and an interaction on an expired intent SHALL publish nothing and SHALL edit the message to expired. The decision-time check SHALL NOT depend on in-process state, so it holds for messages posted before a surface restart. Timing in tests SHALL be driven by the injectable clock, never by sleeping.

#### Scenario: An already-expired intent is surfaced without buttons

- **WHEN** the surface consumes an intent whose `expires_at_ms` is at or before the injected clock (including a non-positive `expires_at_ms`)
- **THEN** a non-interactive expired notice is posted, the delivery is committed, and no interactive actions exist for that intent

#### Scenario: Expiry while pending removes the buttons

- **WHEN** a posted intent's `expires_at_ms` passes the injected clock and the sweep runs
- **THEN** the message is edited to an expired state with its actions removed, and no envelope has been published

#### Scenario: A click racing expiry publishes nothing

- **WHEN** an approve interaction arrives whose action value's `expires_at_ms` is at or before the injected clock — including one arriving after a surface restart emptied the pending map
- **THEN** no envelope is published and the message is edited to expired

### Requirement: Slack I/O sits behind a gateway seam and the surface is fully testable offline

All Slack communication SHALL go through a `SlackGateway` protocol (post, update, and an async stream of decisions), with the real implementation using Socket Mode and importing `slack-sdk` lazily in its constructor, and a scripted in-memory fake shipping alongside it. Every behavior in this capability except the compose-Kafka loop SHALL be covered by tests that run offline with no docker, no Slack workspace, and no `slack-sdk` installed, using FakeLLM to stage real intents and the in-memory source, sink, and gateway. The example SHALL NOT be importable from `beam_agents` and SHALL NOT be included in the wheel; `slack-sdk` SHALL be confined to a dev dependency group.

#### Scenario: The offline loop runs with fakes only

- **WHEN** the surface's full consume → post → decide → publish loop is driven with `InMemoryIntentSource`, `InMemoryMessageSink`, and the fake gateway in an environment without `slack-sdk` installed
- **THEN** every requirement above is exercisable and the tests pass with no docker and no network

#### Scenario: The service modules import without Beam

- **WHEN** the example's service modules (everything except the demo pipeline wiring) are imported with `apache_beam` blocked
- **THEN** the import succeeds, matching the out-of-pipeline boundary the effector documents

### Requirement: The documented demo closes the loop over compose Kafka with FakeLLM

The example SHALL include a documented demo — a FakeLLM demo agent that requests approval and suspends, plus surface wiring against `docker/compose.yaml`'s Redpanda — and an `-m integration` doc-contract test SHALL hold the documentation to it: the demo agent's approval intent crosses a real Kafka topic, the surface (real Kafka source and sink, fake gateway scripted to approve) publishes the approval envelope to a real approvals topic, and the envelope bytes read back from that topic, re-injected into the demo pipeline on the same key, resume the suspended activation with the approved verdict. The test SHALL use FakeLLM only and SHALL require nothing beyond the existing compose services. The doc-contract test SHALL import the same example modules the documentation describes, so documentation and example cannot drift apart silently.

#### Scenario: The closed loop over Redpanda resumes the agent

- **WHEN** the doc-contract integration test runs against compose Redpanda
- **THEN** the approval intent staged by the FakeLLM demo agent is consumed from a real topic, the surface publishes an `AgentEnvelope.Approval` to a real approvals topic keyed by `entity_key`, and those exact bytes re-injected on the same key resume the suspended activation and produce the approved decision on the main output

#### Scenario: The example tracks its documentation

- **WHEN** the doc-contract tests run
- **THEN** they exercise the example modules the documentation page references, so a change to the example that invalidates the documented demo fails the tests
