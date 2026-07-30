"""A Slack approval surface for the beam-agents human-in-the-loop loop.

Demonstrates the out-of-pipeline contract between the approval channel and the
approvals topic: consume `kind = APPROVAL` intents through the effector's
`IntentSource` seam, post one interactive Block Kit message each, publish the
operator's verdict as a deterministically serialized `AgentEnvelope.Approval`
keyed by the raw `entity_key`, and treat expired intents as never actionable.

Sample code, not a supported runtime surface: nothing here enters the wheel or
`beam_agents.__init__`, and out-of-tree copies should pin the beam-agents
version they copied from. See `docs/examples/slack-approval.md`.

This package init deliberately imports nothing: the service modules (`config`,
`blocks`, `slack`, `surface`) import no Beam and no slack-sdk at module level,
and `agent` — the demo pipeline wiring — is the single module that imports Beam.
"""
