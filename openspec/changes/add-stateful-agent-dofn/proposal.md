## Why

The runtime has durable memory and replay-cache facades but no Beam stateful execution boundary that can safely route events, suspend and resume activations, or atomically commit staged effects. A stateful `_AgentDoFn` is required to turn those components into the fault-tolerant per-key agent runtime promised by `RunAgent`.

## What Changes

- Add an internal stateful `_AgentDoFn` with `MEMORY`, `CONTINUATION`, `LLM_CACHE`, `PENDING`, and `SEQ` state specs.
- Add watermark-driven TTL cleanup and real-time HITL timeout handling.
- Add a worker-local asyncio bridge thread that runs activations with a configured timeout and cancels timed-out work.
- Route keyed elements by event, tool-result, approval, and timer kind, including continuation resume and orphan handling.
- Stage all state mutations and emitted records during an activation, then commit them in a defined atomic ordering only after successful completion.
- Add an additive typed `RuntimeError` protobuf for dead-letter output from routing, timeout, and activation failures.
- Add Beam state/timer, timeout, replay, routing, and failure-path tests for the new runtime boundary.

## Capabilities

### New Capabilities

- `stateful-agent-runtime`: Defines keyed state, timers, activation execution, element routing, suspension/resumption, cancellation, and atomic commit behavior for `_AgentDoFn`.

### Modified Capabilities

- `wire-schemas`: Add the typed runtime error message to the committed protobuf bindings, deterministic coder registry, and schema-compatibility contract.

## Impact

- Adds the internal `src/beam_agents/core/dofn.py` runtime boundary and supporting activation-context or routing helpers.
- Integrates existing memory, model facade, replay-cache, protobuf coder, continuation, intent, and error-output surfaces.
- Introduces Apache Beam state/timer behavior into unit and semantics tests without changing the public API.
- Adds one additive protobuf wire message and requires all persisted state and runtime records to use deterministic protobuf coders within existing compatibility limits.
