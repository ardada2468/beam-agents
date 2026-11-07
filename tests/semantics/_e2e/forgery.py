"""The adversary: one unsigned intent with topic-write access and no key.

The effectively-once gate proves each *genuine* intent executes exactly once.
This module supplies the authenticity half on real infrastructure: an intent
an attacker holding a producer credential for the intents topic — but not the
signing key — would publish. It names the registered ``charge`` tool, carries
well-formed arguments, and mints its ``intent_id`` with the runtime's own
deterministic formula, so nothing about it is malformed and the dedup store
is no defense (a novel id claims cleanly). Only the missing signature stands
between it and the ledger — which is the whole point: under ``require`` the
effector dead-letters it verbatim, publishes no ``ToolResult``, and the
ledger never learns its id; under ``off`` or ``permissive`` it executes.

Published from the host through the external listener, exactly as the gate
publishes events: it never passes through the pipeline, so a replay cannot
re-mint it and the "exactly one dead letter" assertion is clean.
"""

from __future__ import annotations

import json

from beam_agents._protos import ToolIntent
from beam_agents.core.agent import intent_id_for
from beam_agents.intent_signing import VerificationResult, verify_intent
from tests.semantics._e2e.stack import HOST_BROKERS, RunConfig

FORGED_PREFIX = b"forged-"

# Long enough that no run can expire it: the gate's claim is that the intent is
# refused for its *signature*, ahead of the expiry check, and an `EXPIRED`
# refusal would be the wrong reason to pass.
FORGED_TTL_MS = 60 * 60 * 1000


def forged_intent(config: RunConfig, *, now_ms: int) -> ToolIntent:
    """An unsigned ``charge`` intent for a key outside every population."""
    key = config.entity_key(FORGED_PREFIX, 0)
    intent = ToolIntent(
        intent_id=intent_id_for(key, 0, 1),
        entity_key=key,
        seq=0,
        step_index=1,
        tool_name="charge",
        args_json=json.dumps({"key": key.hex()}, sort_keys=True, separators=(",", ":")),
        created_at_ms=now_ms,
        expires_at_ms=now_ms + FORGED_TTL_MS,
        kind=ToolIntent.TOOL,
    )
    # Guard against the forgery accidentally carrying a signature envelope: the
    # gate asserts the `unsigned_intent` path, not `bad_signature`.
    assert verify_intent(intent, {}) is VerificationResult.UNSIGNED
    return intent


async def inject(config: RunConfig, intent: ToolIntent) -> bytes:
    """Publish ``intent`` once to the run's intents topic; return the raw bytes.

    Keyed exactly as the outbox keys genuine intents (the raw ``entity_key``),
    so it lands on a partition some live worker owns and is indistinguishable
    from a genuine delivery until verification runs.
    """
    from aiokafka import AIOKafkaProducer

    payload = intent.SerializeToString(deterministic=True)
    producer = AIOKafkaProducer(bootstrap_servers=HOST_BROKERS)
    await producer.start()
    try:
        await producer.send_and_wait(config.intents_topic, key=intent.entity_key, value=payload)
    finally:
        await producer.stop()
    return payload
