"""Authenticated-broker wiring against a live SASL listener (integration).

Covers "SASL settings reach the Kafka clients" end to end, which the offline
lane can only assert at the constructor: a captured kwarg proves the setting
was passed, not that the broker accepted it. This leg proves the effector
consumes an intent and publishes its result through an authenticated listener
with a password supplied by reference.

Requires a SASL-enabled Redpanda listener, named by ``EFFECTOR_SASL_BOOTSTRAP``
(with ``EFFECTOR_SASL_USER``/``EFFECTOR_SASL_PASSWORD``). The compose profile
that provisions it is not committed yet, so the module skips rather than fails
where the listener is absent — a skip is visible in the report; a red test on
missing infrastructure teaches everyone to ignore this file.
"""

from __future__ import annotations

import os
import uuid

import pytest

from beam_agents._protos import ToolIntent, ToolResult
from beam_agents.effector.config import TransportSecurity
from beam_agents.effector.sinks import KafkaMessageSink
from beam_agents.effector.sources import KafkaIntentSource

BOOTSTRAP = os.environ.get("EFFECTOR_SASL_BOOTSTRAP")
USER_ENV = "EFFECTOR_SASL_USER"
PASSWORD_ENV = "EFFECTOR_SASL_PASSWORD"

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not BOOTSTRAP or PASSWORD_ENV not in os.environ,
        reason=(
            f"no SASL-enabled broker: set EFFECTOR_SASL_BOOTSTRAP, {USER_ENV} and {PASSWORD_ENV}"
        ),
    ),
]


def _security() -> TransportSecurity:
    return TransportSecurity(
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_username_reference=f"env:{USER_ENV}",
        sasl_password_reference=f"env:{PASSWORD_ENV}",
    )


async def test_the_effector_consumes_and_publishes_through_authenticated_listeners() -> None:
    # Scenario: SASL settings reach the Kafka clients — proven against a live
    # authenticated listener rather than a captured constructor kwarg.
    assert BOOTSTRAP is not None
    topic = f"intents-sasl-{uuid.uuid4().hex[:8]}"
    results_topic = f"results-sasl-{uuid.uuid4().hex[:8]}"
    intent = ToolIntent(
        intent_id=str(uuid.uuid4()), entity_key=b"customer-7", seq=1, tool_name="charge"
    )

    producer = KafkaMessageSink(BOOTSTRAP, topic, security=_security())
    try:
        await producer.publish(intent.entity_key, intent.SerializeToString(deterministic=True))
    finally:
        await producer.close()

    source = KafkaIntentSource(
        BOOTSTRAP, topic, f"effector-sasl-{uuid.uuid4().hex[:8]}", security=_security()
    )
    await source.start()
    try:
        delivered = await anext(aiter(source))
    finally:
        await source.close()

    assert delivered.intent.intent_id == intent.intent_id

    results = KafkaMessageSink(BOOTSTRAP, results_topic, security=_security())
    try:
        result = ToolResult(
            intent_id=intent.intent_id, entity_key=intent.entity_key, status=ToolResult.OK
        )
        await results.publish(result.entity_key, result.SerializeToString(deterministic=True))
    finally:
        await results.close()
