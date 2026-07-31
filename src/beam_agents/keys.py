"""Hot-key sharding: fan one logical entity key across ``key#0..key#N-1``.

**Shard only memory-free agents.** Sharding is safe when, and only when, the
agent:

1. **keeps no per-key memory** — no ``ctx.memory`` read or write that must span
   events. Every physical shard has its own ``MEMORY`` cell, so a sharded
   memory-carrying agent silently accumulates *N* independent, divergent
   working memories, each seeing roughly 1/N of the entity's events;
2. **requires no per-key ordering** — Beam serializes elements per key, so
   sharding trades a logical entity's one serial lane for *N* concurrent ones;
3. **runs no HITL flow whose approvals are keyed by the logical entity** — a
   continuation stored under ``entity#3`` cannot be resumed by an approval that
   arrives keyed ``entity``; it dead-letters as ``orphaned_result``. (The
   ordinary intent → effector → result path is unaffected: the physical key
   rides ``ToolIntent.entity_key`` and comes back on the shard that emitted it.)

The runtime does not, and cannot, detect a violation: a sharded key is an
ordinary ``bytes`` key to the DoFn's state specs, and "memory-free" is a
property of what the agent does at activation time, not of the pipeline graph.
The contract is documentary — see ``docs/sharding.md`` for the throughput math
that motivates sharding and the full when-NOT-to-shard list.

Usage — on the **events branch only**, after ``WithKeys``::

    keyed = events | beam.WithKeys(lambda env: env.entity_key).with_output_types(
        tuple[bytes, AgentEnvelope]
    )
    outputs = keyed | ShardKeys(8) | RunAgent(agent, config=config)

Regrouping downstream is ordinary Beam: key whatever the agent emitted by its
physical key, then map that key through :func:`unshard_key` to get the logical
entity back. There is no cross-shard aggregation transform — this module
supplies the key function, not an aggregation DSL.

Shard assignment is a correctness input, not a load-balancing detail:
``intent_id = uuid5(NAMESPACE, key + seq + step_index)`` and the replay cache
key both contain the physical key, so an assignment that differed between a
bundle's first attempt and its retry would re-mint intent IDs (duplicate side
effects) and miss the cache (extra provider calls). The default assignment is
therefore a SHA-256 hash of the element payload — never Python's per-process
salted ``hash()``.

Importing this module has no side effects.
"""

from __future__ import annotations

import hashlib
import itertools
import threading
from typing import TYPE_CHECKING, Literal

import apache_beam as beam

from beam_agents._protos import AgentEnvelope
from beam_agents.actions.write_intents import _is_kv_shaped

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Iterator

__all__ = [
    "SHARD_DELIMITER",
    "Assignment",
    "ShardKeys",
    "shard_key",
    "unshard_key",
]

#: The shard-suffix delimiter. A physical key is ``<logical key>#<decimal index>``.
SHARD_DELIMITER = b"#"

#: Digest bytes folded into the shard index. Eight bytes is ~1.8e19 of range
#: against a modulus realistically below 1024, so the modulo bias is nil.
_DIGEST_BYTES = 8

#: Assignment modes. ``"hash"`` is the default because it is the safe one.
Assignment = Literal["hash", "round_robin"]
_ASSIGNMENTS: tuple[str, ...] = ("hash", "round_robin")

# Worker-local round-robin counter (a documented worker-local singleton, like
# the circuit breakers): shared by every `ShardKeys(assignment="round_robin")`
# DoFn instance in a worker process so a bundle boundary does not restart the
# rotation. Deliberately NOT durable and NOT replay-stable -- that is exactly
# the caveat the mode carries.
_ROUND_ROBIN: Iterator[int] = itertools.count()
_ROUND_ROBIN_LOCK = threading.Lock()


def shard_key(key: bytes, n: int, *, payload: bytes) -> bytes:
    """Return the physical shard key ``key + b"#" + <index>`` for ``payload``.

    The index is ``int.from_bytes(sha256(payload).digest()[:8]) % n`` — a pure
    function of ``(payload, n)``. It depends on nothing else: not process or
    worker identity, not element order, not the wall clock, and never on
    Python's ``hash()``, which is salted per process by ``PYTHONHASHSEED`` and
    would hand the same element to different shards on different workers. That
    determinism is what preserves ``(key, seq)`` replay-cache identity and
    byte-identical ``intent_id``s across a bundle retry.

    ``n = 1`` still appends ``#0``: the shape of a sharded key never depends on
    the shard count, so ``unshard_key`` works uniformly. ``n < 1`` raises
    ``ValueError``.

    Note the convention's one ambiguity (mirrored in :func:`unshard_key`): a
    logical ``key`` that itself ends in ``#<digits>`` is indistinguishable from
    an already-sharded key, so such keys should not be sharded.
    """
    if n < 1:
        raise ValueError(f"shard count must be >= 1, got {n}")
    index = int.from_bytes(hashlib.sha256(payload).digest()[:_DIGEST_BYTES]) % n
    return key + SHARD_DELIMITER + str(index).encode("ascii")


def unshard_key(key: bytes) -> bytes:
    """Return the logical key behind a physical shard key.

    Strips exactly one trailing ``#<digits>`` group, so
    ``unshard_key(shard_key(k, n, payload=p)) == k`` for every valid input.

    Defined only over keys produced by :func:`shard_key`: a key with no trailing
    ``#<digits>`` raises ``ValueError`` rather than passing through, so a
    mis-wired regroup fails loudly instead of silently merging outputs under the
    wrong key. The inverse cannot resolve the residual ambiguity — ``b"user#7"``
    is both a plausible logical key and shard 7 of ``b"user"`` — so it resolves
    it in favour of the sharded reading; do not shard keys that already end in
    ``#<digits>``.
    """
    logical, delimiter, digits = key.rpartition(SHARD_DELIMITER)
    if not delimiter or not digits.isdigit():
        raise ValueError(
            f"not a sharded key: {key!r} has no trailing shard suffix. Expected the "
            "key#<shard> shape produced by beam_agents.shard_key (e.g. b'entity#3')."
        )
    return logical


def _hash_payload(envelope: AgentEnvelope) -> bytes:
    """The bytes hash assignment reduces for one element.

    An external event hashes on its own opaque payload — the natural unit of
    entropy, and stable under any re-serialization of the envelope around it.
    The other payload variants (tool results, approvals) have no such field;
    they must never reach ``ShardKeys`` at all (they already carry the physical
    key), but if one does it hashes on its deterministic serialization rather
    than collapsing every such element onto one shard.
    """
    if envelope.WhichOneof("payload") == "external_event":
        return envelope.external_event
    return envelope.SerializeToString(deterministic=True)


class _ShardElement(beam.DoFn):
    """Rewrites one element's KV key and its envelope's ``entity_key`` together."""

    def __init__(self, n: int, assignment: Assignment) -> None:
        super().__init__()
        self._n = n
        self._assignment = assignment

    def _index_key(self, key: bytes, envelope: AgentEnvelope) -> bytes:
        if self._assignment == "hash":
            return shard_key(key, self._n, payload=_hash_payload(envelope))
        with _ROUND_ROBIN_LOCK:
            index = next(_ROUND_ROBIN) % self._n
        return key + SHARD_DELIMITER + str(index).encode("ascii")

    def process(
        self, element: tuple[bytes, AgentEnvelope]
    ) -> Iterator[tuple[bytes, AgentEnvelope]]:
        key, envelope = element
        physical = self._index_key(key, envelope)
        # A copy, never a mutation: the input envelope may be shared with other
        # consumers of the same PCollection, and Beam forbids mutating an
        # element after it has been emitted.
        sharded = AgentEnvelope()
        sharded.CopyFrom(envelope)
        sharded.entity_key = physical
        yield physical, sharded


class ShardKeys(beam.PTransform):
    """Fan a hot logical key across ``n`` physical shards, upstream of ``RunAgent``.

    **Memory-free agents only.** Every shard gets its own ``MEMORY``,
    ``CONTINUATION``, ``LLM_CACHE``, ``PENDING`` and ``SEQ`` cells, so an agent
    that carries state between events, depends on per-key ordering, or takes
    HITL approvals keyed by the logical entity is broken — silently — by
    sharding. The module docstring and ``docs/sharding.md`` state the contract
    in full; the runtime performs no detection.

    Placement: the **events branch only**, after ``WithKeys(entity_key)`` and
    before any ``Flatten`` with the tool-results and approvals streams. Those
    streams already carry the physical shard key (the effector echoes
    ``ToolIntent.entity_key``), so re-sharding them would either double-suffix
    the key or route a result to the wrong shard, where it finds no
    continuation and dead-letters as ``orphaned_result``.

    Consumes and produces ``PCollection[KV[bytes, AgentEnvelope]]``, rewriting
    the KV key and the envelope's own ``entity_key`` to the same physical key so
    the state layout and the envelope can never disagree. Input shape is
    validated at pipeline-construction time.

    ``assignment``:

    - ``"hash"`` (default) — SHA-256 of the element payload modulo ``n``. Pure,
      so a bundle retry reproduces the same physical keys, the same ``(key,
      seq)`` cache hits, and byte-identical ``intent_id``s. Its failure mode is
      skew: low-entropy payloads (many identical events) land on one shard and
      the fan-out collapses, so verify the spread before trusting ``n``.
    - ``"round_robin"`` — an explicit opt-in for exactly that skew case, using a
      worker-local counter. **It forfeits deterministic shard assignment under
      bundle retries:** a retry's counter state differs, so the element lands on
      a different shard, mints different ``intent_id``s (the effector's dedup no
      longer suppresses the duplicate side effect) and misses the replay cache
      (extra provider calls on every retry). Use it only for agents that emit no
      intents — or whose effects are idempotent independently of ``intent_id`` —
      and only where duplicate provider calls on a retry are an accepted cost.
    """

    def __init__(self, n: int, *, assignment: Assignment = "hash") -> None:
        super().__init__()
        if n < 1:
            raise ValueError(f"ShardKeys shard count must be >= 1, got {n}")
        if assignment not in _ASSIGNMENTS:
            raise ValueError(
                f"unknown ShardKeys assignment {assignment!r}; expected one of {_ASSIGNMENTS}"
            )
        self._n = n
        self._assignment: Assignment = assignment

    def expand(self, pcoll: beam.pvalue.PCollection) -> beam.pvalue.PCollection:
        """Fan each logical entity across ``shards`` physical keys.

        Raises ``ValueError`` at pipeline-construction time on non-KV input,
        naming the ``beam.WithKeys`` call the caller is missing. Safe for
        memory-free agents only — see ``docs/sharding.md``.
        """
        if not _is_kv_shaped(pcoll.element_type):
            raise ValueError(
                "ShardKeys requires a PCollection[KV[bytes, AgentEnvelope]] input "
                f"(pre-keyed by entity_key); got element type {pcoll.element_type!r}. Key "
                "upstream with beam.WithKeys(entity_key)"
                ".with_output_types(tuple[bytes, AgentEnvelope]) before ShardKeys."
            )
        return pcoll | "Shard" >> beam.ParDo(
            _ShardElement(self._n, self._assignment)
        ).with_output_types(tuple[bytes, AgentEnvelope])
