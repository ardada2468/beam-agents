"""The three-state dedup claim protocol and its stores.

Dedup is the effector's half of the effectively-once argument (correctness
invariant 2): the pipeline guarantees a replayed bundle re-mints byte-identical
``intent_id``s, and this module guarantees at most one execution per
``intent_id``.

``claim`` returns exactly one of three outcomes (see the change design, D4):

- :class:`Claimed` — the caller owns execution, and carries the ownership token
  every later mutation is conditional on.
- :class:`InFlight` — a live lease is held elsewhere. The caller **waits**; it
  must never skip-and-commit, which would drop the effect if the owner is dead.
- :class:`Done` — a terminal record already exists. The caller republishes the
  stored result and does not execute. ``Done.result`` is ``None`` for an
  approval-kind intent, which is marked terminal (so redelivery cannot
  double-notify) but publishes no ``ToolResult``.

Client libraries are imported inside the store constructors: they are optional
dependencies and ``import beam_agents.effector`` must work without them.
"""

from __future__ import annotations

import struct
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, cast, runtime_checkable

from beam_agents._protos import ToolResult

if TYPE_CHECKING:
    from collections.abc import Callable

    from google.cloud.bigtable.data.row_filters import RowFilter

# Value tags framing a Redis/Bigtable record, so claim state and terminal state
# live in one value and one atomic operation each.
_TAG_CLAIM = b"C"
_TAG_DONE = b"D"

# A terminal record for an intent that produced no ToolResult (approval-kind).
_DONE_NO_RESULT = b""


@dataclass(frozen=True)
class Claimed:
    """Exclusive ownership of an ``intent_id`` for the lease duration."""

    token: str


@dataclass(frozen=True)
class InFlight:
    """A live lease is held by another worker."""


@dataclass(frozen=True)
class Done:
    """A terminal record exists. ``result`` is ``None`` for a routed approval."""

    result: ToolResult | None


ClaimOutcome = Claimed | InFlight | Done


@runtime_checkable
class DedupStore(Protocol):
    """Atomic claim/complete/release over ``intent_id``.

    ``claim`` MUST be atomic: two concurrent claims on one ``intent_id`` yield
    at most one :class:`Claimed`. ``complete`` and ``release`` MUST be
    conditional on still owning ``token``, so a worker whose lease expired
    mid-flight can never overwrite or free its successor's record.
    """

    async def claim(self, intent_id: str, lease_ms: int) -> ClaimOutcome: ...

    async def complete(
        self, intent_id: str, token: str, result: ToolResult | None, ttl_ms: int
    ) -> bool: ...

    async def release(self, intent_id: str, token: str) -> bool: ...

    async def close(self) -> None: ...


def _new_token() -> str:
    return uuid.uuid4().hex


def _encode_done(result: ToolResult | None) -> bytes:
    payload = _DONE_NO_RESULT if result is None else result.SerializeToString(deterministic=True)
    return _TAG_DONE + payload


def _decode_done(payload: bytes) -> Done:
    if payload == _DONE_NO_RESULT:
        return Done(result=None)
    stored = ToolResult()
    stored.ParseFromString(payload)
    return Done(result=stored)


def _encode_claim(token: str) -> bytes:
    return _TAG_CLAIM + token.encode()


@dataclass
class _Record:
    """One store entry: either a live claim or a terminal result."""

    tag: bytes
    payload: bytes
    expires_at_ms: int


@dataclass
class InMemoryDedupStore:
    """Process-local `DedupStore` for tests and single-worker deployments.

    Expiry is driven by an injectable ``clock`` (unix-epoch milliseconds) so
    lease and TTL scenarios are testable without sleeping. It is not shared
    across processes: two effector replicas pointed at it dedup independently,
    which is why real deployments use Redis or Bigtable.

    Every method completes without awaiting, so under one asyncio loop the
    claim is atomic by construction.
    """

    clock: Callable[[], int] = field(default_factory=lambda: _wall_clock_ms)
    _records: dict[str, _Record] = field(default_factory=dict, init=False)

    def _live(self, intent_id: str) -> _Record | None:
        record = self._records.get(intent_id)
        if record is None:
            return None
        if record.expires_at_ms <= self.clock():
            # An expired claim is re-claimable; an expired terminal record reads
            # as unseen. Both mean: forget it.
            del self._records[intent_id]
            return None
        return record

    async def claim(self, intent_id: str, lease_ms: int) -> ClaimOutcome:
        record = self._live(intent_id)
        if record is not None:
            if record.tag == _TAG_DONE:
                return _decode_done(record.payload)
            return InFlight()
        token = _new_token()
        self._records[intent_id] = _Record(
            tag=_TAG_CLAIM, payload=token.encode(), expires_at_ms=self.clock() + lease_ms
        )
        return Claimed(token=token)

    async def complete(
        self, intent_id: str, token: str, result: ToolResult | None, ttl_ms: int
    ) -> bool:
        record = self._live(intent_id)
        if record is None or record.tag != _TAG_CLAIM or record.payload != token.encode():
            return False
        payload = _encode_done(result)[len(_TAG_DONE) :]
        self._records[intent_id] = _Record(
            tag=_TAG_DONE, payload=payload, expires_at_ms=self.clock() + ttl_ms
        )
        return True

    async def release(self, intent_id: str, token: str) -> bool:
        record = self._live(intent_id)
        if record is None or record.tag != _TAG_CLAIM or record.payload != token.encode():
            return False
        del self._records[intent_id]
        return True

    async def close(self) -> None:
        return None


def _wall_clock_ms() -> int:
    import time

    return int(time.time() * 1000)


# Compare-and-set / compare-and-delete: `complete` and `release` must be
# conditional on the caller still owning the claim, and Redis has no primitive
# for "set only if the current value is X", so both run server-side as scripts.
_COMPLETE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3]) and 1 or 0
end
return 0
"""

_RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


class RedisDedupStore:
    """`DedupStore` over Redis: ``SET NX PX`` for the claim, scripts for the rest.

    Lease and TTL semantics come from Redis key expiry rather than a
    client-side clock, so they hold across workers with skewed clocks. The
    stored value is tag-framed (``C<token>`` / ``D<serialized ToolResult>``), so
    one ``GET`` distinguishes `InFlight` from `Done` and a claim is a single
    round trip on the common path.
    """

    def __init__(self, uri: str, *, key_prefix: str = "beam-agents:intent:") -> None:
        from redis import asyncio as redis_asyncio

        self._redis = redis_asyncio.from_url(uri)
        self._prefix = key_prefix
        self._complete = self._redis.register_script(_COMPLETE_SCRIPT)
        self._release = self._redis.register_script(_RELEASE_SCRIPT)

    def _key(self, intent_id: str) -> str:
        return f"{self._prefix}{intent_id}"

    async def claim(self, intent_id: str, lease_ms: int) -> ClaimOutcome:
        key = self._key(intent_id)
        token = _new_token()
        if await self._redis.set(key, _encode_claim(token), nx=True, px=lease_ms):
            return Claimed(token=token)
        # The client is typed as decoding to `str` when configured to, but this
        # store always writes and reads tag-framed bytes.
        existing = cast("bytes | None", await self._redis.get(key))
        if existing is None:
            # The record expired between the SET and the GET; the next delivery
            # (or the caller's retry) claims it. Reporting InFlight is the safe
            # reading — it makes the caller wait rather than skip.
            return InFlight()
        if existing[:1] == _TAG_DONE:
            return _decode_done(existing[1:])
        return InFlight()

    async def complete(
        self, intent_id: str, token: str, result: ToolResult | None, ttl_ms: int
    ) -> bool:
        outcome = await self._complete(
            keys=[self._key(intent_id)],
            args=[_encode_claim(token), _encode_done(result), ttl_ms],
        )
        return bool(outcome)

    async def release(self, intent_id: str, token: str) -> bool:
        outcome = await self._release(keys=[self._key(intent_id)], args=[_encode_claim(token)])
        return bool(outcome)

    async def close(self) -> None:
        await self._redis.aclose()


# Lease expiry has to be expressible as a Bigtable filter, and Bigtable value
# filters compare bytes lexicographically. Big-endian fixed-width encoding makes
# lexicographic order agree with numeric order for non-negative values, so
# "lease not yet expired" becomes a ValueRange lower-bounded at now.
def encode_lease_expiry(expires_at_ms: int) -> bytes:
    """Encode a lease expiry so byte order matches numeric order."""
    if expires_at_ms < 0:
        raise ValueError(f"lease expiry must be non-negative, got {expires_at_ms}")
    return struct.pack(">Q", expires_at_ms)


def decode_lease_expiry(encoded: bytes) -> int:
    """Inverse of :func:`encode_lease_expiry`.

    Reads back the expiry a claim cell carries — used by the property test that
    pins the order-preserving property the Bigtable range filter depends on, and
    by operators inspecting a wedged claim.
    """
    return int(struct.unpack(">Q", encoded[:8])[0])


class BigtableDedupStore:
    """`DedupStore` over Bigtable: ``CheckAndMutateRow`` for conditional claims.

    Row key is the ``intent_id`` (a uuid5, already uniformly distributed, so no
    salting is needed). Column family ``d`` holds four columns: ``claim`` (the
    big-endian lease expiry), ``owner`` (the ownership token), ``result`` (a
    serialized ``ToolResult``, empty for a routed approval), and ``rexp`` (the
    big-endian terminal-record expiry).

    **Terminal expiry is a read-time predicate, not the GC rule.** The column
    family's ``maxage`` rule reclaims space, but it cannot be what decides
    expiry: it is table-level, so a per-call ``result_ttl_ms`` is
    unrepresentable, and it is asynchronous and best-effort, so a record can be
    served long after its TTL elapsed. ``complete`` therefore stamps ``rexp``
    and every read filters on it, exactly as the lease does.

    Every value predicate is limited to the **most recent cell version**.
    Bigtable columns are versioned: re-claiming after a lease expiry leaves the
    superseded owner token behind as an older cell, and a predicate free to
    match any version would let a worker whose lease expired ``complete`` over
    its successor's claim.

    Expiry and ownership live in *separate* columns on purpose. Bigtable value
    filters are RE2 over raw bytes, and RE2's ``.`` does not match a newline
    byte — so an ownership regex applied to a value that begins with eight
    arbitrary bytes of timestamp would silently fail to match whenever the
    timestamp happened to contain ``0x0A``. Splitting them keeps the ownership
    predicate an exact match on an ASCII-hex token and the expiry predicate a
    numeric range, with neither ever matching over binary.

    ``ReadModifyWriteRow`` is atomic but unconditional and so cannot express
    "only if unclaimed", which is the entire operation; hence check-and-mutate.
    """

    COLUMN_FAMILY = "d"
    CLAIM_COLUMN = b"claim"
    OWNER_COLUMN = b"owner"
    RESULT_COLUMN = b"result"
    RESULT_EXPIRY_COLUMN = b"rexp"

    def __init__(
        self,
        project: str,
        instance: str,
        table: str,
        *,
        clock: Callable[[], int] = _wall_clock_ms,
    ) -> None:
        from google.cloud.bigtable.data import BigtableDataClientAsync

        self._client = BigtableDataClientAsync(project=project)
        self._table = self._client.get_table(instance, table)
        self._clock = clock

    def _live_claim_filter(self, now_ms: int) -> RowFilter:
        # claim column present AND its value (lease expiry, big-endian) is
        # strictly past `now` — i.e. the lease has not run out yet. The
        # exclusive lower bound matches the in-memory store, where a lease
        # expiring exactly at `now` reads as expired.
        return self._live_expiry_filter(self.CLAIM_COLUMN, now_ms)

    def _live_expiry_filter(self, column: bytes, now_ms: int) -> RowFilter:
        """Match ``column`` when its latest cell holds an expiry past ``now_ms``.

        ``CellsColumnLimitFilter(1)`` is load-bearing, not tidiness: the column
        keeps every superseded version, so without it a stale cell can satisfy
        the predicate on behalf of a record that has since moved on.
        """
        from google.cloud.bigtable.data import row_filters

        return row_filters.RowFilterChain(
            filters=[
                row_filters.FamilyNameRegexFilter(self.COLUMN_FAMILY),
                row_filters.ColumnQualifierRegexFilter(column),
                row_filters.CellsColumnLimitFilter(1),
                row_filters.ValueRangeFilter(
                    start_value=encode_lease_expiry(now_ms), inclusive_start=False
                ),
            ]
        )

    def _live_result_filter(self, now_ms: int) -> RowFilter:
        """Match a terminal record that has not yet reached its ``rexp``.

        Gated on the expiry column rather than on `result`'s presence: an
        expired terminal record has to read as unseen, and a `ToolResult` whose
        serialization is empty (a routed approval) cannot carry its own expiry.
        """
        return self._live_expiry_filter(self.RESULT_EXPIRY_COLUMN, now_ms)

    def _taken_filter(self, now_ms: int) -> RowFilter:
        """Matches a row that is either live-claimed or already terminal.

        Both branches mean "do not claim this"; they are distinguished by a
        follow-up read, which keeps the common (unclaimed) path at one RPC.
        """
        from google.cloud.bigtable.data import row_filters

        return row_filters.RowFilterUnion(
            filters=[self._live_claim_filter(now_ms), self._live_result_filter(now_ms)]
        )

    async def _read_state(self, intent_id: str, now_ms: int) -> ClaimOutcome:
        from google.cloud.bigtable.data import ReadRowsQuery, row_filters

        query = ReadRowsQuery(
            row_keys=[intent_id.encode()],
            row_filter=row_filters.RowFilterChain(
                filters=[
                    row_filters.FamilyNameRegexFilter(self.COLUMN_FAMILY),
                    row_filters.CellsColumnLimitFilter(1),
                ]
            ),
        )
        for row in await self._table.read_rows(query):
            cells: dict[bytes, bytes] = {}
            for cell in row.cells:
                cells[bytes(cell.qualifier)] = bytes(cell.value)
            stored = cells.get(self.RESULT_COLUMN)
            expiry = cells.get(self.RESULT_EXPIRY_COLUMN)
            # A *live* terminal record wins over any claim cell still present.
            # An expired one does not: the row was re-claimable, so the caller
            # is mid-claim against it, not looking at a real result.
            if stored is not None and expiry is not None and now_ms < decode_lease_expiry(expiry):
                return _decode_done(stored)
        # Otherwise the row is live-claimed (the common case), or its claim
        # expired between the conditional mutation and this read. Both are
        # safest reported as InFlight: the caller waits and re-claims rather
        # than skipping, which is the one outcome that could drop an effect.
        return InFlight()

    async def claim(self, intent_id: str, lease_ms: int) -> ClaimOutcome:
        from google.cloud.bigtable.data import SetCell

        now_ms = self._clock()
        token = _new_token()
        # One conditional mutation decides the common path: if the row is
        # neither live-claimed nor terminal, the false branch writes our claim
        # and we own it. Writing only in the false branch is what keeps a
        # completed row from collecting a stray claim cell.
        taken = await self._table.check_and_mutate_row(
            intent_id.encode(),
            self._taken_filter(now_ms),
            true_case_mutations=None,
            false_case_mutations=[
                SetCell(
                    self.COLUMN_FAMILY, self.CLAIM_COLUMN, encode_lease_expiry(now_ms + lease_ms)
                ),
                SetCell(self.COLUMN_FAMILY, self.OWNER_COLUMN, token.encode()),
            ],
        )
        if taken:
            return await self._read_state(intent_id, now_ms)
        return Claimed(token=token)

    def _owner_filter(self, token: str) -> RowFilter:
        from google.cloud.bigtable.data import row_filters

        # An exact value match on the ASCII-hex token — no regex metacharacters
        # and no binary, so RE2's newline handling cannot bite. Limited to the
        # latest cell: a re-claim after a lease expiry leaves the previous
        # owner's token behind as an older version, and matching that would let
        # a worker whose lease expired complete over its successor's claim.
        return row_filters.RowFilterChain(
            filters=[
                row_filters.FamilyNameRegexFilter(self.COLUMN_FAMILY),
                row_filters.ColumnQualifierRegexFilter(self.OWNER_COLUMN),
                row_filters.CellsColumnLimitFilter(1),
                row_filters.ValueRegexFilter(token.encode()),
            ]
        )

    async def complete(
        self, intent_id: str, token: str, result: ToolResult | None, ttl_ms: int
    ) -> bool:
        from google.cloud.bigtable.data import DeleteRangeFromColumn, SetCell

        stored = _encode_done(result)[len(_TAG_DONE) :]
        owned = await self._table.check_and_mutate_row(
            intent_id.encode(),
            self._owner_filter(token),
            true_case_mutations=[
                SetCell(self.COLUMN_FAMILY, self.RESULT_COLUMN, stored),
                # The record's own expiry. Every read gates on this; the GC rule
                # only reclaims the space afterwards.
                SetCell(
                    self.COLUMN_FAMILY,
                    self.RESULT_EXPIRY_COLUMN,
                    encode_lease_expiry(self._clock() + ttl_ms),
                ),
                DeleteRangeFromColumn(self.COLUMN_FAMILY, self.CLAIM_COLUMN),
                DeleteRangeFromColumn(self.COLUMN_FAMILY, self.OWNER_COLUMN),
            ],
            false_case_mutations=None,
        )
        return bool(owned)

    async def release(self, intent_id: str, token: str) -> bool:
        from google.cloud.bigtable.data import DeleteRangeFromColumn

        owned = await self._table.check_and_mutate_row(
            intent_id.encode(),
            self._owner_filter(token),
            true_case_mutations=[
                DeleteRangeFromColumn(self.COLUMN_FAMILY, self.CLAIM_COLUMN),
                DeleteRangeFromColumn(self.COLUMN_FAMILY, self.OWNER_COLUMN),
            ],
            false_case_mutations=None,
        )
        return bool(owned)

    async def close(self) -> None:
        await self._client.close()


def build_dedup_store(scheme: str, parts: tuple[str, ...]) -> DedupStore:
    """Construct the store a parsed dedup URI names.

    Called once at service start; the client import happens inside the chosen
    store's constructor, never here.
    """
    if scheme == "memory":
        return InMemoryDedupStore()
    if scheme == "redis":
        return RedisDedupStore(parts[0])
    project, instance, table = parts
    return BigtableDedupStore(project, instance, table)
