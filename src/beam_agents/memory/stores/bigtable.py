"""`MemoryStore` over Bigtable: ``CheckAndMutateRow`` seq-guarded upserts.

Row key ``hex(entity_key) + "#" + key`` in a single column family ``m``
holding a ``seq`` column (8-byte big-endian, the order-preserving encoding
that makes "greater" expressible as a ``ValueRangeFilter``) and a ``rec``
column (the envelope bytes). ``save`` is one conditional mutation: predicate =
latest ``seq`` cell strictly greater than the incoming seq — true branch
writes nothing (the stored row is newer), false branch writes both cells
(design D8). Every value predicate is limited to the most recent cell version
(``CellsColumnLimitFilter(1)``), the lesson already encoded in the dedup
store: columns are versioned and a superseded older cell must never decide
the guard. ``search`` is a bounded row-range prefix scan; UTF-8 byte order
preserves code-point order, so row order is key order (D7).

The client library is imported inside the constructor: it belongs to the
optional ``memory-stores`` extra (and rides into some environments via
``apache-beam[gcp]``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from beam_agents.memory.stores.base import (
    MemoryRecord,
    MemoryStore,
    decode_envelope,
    encode_envelope,
    encode_seq,
    missing_client_error,
)

if TYPE_CHECKING:
    from google.cloud.bigtable.data.row_filters import RowFilter


def _prefix_successor(prefix: bytes) -> bytes | None:
    """The smallest byte string greater than every string prefixed by ``prefix``.

    ``None`` for an all-``0xff`` prefix, meaning "no upper bound".
    """
    trimmed = prefix.rstrip(b"\xff")
    if not trimmed:
        return None
    return trimmed[:-1] + bytes([trimmed[-1] + 1])


class BigtableMemoryStore(MemoryStore):
    """`MemoryStore` over Bigtable; see the module docstring for the layout."""

    COLUMN_FAMILY = "m"
    SEQ_COLUMN = b"seq"
    RECORD_COLUMN = b"rec"

    def __init__(self, project: str, instance: str, table: str) -> None:
        try:
            from google.cloud.bigtable.data import BigtableDataClientAsync
        except ImportError as exc:
            raise missing_client_error("BigtableMemoryStore", "google-cloud-bigtable", exc) from exc

        self._client = BigtableDataClientAsync(project=project)
        self._table = self._client.get_table(instance, table)

    @staticmethod
    def _row_key(entity_key: bytes, key: str) -> bytes:
        return entity_key.hex().encode("ascii") + b"#" + key.encode("utf-8")

    def _stored_newer_filter(self, incoming_seq: int) -> RowFilter:
        """Match a row whose *latest* stored seq is strictly greater than
        ``incoming_seq`` — the predicate-true, write-nothing branch.

        ``CellsColumnLimitFilter(1)`` is load-bearing, not tidiness: the seq
        column keeps every superseded version, and without it a stale older
        cell could satisfy or defeat the guard on the row's behalf.
        """
        from google.cloud.bigtable.data import row_filters

        return row_filters.RowFilterChain(
            filters=[
                row_filters.FamilyNameRegexFilter(self.COLUMN_FAMILY),
                row_filters.ColumnQualifierRegexFilter(self.SEQ_COLUMN),
                row_filters.CellsColumnLimitFilter(1),
                row_filters.ValueRangeFilter(
                    start_value=encode_seq(incoming_seq), inclusive_start=False
                ),
            ]
        )

    def _latest_cells_filter(self) -> RowFilter:
        from google.cloud.bigtable.data import row_filters

        return row_filters.RowFilterChain(
            filters=[
                row_filters.FamilyNameRegexFilter(self.COLUMN_FAMILY),
                row_filters.CellsColumnLimitFilter(1),
            ]
        )

    async def _load(self, entity_key: bytes, key: str) -> MemoryRecord | None:
        from google.cloud.bigtable.data import ReadRowsQuery

        query = ReadRowsQuery(
            row_keys=[self._row_key(entity_key, key)],
            row_filter=self._latest_cells_filter(),
        )
        for row in await self._table.read_rows(query):
            envelope = self._record_cell(row)
            if envelope is not None:
                return decode_envelope(entity_key, envelope)
        return None

    def _record_cell(self, row: object) -> bytes | None:
        for cell in row.cells:  # type: ignore[attr-defined]
            if bytes(cell.qualifier) == self.RECORD_COLUMN:
                return bytes(cell.value)
        return None

    async def _save(self, record: MemoryRecord) -> bool:
        from google.cloud.bigtable.data import SetCell

        # One conditional mutation decides and applies: if the stored (latest)
        # seq is strictly greater, the true branch writes nothing; otherwise
        # the false branch replaces both cells. `save` reports "applied" iff
        # the false branch ran.
        stored_is_newer = await self._table.check_and_mutate_row(
            self._row_key(record.entity_key, record.key),
            self._stored_newer_filter(record.seq),
            true_case_mutations=None,
            false_case_mutations=[
                SetCell(self.COLUMN_FAMILY, self.SEQ_COLUMN, encode_seq(record.seq)),
                SetCell(self.COLUMN_FAMILY, self.RECORD_COLUMN, encode_envelope(record)),
            ],
        )
        return not bool(stored_is_newer)

    async def _search(self, entity_key: bytes, prefix: str, limit: int) -> list[MemoryRecord]:
        from google.cloud.bigtable.data import ReadRowsQuery, RowRange

        start = self._row_key(entity_key, prefix)
        end = _prefix_successor(start)
        row_range = (
            RowRange(start_key=start, end_key=end) if end is not None else RowRange(start_key=start)
        )
        query = ReadRowsQuery(
            row_ranges=[row_range],
            limit=limit,
            row_filter=self._latest_cells_filter(),
        )
        records: list[MemoryRecord] = []
        for row in await self._table.read_rows(query):
            envelope = self._record_cell(row)
            if envelope is not None:
                records.append(decode_envelope(entity_key, envelope))
        return records

    async def close(self) -> None:
        await self._client.close()
