"""Offline call-shape tests for the Bigtable `MemoryStore`.

Covers "The Bigtable store guards upserts with CheckAndMutateRow" without an
emulator: a fake data client records the single conditional mutation `save`
issues, so both predicate branches, the filter construction (including the
load-bearing `CellsColumnLimitFilter(1)` — "Only the latest seq cell decides
the predicate"), and the row-key/column layout are pinned as call shapes. The
read half — `load`/`search` query shaping, `_prefix_successor`'s ordering
contract, record-cell extraction, and `close` — is pinned the same way
(harden-memory-stores-offline). The same requirements run for real against
the compose emulator in `test_bigtable_emulator.py` under `-m integration`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from hypothesis import given
from hypothesis import strategies as st

if TYPE_CHECKING:
    from collections.abc import Iterator

# google-cloud-bigtable rides into the unit lane via apache-beam[gcp]; if a
# trimmed environment lacks it, these call-shape tests skip and the emulator
# leg still covers the requirement.
row_filters = pytest.importorskip("google.cloud.bigtable.data.row_filters")

from google.cloud.bigtable.data import ReadRowsQuery  # noqa: E402

from beam_agents.memory.stores.base import _encode_envelope, _encode_seq  # noqa: E402
from beam_agents.memory.stores.bigtable import (  # noqa: E402
    BigtableMemoryStore,
    _prefix_successor,
)

from ._conformance import ENTITY_A, a_record  # noqa: E402


class _FakeCell:
    def __init__(self, qualifier: bytes, value: bytes) -> None:
        self.qualifier = qualifier
        self.value = value


class _FakeRow:
    def __init__(self, *cells: _FakeCell) -> None:
        self.cells = list(cells)


class _FakeTable:
    """Records `check_and_mutate_row`/`read_rows` calls; scripted replies."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.predicate_matched = False
        self.read_rows_queries: list[ReadRowsQuery] = []
        self.rows: list[_FakeRow] = []

    async def check_and_mutate_row(
        self,
        row_key: bytes,
        predicate: object,
        *,
        true_case_mutations: object = None,
        false_case_mutations: object = None,
    ) -> bool:
        self.calls.append(
            {
                "row_key": row_key,
                "predicate": predicate,
                "true_case_mutations": true_case_mutations,
                "false_case_mutations": false_case_mutations,
            }
        )
        return self.predicate_matched

    async def read_rows(self, query: ReadRowsQuery) -> list[_FakeRow]:
        self.read_rows_queries.append(query)
        return self.rows


class _FakeClient:
    def __init__(self, *, project: str) -> None:
        self.project = project
        self.table = _FakeTable()
        self.closed = False

    def get_table(self, instance: str, table: str) -> _FakeTable:
        return self.table

    async def close(self) -> None:
        self.closed = True


def _rec_cell(record_key: str, seq: int = 7, value: bytes = b"v") -> _FakeCell:
    record = a_record(record_key, seq=seq, value=value)
    return _FakeCell(BigtableMemoryStore.RECORD_COLUMN, _encode_envelope(record))


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[BigtableMemoryStore, _FakeTable]]:
    monkeypatch.setattr("google.cloud.bigtable.data.BigtableDataClientAsync", _FakeClient)
    built = BigtableMemoryStore("proj", "inst", "table")
    client = built._client
    assert isinstance(client, _FakeClient)
    yield built, client.table


# -- Scenario: The conditional mutation enforces the guard in one RPC ---------


async def test_a_stale_save_takes_the_predicate_true_branch_and_writes_nothing(
    store: tuple[BigtableMemoryStore, _FakeTable],
) -> None:
    bt, table = store
    table.predicate_matched = True  # stored seq strictly greater than incoming

    applied = await bt.save(a_record("profile", seq=5))

    assert not applied
    assert len(table.calls) == 1
    call = table.calls[0]
    # The true branch (stored newer) writes nothing; only the false branch
    # carries mutations. One RPC decides and applies.
    assert call["true_case_mutations"] is None
    assert call["false_case_mutations"] is not None


async def test_a_winning_save_writes_both_cells_in_the_false_branch(
    store: tuple[BigtableMemoryStore, _FakeTable],
) -> None:
    bt, table = store
    table.predicate_matched = False
    record = a_record("profile", seq=8)

    applied = await bt.save(record)

    assert applied
    call = table.calls[0]
    mutations = call["false_case_mutations"]
    assert isinstance(mutations, list) and len(mutations) == 2
    by_qualifier = {bytes(m.qualifier): bytes(m.new_value) for m in mutations}
    assert by_qualifier == {
        BigtableMemoryStore.SEQ_COLUMN: _encode_seq(8),
        BigtableMemoryStore.RECORD_COLUMN: _encode_envelope(record),
    }
    assert all(m.family == BigtableMemoryStore.COLUMN_FAMILY for m in mutations)


async def test_the_row_key_is_hex_entity_hash_key(
    store: tuple[BigtableMemoryStore, _FakeTable],
) -> None:
    bt, table = store
    table.predicate_matched = False

    await bt.save(a_record("case/1", seq=1))

    assert table.calls[0]["row_key"] == ENTITY_A.hex().encode() + b"#case/1"


# -- Scenario: Only the latest seq cell decides the predicate -----------------


async def test_the_predicate_is_scoped_to_the_latest_seq_cell(
    store: tuple[BigtableMemoryStore, _FakeTable],
) -> None:
    bt, table = store
    table.predicate_matched = False
    record = a_record("profile", seq=7)

    await bt.save(record)

    predicate = table.calls[0]["predicate"]
    assert isinstance(predicate, row_filters.RowFilterChain)
    chain = predicate.filters
    assert isinstance(chain[0], row_filters.FamilyNameRegexFilter)
    assert isinstance(chain[1], row_filters.ColumnQualifierRegexFilter)
    # Load-bearing: without the limit, a superseded older seq cell could
    # satisfy (or defeat) the range predicate on the row's behalf.
    assert isinstance(chain[2], row_filters.CellsColumnLimitFilter)
    assert chain[2].num_cells == 1
    # "Stored strictly greater than incoming": an exclusive lower bound at the
    # incoming seq, expressible only because the encoding is order-preserving.
    value_range = chain[3]
    assert isinstance(value_range, row_filters.ValueRangeFilter)
    assert value_range.start_value == _encode_seq(7)
    assert value_range.inclusive_start is False


# -- `_prefix_successor`: what makes the row-range scan exactly "this prefix" --


@pytest.mark.parametrize(
    ("prefix", "successor"),
    [
        (b"ab", b"ac"),
        (b"a\xff", b"b"),  # trailing 0xff carries into the previous byte
        (b"\xff\xff", None),  # no byte string is greater than every 0xff-run
        (b"", None),  # the empty prefix matches everything: no upper bound
    ],
)
def test_prefix_successor_pinned_examples(prefix: bytes, successor: bytes | None) -> None:
    assert _prefix_successor(prefix) == successor


@given(prefix=st.binary(max_size=8), suffix=st.binary(max_size=8))
def test_every_prefixed_key_sorts_inside_the_successor_bound(prefix: bytes, suffix: bytes) -> None:
    # The ordering contract the range scan relies on: every row key with this
    # prefix falls in [prefix, successor), and `None` means "no upper bound"
    # exactly when no upper bound exists (an all-0xff prefix).
    successor = _prefix_successor(prefix)
    prefixed = prefix + suffix

    assert prefixed >= prefix
    if successor is None:
        assert prefix == b"\xff" * len(prefix)
    else:
        assert prefixed < successor


# -- Scenario: Load returns the saved record or None --------------------------


async def test_load_queries_the_row_key_with_the_latest_cells_filter(
    store: tuple[BigtableMemoryStore, _FakeTable],
) -> None:
    bt, table = store
    table.rows = [_FakeRow(_rec_cell("profile"))]

    await bt.load(ENTITY_A, "profile")

    (query,) = table.read_rows_queries
    assert query.row_keys == [ENTITY_A.hex().encode() + b"#profile"]
    chain = query.filter
    assert isinstance(chain, row_filters.RowFilterChain)
    assert isinstance(chain.filters[0], row_filters.FamilyNameRegexFilter)
    # The same latest-cell discipline as the predicate: a superseded older
    # `rec` version must never be returned as the row's record.
    assert isinstance(chain.filters[1], row_filters.CellsColumnLimitFilter)
    assert chain.filters[1].num_cells == 1


async def test_load_decodes_the_rec_cell(
    store: tuple[BigtableMemoryStore, _FakeTable],
) -> None:
    bt, table = store
    record = a_record("profile", seq=7, value=b"v")
    table.rows = [
        _FakeRow(
            _FakeCell(BigtableMemoryStore.SEQ_COLUMN, _encode_seq(7)),
            _FakeCell(BigtableMemoryStore.RECORD_COLUMN, _encode_envelope(record)),
        )
    ]

    assert await bt.load(ENTITY_A, "profile") == record


async def test_load_returns_none_for_an_absent_row(
    store: tuple[BigtableMemoryStore, _FakeTable],
) -> None:
    bt, table = store
    table.rows = []

    assert await bt.load(ENTITY_A, "missing") is None


async def test_load_returns_none_for_a_row_without_a_rec_cell(
    store: tuple[BigtableMemoryStore, _FakeTable],
) -> None:
    # A row carrying only a seq cell has no envelope to decode; the cell walk
    # must exhaust and report absence rather than misread the seq bytes.
    bt, table = store
    table.rows = [_FakeRow(_FakeCell(BigtableMemoryStore.SEQ_COLUMN, _encode_seq(3)))]

    assert await bt.load(ENTITY_A, "profile") is None


# -- Scenario: Prefix search returns ordered, bounded, entity-scoped results --


async def test_search_shapes_the_entity_scoped_prefix_row_range(
    store: tuple[BigtableMemoryStore, _FakeTable],
) -> None:
    bt, table = store

    await bt.search(ENTITY_A, "case/", limit=2)

    (query,) = table.read_rows_queries
    start = ENTITY_A.hex().encode() + b"#case/"
    (row_range,) = query.row_ranges
    assert row_range.start_key == start
    # '/' + 1 == '0': the end key is the smallest key beyond every row the
    # prefix can own, which is what keeps entity B's rows out of the scan.
    assert row_range.end_key == _prefix_successor(start) == ENTITY_A.hex().encode() + b"#case0"
    assert query.limit == 2
    chain = query.filter
    assert isinstance(chain, row_filters.RowFilterChain)
    assert isinstance(chain.filters[1], row_filters.CellsColumnLimitFilter)


async def test_search_decodes_rows_in_range_order_and_skips_rec_less_rows(
    store: tuple[BigtableMemoryStore, _FakeTable],
) -> None:
    bt, table = store
    first = a_record("case/1", value=b"one")
    second = a_record("case/2", value=b"two")
    table.rows = [
        _FakeRow(_FakeCell(BigtableMemoryStore.RECORD_COLUMN, _encode_envelope(first))),
        _FakeRow(_FakeCell(BigtableMemoryStore.SEQ_COLUMN, _encode_seq(9))),  # no rec cell
        _FakeRow(_FakeCell(BigtableMemoryStore.RECORD_COLUMN, _encode_envelope(second))),
    ]

    results = await bt.search(ENTITY_A, "case/", limit=10)

    # Bigtable returns rows in key order; the store preserves it and a
    # rec-less row contributes nothing rather than a decode error.
    assert results == [first, second]


# -- Close releases the data client -------------------------------------------


async def test_close_closes_the_data_client(
    store: tuple[BigtableMemoryStore, _FakeTable],
) -> None:
    bt, _ = store
    client = bt._client
    assert isinstance(client, _FakeClient)

    await bt.close()

    assert client.closed
