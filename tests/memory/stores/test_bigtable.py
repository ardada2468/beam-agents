"""Offline call-shape tests for the Bigtable `MemoryStore`.

Covers "The Bigtable store guards upserts with CheckAndMutateRow" without an
emulator: a fake data client records the single conditional mutation `save`
issues, so both predicate branches, the filter construction (including the
load-bearing `CellsColumnLimitFilter(1)` — "Only the latest seq cell decides
the predicate"), and the row-key/column layout are pinned as call shapes.
The same requirements run for real against the compose emulator in
`test_bigtable_emulator.py` under `-m integration`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

# google-cloud-bigtable rides into the unit lane via apache-beam[gcp]; if a
# trimmed environment lacks it, these call-shape tests skip and the emulator
# leg still covers the requirement.
row_filters = pytest.importorskip("google.cloud.bigtable.data.row_filters")

from beam_agents.memory.stores.base import _encode_envelope, _encode_seq  # noqa: E402
from beam_agents.memory.stores.bigtable import BigtableMemoryStore  # noqa: E402

from ._conformance import ENTITY_A, a_record  # noqa: E402


class _FakeTable:
    """Records `check_and_mutate_row` calls; scripted predicate outcome."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.predicate_matched = False

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


class _FakeClient:
    def __init__(self, *, project: str) -> None:
        self.project = project
        self.table = _FakeTable()

    def get_table(self, instance: str, table: str) -> _FakeTable:
        return self.table

    async def close(self) -> None:
        return None


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
