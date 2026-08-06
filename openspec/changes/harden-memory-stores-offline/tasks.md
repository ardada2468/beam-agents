# Tasks: harden-memory-stores-offline

**The standing rule:** every test is derived from a `memory-stores` spec requirement or scenario
and names it; no fake reimplements a backend's guard semantics (design D2); the emulator
conformance suite is not touched. If a test surfaces a behavioral defect in a store, it is filed
as its own change, not silently fixed here.

## 1. Baseline

- [x] 1.1 Record the offline branch-coverage baseline for the three backends from
  `uv run pytest tests/memory -q -m "not integration and not semantics and not dataflow and not smoke" --cov=beam_agents.memory --cov-branch`:
  expected bigtable 0/14, firestore 0/8, redis 0/10 (the M2 note's gap on the current tree). —
  Confirmed exactly: bigtable **0/14**, firestore **0/8**, redis **0/10** covered branches
  (statement coverage 39%/28%/26%); 117 offline tests before this change.

## 2. Firestore offline suite (`tests/memory/stores/test_firestore.py`)

- [x] 2.1 Client-fake seam: a fixture injecting a typed fake `google.cloud.firestore` module (and
  `google.cloud.firestore_v1.base_query`) into `sys.modules`, patching the `google.cloud`
  attribute too, so `FirestoreMemoryStore` constructs against the fake in every lane (design D1).
  — `_install_fake_firestore` + the `store` fixture; verified against a meta-path blocker that
  deletes and blocks the real client modules (the trimmed-lane simulation): 35 redis+firestore
  tests pass with `redis` and `google.cloud.firestore*` unimportable.
- [x] 2.2 Document-ID encoding invariants (spec: "A hierarchical key round-trips"; the D-3 class):
  `/`, `#`, `%`, and unicode keys produce IDs with no `/`, immune to reserved forms, injective
  (hypothesis), round-tripping; the `key` FIELD stays verbatim in the save payload. — Five tests:
  the pinned `case/2 → hex#case%2F2` D-3 shape, a six-key round-trip parametrize, two hypothesis
  properties (no `/`/no reserved form over arbitrary keys; injectivity over distinct key pairs and
  both entities), and cross-entity distinctness; the verbatim `key` field is asserted in the
  whole-payload save test.
- [x] 2.3 The transactional guard's client-side branches (spec: "Transactional guard under the
  conformance suite"; "Save is an idempotent upsert guarded by seq"): absent doc applies, stored
  newer seq refuses with no `set`, equal seq applies; the read runs inside the transaction. —
  Four tests, including `test_the_guarded_read_runs_inside_the_transaction` asserting the `get`
  received the transaction object (the atomicity precondition the fake can honestly check).
- [x] 2.4 Load paths (spec: "Load returns the saved record or None"): absent snapshot is `None`,
  present snapshot decodes the envelope from the `rec` field via the encoded document ID. — Two
  tests; the present-path key round-trips as `case/2`, never an encoded form.
- [x] 2.5 Search request shaping (spec: "Prefix search is unaffected by document-ID encoding";
  "Search is a bounded per-entity key-prefix scan"): entity `==` filter on the hex, `key >= prefix`
  and `key < prefix + "￿"` bounds, `order_by(key)`, `limit`, decoded results in stream order. —
  Two tests (populated prefix query; the empty-prefix clause still scoped and bounded).
- [x] 2.6 `close` handles both the sync and coroutine client variants. — Both variants, via a
  sync-`close` fake client subclass.

## 3. Redis offline suite (`tests/memory/stores/test_redis.py`)

- [x] 3.1 Client-fake seam: a fixture injecting a typed fake `redis` module (with an `asyncio`
  namespace and recording `from_url` client) into `sys.modules` (design D1). —
  `_install_fake_redis` + the `store` fixture; covered by the same trimmed-lane blocker run as 2.1.
- [x] 3.2 Save call shape (spec: "The Redis store guards upserts with a server-side script"): the
  script is registered once, invoked with the prefixed `hex(entity_key)` hash key and the argument
  vector `[key, 8-byte big-endian seq, envelope bytes]`; the 1/0 reply maps to applied/not-applied.
  — Three tests; the applied verdict is asserted to be the backend's reply, never a client-side
  re-check.
- [x] 3.3 Load paths: absent field is `None`; a present framed value decodes the envelope from
  byte 8 on (the seq-prefix framing the requirement pins). — Two tests against the `_frame` layout
  helper (8-byte seq + envelope).
- [x] 3.4 Search assembly (spec: "Prefix search returns ordered, bounded, entity-scoped results";
  "Prefix metacharacters are literal"): `HSCAN` match is the escaped literal glob (`? * [ ] \`
  escaped, `*` appended), the client-side startswith belt drops a glob-only match, results are
  sorted by key and truncated to `limit`, unicode field names decode as UTF-8. — Four tests: a
  six-case escaping parametrize, the literal match forwarded to `hscan_iter`, the
  sort/limit/belt assembly over unordered scan output with a non-startswith artifact, and the
  UTF-8 field decode.
- [x] 3.5 `close` closes the client pool (`aclose`). — Done.

## 4. Bigtable offline extension (`tests/memory/stores/test_bigtable.py`)

- [x] 4.1 `_prefix_successor` contract (spec: "search SHALL be a row-range prefix scan"): pinned
  examples (carry, all-`0xff` → `None`) plus the hypothesis ordering property (design D3). — Four
  pinned examples (including the empty prefix meaning "no upper bound") and the property: every
  `prefix + suffix` sorts in `[prefix, successor)`, with `None` exactly for all-`0xff` prefixes.
- [x] 4.2 Load paths (spec: "Load returns the saved record or None"): the read query names the row
  key and the latest-cells filter; a row's `rec` cell decodes; a row without a `rec` cell and an
  empty read both return `None`. — Four tests; the latest-cells discipline (`
  CellsColumnLimitFilter(1)`) is asserted on the read path too, not just the predicate.
- [x] 4.3 Search request shaping (spec: "Prefix search returns ordered, bounded, entity-scoped
  results"): row range `[row_key(entity, prefix), successor)` (and unbounded when the successor is
  `None`), `limit` forwarded, latest-cells filter applied, rows decoded and `rec`-less rows
  skipped. — Two tests; the pinned end key `hex#case0` documents the `'/'+1 == '0'` bound that
  keeps other entities out. The unbounded arm is unreachable through `_search` (row keys always
  contain hex and `#`, never all-`0xff`), so it is covered where it is reachable: the
  `_prefix_successor` unit/property tests of 4.1.
- [x] 4.4 `close` closes the data client. — Done (the fake client records closure).

## 5. Gates

- [x] 5.1 `uv run pytest tests/memory -q -m "not integration and not semantics and not dataflow and not smoke"`
  green; the integration-marked suites still deselect cleanly. — **164 passed, 33 deselected**
  (117 → 164: +20 firestore, +15 redis, +12 bigtable); the 33 integration tests still collect
  under `-m integration` and stay out of the offline selection.
- [x] 5.2 `uv run ruff check src/beam_agents/memory tests/memory` and
  `uv run ruff format --check` over the touched files clean. — "All checks passed!"; the three
  files are format-clean.
- [x] 5.3 `uv run mypy src/beam_agents/memory` (strict) clean, and the new test files clean under
  the repo-wide `uv run mypy` selection. — `mypy src/beam_agents/memory tests/memory`: "Success:
  no issues found in 32 source files". The fixtures widen `_client` to `object` before narrowing
  to the fake so the file typechecks both with and without the real client installed (the same
  two-environment constraint `stores/firestore.py`'s own `close` documents).
- [x] 5.4 Re-measure and record per-module branch coverage movement for the three backends
  (before → after), same command as 1.1. — bigtable **0/14 → 14/14**, firestore **0/8 → 8/8**,
  redis **0/10 → 10/10** covered branches; statement coverage 39%/28%/26% → 97%/97%/96% (the
  only remaining misses are each constructor's `except ImportError` raise, which
  `test_import_boundary.py` exercises out-of-process). `beam_agents.memory` as a whole:
  82.49% → **97.73%** in the offline selection.
- [x] 5.5 Changelog fragment `changelog.d/harden-memory-stores-offline.internal.md` (test-only, so
  `internal`, per `changelog.d/README.md`). — Written.
- [x] 5.6 `openspec validate harden-memory-stores-offline --strict` passes. — Valid.
