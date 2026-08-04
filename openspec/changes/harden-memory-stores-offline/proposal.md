# Proposal: harden-memory-stores-offline

## Why

The offline lane is blind to three of the four `MemoryStore` backends. `coverage-baseline.toml`'s
M2 entry records it explicitly: `memory/stores/{redis,bigtable,firestore}.py` sit at **0.00 branch
coverage** in the lane the ratchet measures, because every branch they own is inside a live-client
call path and their only behavioral tests are the `integration`-marked emulator conformance suites.
Measured on this tree (`tests/memory`, offline selection): bigtable **0/14** branches, firestore
**0/8**, redis **0/10** — the 16-branch M2 gap plus the branches added since.

That blindness has already cost a real defect. D-3 (`verification-report.md`, fixed by
`fix-firestore-store-key-encoding`): the Firestore store put the record key verbatim into the
document ID, so any hierarchical key — `case/2`, which the shared conformance suite itself uses —
was rejected as an invalid document path. The bug was pure client-side logic (an encoding decision,
no I/O required to observe it), yet it stayed invisible until the first-ever emulator run, months
after the code merged, because nothing offline ever looked at the document ID the store constructs.

The gap is structural, not accidental: the `ci` and `quality` lanes sync only
`lint`/`typecheck`/`test` groups, which do not install the Redis or Firestore clients (Bigtable
rides in via `apache-beam[gcp]`). So an offline test for these backends cannot lean on the real
client library at all — it has to fake the client at the import seam.

## What Changes

- **Offline unit tests for the three live-client backends**, driving their client-side logic —
  key/document-ID encoding, request shaping, response decoding, error/`applied` mapping, close
  semantics — through faked clients injected at the lazy-import seam (`sys.modules`), so they run
  and count in every lane, including the trimmed `ci`/`quality` environments:
  - **Firestore**: document-ID encoding invariants that would have caught D-3 the day it was
    written (no `/` in any ID for any key, injectivity, round-trip, reserved-form immunity, the
    `key` field verbatim), the transactional guard's compare branches, search request shaping
    (entity scoping, prefix range, order, limit), and both `close` variants.
  - **Redis**: hash-key/entity scoping, the framed seq-prefix value layout on save and its slicing
    on load, literal-glob escaping for `HSCAN`, the client-side startswith belt, ordering and limit
    assembly, and `aclose`.
  - **Bigtable** (extending the existing offline call-shape file): the read paths the current tests
    never touch — `_load`/`_search` query shaping, row-range construction from `_prefix_successor`
    (with a property test for the successor's ordering contract), record-cell extraction including
    the absent-cell path — and `close`.
- **No weakening or duplication of the emulator conformance suite.** The shared
  `MemoryStoreConformance` suite stays the interchangeability authority; the fakes never
  reimplement a backend's guard semantics (no fake Lua, no fake `CheckAndMutateRow` predicate
  evaluation, no fake transaction contention). The offline tests pin what the store *sends* and how
  it decodes what it *receives* — exactly the layer where D-3 lived.
- **No `src/` change expected.** The logic under test is already reachable: the pure helpers
  (`_literal_glob_prefix`, `_prefix_successor`, `_doc_id`) are module-level or static, and the
  client seam is the constructors' lazy import. If a test reveals an actual behavioral defect,
  that is filed rather than silently fixed here.

## Capabilities

### Modified Capabilities

- `memory-stores`: one ADDED requirement — backend client-side logic (encoding, request shaping,
  decoding, teardown) is exercised offline through faked clients, with the emulator conformance
  suite staying the sole interchangeability authority. No existing requirement changes: runtime
  behavior is exactly as specified today, and every new test names the requirement or scenario it
  is derived from.

## Impact

- **Modified:** `tests/memory/stores/` (two new offline suites, one extended), `changelog.d/`
  (internal fragment).
- **Coverage:** the three backends' offline branch coverage moves from 0.00 to measured-and-gated;
  the project ratchet can rise but `coverage-baseline.toml` is not touched here (it ratchets
  against CI-measured runs).
- **Gates:** offline `tests/memory` selection, `ruff`, `mypy --strict`, and
  `openspec validate harden-memory-stores-offline --strict` all green; integration-marked suites
  unchanged and still deselect cleanly offline.
- **Not in scope:** folding the integration lane into the ratcheted coverage.xml (a workflow
  change, tracked by the baseline's own notes), compaction's two residual partial branches (covered
  by suites outside `tests/memory`), and any Postgres conformance leg.
