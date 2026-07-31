# Changelog fragments

One file per OpenSpec change, named after the change folder that backs it:

```
changelog.d/<openspec-change-name>.<type>.md
```

e.g. `changelog.d/add-adapter-conformance-matrix.added.md`. The file holds one
or two sentences in **user voice** — what someone installing the release can
now do, or must now do differently. Not "implemented `X` in `core/dofn.py`".

The type registry is **closed**. An unregistered type fails both the
`changelog-fragment-required` pre-commit hook and `make changelog`:

| type       | renders as         | version component                    |
| ---------- | ------------------ | ------------------------------------ |
| `breaking` | Breaking changes   | requires MINOR (name the migration)  |
| `added`    | Added              | requires MINOR                       |
| `changed`  | Changed            | requires MINOR                       |
| `fixed`    | Fixed              | PATCH-compatible                     |
| `docs`     | Documentation      | PATCH-compatible                     |
| `internal` | *not rendered*     | PATCH-compatible                     |

`internal` exists so a change with no user-observable effect (a refactor, a
test-only change) can satisfy the fragment requirement without inventing a
release note. It is deliberately not registered as a towncrier type, so
assembly drops it.

A commit touching `src/` with no fragment here is blocked by
`scripts/check_changelog_fragment.sh`; the escape hatch is
`BEAM_AGENTS_ALLOW_NO_FRAGMENT=1`, and reviewers will ask why.

`make changelog VERSION=X.Y.Z` assembles every pending fragment into a dated
`CHANGELOG.md` section and deletes the fragments it consumed, so each fragment
is published in exactly one release. `make changelog-draft` prints the pending
section and writes nothing.

Full policy: [`docs/releasing.md`](../docs/releasing.md).
