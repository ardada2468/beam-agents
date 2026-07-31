"""The console's SQLite schema and its migration ladder.

Kept separate from ``_store.py`` so the DDL is reviewable as a document rather
than as string literals threaded through connection handling, and so a schema
change is a diff with an obvious shape.

The store is a single WAL-mode file (design D6): it is the only option that
satisfies "one ``docker run`` and it works" without a second container, survives
a restart, and still supports the grouping and time-bucketing the UI needs.

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlite3

__all__ = [
    "SCHEMA_VERSION",
    "apply_schema",
    "schema_version_of",
]

# Bumped whenever `apply_schema` gains a step. The store records this in
# `PRAGMA user_version`, so an existing file can be brought forward without the
# operator being asked to delete it — a console is opened *because* something
# went wrong, and destroying the history at that moment is the wrong default.
SCHEMA_VERSION = 1


def apply_schema(connection: sqlite3.Connection) -> int:
    """Create or migrate the schema on ``connection``; return the resulting version.

    Idempotent: applying it to an already-current database is a no-op, so it is
    safe to call on every open. Applies exactly the migration steps between the
    file's recorded ``user_version`` and :data:`SCHEMA_VERSION`.
    """
    raise NotImplementedError


def schema_version_of(connection: sqlite3.Connection) -> int:
    """Return the schema version recorded in ``connection``'s ``user_version``."""
    raise NotImplementedError
