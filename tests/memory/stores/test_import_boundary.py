"""The memory-stores import boundary.

Covers the "The subpackage imports without any client library" scenario: every
`beam_agents.memory.stores` module imports with the Bigtable, Redis, Firestore,
and SQLAlchemy roots blocked, and only *constructing* a backend store raises —
with an error naming the `memory-stores` extra.

Like the effector's boundary test, the checks run in a subprocess with a
raising meta-path blocker (so a library that happens to be installed in this
environment is still unimportable) and stub parent packages standing in for
``beam_agents``/``beam_agents.memory`` — whose ``__init__``s import the Beam
-facing surface, a property of the package layout rather than of the stores.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import beam_agents

_PKG_DIR = Path(beam_agents.__file__).parent

# The four optional client roots the `memory-stores` extra owns. `aiosqlite` is
# blocked alongside sqlalchemy: it is the offline SQL leg's driver, not a core
# dependency.
_BLOCKED = (
    "google.cloud.bigtable",
    "google.cloud.firestore",
    "redis",
    "sqlalchemy",
    "aiosqlite",
)

_STORE_MODULES = (
    "beam_agents.memory.stores",
    "beam_agents.memory.stores.base",
    "beam_agents.memory.stores.bigtable",
    "beam_agents.memory.stores.redis",
    "beam_agents.memory.stores.firestore",
    "beam_agents.memory.stores.sql",
)

_PREAMBLE = textwrap.dedent(
    f"""
    import sys, types

    BLOCKED = {_BLOCKED!r}

    class _Blocker:
        # Raising (rather than returning None) makes the block absolute: a
        # client that is installed in this environment is still unimportable.
        def find_spec(self, fullname, path=None, target=None):
            for blocked in BLOCKED:
                if fullname == blocked or fullname.startswith(blocked + "."):
                    raise ImportError(f"blocked by the memory-stores boundary test: {{fullname}}")
            return None

    sys.meta_path.insert(0, _Blocker())

    # Stand in for beam_agents/__init__.py and beam_agents/memory/__init__.py,
    # which import the Beam-facing runtime surface; the stores subpackage's own
    # closure must not depend on evaluating either.
    _pkg = types.ModuleType("beam_agents")
    _pkg.__path__ = [{str(_PKG_DIR)!r}]
    sys.modules["beam_agents"] = _pkg
    _mem = types.ModuleType("beam_agents.memory")
    _mem.__path__ = [{str(_PKG_DIR / "memory")!r}]
    sys.modules["beam_agents.memory"] = _mem
    """
)


def _run_with_blocked_imports(body: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", _PREAMBLE + textwrap.dedent(body)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def test_every_store_module_imports_with_all_clients_blocked() -> None:
    # Scenario: The subpackage imports without any client library.
    proc = _run_with_blocked_imports(
        f"""
        import importlib
        for module in {_STORE_MODULES!r}:
            importlib.import_module(module)
        print("imported-ok")
        """
    )
    assert proc.returncode == 0, proc.stderr
    assert "imported-ok" in proc.stdout


def test_constructing_each_backend_names_the_memory_stores_extra() -> None:
    # Scenario: The subpackage imports without any client library — the
    # constructor half: only construction raises, and the error is actionable.
    proc = _run_with_blocked_imports(
        """
        from beam_agents.memory.stores.bigtable import BigtableMemoryStore
        from beam_agents.memory.stores.firestore import FirestoreMemoryStore
        from beam_agents.memory.stores.redis import RedisMemoryStore
        from beam_agents.memory.stores.sql import SqlMemoryStore

        attempts = (
            lambda: BigtableMemoryStore("proj", "inst", "table"),
            lambda: RedisMemoryStore("redis://localhost:6379"),
            lambda: FirestoreMemoryStore("proj", "coll"),
            lambda: SqlMemoryStore("sqlite+aiosqlite:///:memory:"),
        )
        for attempt in attempts:
            try:
                attempt()
            except ImportError as exc:
                assert "memory-stores" in str(exc), str(exc)
            else:
                raise AssertionError("constructor succeeded with its client blocked")
        print("constructors-refused")
        """
    )
    assert proc.returncode == 0, proc.stderr
    assert "constructors-refused" in proc.stdout


def test_the_inmemory_store_and_factory_work_with_all_clients_blocked() -> None:
    # The ABC and in-memory store must be *usable*, not merely importable,
    # without any client installed.
    proc = _run_with_blocked_imports(
        """
        import asyncio
        from beam_agents.memory.stores import (
            InMemoryMemoryStore,
            MemoryRecord,
            build_memory_store,
            parse_memory_store_uri,
        )

        scheme, parts = parse_memory_store_uri("memory://")
        store = build_memory_store(scheme, parts)
        assert isinstance(store, InMemoryMemoryStore)

        async def roundtrip() -> None:
            record = MemoryRecord(
                entity_key=b"e", key="k", value=b"v", seq=1, updated_at_ms=2
            )
            assert await store.save(record)
            assert await store.load(b"e", "k") == record

        asyncio.run(roundtrip())
        print("inmemory-ok")
        """
    )
    assert proc.returncode == 0, proc.stderr
    assert "inmemory-ok" in proc.stdout
