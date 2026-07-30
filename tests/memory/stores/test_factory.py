"""The memory-store URI factory and `AgentConfig.longterm_memory` validation.

Covers "A URI-scheme factory builds stores with import-free validation": scheme
dispatch (`build_memory_store`), the import-free URI grammar
(`parse_memory_store_uri`), and the config surface that calls it at
construction time. Backend constructors are stubbed for the dispatch checks so
no client is needed — construction against real clients belongs to the
backend-specific and integration suites.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, ClassVar

import pytest

from beam_agents.core.transform import AgentConfig
from beam_agents.memory.stores import (
    InMemoryMemoryStore,
    build_memory_store,
    parse_memory_store_uri,
)
from tests.core._dofn_helpers import make_pong_provider

if TYPE_CHECKING:
    from collections.abc import Iterator

# The four client roots that must stay unimported by URI validation.
_CLIENT_ROOTS = ("google.cloud.bigtable", "google.cloud.firestore", "redis", "sqlalchemy")


class _RaisingBlocker:
    """Meta-path hook that makes any client import an immediate failure."""

    def find_spec(self, fullname: str, path: object = None, target: object = None) -> None:
        for blocked in _CLIENT_ROOTS:
            if fullname == blocked or fullname.startswith(blocked + "."):
                raise ImportError(f"client import attempted during validation: {fullname}")


@pytest.fixture
def no_client_imports() -> Iterator[None]:
    blocker = _RaisingBlocker()
    sys.meta_path.insert(0, blocker)
    try:
        yield
    finally:
        sys.meta_path.remove(blocker)


class _Stub:
    """Stands in for a backend store class; records its construction args."""

    instances: ClassVar[list[_Stub]] = []

    def __init__(self, *args: object) -> None:
        self.args = args
        type(self).instances.append(self)


# -- Scenario: Each scheme builds its store -----------------------------------


def test_the_memory_scheme_builds_the_in_memory_store(no_client_imports: None) -> None:
    scheme, parts = parse_memory_store_uri("memory://")
    assert isinstance(build_memory_store(scheme, parts), InMemoryMemoryStore)


@pytest.mark.parametrize(
    ("uri", "module", "cls", "expected_args"),
    [
        (
            "redis://localhost:6379/0",
            "beam_agents.memory.stores.redis",
            "RedisMemoryStore",
            ("redis://localhost:6379/0",),
        ),
        (
            "bigtable://proj/inst/table",
            "beam_agents.memory.stores.bigtable",
            "BigtableMemoryStore",
            ("proj", "inst", "table"),
        ),
        (
            "firestore://proj/coll",
            "beam_agents.memory.stores.firestore",
            "FirestoreMemoryStore",
            ("proj", "coll"),
        ),
        (
            "sqlite+aiosqlite:///:memory:",
            "beam_agents.memory.stores.sql",
            "SqlMemoryStore",
            ("sqlite+aiosqlite:///:memory:",),
        ),
        (
            "postgresql+asyncpg://u:p@host/db",
            "beam_agents.memory.stores.sql",
            "SqlMemoryStore",
            ("postgresql+asyncpg://u:p@host/db",),
        ),
    ],
)
def test_each_scheme_dispatches_to_its_store(
    monkeypatch: pytest.MonkeyPatch,
    no_client_imports: None,
    uri: str,
    module: str,
    cls: str,
    expected_args: tuple[str, ...],
) -> None:
    # Scenario: Each scheme builds its store — dispatch is checked against a
    # stub so the check needs no client; grammar validation runs for real and,
    # via the `no_client_imports` blocker, provably imports none of them.
    class Recorder(_Stub):
        instances: ClassVar[list[_Stub]] = []

    monkeypatch.setattr(f"{module}.{cls}", Recorder)
    scheme, parts = parse_memory_store_uri(uri)

    built = build_memory_store(scheme, parts)

    assert isinstance(built, Recorder)
    assert Recorder.instances[0].args == expected_args


# -- Scenario: A malformed URI fails at construction time ---------------------


@pytest.mark.parametrize(
    ("uri", "match"),
    [
        ("bigtable://proj/inst", r"bigtable://<project>/<instance>/<table>"),
        ("bigtable://proj/inst/table/extra", r"bigtable://<project>/<instance>/<table>"),
        ("bigtable://proj//table", r"bigtable://<project>/<instance>/<table>"),
        ("firestore://proj", r"firestore://<project>/<collection>"),
        ("firestore://proj/coll/extra", r"firestore://<project>/<collection>"),
        ("not-a-uri", r"longterm_memory"),
        ("://missing-scheme", r"longterm_memory"),
    ],
)
def test_a_malformed_uri_fails_at_construction_time(uri: str, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        parse_memory_store_uri(uri)


def test_the_bigtable_error_names_the_field_and_grammar() -> None:
    # Scenario: A malformed URI fails at construction time — the message names
    # the field and the expected grammar, before any pipeline exists.
    with pytest.raises(ValueError) as excinfo:
        parse_memory_store_uri("bigtable://proj/inst")

    message = str(excinfo.value)
    assert "longterm_memory" in message
    assert "bigtable://<project>/<instance>/<table>" in message


# -- AgentConfig.longterm_memory ----------------------------------------------


def test_agentconfig_validates_the_longterm_uri_without_client_imports(
    no_client_imports: None,
) -> None:
    for uri in (
        "memory://",
        "redis://localhost:6379",
        "bigtable://proj/inst/table",
        "firestore://proj/coll",
        "sqlite+aiosqlite:///tmp/x.db",
    ):
        AgentConfig(provider_factory=make_pong_provider, longterm_memory=uri)


def test_agentconfig_rejects_a_malformed_longterm_uri() -> None:
    with pytest.raises(ValueError, match="longterm_memory"):
        AgentConfig(provider_factory=make_pong_provider, longterm_memory="bigtable://proj/inst")


def test_agentconfig_defaults_to_no_longterm_store() -> None:
    config = AgentConfig(provider_factory=make_pong_provider)
    assert config.longterm_memory is None


def test_validation_does_not_import_store_client_modules() -> None:
    # Belt over the blocker's braces: after validating every scheme, none of
    # the client roots became importable *modules* of this process unless they
    # already were before the test ran.
    already = {root for root in _CLIENT_ROOTS if root in sys.modules}
    for uri in ("redis://h", "bigtable://p/i/t", "firestore://p/c"):
        parse_memory_store_uri(uri)
    now = {root for root in _CLIENT_ROOTS if root in sys.modules}
    assert now == already
