"""The `state_schema_version` regime for keyed state: one constant, one registry.

Three keyed-state messages carry a ``state_schema_version`` and live in
``ReadModifyWriteState`` — :data:`VERSIONED_MESSAGE_TYPES` (``MemoryBlob``,
``Continuation``, ``LlmCacheBlob``). This module owns:

- :data:`CURRENT_STATE_SCHEMA_VERSION`, the one authoritative version every
  runtime writer stamps (``Memory.to_blob``, ``ReplayCache.to_blob``,
  ``ActivationContext.build_continuation``) — a bump is one edit here, in the
  same module the completeness tests interrogate, so the constant and the gate
  cannot drift apart;
- a registry of single-step migrations keyed ``(message type, from_version)``,
  each taking a version-``n`` message and returning a version-``n + 1`` one,
  registered via :func:`migration` at this module's import only (empty at
  version 1 — the first breaking change registers the first steps);
- :func:`migrate_to_current`, the lazy hook ``_AgentDoFn`` applies at every
  keyed-state read before interpreting any field. Current-version messages
  take an identity fast path (one integer compare, no copy); older ones walk
  the chain one step at a time; a version from the future raises
  :class:`StateSchemaFromFutureError`, which the DoFn deliberately does not
  catch — the bundle fails with zero state mutation and the key wedges until
  the binary is rolled forward (design D4 of add-state-schema-migration).

Migration functions MUST be pure and deterministic — no clocks, randomness, or
I/O. They run inside ``process()`` on the element path, and a replayed bundle
must produce the same migrated view (feeding the same cache keys and intent
IDs) as the original attempt, or retry determinism breaks.

Migration operates on *decoded* messages, so old bytes must always parse under
the current descriptor first: an existing field number is never retyped or
reused, at any version — no bump can buy that back. The full evolution policy
and bump checklist live in ``docs/state-migration.md``.

Importing this module has no side effects beyond populating its own registry.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final, TypeVar, cast, overload

from beam_agents._protos import Continuation, LlmCacheBlob, MemoryBlob

if TYPE_CHECKING:
    from collections.abc import Callable

    from google.protobuf.message import Message

__all__ = [
    "CURRENT_STATE_SCHEMA_VERSION",
    "VERSIONED_MESSAGE_TYPES",
    "MigrationStepError",
    "MissingMigrationError",
    "StateMigrationError",
    "StateSchemaFromFutureError",
    "migrate_to_current",
    "migration",
]

# The one authoritative schema version for keyed state. Bumping it is the
# gated event: CI's completeness tests fail until a migration step exists for
# every versioned message at every version below it and the golden corpus has
# a frozen directory for the outgoing version plus fixtures for the new one.
CURRENT_STATE_SCHEMA_VERSION: Final[int] = 1

# A parsed version of 0 is the pre-versioned baseline: proto3 zero-defaults
# mean a default-constructed blob reads 0, and the runtime has only ever
# written 1, so baseline semantics are the only semantics it can have.
_BASELINE_VERSION: Final[int] = 1

# The versioned set is exactly the three state blobs (design D1): the messages
# that carry `state_schema_version` and are read exclusively by the runtime
# from ReadModifyWriteState. Wire messages (ToolIntent, ToolResult, TraceEvent,
# AgentEnvelope, ActivationErrorRecord) cross service boundaries a lazy
# in-pipeline migration cannot reach; they stay additive-only forever.
VERSIONED_MESSAGE_TYPES: Final[tuple[type[Message], ...]] = (
    MemoryBlob,
    Continuation,
    LlmCacheBlob,
)

# The three messages `migrate_to_current` accepts. Constrained (not bound) so
# a call site keeps its concrete blob type through the hook.
_M = TypeVar("_M", MemoryBlob, Continuation, LlmCacheBlob)


class StateMigrationError(Exception):
    """Base for every failure of the state-schema-version regime."""


class MissingMigrationError(StateMigrationError):
    """A migration chain has a gap: no step registered at ``from_version``.

    Raised mid-walk with no partially-migrated value returned. Reaching this
    means a ``CURRENT_STATE_SCHEMA_VERSION`` bump shipped without its steps —
    exactly what the completeness tests exist to catch before merge.
    """

    def __init__(
        self, message_type: type[Message], from_version: int, current_version: int
    ) -> None:
        self.message_type = message_type
        self.from_version = from_version
        self.current_version = current_version
        super().__init__(
            f"no migration registered for {message_type.__name__} at version "
            f"{from_version} (current version is {current_version}); every step in "
            f"1..{current_version - 1} must be registered in core/migration.py"
        )


class StateSchemaFromFutureError(StateMigrationError):
    """A stored blob's version exceeds this binary's current version.

    A newer binary already ran on this key (e.g. ``--update`` forward, then a
    rollback). Interpreting the blob under older semantics would corrupt state
    and dead-lettering the element would silently lose it, so the DoFn lets
    this fail the bundle: the key wedges, loudly, until the binary is rolled
    forward — at which point the retry succeeds because nothing was mutated.
    """

    def __init__(
        self, message_type: type[Message], found_version: int, current_version: int
    ) -> None:
        self.message_type = message_type
        self.found_version = found_version
        self.current_version = current_version
        super().__init__(
            f"{message_type.__name__} state_schema_version={found_version} exceeds this "
            f"binary's CURRENT_STATE_SCHEMA_VERSION={current_version}; refusing to "
            f"interpret newer state — roll the binary forward, do not roll back"
        )


class MigrationStepError(StateMigrationError):
    """A registered step returned a message whose stamp did not advance by one."""

    def __init__(
        self, message_type: type[Message], from_version: int, produced_version: int
    ) -> None:
        self.message_type = message_type
        self.from_version = from_version
        self.produced_version = produced_version
        super().__init__(
            f"migration step for {message_type.__name__} at version {from_version} "
            f"returned state_schema_version={produced_version}, expected {from_version + 1}"
        )


# (message type, from_version) -> single-step migration producing from_version
# + 1. Populated by the `migration` decorator at this module's import, then
# only read — the registry is not a runtime extension point.
_REGISTRY: dict[tuple[type[Message], int], Callable[[Message], Message]] = {}


def migration(
    message_type: type[_M], *, from_version: int
) -> Callable[[Callable[[_M], _M]], Callable[[_M], _M]]:
    """Register a pure single-step migration for ``message_type``.

    The decorated function takes a version-``from_version`` message and returns
    a **new** message reading ``from_version + 1``. One step per
    ``(type, from_version)``: a duplicate registration raises rather than
    silently letting two modules disagree about a version's meaning.
    """
    if from_version < _BASELINE_VERSION:
        raise ValueError(
            f"from_version must be >= {_BASELINE_VERSION}; got {from_version!r} "
            f"for {message_type.__name__}"
        )

    def register(step: Callable[[_M], _M]) -> Callable[[_M], _M]:
        key = (message_type, from_version)
        if key in _REGISTRY:
            raise ValueError(
                f"a migration for {message_type.__name__} at version {from_version} "
                f"is already registered"
            )
        _REGISTRY[key] = cast("Callable[[Message], Message]", step)
        return step

    return register


@overload
def migrate_to_current(message: None) -> None: ...
@overload
def migrate_to_current(message: _M) -> _M: ...
def migrate_to_current(message: _M | None) -> _M | None:
    """Upgrade a keyed-state blob to ``CURRENT_STATE_SCHEMA_VERSION``, lazily.

    ``None`` (absent state) passes through untouched; so does a blob already at
    the current version — the identical object, no copy, no step consulted.
    Version ``0`` normalizes to the baseline ``1``. Anything older than current
    walks the registered chain one step at a time, verifying each step advanced
    the stamp; anything newer raises :class:`StateSchemaFromFutureError`.

    Pure with respect to state: migrating writes nothing — the migrated value
    reaches durable state only through the next successful commit's writes,
    which stamp the current version.
    """
    if message is None:
        return None
    current = CURRENT_STATE_SCHEMA_VERSION
    version = message.state_schema_version or _BASELINE_VERSION
    if version == current:
        return message
    if version > current:
        raise StateSchemaFromFutureError(type(message), version, current)

    migrated = message
    while version < current:
        step = _REGISTRY.get((type(message), version))
        if step is None:
            raise MissingMigrationError(type(message), version, current)
        migrated = cast("_M", step(migrated))
        if migrated.state_schema_version != version + 1:
            raise MigrationStepError(type(message), version, migrated.state_schema_version)
        version += 1
    return migrated
