"""Tests for the human-in-the-loop policy surface and the effector guard.

Covers the `human-in-the-loop` scenarios that are pure functions of their
inputs: the layer-2 expiry guard, the routing policy's shape and validation,
and the picklability the DirectRunner requires. The pipeline-side scenarios
(timer dispatch, resume admission) live with the DoFn tests.
"""

from __future__ import annotations

import ast
import dataclasses
import pickle
from pathlib import Path

import pytest

import beam_agents
from beam_agents._protos import ToolIntent, ToolResult
from beam_agents.core.agent import FallbackContext
from beam_agents.hitl import (
    DEFAULT_APPROVAL_CHANNEL,
    HITL_TIMEOUT_OUTPUT,
    Deny,
    Drop,
    Escalate,
    HitlPolicy,
    deny,
    intent_expired,
    refuse_expired,
)

_NOW = 1_700_000_000_000


def _intent(expires_at_ms: int) -> ToolIntent:
    return ToolIntent(
        intent_id="11111111-2222-5333-8444-555555555555",
        entity_key=b"entity-1",
        seq=7,
        step_index=2,
        tool_name="approval",
        args_json="{}",
        created_at_ms=_NOW,
        expires_at_ms=expires_at_ms,
        kind=ToolIntent.APPROVAL,
    )


# --- Requirement: The effector guard refuses expired intents -------------------


def test_expired_intent_is_refused_with_a_correlated_expired_result() -> None:
    # Scenario: An expired intent is refused, not executed.
    intent = _intent(expires_at_ms=_NOW - 1)

    assert intent_expired(intent, _NOW) is True
    result = refuse_expired(intent, _NOW)

    assert result is not None
    assert result.status == ToolResult.EXPIRED
    assert result.intent_id == intent.intent_id
    assert result.entity_key == intent.entity_key
    assert result.seq == intent.seq
    assert result.completed_at_ms == _NOW


def test_intent_expiring_exactly_now_is_expired() -> None:
    # The boundary is inclusive: an intent is live strictly before its expiry.
    intent = _intent(expires_at_ms=_NOW)
    assert intent_expired(intent, _NOW) is True
    assert refuse_expired(intent, _NOW) is not None


def test_unexpired_intent_passes_the_guard() -> None:
    # Scenario: An unexpired intent passes the guard.
    intent = _intent(expires_at_ms=_NOW + 1)
    assert intent_expired(intent, _NOW) is False
    assert refuse_expired(intent, _NOW) is None


@pytest.mark.parametrize("expires_at_ms", [0, -1])
def test_non_positive_expiry_is_treated_as_expired(expires_at_ms: int) -> None:
    # Scenario: A zero expiry is treated as expired (never as unbounded).
    intent = _intent(expires_at_ms=expires_at_ms)
    assert intent_expired(intent, _NOW) is True
    assert intent_expired(intent, 0) is True


def _module_path(name: str) -> Path:
    """Source file for a `beam_agents.*` module (package or plain module)."""
    root = Path(beam_agents.__file__).parent
    parts = name.split(".")[1:]
    path = root.joinpath(*parts).with_suffix(".py")
    return path if path.exists() else root.joinpath(*parts, "__init__.py")


def _runtime_imports(name: str) -> list[str]:
    """Modules `name` imports at runtime, skipping `if TYPE_CHECKING:` blocks.

    A type-only import never executes, so it cannot drag a dependency into the
    effector's process -- which is exactly why those guards exist.
    """
    imported: list[str] = []

    def visit(node: ast.AST) -> None:
        if (
            isinstance(node, ast.If)
            and isinstance(node.test, ast.Name)
            and node.test.id == "TYPE_CHECKING"
        ):
            for orelse_child in node.orelse:
                visit(orelse_child)
            return
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
        for child in ast.iter_child_nodes(node):
            visit(child)

    visit(ast.parse(_module_path(name).read_text()))
    return imported


def _first_party_import_closure(module: str) -> set[str]:
    """Every `beam_agents.*` module reachable from `module` at runtime."""
    seen: set[str] = set()
    pending = [module]
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        seen.add(name)
        pending += [n for n in _runtime_imports(name) if n.startswith("beam_agents.")]
    return seen


def test_hitl_import_closure_is_beam_free() -> None:
    # The guard runs inside the effector -- a separate service that has no
    # reason to depend on Beam -- so nothing `hitl.py` reaches may import it.
    # Checked statically over the first-party closure rather than by importing:
    # importing any submodule also runs `beam_agents/__init__.py`, which pulls
    # in `RunAgent` and therefore Beam. That is a property of the package
    # entry point, not of this module's dependencies.
    for name in sorted(_first_party_import_closure("beam_agents.hitl")):
        offenders = [m for m in _runtime_imports(name) if m.startswith("apache_beam")]
        assert offenders == [], f"{name} imports {offenders}; hitl's closure must stay Beam-free"


# --- Requirement: HitlPolicy routes a timeout to deny, drop, or escalate -------


def _fallback() -> FallbackContext:
    return FallbackContext(
        entity_key=b"entity-1",
        seq=7,
        snapshot=b"snap",
        kind="timer",
        deadline_ms=_NOW,
        fired_at_ms=_NOW,
        pending_intent_ids=("11111111-2222-5333-8444-555555555555",),
    )


def test_default_policy_denies_with_the_runtime_timeout_output() -> None:
    # Scenario: The default policy preserves existing behavior.
    policy = HitlPolicy()
    route = policy.on_timeout(_fallback())

    assert route == Deny(HITL_TIMEOUT_OUTPUT)
    assert deny(_fallback()) == Deny(HITL_TIMEOUT_OUTPUT)
    assert policy.approval_channel == DEFAULT_APPROVAL_CHANNEL
    assert policy.max_escalations == 0


def test_routes_are_frozen_values() -> None:
    # A route crosses from user code into a timer callback that may re-execute;
    # it must be a value, not something the runtime can be talked into changing.
    # Checked reflectively: assigning to a frozen field is a *static* error, so
    # a direct assignment here would fail type-checking rather than run.
    for route in (Deny(b"x"), Drop("hitl_timeout"), Escalate(tool_name="pager")):
        assert dataclasses.is_dataclass(route)
        params = getattr(type(route), "__dataclass_params__")  # noqa: B009
        assert params.frozen is True
        field_name = dataclasses.fields(route)[0].name
        with pytest.raises((AttributeError, TypeError)):
            setattr(route, field_name, "mutated")


def test_policy_and_default_route_pickle() -> None:
    # The DoFn holds the policy and must serialize for the DirectRunner.
    policy = HitlPolicy()
    restored = pickle.loads(pickle.dumps(policy))
    assert restored.on_timeout(_fallback()) == Deny(HITL_TIMEOUT_OUTPUT)
    assert pickle.loads(pickle.dumps(Escalate(tool_name="pager"))) == Escalate(tool_name="pager")


# --- Requirement: HitlPolicy is validated at pipeline-construction time --------


@pytest.mark.parametrize(
    ("kwargs", "field_name"),
    [
        ({"timeout_ms": 0}, "timeout_ms"),
        ({"timeout_ms": -1}, "timeout_ms"),
        ({"intent_ttl_ms": 0}, "intent_ttl_ms"),
        ({"intent_ttl_ms": -1}, "intent_ttl_ms"),
        ({"max_escalations": -1}, "max_escalations"),
        ({"approval_channel": ""}, "approval_channel"),
    ],
)
def test_invalid_policy_fields_are_rejected_by_name(
    kwargs: dict[str, object], field_name: str
) -> None:
    # Scenarios: A non-positive timeout / an empty approval channel is rejected.
    with pytest.raises(ValueError, match=field_name):
        HitlPolicy(**kwargs)  # type: ignore[arg-type]
