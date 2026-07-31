"""The executable half of the deprecation policy: ``beam_agents._deprecation``.

CONTRIBUTING.md defines the window (at least one minor release in which the old
name still works and warns). What CI can check is that the mechanism behind it
works: a name inside its window resolves, warns exactly once per access, and the
warning tells a reader both what to move to and when the name may vanish. A
warning that says only "deprecated" leaves the user with no migration to make,
which is the failure mode this test exists to prevent.

The helper is driven through a fixture module's ``__getattr__`` -- the PEP 562
shape ``beam_agents/__init__.py`` already uses for lazy adapter resolution --
because that is how the policy says a deprecated name is served: out of the
module's living namespace, so it neither shows up in ``dir()`` nor lands back in
the frozen surface snapshot.
"""

from __future__ import annotations

import types
import warnings

import pytest

from beam_agents._deprecation import deprecated_attribute

_KEPT = "the replacement object"


def _fixture_module() -> types.ModuleType:
    """A module serving one deprecated alias through ``__getattr__``.

    ``kept`` is a live name in the module namespace; ``old_name`` exists only
    inside its deprecation window and resolves to the same object.
    """
    module = types.ModuleType("beam_agents_fixture")
    module.kept = _KEPT  # type: ignore[attr-defined]

    def __getattr__(name: str) -> object:
        if name == "old_name":
            return deprecated_attribute(
                name,
                replacement="beam_agents_fixture.kept",
                removed_in="0.7.0",
                value=_KEPT,
                module=module.__name__,
            )
        raise AttributeError(f"module {module.__name__!r} has no attribute {name!r}")

    # The PEP 562 module `__getattr__`, installed dynamically because the
    # fixture module is built at runtime; mypy reads it as assigning a method.
    module.__getattr__ = __getattr__  # type: ignore[method-assign]
    return module


def test_a_deprecated_name_warns_and_still_works() -> None:
    module = _fixture_module()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        value = module.old_name

    assert value == "the replacement object"
    assert len(caught) == 1, "one access must emit exactly one DeprecationWarning"
    assert issubclass(caught[0].category, DeprecationWarning)
    message = str(caught[0].message)
    assert "beam_agents_fixture.old_name" in message
    assert "beam_agents_fixture.kept" in message, "the warning must name the replacement"
    assert "0.7.0" in message, "the warning must name the first release that may remove it"


def test_a_live_name_is_not_warned() -> None:
    module = _fixture_module()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert module.kept == "the replacement object"
    assert caught == []


def test_a_name_with_no_replacement_says_so() -> None:
    # Removal without a successor is legal; the warning must still tell the
    # reader that, rather than leaving them hunting for a rename that does not
    # exist.
    with pytest.warns(DeprecationWarning) as caught:
        deprecated_attribute(
            "gone", replacement=None, removed_in="1.1.0", value=None, module="beam_agents.demo"
        )
    message = str(caught[0].message)
    assert "no replacement" in message
    assert "1.1.0" in message


def test_an_unknown_attribute_still_raises_attribute_error() -> None:
    module = _fixture_module()
    with pytest.raises(AttributeError, match="has no attribute 'never_existed'"):
        _ = module.never_existed
