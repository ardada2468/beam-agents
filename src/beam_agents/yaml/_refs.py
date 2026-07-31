"""``module:object`` reference parsing and resolution for the YAML surface.

A Beam YAML document can carry strings, numbers, booleans, and mappings — not
callables. Agents, provider factories, decoders, HITL routes, and tool
registries are all Python objects, so the YAML config *names* them with the
setuptools entry-point spelling: an importable module path, a colon, then a
dotted attribute path (``my_pkg.agents:fraud_agent``,
``my_pkg.agents:triage.agent``).

Resolution happens when the transform constructor runs — Beam YAML expansion,
i.e. pipeline-construction time — so a typo'd module, a missing attribute, or an
object that cannot serve in its position raises ``ValueError`` before any runner
is involved, never inside a bundle. Every message quotes the offending
reference, names the failing step, and says what was expected.

**Trust boundary.** Resolving a reference imports a module, and importing runs
its top level. That is not an escalation — a YAML provider's ``packages:`` list
already installs arbitrary code onto workers — but it does mean a YAML document
carrying a ``beam-agents`` provider must be reviewed exactly like Python
pipeline code. What this module refuses is *dynamic code in the document*:
there is no ``eval`` arm, no inline source, and no file-path arm. References
resolve only against installed modules.

Importing this module has no side effects.
"""

from __future__ import annotations

import importlib
from types import ModuleType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from beam_agents.core.agent import Agent

REFERENCE_GRAMMAR = (
    "expected a 'module.path:attribute' reference (for example 'my_pkg.agents:fraud_agent')"
)


def parse_reference(reference: object, *, field: str) -> tuple[str, str]:
    """Split ``module:attribute``; raise ``ValueError`` with the grammar if malformed."""
    if not isinstance(reference, str):
        raise ValueError(f"{field}: {reference!r} is not a reference string; {REFERENCE_GRAMMAR}")
    module_path, separator, attribute_path = reference.partition(":")
    if not separator or not module_path or not attribute_path:
        raise ValueError(f"{field}: malformed reference {reference!r}; {REFERENCE_GRAMMAR}")
    return module_path, attribute_path


def resolve_reference(reference: object, *, field: str, expected: str) -> object:
    """Import ``reference``'s module and walk its attribute path.

    ``expected`` is the human phrase for what belongs in this position; it is
    carried into the type-mismatch messages raised by the callers below.
    """
    module_path, attribute_path = parse_reference(reference, field=field)
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise ValueError(
            f"{field}: cannot import module {module_path!r} from reference {reference!r} "
            f"({expected}); is the package installed in the launch environment?"
        ) from exc
    resolved: object = module
    for attribute in attribute_path.split("."):
        try:
            resolved = getattr(resolved, attribute)
        except AttributeError as exc:
            raise ValueError(
                f"{field}: reference {reference!r} names attribute "
                f"{attribute_path!r}, but {attribute!r} was not found on "
                f"{module_path!r} ({expected})"
            ) from exc
    return resolved


def _reject(reference: object, resolved: object, *, field: str, expected: str) -> ValueError:
    return ValueError(
        f"{field}: reference {reference!r} resolved to a "
        f"{type(resolved).__name__} ({resolved!r:.60}), but {expected}"
    )


def resolve_agent(reference: object, *, field: str = "agent") -> Agent:
    """Resolve an agent reference, structurally checking what it found.

    The check is deliberately shallow: ``Agent`` is a protocol over ``__call__``
    and ``StreamAgent`` one over ``activate``, so "callable, or has an
    ``activate`` attribute" is enough to reject a module or a string constant
    without pretending to verify async-ness or signature at import time. The
    DoFn stays the backstop for a plausible-but-wrong object.
    """
    expected = "an agent must be an async callable or expose an `activate` method"
    resolved = resolve_reference(reference, field=field, expected=expected)
    # A module is never an agent, even though it can carry an `activate`
    # attribute by accident: `agent: "my_pkg:agents"` is the mistake this catches.
    if isinstance(resolved, ModuleType):
        raise _reject(reference, resolved, field=field, expected=expected)
    if not callable(resolved) and not hasattr(resolved, "activate"):
        raise _reject(reference, resolved, field=field, expected=expected)
    return resolved  # type: ignore[return-value]


def resolve_callable(reference: object, *, field: str, expected: str) -> Callable[..., object]:
    """Resolve a reference that must land on a callable (provider, decode, route)."""
    resolved = resolve_reference(reference, field=field, expected=expected)
    if not callable(resolved):
        raise _reject(reference, resolved, field=field, expected=expected)
    return resolved


def resolve_instance(reference: object, *, field: str, cls: type, expected: str) -> object:
    """Resolve a reference that must land on an instance of ``cls``."""
    resolved = resolve_reference(reference, field=field, expected=expected)
    if not isinstance(resolved, cls):
        raise _reject(reference, resolved, field=field, expected=expected)
    return resolved
