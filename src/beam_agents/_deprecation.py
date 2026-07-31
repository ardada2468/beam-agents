"""The executable half of the deprecation policy CONTRIBUTING.md defines.

After the 1.0 freeze a public name may only be removed or renamed at the end of
a deprecation window of at least one minor release, during which the old name
keeps working and emits a ``DeprecationWarning``. This module holds the single
helper that formats and emits that warning, so every deprecation in the package
says the same two things a caller needs to act: what to move to, and the first
release that may take the old name away.

The intended shape is a module-level ``__getattr__`` (PEP 562), the pattern
``beam_agents/__init__.py`` already uses to resolve optional-extra adapter
classes lazily::

    _RENAMED = {"old_name": "beam_agents.pkg.new_name"}

    def __getattr__(name: str) -> object:
        if name == "old_name":
            return deprecated_attribute(
                name, replacement=_RENAMED[name], removed_in="1.1.0",
                value=new_name, module=__name__,
            )
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

Serving the old name from ``__getattr__`` rather than from the module namespace
keeps it out of ``dir()`` and out of the module's declared surface, so
``public-surface.toml`` records the deprecation as the removal of a declared
name — which is exactly the reviewable diff the policy wants — while the name
still resolves for anyone who has not migrated.

This module is private: the policy is contract, the helper is machinery.
"""

from __future__ import annotations

import warnings

__all__ = ["deprecated_attribute"]


def deprecated_attribute(
    name: str,
    *,
    replacement: str | None,
    removed_in: str,
    value: object,
    module: str,
    stacklevel: int = 3,
) -> object:
    """Emit the deprecation warning for ``name`` and return its still-working value.

    ``name`` is the deprecated attribute as the caller spelled it and ``module``
    the module serving it; the two are joined into the dotted path the warning
    names, so a user reading a traceback sees the import they must change rather
    than a bare identifier. ``replacement`` is the dotted path to move to, or
    ``None`` when the name is going away with no successor — in which case the
    warning says so explicitly instead of leaving the reader to search for a
    rename that does not exist. ``removed_in`` is the first release that MAY
    drop the name; it is a promise about the window's floor, not a schedule.

    ``value`` is returned unchanged: a name inside its window must keep working,
    so this helper never raises and never substitutes a shim object. The default
    ``stacklevel`` of 3 attributes the warning to the code that touched the
    attribute rather than to the module ``__getattr__`` that intercepted it,
    which is the frame a user can actually edit; pass a different value when
    calling from anywhere other than a module-level ``__getattr__``.
    """
    target = f"use {replacement} instead" if replacement else "there is no replacement"
    warnings.warn(
        f"{module}.{name} is deprecated and may be removed in {removed_in}; {target}.",
        DeprecationWarning,
        stacklevel=stacklevel,
    )
    return value
