#!/usr/bin/env python
"""Generate the website's API reference by introspecting the installed package.

The reference is *generated*, never written. Every field below comes from
``inspect`` against a real import of ``beam_agents``: signatures from
``inspect.signature``, prose from ``__doc__`` verbatim, locations from
``inspect.getsourcefile``/``getsourcelines``. There is no place to put a
hand-written description, which is the point — a written reference drifts from
the code silently, and this one cannot.

The output (``website/generated/api.json``) is committed and drift-checked in
CI, mirroring how this repository already gates generated protobuf bindings:

    uv run python scripts/gen_api_reference.py            # regenerate
    uv run python scripts/gen_api_reference.py --check    # fail on drift

Symbols that ``beam_agents.__init__`` exposes lazily through ``__getattr__``
(the optional-dependency adapters) are documented with the extra they need.
They are never silently omitted: if a name in ``__all__`` cannot be resolved,
this script exits non-zero naming the name and the extra, because a reference
that quietly drops the adapter is exactly the kind of half-truth the site
exists to prevent.
"""

from __future__ import annotations

import argparse
import difflib
import inspect
import json
import re
import sys
import tomllib
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT = REPO_ROOT / "website" / "generated" / "api.json"

# Which extra provides which lazily-exported name. Keyed by the name in
# `__all__`; the value is the `[project.optional-dependencies]` key.
LAZY_EXTRAS = {"LangGraphAgent": "langgraph"}

# Modules a reader must import from to use the runtime today, beyond the names
# `beam_agents/__init__.py` re-exports. This list is published on the API index
# with the note attached: the project documents everything outside `__all__` as
# private, and the gap between that policy and what writing an agent actually
# requires is a fact about the current state, not something to paper over.
DOCUMENTED_INTERNAL = {
    "beam_agents.core.agent": (
        "Activation outcomes (`Complete`, `Suspend`) and the agent protocols. "
        "An agent function cannot be written without these."
    ),
    "beam_agents.core.context": (
        "`ActivationContext` — the object every agent receives. Its methods "
        "(`act`, `call_model`, `run_tool`, `emit`, `memory`) are the runtime's "
        "real authoring surface."
    ),
    "beam_agents.tools": ("`@tool` and `ToolRegistry`, needed to declare and register tools."),
    "beam_agents.model.fake": (
        "`FakeLLM` and its matchers — the model used in tests and examples."
    ),
    "beam_agents.model.client": (
        "`LlmRequest`/`LlmResponse`/`ProviderError`, the model-call types."
    ),
    "beam_agents._protos": (
        "Generated protobuf bindings, including `AgentEnvelope` — the input "
        "element type `RunAgent` requires. Underscore-prefixed, yet "
        "unavoidable when constructing pipeline input."
    ),
}


def _pyproject_version() -> str:
    with (REPO_ROOT / "pyproject.toml").open("rb") as handle:
        return str(tomllib.load(handle)["project"]["version"])


def _source_ref(obj: object) -> dict[str, Any] | None:
    """Repository-relative path and 1-indexed line of ``obj``'s definition."""
    try:
        file = inspect.getsourcefile(obj)  # type: ignore[arg-type]
        _, line = inspect.getsourcelines(obj)  # type: ignore[arg-type]
    except (TypeError, OSError):
        return None
    if file is None:
        return None
    path = Path(file).resolve()
    try:
        relative = path.relative_to(REPO_ROOT)
    except ValueError:
        return None
    return {"path": relative.as_posix(), "line": line}


# `repr()` of a function or an object default embeds its memory address —
# `<function deny at 0x10996d620>` — which changes every interpreter run. Left
# alone, the committed reference could never be drift-free, and the drift check
# would fail on every CI run for no real reason. Stripping the address keeps
# the useful part (the name) and makes the output deterministic.
_ADDRESS = re.compile(r" at 0x[0-9a-fA-F]+>")


def _signature(obj: object) -> str | None:
    try:
        rendered = str(inspect.signature(obj))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return _ADDRESS.sub(">", rendered)


def _kind(obj: object) -> str:
    if inspect.isclass(obj):
        return "dataclass" if is_dataclass(obj) else "class"
    if inspect.isfunction(obj) or inspect.isbuiltin(obj):
        return "function"
    return "constant"


def _doc(obj: object) -> str | None:
    raw = inspect.getdoc(obj)
    return raw if raw else None


def _members(obj: object) -> list[dict[str, Any]]:
    """Public members of a class: dataclass fields, then methods.

    Underscore-prefixed names are skipped throughout — the project's convention
    is that they are private, and the reference must not present them as API.
    """
    if not inspect.isclass(obj):
        return []
    members: list[dict[str, Any]] = []
    if is_dataclass(obj):
        for field in fields(obj):
            if field.name.startswith("_"):
                continue
            members.append(
                {
                    "name": field.name,
                    "kind": "field",
                    "signature": f"{field.name}: {_annotation(field.type)}",
                    "doc": None,
                    "source": None,
                }
            )
    for name, value in inspect.getmembers(obj):
        if name.startswith("_"):
            continue
        if not (inspect.isfunction(value) or inspect.ismethod(value)):
            continue
        # Inherited machinery (Beam's PTransform surface) is not this
        # project's API; only methods defined on the class itself are.
        if name not in vars(obj):
            continue
        members.append(
            {
                "name": name,
                "kind": "method",
                "signature": _signature(value),
                "doc": _doc(value),
                "source": _source_ref(value),
            }
        )
    members.sort(key=lambda member: (member["kind"] != "field", member["name"]))
    return members


def _annotation(annotation: object) -> str:
    if isinstance(annotation, str):
        return annotation
    return getattr(annotation, "__name__", str(annotation))


def _resolve(package: object, name: str) -> object:
    """Resolve ``name`` from the package, failing loudly and specifically.

    A ``ModuleNotFoundError`` behind a lazy export means the extra is missing
    from this environment; anything else is a real defect in the package and is
    re-raised untouched.
    """
    try:
        return getattr(package, name)
    except (AttributeError, ImportError) as exc:
        extra = LAZY_EXTRAS.get(name)
        if extra is not None:
            raise SystemExit(
                f"cannot resolve `beam_agents.{name}`: it is exported lazily and needs the "
                f"`{extra}` extra. Install it and rerun:\n"
                f"    uv sync --extra {extra}\n"
                f"    uv run python scripts/gen_api_reference.py\n"
                f"(underlying error: {exc!r})"
            ) from exc
        raise


def build_reference() -> dict[str, Any]:
    # Deferred on purpose: `--help` and argument errors must work in an
    # environment where the package is not installed, and the failure for a
    # missing package should come from here with context, not from an import
    # error at module load.
    import beam_agents  # noqa: PLC0415

    names = list(beam_agents.__all__)
    symbols: list[dict[str, Any]] = []
    for name in sorted(names):
        obj = _resolve(beam_agents, name)
        symbols.append(
            {
                "name": name,
                "qualname": f"{getattr(obj, '__module__', 'beam_agents')}.{name}",
                "kind": _kind(obj),
                "signature": _signature(obj),
                "doc": _doc(obj),
                "source": _source_ref(obj),
                "requires_extra": LAZY_EXTRAS.get(name),
                "members": _members(obj),
            }
        )
    modules = [
        {"module": "beam_agents", "visibility": "public", "note": "The declared public surface."},
        *(
            {"module": module, "visibility": "documented-internal", "note": note}
            for module, note in sorted(DOCUMENTED_INTERNAL.items())
        ),
    ]
    return {
        "generated_by": "scripts/gen_api_reference.py",
        "package": "beam_agents",
        "package_version": _pyproject_version(),
        "public_surface": sorted(names),
        "symbols": symbols,
        "modules": modules,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail if the committed reference differs from a fresh generation",
    )
    args = parser.parse_args()

    payload = json.dumps(build_reference(), indent=2, sort_keys=False) + "\n"

    if args.check:
        if not OUTPUT.exists():
            print(
                f"{OUTPUT.relative_to(REPO_ROOT)} is missing. Regenerate it with:\n"
                "    make api-reference",
                file=sys.stderr,
            )
            return 1
        current = OUTPUT.read_text(encoding="utf-8")
        if current != payload:
            print(
                f"{OUTPUT.relative_to(REPO_ROOT)} is out of date with the installed package.\n"
                "The public API changed without regenerating the reference. Fix with:\n"
                "    make api-reference",
                file=sys.stderr,
            )
            _print_diff(current, payload)
            return 1
        return 0

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(payload, encoding="utf-8")
    print(f"wrote {OUTPUT.relative_to(REPO_ROOT)}")
    return 0


def _print_diff(current: str, generated: str) -> None:
    diff = difflib.unified_diff(
        current.splitlines(keepends=True),
        generated.splitlines(keepends=True),
        fromfile="committed",
        tofile="generated",
        n=2,
    )
    sys.stderr.writelines(diff)


if __name__ == "__main__":
    raise SystemExit(main())
