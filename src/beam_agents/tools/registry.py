"""The `@tool` decorator, Pydantic-derived schema generation, and `ToolRegistry`.

See the change design (``openspec/changes/add-tool-registry/design.md``) for
the load-bearing decisions: a Pydantic v2 model generated per tool via
`create_model` from the signature (D1), eager `ToolDefinitionError` on
un-annotated parameters (D2), and the `side_effect` guard living on
`Tool.__call__` (D3).
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import overload

from pydantic import BaseModel, create_model

from beam_agents.tools.errors import (
    SideEffectToolError,
    ToolDefinitionError,
    ToolError,
    ToolNotFoundError,
)


def _build_argument_model(func: Callable[..., object], *, model_name: str) -> type[BaseModel]:
    """Derive a Pydantic v2 model from `func`'s parameters: no default -> required."""
    signature = inspect.signature(func)
    fields: dict[str, tuple[object, object]] = {}
    for param_name, param in signature.parameters.items():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            raise ToolDefinitionError(
                f"tool parameter {param_name!r} uses *args/**kwargs, which cannot be modeled "
                "as a tool schema"
            )
        if param.annotation is inspect.Parameter.empty:
            raise ToolDefinitionError(f"tool parameter {param_name!r} is missing a type annotation")
        default = ... if param.default is inspect.Parameter.empty else param.default
        fields[param_name] = (param.annotation, default)
    return create_model(model_name, **fields)  # type: ignore[call-overload, no-any-return]


class Tool:
    """A registered tool: name, description, `side_effect` flag, derived Pydantic
    argument model, and the provider-facing JSON `schema` built from it.

    Calling a `Tool` directly runs the wrapped callable with its original
    semantics for `side_effect=False` tools (no argument validation — that is
    `ToolRunner`'s job); it raises `SideEffectToolError` for `side_effect=True`
    tools, enforcing correctness invariant 5 that side effects only ever
    execute through `ctx.act(...)`.
    """

    def __init__(
        self,
        func: Callable[..., object],
        *,
        name: str,
        description: str,
        side_effect: bool,
    ) -> None:
        self._func = func
        self.name = name
        self.description = description
        self.side_effect = side_effect
        self.argument_model = _build_argument_model(func, model_name=f"{name}Args")
        self.schema: dict[str, object] = {
            "name": name,
            "description": description,
            "parameters": self.argument_model.model_json_schema(),
        }

    def __call__(self, *args: object, **kwargs: object) -> object:
        if self.side_effect:
            raise SideEffectToolError(self.name)
        return self._func(*args, **kwargs)


@overload
def tool(func: Callable[..., object]) -> Tool: ...
@overload
def tool(
    *,
    name: str | None = None,
    description: str | None = None,
    side_effect: bool = False,
) -> Callable[[Callable[..., object]], Tool]: ...
def tool(
    func: Callable[..., object] | None = None,
    *,
    name: str | None = None,
    description: str | None = None,
    side_effect: bool = False,
) -> Tool | Callable[[Callable[..., object]], Tool]:
    """Register a callable as a `Tool`. Usable bare (`@tool`) or parameterized
    (`@tool(name=..., side_effect=True)`). `name` defaults to `func.__name__`;
    `description` defaults to `func.__doc__` (or `""` if there is none).
    """

    def decorate(fn: Callable[..., object]) -> Tool:
        return Tool(
            fn,
            name=name if name is not None else fn.__name__,
            description=description if description is not None else (fn.__doc__ or ""),
            side_effect=side_effect,
        )

    if func is not None:
        return decorate(func)
    return decorate


class ToolRegistry:
    """Collects `Tool`s by name and exposes their aggregate `tools_schema`.

    Owned by an agent config / loop driver instance, never module-global
    state, honoring the project's no-global-mutable-state convention.
    """

    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}

    def register(self, t: Tool) -> None:
        if t.name in self._tools:
            raise ToolError(f"tool {t.name!r} is already registered")
        self._tools[t.name] = t

    def get(self, name: str) -> Tool:
        try:
            return self._tools[name]
        except KeyError:
            raise ToolNotFoundError(name) from None

    @property
    def tools_schema(self) -> list[dict[str, object]]:
        return [t.schema for t in self._tools.values()]
