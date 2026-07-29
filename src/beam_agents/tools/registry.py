"""The `@tool` decorator, Pydantic-derived schema generation, and `ToolRegistry`.

See the change design (``openspec/changes/add-tool-registry/design.md``) for
the load-bearing decisions: a Pydantic v2 model generated per tool via
`create_model` from the signature (D1), eager `ToolDefinitionError` on
un-annotated parameters (D2), and the `side_effect` guard living on
`Tool.__call__` (D3).
"""

from __future__ import annotations

import inspect
import typing
from collections.abc import Callable
from typing import overload

from pydantic import BaseModel, create_model, model_validator

from beam_agents.tools.errors import (
    SideEffectToolError,
    ToolDefinitionError,
    ToolError,
    ToolNotFoundError,
)
from beam_agents.tools.intent_info import IntentInfo

# The reserved, runtime-injected parameter name. See the change design
# (``openspec/changes/add-intent-info-for-tools/design.md``): recognition is
# name + kind + annotation, near-misses fail fast at decoration time.
_INTENT_PARAM = "intent"


def _annotation_is_intent_info(annotation: object) -> bool:
    if annotation is IntentInfo:
        return True
    # Fallback for `from __future__ import annotations` modules where full
    # hint evaluation failed: match the literal (possibly dotted) name.
    return isinstance(annotation, str) and annotation.split(".")[-1] == IntentInfo.__name__


def _detect_intent_param(func: Callable[..., object], *, side_effect: bool) -> bool:
    """Return whether `func` opts in to intent-identity injection.

    A keyword-only ``intent: IntentInfo`` parameter on a side-effecting tool
    opts in. Any other `IntentInfo`-annotated parameter is a near-miss the
    author clearly meant as an opt-in, so it fails fast rather than silently
    becoming a nonsense LLM-visible argument.
    """
    signature = inspect.signature(func)
    try:
        hints: dict[str, object] = dict(typing.get_type_hints(func))
    except Exception:  # unresolvable forward references: fall back to literals
        hints = {}
    accepts_intent = False
    for param_name, param in signature.parameters.items():
        if not _annotation_is_intent_info(hints.get(param_name, param.annotation)):
            continue
        if param_name != _INTENT_PARAM or param.kind is not inspect.Parameter.KEYWORD_ONLY:
            raise ToolDefinitionError(
                f"tool parameter {param_name!r} is annotated IntentInfo; intent identity is "
                f"injected only through a keyword-only parameter named {_INTENT_PARAM!r} "
                f"(declare it as `*, {_INTENT_PARAM}: IntentInfo`)"
            )
        if not side_effect:
            raise ToolDefinitionError(
                f"tool declares {_INTENT_PARAM!r}: IntentInfo but is side_effect=False; intent "
                "identity exists only for side-effecting tools executed by the effector"
            )
        accepts_intent = True
    return accepts_intent


def _reject_injected_intent(cls: type[BaseModel], data: object) -> object:
    if isinstance(data, dict) and _INTENT_PARAM in data:
        raise ValueError(
            f"{_INTENT_PARAM!r} is injected by the runtime and cannot be supplied as an argument"
        )
    return data


def _build_argument_model(
    func: Callable[..., object], *, model_name: str, accepts_intent: bool
) -> type[BaseModel]:
    """Derive a Pydantic v2 model from `func`'s parameters: no default -> required.

    A recognized ``intent`` parameter is excluded — it is runtime-injected,
    never caller-supplied — and the model then rejects an ``intent`` key
    outright so an intent arriving inside ``args_json`` is REJECTED, never
    silently shadowed by the injected identity.
    """
    signature = inspect.signature(func)
    fields: dict[str, tuple[object, object]] = {}
    for param_name, param in signature.parameters.items():
        if accepts_intent and param_name == _INTENT_PARAM:
            continue
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            raise ToolDefinitionError(
                f"tool parameter {param_name!r} uses *args/**kwargs, which cannot be modeled "
                "as a tool schema"
            )
        if param.annotation is inspect.Parameter.empty:
            raise ToolDefinitionError(f"tool parameter {param_name!r} is missing a type annotation")
        default = ... if param.default is inspect.Parameter.empty else param.default
        fields[param_name] = (param.annotation, default)
    validators = (
        {"_reject_injected_intent": model_validator(mode="before")(_reject_injected_intent)}
        if accepts_intent
        else None
    )
    return create_model(  # type: ignore[call-overload, no-any-return]
        model_name, __validators__=validators, **fields
    )


class Tool:
    """A registered tool: name, description, `side_effect` flag, `accepts_intent`
    flag (a keyword-only `intent: IntentInfo` parameter opts in to runtime
    intent-identity injection and never appears in the schema), derived
    Pydantic argument model, and the provider-facing JSON `schema` built
    from it.

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
        self.accepts_intent = _detect_intent_param(func, side_effect=side_effect)
        self.argument_model = _build_argument_model(
            func, model_name=f"{name}Args", accepts_intent=self.accepts_intent
        )
        self.schema: dict[str, object] = {
            "name": name,
            "description": description,
            "parameters": self.argument_model.model_json_schema(),
        }

    def __call__(self, *args: object, **kwargs: object) -> object:
        if self.side_effect:
            raise SideEffectToolError(self.name)
        return self._func(*args, **kwargs)

    def unwrap(self) -> Callable[..., object]:
        """Return the wrapped callable, bypassing the `side_effect` guard.

        The effector's ``EffectorToolRunner`` is the **only** sanctioned caller:
        side-effecting tools must execute outside the pipeline, and that path
        needs a way past :meth:`__call__`'s guard. Keeping it a named accessor
        rather than a private-attribute poke is deliberate — the one permitted
        bypass of correctness invariant 5 should be greppable, documented, and
        testable. Nothing inside the pipeline may call this.
        """
        return self._func


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
