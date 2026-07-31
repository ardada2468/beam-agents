"""The YAML config surface and its total mapping onto ``AgentConfig``.

The keyword surface is fixed and documented (``docs/yaml.md``). This module owns
*shape* only — the key set, the reference grammar, and the mapping types — and
delegates every value check downward: scalar ranges to
:class:`~beam_agents.core.transform.AgentConfig`, HITL ranges to
:class:`~beam_agents.hitl.HitlPolicy`, and sink URIs to the configured
``SinkResolver``. The two layers therefore cannot drift apart on what a valid
config is.

The provider factory is built here rather than named: ``provider`` resolves to a
module-level callable and ``provider_config``'s keyword arguments are bound onto
it with :func:`functools.partial`, producing the picklable zero-argument
``provider_factory`` the config requires. The factory is deliberately *not*
invoked (a factory may open network clients), so two construction-time probes
stand in for calling it: a ``pickle.dumps`` round trip, and a signature check of
the supplied keyword names where the callable's signature is introspectable.

Importing this module has no side effects.
"""

from __future__ import annotations

import functools
import inspect
import pickle
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, cast

from beam_agents.core.transform import AgentConfig
from beam_agents.hitl import HitlPolicy
from beam_agents.tools import ToolRegistry
from beam_agents.yaml._refs import resolve_callable, resolve_instance

if TYPE_CHECKING:
    from collections.abc import Callable

    from beam_agents.model.client import LLMClient

#: The accepted top-level config keys, in documentation order. `sink_resolver`
#: is deliberately absent: it is an advanced Python seam, and a pipeline that
#: needs a custom resolver has crossed back into Python authoring (design D4).
CONFIG_KEYS: tuple[str, ...] = (
    "agent",
    "provider",
    "provider_config",
    "decode",
    "tool_registry",
    "activation_timeout_s",
    "ttl_ms",
    "cancel_grace_s",
    "intents_to",
    "traces_to",
    "errors_to",
    "hitl",
    "key_field",
    "payload_field",
    "event_time_field",
)

#: The accepted keys inside the nested ``hitl`` mapping.
HITL_KEYS: tuple[str, ...] = (
    "timeout_ms",
    "intent_ttl_ms",
    "approval_channel",
    "max_escalations",
    "on_timeout",
)


def reject_unknown_keys(unknown: Mapping[str, object]) -> None:
    """Raise ``ValueError`` naming every unrecognized top-level key."""
    if unknown:
        raise ValueError(
            f"RunAgent: unknown config key(s) {sorted(unknown)}; "
            f"accepted keys are {list(CONFIG_KEYS)}"
        )


def _require_mapping(value: object, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(
            f"{field}: expected a mapping of keys to values, got {type(value).__name__}"
        )
    return value


def build_hitl_policy(hitl: object) -> HitlPolicy:
    """Map the nested ``hitl`` block onto :class:`HitlPolicy`.

    Nested unknown keys are rejected exactly as top-level ones are, so ``ttl``
    for ``intent_ttl_ms`` is caught at the document rather than defaulted over.
    """
    mapping = _require_mapping(hitl, field="hitl")
    unknown = set(mapping) - set(HITL_KEYS)
    if unknown:
        raise ValueError(
            f"hitl: unknown key(s) {sorted(unknown)}; accepted keys are {list(HITL_KEYS)}"
        )
    fields: dict[str, Any] = {key: mapping[key] for key in mapping if key != "on_timeout"}
    if "on_timeout" in mapping:
        fields["on_timeout"] = resolve_callable(
            mapping["on_timeout"],
            field="hitl.on_timeout",
            expected=(
                "a HITL timeout route must be a module-level function taking a "
                "FallbackContext and returning Deny/Drop/Escalate"
            ),
        )
    # HitlPolicy validates itself; range errors surface from there, not here.
    return HitlPolicy(**fields)


def build_provider_factory(provider: object, provider_config: object) -> Callable[[], LLMClient]:
    """Resolve ``provider`` and bind ``provider_config`` into a zero-arg factory."""
    resolved = resolve_callable(
        provider,
        field="provider",
        expected="a provider must be a module-level callable returning an LLMClient",
    )
    if provider_config is None:
        kwargs: dict[str, Any] = {}
    else:
        kwargs = dict(_require_mapping(provider_config, field="provider_config"))
    _check_kwarg_names(resolved, kwargs, reference=provider)
    factory = functools.partial(resolved, **kwargs)
    _check_picklable(factory, reference=provider)
    # The resolved callable's own return type is the contract (it is named, not
    # typed, by the document); the DoFn is the backstop for one that does not
    # actually return an `LLMClient`.
    return cast("Callable[[], LLMClient]", factory)


def _check_kwarg_names(
    resolved: Callable[..., object], kwargs: Mapping[str, Any], *, reference: object
) -> None:
    """Reject a misspelled ``provider_config`` key at the YAML boundary.

    The factory itself is never called here, so without this a typo'd keyword
    would first surface on a worker's first model call. C-implemented callables
    have no introspectable signature; for those the check is skipped rather than
    guessed at.
    """
    try:
        signature = inspect.signature(resolved)
    except (TypeError, ValueError):
        return
    if any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    ):
        return
    accepted = {
        name
        for name, parameter in signature.parameters.items()
        if parameter.kind
        in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    }
    unknown = sorted(set(kwargs) - accepted)
    if unknown:
        raise ValueError(
            f"provider_config: keyword(s) {unknown} are not accepted by "
            f"{reference!r}; its signature accepts {sorted(accepted)}"
        )


def _check_picklable(factory: object, *, reference: object) -> None:
    """Probe picklability now, where the message can still name the reference.

    ``RunAgent`` serializes the factory into the runner, so a closure or a
    locally-defined function would otherwise fail deep inside job submission.
    """
    try:
        pickle.dumps(factory)
    except Exception as exc:
        raise ValueError(
            f"provider: {reference!r} does not pickle, so it cannot be shipped to "
            "the runner; reference a module-level callable, not a closure or a "
            f"locally-defined function ({type(exc).__name__}: {exc})"
        ) from exc


def build_agent_config(
    *,
    provider: object,
    provider_config: object,
    decode: object,
    tool_registry: object,
    activation_timeout_s: object,
    ttl_ms: object,
    cancel_grace_s: object,
    intents_to: object,
    traces_to: object,
    errors_to: object,
    hitl: object,
) -> AgentConfig:
    """Build the ``AgentConfig`` for a YAML config mapping.

    Unset (``None``) knobs are omitted so ``AgentConfig``'s own defaults apply;
    sink URIs pass through verbatim to the resolver's grammar.
    """
    fields: dict[str, Any] = {"provider_factory": build_provider_factory(provider, provider_config)}
    if decode is not None:
        fields["decode"] = resolve_callable(
            decode,
            field="decode",
            expected="a decode must be a module-level callable returning token counts",
        )
    if tool_registry is not None:
        fields["tool_registry"] = resolve_instance(
            tool_registry,
            field="tool_registry",
            cls=ToolRegistry,
            expected="a tool_registry must be a prebuilt, module-level ToolRegistry",
        )
    for name, value in (
        ("activation_timeout_s", activation_timeout_s),
        ("ttl_ms", ttl_ms),
        ("cancel_grace_s", cancel_grace_s),
        ("intents_to", intents_to),
        ("traces_to", traces_to),
        ("errors_to", errors_to),
    ):
        if value is not None:
            fields[name] = value
    if hitl is not None:
        fields["hitl_policy"] = build_hitl_policy(hitl)
    return AgentConfig(**fields)
