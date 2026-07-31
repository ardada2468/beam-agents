"""Executing side-effecting tools, and mapping the outcome to a `ToolResult`.

`EffectorToolRunner` is the mirror image of the in-pipeline
:class:`~beam_agents.tools.runner.ToolRunner`: it **requires**
``side_effect=True`` where the other refuses it, and it reaches the callable
through :meth:`Tool.unwrap` — the single sanctioned bypass of correctness
invariant 5. The two runners are disjoint, so neither can execute the other's
class of tool and "side effects only via intents" stays a closed statement.

The status mapping in :func:`_execute_intent` is total, and its dividing line is
whether the callable ran: ``REJECTED`` means it never did (unknown tool,
read-only tool, bad arguments), while ``ERROR`` means it ran and its effect is
unknown (it raised, timed out, or returned something unencodable). That
distinction is what lets an agent decide whether retrying is safe.

The tool is invoked **at most once per claim**: a side-effecting tool that
raised may already have performed part of its effect, so a blind re-invocation
would be a second effect, not a retry.
"""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

from pydantic import ValidationError

from beam_agents._protos import ToolIntent, ToolResult
from beam_agents.tools.errors import ToolArgumentError, ToolError, ToolNotFoundError
from beam_agents.tools.intent_info import IntentInfo

if TYPE_CHECKING:
    from beam_agents.tools.registry import Tool, ToolRegistry

__all__ = [
    "EffectorToolRunner",
    "ReadOnlyToolError",
]


class ReadOnlyToolError(ToolError):
    """A `side_effect=False` tool reached the effector.

    Read-only tools belong to the pipeline's fast path; one arriving on the
    outbox means the pipeline failed to run it inline. That is a bug to
    surface, not an effect to perform.
    """

    def __init__(self, tool_name: str) -> None:
        super().__init__(
            f"tool {tool_name!r} is read-only (side_effect=False) and must run inline in the "
            "pipeline, not in the effector"
        )
        self.tool_name = tool_name


class EffectorToolRunner:
    """Validates arguments, then invokes a side-effecting tool off the event loop.

    Sync callables run via :func:`asyncio.to_thread` so a blocking tool (an HTTP
    call, a database write — the normal shape of a side effect) cannot stall the
    other partitions' tasks. A timeout therefore stops *waiting* on a sync tool
    without stopping the tool itself, which is precisely why a timeout maps to
    ``ERROR``: the effect is unknown, not un-attempted.
    """

    def __init__(self, *, tool_timeout_ms: int) -> None:
        self.tool_timeout_ms = tool_timeout_ms

    async def run(
        self,
        t: Tool,
        arguments: Mapping[str, object],
        *,
        on_invoke: Callable[[], None] | None = None,
        intent_info: IntentInfo | None = None,
    ) -> object:
        """Run ``t``; ``on_invoke`` fires once, immediately before the callable.

        The callback is how a caller learns that the effect may now have
        happened. Everything before it — resolving the tool, validating
        arguments — is still safely abandonable; everything after it is not.

        ``intent_info`` is injected as the keyword argument ``intent`` iff the
        tool declares it (`accepts_intent`); it never participates in argument
        validation, which covers exactly the parsed ``args_json``.
        """
        if not t.side_effect:
            raise ReadOnlyToolError(t.name)
        try:
            validated = t.argument_model.model_validate(dict(arguments))
        except ValidationError as exc:
            raise ToolArgumentError(t.name, str(exc)) from exc
        if on_invoke is not None:
            on_invoke()
        call_kwargs = validated.model_dump()
        if t.accepts_intent:
            call_kwargs["intent"] = intent_info
        return await asyncio.wait_for(
            self._invoke(t, call_kwargs), timeout=self.tool_timeout_ms / 1000
        )

    async def _invoke(self, t: Tool, arguments: dict[str, object]) -> object:
        func = t.unwrap()
        if inspect.iscoroutinefunction(func):
            return await func(**arguments)
        result = await asyncio.to_thread(func, **arguments)
        if inspect.isawaitable(result):
            return await result
        return result


def _encode_payload(value: object) -> bytes:
    """Encode a tool's return value as canonical JSON.

    Matches the encoding ``ctx.act`` uses for ``args_json`` (sorted keys, tight
    separators, no NaN), so both directions of the intent/result exchange carry
    the same canonical form.
    """
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()


def _result(
    intent: ToolIntent,
    status: ToolResult.Status,
    now_ms: int,
    *,
    payload: bytes = b"",
    error_message: str = "",
) -> ToolResult:
    return ToolResult(
        intent_id=intent.intent_id,
        entity_key=intent.entity_key,
        seq=intent.seq,
        status=status,
        payload=payload,
        error_message=error_message,
        completed_at_ms=now_ms,
    )


def _parse_arguments(intent: ToolIntent) -> Mapping[str, object]:
    decoded = json.loads(intent.args_json)
    if not isinstance(decoded, dict):
        raise ToolArgumentError(
            intent.tool_name,
            f"args_json must decode to a JSON object, got {type(decoded).__name__}",
        )
    return decoded


async def _execute_intent(
    intent: ToolIntent,
    registry: ToolRegistry,
    runner: EffectorToolRunner,
    *,
    now_ms: int,
    on_invoke: Callable[[], None] | None = None,
) -> ToolResult:
    """Execute one tool intent and map the outcome to a terminal `ToolResult`.

    Never raises: every failure mode is a status. An exception escaping here
    would leave an intent claimed with no result, which is the one outcome the
    dedup protocol cannot recover from without waiting out the lease.
    """
    try:
        tool = registry.get(intent.tool_name)
    except ToolNotFoundError as exc:
        return _result(intent, ToolResult.REJECTED, now_ms, error_message=str(exc))

    try:
        arguments = _parse_arguments(intent)
    except (json.JSONDecodeError, ToolArgumentError) as exc:
        return _result(intent, ToolResult.REJECTED, now_ms, error_message=str(exc))

    # For a declaring tool, hand over the intent's own wire fields, verbatim:
    # deterministic across replays and redeliveries, which is what makes the
    # injected identity an idempotency key. For a non-declaring tool the call
    # is byte-identical to the pre-IntentInfo behavior — the keyword is not
    # even passed, so runner subclasses with the historical `run` signature
    # keep working.
    intent_kwargs: dict[str, IntentInfo] = (
        {
            "intent_info": IntentInfo(
                intent_id=intent.intent_id,
                entity_key=intent.entity_key,
                seq=intent.seq,
                step_index=intent.step_index,
                attempt=intent.attempt,
            )
        }
        if tool.accepts_intent
        else {}
    )

    try:
        value = await runner.run(tool, arguments, on_invoke=on_invoke, **intent_kwargs)
    except (ReadOnlyToolError, ToolArgumentError) as exc:
        # The callable was never invoked.
        return _result(intent, ToolResult.REJECTED, now_ms, error_message=str(exc))
    except TimeoutError:
        return _result(
            intent,
            ToolResult.ERROR,
            now_ms,
            error_message=(
                f"tool {intent.tool_name!r} timed out after "
                f"{runner.tool_timeout_ms}ms; its effect is unknown"
            ),
        )
    except asyncio.CancelledError:
        # Shutdown or partition revocation: not an outcome to publish. The
        # intent stays claimed until its lease expires, then re-executes.
        raise
    except Exception as exc:  # every tool failure is a status, never a crash
        return _result(
            intent,
            ToolResult.ERROR,
            now_ms,
            error_message=f"{type(exc).__name__}: {exc}",
        )

    return _encode_result(intent, value, now_ms)


def _encode_result(intent: ToolIntent, value: object, now_ms: int) -> ToolResult:
    """Encode a successful call's return value, or report why it could not be.

    An unencodable return value is an ERROR rather than a dropped intent: the
    effect happened, and the agent has to learn that even if the value is lost.
    """
    try:
        payload = _encode_payload(value)
    except (TypeError, ValueError) as exc:
        return _result(
            intent,
            ToolResult.ERROR,
            now_ms,
            error_message=(
                f"tool {intent.tool_name!r} succeeded but its return value could not be "
                f"encoded: {type(exc).__name__}: {exc}"
            ),
        )
    return _result(intent, ToolResult.OK, now_ms, payload=payload)
