"""The reference conformance factory: the plain protocol agent every framework
must match.

One directive-interpreting agent realizes every ``ScenarioSpec`` directly
against ``ActivationContext``: the scripted model responses (the shared JSON
directive vocabulary in ``_spec.turn_response``) drive which runtime surface
the activation touches — ``run_tool`` inline, ``act``/``request_approval`` +
``Suspend``, or ``Complete``. The agent's own code contains no per-scenario
branching beyond interpreting the conversation, so the *script* is what a cell
runs, exactly as the design requires.

Everything is module-level (or an instance of a module-level class carrying
only strings), so the DirectRunner can pickle the DoFn holding it and the
Flink leg can rebuild it worker-side by name.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from beam_agents.core.agent import Complete, Outcome, Suspend
from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import Behavior, FakeLLM, Matcher, respond_with
from tests.conformance._spec import SCENARIOS_BY_NAME, ScenarioSpec, turn_response

if TYPE_CHECKING:
    from beam_agents.core.context import ActivationContext


def _request(spec: ScenarioSpec, transcript: list[str]) -> LlmRequest:
    return LlmRequest(
        model_id=spec.model_id, messages=list(transcript), tools_schema=None, sampling_params=None
    )


def _canonical(args: dict[str, object]) -> str:
    return json.dumps(args, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _snapshot(transcript: list[str], tool_values: list[str]) -> bytes:
    return json.dumps({"transcript": transcript, "tool_values": tool_values}).encode()


def _output(
    spec: ScenarioSpec,
    ctx: ActivationContext,
    tool_values: list[str],
    answer: str | None,
    *,
    resumed: str | None = None,
) -> bytes:
    """The canonical terminal-output shape shared with the other factories:
    ``seen=<n>`` (memory scenarios) | tool results | ``resumed:<payload>`` |
    the model's final answer — pipe-joined, empty segments omitted."""
    segments: list[str] = []
    if spec.uses_memory:
        segments.append(f"seen={len(ctx.memory.ring('log'))}")
    if tool_values:
        segments.append(",".join(tool_values))
    if resumed is not None:
        segments.append(f"resumed:{resumed}")
    if answer:
        segments.append(answer)
    return "|".join(segments).encode()


class ReferenceAgent:
    """The baseline protocol agent for one scenario; picklable by scenario name."""

    def __init__(self, scenario: str) -> None:
        self._scenario = scenario

    def __reduce__(self) -> tuple[type[ReferenceAgent], tuple[str]]:
        return (ReferenceAgent, (self._scenario,))

    async def __call__(self, ctx: ActivationContext) -> Outcome:
        spec = SCENARIOS_BY_NAME[self._scenario]
        if ctx.is_resume:
            return await _resume(spec, ctx)
        return await _start(spec, ctx)


async def _start(spec: ScenarioSpec, ctx: ActivationContext) -> Outcome:
    if spec.uses_memory:
        ctx.memory.append("log", ctx.event or b"-", max_items=64)
    transcript = [f"event:{ctx.event.decode()}"]
    tool_values: list[str] = []
    while True:
        response = await ctx.call_model(_request(spec, transcript))
        directive = json.loads(response.response)
        if "run_tool" in directive:
            call = directive["run_tool"]
            value = await ctx.run_tool(call["name"], call["args"])
            tool_values.append(str(value))
            transcript.append(f"tool:{call['name']}={value}")
            continue
        if "act" in directive:
            call = directive["act"]
            ctx.act(call["name"], _canonical(call["args"]))
            return Suspend(
                snapshot=_snapshot(transcript, tool_values),
                adapter="reference",
                timeout_ms=spec.hitl_timeout_ms,
            )
        if "request_approval" in directive:
            ctx.request_approval(_canonical(directive["request_approval"]["args"]))
            return Suspend(
                snapshot=_snapshot(transcript, tool_values),
                adapter="reference",
                timeout_ms=spec.hitl_timeout_ms,
            )
        return Complete(_output(spec, ctx, tool_values, directive["answer"]))


async def _resume(spec: ScenarioSpec, ctx: ActivationContext) -> Outcome:
    state = json.loads(ctx.snapshot)
    transcript: list[str] = state["transcript"]
    tool_values: list[str] = state["tool_values"]
    if ctx.resume_approval is not None:
        verdict = "approved" if ctx.resume_approval.approved else "denied"
        return Complete(_output(spec, ctx, tool_values, None, resumed=verdict))
    assert ctx.resume_result is not None
    payload = ctx.resume_result.payload.decode()
    if spec.turns[-1].directive == "answer":
        # The conversation continues after the side effect: feed the result
        # back and let the script's final turn answer.
        transcript.append(f"result:{payload}")
        response = await ctx.call_model(_request(spec, transcript))
        answer = json.loads(response.response)["answer"]
        return Complete(_output(spec, ctx, tool_values, answer, resumed=payload))
    # No post-resume turn: repeat the identical pre-suspend request (served by
    # the suspend-committed replay cache — zero provider calls) and finish.
    await ctx.call_model(_request(spec, transcript))
    return Complete(_output(spec, ctx, tool_values, None, resumed=payload))


# -- the scripted provider --------------------------------------------------------


def _match_turn(model_id: str, transcript_len: int) -> Matcher:
    """Turn *i* of a reference conversation carries ``i+1`` transcript entries
    (the event plus one entry per completed prior turn)."""

    def matcher(request: LlmRequest) -> bool:
        messages = request.messages
        return (
            request.model_id == model_id
            and isinstance(messages, list)
            and len(messages) == transcript_len
        )

    return matcher


def reference_rules(spec: ScenarioSpec) -> list[tuple[Matcher, Behavior]]:
    """One FakeLLM rule per scripted turn, scoped by the scenario's model_id."""
    return [
        (_match_turn(spec.model_id, index + 1), respond_with(turn_response(turn)))
        for index, turn in enumerate(spec.turns)
    ]


def build_reference_provider(spec: ScenarioSpec) -> FakeLLM:
    return FakeLLM(reference_rules(spec))


def build_reference_agent(spec: ScenarioSpec) -> ReferenceAgent:
    return ReferenceAgent(spec.name)
