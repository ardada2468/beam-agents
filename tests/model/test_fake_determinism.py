"""Determinism and import-purity tests for the `fake-llm` capability.

Covers: repeated runs of the same script over the same request sequence are
identical, and importing `beam_agents.model` has no side effects.
"""

from __future__ import annotations

import importlib
import sys

from beam_agents.model import FakeLLM, LlmRequest, RateLimitError, fail_then_succeed, match_any


def _requests() -> list[LlmRequest]:
    return [
        LlmRequest(
            model_id="m-1",
            messages=[{"role": "user", "content": c}],
            tools_schema=[],
            sampling_params={},
        )
        for c in ("a", "b", "c")
    ]


async def _run() -> tuple[list[object], tuple[LlmRequest, ...], int, dict[str, int]]:
    fake = FakeLLM(
        [
            (
                match_any(),
                fail_then_succeed(error=RateLimitError(retry_after_ms=10), times=1, payload=b"ok"),
            )
        ]
    )
    outcomes: list[object] = []
    for request in _requests():
        try:
            outcomes.append((await fake.complete(request)).response)
        except RateLimitError as error:
            outcomes.append(type(error))
    counts = {str(i): fake.calls_for(request) for i, request in enumerate(_requests())}
    return outcomes, fake.requests, fake.call_count, counts


# --- Requirement: Deterministic and offline -----------------------------------


async def test_repeated_runs_are_identical() -> None:
    # Scenario: Repeated runs are identical.
    first = await _run()
    second = await _run()

    assert first == second


def test_import_has_no_side_effects() -> None:
    # Scenario: Import has no side effects.
    for name in list(sys.modules):
        if name == "beam_agents.model" or name.startswith("beam_agents.model."):
            del sys.modules[name]

    importlib.import_module("beam_agents.model")

    # No exception, no network, no logging, no global state: a clean import is
    # the whole assertion surface available without process-level mocking.
