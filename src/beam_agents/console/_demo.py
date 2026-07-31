"""A pipeline that produces the runtime's full event vocabulary.

An empty console teaches nothing. Someone evaluating this library runs one
command and needs to land on a screen that shows what the runtime records —
which means the demo has to produce not just successful activations but the
*interesting* ones: a suspension awaiting approval, a tool that fails, a cache
hit, a budget exhausted, a TTL that wipes a live suspension, a dead-lettered
intent. Those are the records the error views and the approval queue exist to
render, and a demo that emits only happy-path completions leaves most of the UI
looking broken.

Runs on ``DirectRunner`` over the fake provider (``model/fake.py``), so it needs
no API key, no broker, and no network. Deterministic by construction — the
runtime's identity is ``uuid5`` over ``(entity_key, seq)`` and the fake provider
replays scripted responses — so the same seed produces the same console every
time, which is what makes the screenshots in the docs reproducible.

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from beam_agents.console._store import ConsoleStore

__all__ = [
    "DEFAULT_ENTITY_COUNT",
    "SCENARIOS",
    "main",
    "run_demo",
]

# Every scenario the demo drives, each chosen because some part of the UI is
# unreachable without it.
SCENARIOS = (
    "completion",
    "multi_tool",
    "cache_hit",
    "suspension_approved",
    "suspension_denied",
    "suspension_timeout",
    "tool_error",
    "activation_error",
    "budget_exceeded",
    "orphaned_result",
    "intent_dead_letter",
    "batch_overflow",
)

DEFAULT_ENTITY_COUNT = 12


def run_demo(
    *,
    console: str | None = None,
    store: ConsoleStore | None = None,
    entities: int = DEFAULT_ENTITY_COUNT,
    scenarios: tuple[str, ...] = SCENARIOS,
    seed: int = 0,
    loop: bool = False,
    **options: Any,
) -> int:
    """Run the demo pipeline; return the number of activations produced.

    Delivers either to a running console over ``console://`` or straight into an
    in-process ``store``. The second path is what makes the demo usable as test
    data without standing a server up.
    """
    raise NotImplementedError


def main(argv: list[str] | None = None) -> int:
    """Entry point for ``python -m beam_agents.console._demo``."""
    raise NotImplementedError
