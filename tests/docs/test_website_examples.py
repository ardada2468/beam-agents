"""Execute every example the documentation site publishes.

The site embeds example source by file reference rather than transcribing it,
which means published code cannot drift from the file on disk. This module
closes the other half of the loop: the file on disk cannot drift from a working
runtime either, because a change under `src/` that breaks a published example
fails the required `ci` check.

Discovery is by glob, deliberately. A per-file registration list would let a
new example be added and silently never run — the exact failure this guards
against.

The examples run in the default (offline, no-docker) tier: each uses
`DirectRunner` and `FakeLLM`, so they need no network, no credentials, and no
compose stack. An example needing an optional extra declares it with a
`# requires-extra: <name>` marker and skips with a stated reason when the extra
is absent.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
EXAMPLES_ROOT = REPO_ROOT / "website" / "examples"

_EXTRA_MARKER = re.compile(r"^#\s*requires-extra:\s*(\S+)\s*$", re.MULTILINE)

# The topics the example set must cover. `docs-examples` requires each of these
# to map to at least one file, so an example cannot be deleted without either
# replacing its coverage or amending this list in the same diff.
REQUIRED_TOPICS = {
    "fast path": "fast_path.py",
    "intent and re-injection": "intents_and_resume.py",
    "human-in-the-loop with timeout fallback": "human_in_the_loop.py",
    "all four RunAgent outputs": "four_outputs.py",
    "LangGraph adapter": "langgraph_adapter.py",
    "read-only tools versus side effects": "read_only_tools.py",
}

# Import name per extra, for the skip decision.
_EXTRA_IMPORTS = {"langgraph": "langgraph"}


def _example_files() -> list[Path]:
    return sorted(path for path in EXAMPLES_ROOT.glob("*.py") if not path.name.startswith("_"))


def _required_extra(source: str) -> str | None:
    match = _EXTRA_MARKER.search(source)
    return match.group(1) if match else None


@pytest.mark.parametrize("example", _example_files(), ids=lambda path: path.name)
def test_example_runs_offline(example: Path) -> None:
    """Scenario: every published example runs, offline, with no arguments."""
    source = example.read_text(encoding="utf-8")

    extra = _required_extra(source)
    if extra is not None:
        module = _EXTRA_IMPORTS.get(extra, extra)
        pytest.importorskip(
            module,
            reason=(
                f"{example.name} needs the `{extra}` extra; install it with "
                f"`uv sync --extra {extra}`"
            ),
        )

    result = subprocess.run(
        [sys.executable, str(example)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )

    assert result.returncode == 0, (
        f"website/examples/{example.name} failed (exit {result.returncode}).\n"
        f"Reproduce with:\n    uv run python website/examples/{example.name}\n\n"
        f"stdout:\n{result.stdout[-4000:]}\n\nstderr:\n{result.stderr[-4000:]}"
    )
    # Every example ends by announcing itself, so a silently-empty run (an
    # early return, a swallowed assertion) is a failure rather than a pass.
    assert f"{example.stem}: ok" in result.stdout, (
        f"website/examples/{example.name} exited 0 but never printed its "
        f'completion marker "{example.stem}: ok"; stdout:\n{result.stdout[-2000:]}'
    )


def test_examples_have_module_docstrings() -> None:
    """Scenario: every example states what it demonstrates."""
    missing = [
        path.name
        for path in _example_files()
        if not path.read_text(encoding="utf-8").lstrip().startswith('"""')
    ]
    assert not missing, f"examples without a module docstring: {missing}"


def test_required_topics_are_covered() -> None:
    """Scenario: the required example topics all map to a file."""
    present = {path.name for path in _example_files()}
    missing = {
        topic: filename for topic, filename in REQUIRED_TOPICS.items() if filename not in present
    }
    assert not missing, (
        "the example set no longer covers required topics "
        f"(topic -> expected file): {missing}. Add the example back, or amend "
        "REQUIRED_TOPICS and the docs-examples spec in the same change."
    )


def test_examples_avoid_undeclared_private_imports() -> None:
    """Scenario: examples import only from the documented module surface.

    `beam_agents._protos` is on the allowed list despite its underscore: an
    `AgentEnvelope` is the input element type `RunAgent` requires, so a
    pipeline cannot be constructed without it. That gap between the project's
    stated public surface and what using the runtime actually takes is
    disclosed on the site's API page rather than hidden here.
    """
    allowed = {
        "beam_agents",
        "beam_agents._protos",
        "beam_agents.adapters.langgraph",
        "beam_agents.core.agent",
        "beam_agents.core.context",
        "beam_agents.hitl",
        "beam_agents.model.client",
        "beam_agents.model.fake",
        "beam_agents.tools",
        "beam_agents.tools.errors",
    }
    pattern = re.compile(r"^from\s+(beam_agents[\w.]*)\s+import", re.MULTILINE)

    offenders: dict[str, set[str]] = {}
    for path in _example_files():
        modules = set(pattern.findall(path.read_text(encoding="utf-8")))
        extra = modules - allowed
        if extra:
            offenders[path.name] = extra

    assert not offenders, (
        f"examples import undeclared modules: {offenders}. Either use a listed "
        "module, or add it to this allowlist AND to DOCUMENTED_INTERNAL in "
        "scripts/gen_api_reference.py so the site discloses it."
    )
