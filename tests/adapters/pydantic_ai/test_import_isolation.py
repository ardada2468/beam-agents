"""Spec: pydantic-ai-adapter / Requirement: PydanticAIAgent runs a Pydantic AI
agent as an activation — the import-isolation scenarios.

The unit environment installs Pydantic AI (the adapter's own tests need it), so
absence is simulated in a subprocess with a meta-path blocker rather than by
uninstalling: the boundary under proof is *what core imports*, not what the
environment happens to contain.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest

# Raises on any import of the Pydantic AI distribution family before
# beam_agents is imported, so a core module that (transitively) imports them
# fails loudly.
_BLOCKER = textwrap.dedent(
    """
    import sys

    class _BlockPydanticAI:
        _blocked = ("pydantic_ai", "pydantic_graph")

        def find_spec(self, fullname, path=None, target=None):
            if fullname.partition(".")[0] in self._blocked:
                raise ModuleNotFoundError(
                    f"import of {fullname!r} blocked for test", name=fullname
                )
            return None

    sys.meta_path.insert(0, _BlockPydanticAI())
    """
)


def _run(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", _BLOCKER + textwrap.dedent(script)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_core_import_works_without_the_extra() -> None:
    # Scenario: Core import works without the extra (first half): `import
    # beam_agents` and the whole non-adapter public surface work with the
    # Pydantic AI distributions unimportable.
    result = _run(
        """
        import beam_agents

        assert beam_agents.RunAgent is not None
        assert beam_agents.AgentConfig is not None
        print("core-import-ok")
        """
    )
    assert result.returncode == 0, result.stderr
    assert "core-import-ok" in result.stdout


def test_pydantic_ai_agent_access_without_extra_names_the_extra() -> None:
    # Scenario: Core import works without the extra (second half): accessing
    # the lazy adapter export raises ImportError naming
    # `beam-agents[pydantic-ai]`.
    result = _run(
        """
        import beam_agents

        try:
            beam_agents.PydanticAIAgent
        except ImportError as exc:
            assert "beam-agents[pydantic-ai]" in str(exc), str(exc)
            print("names-the-extra")
        else:
            raise SystemExit("expected ImportError for PydanticAIAgent without the extra")
        """
    )
    assert result.returncode == 0, result.stderr
    assert "names-the-extra" in result.stdout


def test_pydantic_ai_agent_resolves_with_extra_installed() -> None:
    # With the extra present (as in this environment), the lazy export resolves
    # to the adapter class.
    pytest.importorskip("pydantic_ai")
    import beam_agents  # noqa: PLC0415 - imported here so the blocker tests above stay pure

    assert beam_agents.PydanticAIAgent is not None
