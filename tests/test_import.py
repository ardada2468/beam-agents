import ast
from pathlib import Path

import beam_agents


def test_import_succeeds() -> None:
    assert beam_agents is not None


def test_public_surface_is_run_agent_config_and_outputs() -> None:
    # `beam_agents/__init__.py` re-exports exactly RunAgent, AgentConfig, and
    # RunAgentOutputs — the public API surface named in project.md.
    assert beam_agents.RunAgent is not None
    assert beam_agents.AgentConfig is not None
    assert beam_agents.RunAgentOutputs is not None
    assert set(beam_agents.__all__) == {"AgentConfig", "RunAgent", "RunAgentOutputs"}


def test_import_has_no_side_effects() -> None:
    # Importing the package must not mutate global state (e.g. Beam's coder
    # registry): `beam_agents/__init__.py` only imports and re-exports names.
    # Inspect the source rather than `dir(beam_agents)`, which picks up
    # submodules once imported and instrumentation artifacts (mutmut injects a
    # `MutantDict`), neither of which are re-exports.
    assert beam_agents.__file__ is not None
    source = Path(beam_agents.__file__).read_text()
    body = ast.parse(source).body
    offenders = [
        node
        for node in body
        if not isinstance(
            node,
            (ast.Expr, ast.ImportFrom, ast.Import, ast.Assign, ast.AnnAssign),
        )
    ]
    assert offenders == [], "beam_agents/__init__.py must only import and re-export names"
