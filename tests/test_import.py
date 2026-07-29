import ast
from pathlib import Path

import beam_agents
from beam_agents.hitl import HITL_TIMEOUT_OUTPUT


def test_import_succeeds() -> None:
    assert beam_agents is not None


def test_public_surface_is_run_agent_config_outputs_and_hitl_policy() -> None:
    # `beam_agents/__init__.py` re-exports exactly the transform surface named
    # in project.md plus the HITL policy types a caller must name to configure
    # a timeout route.
    assert beam_agents.RunAgent is not None
    assert beam_agents.AgentConfig is not None
    assert beam_agents.RunAgentOutputs is not None
    assert set(beam_agents.__all__) == {
        "AgentConfig",
        "Deny",
        "Drop",
        "Escalate",
        "FallbackContext",
        "HitlPolicy",
        "LangGraphAgent",
        "RunAgent",
        "RunAgentOutputs",
    }


def test_hitl_timeout_output_keeps_its_value() -> None:
    # The default Deny payload is the byte string the runtime has always
    # emitted on a HITL timeout; changing it would silently break consumers.
    assert HITL_TIMEOUT_OUTPUT == b"__hitl_timeout__"


def test_import_has_no_side_effects() -> None:
    # Importing the package must not mutate global state (e.g. Beam's coder
    # registry): `beam_agents/__init__.py` only imports and re-exports names.
    # Inspect the source rather than `dir(beam_agents)`, which picks up
    # submodules once imported and instrumentation artifacts (mutmut injects a
    # `MutantDict`), neither of which are re-exports.
    #
    # The single sanctioned function is the module-level `__getattr__` that
    # lazily resolves optional-extra adapter classes (PEP 562): defining it has
    # no import-time side effect, and it is what keeps adapter frameworks out
    # of the core import graph.
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
        and not (isinstance(node, ast.FunctionDef) and node.name == "__getattr__")
    ]
    assert offenders == [], "beam_agents/__init__.py must only import and re-export names"
