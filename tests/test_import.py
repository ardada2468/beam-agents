import ast
from pathlib import Path

import beam_agents
from beam_agents.core.agent import StreamAgent as CanonicalStreamAgent
from beam_agents.hitl import HITL_TIMEOUT_OUTPUT
from beam_agents.tools.registry import tool as canonical_tool


def test_import_succeeds() -> None:
    assert beam_agents is not None


def test_public_surface_is_run_agent_config_outputs_and_hitl_policy() -> None:
    # `beam_agents/__init__.py` re-exports exactly the transform surface named
    # in project.md, plus the HITL policy types a caller must name to configure
    # a timeout route, the caller-side key-sharding utilities, and — since the
    # 1.0 API freeze resolved the constitution-versus-code drift — the two names
    # project.md always promised: the `tool` authoring decorator and the
    # `StreamAgent` protocol adapter authors implement.
    assert beam_agents.RunAgent is not None
    assert beam_agents.AgentConfig is not None
    assert beam_agents.RunAgentOutputs is not None
    assert beam_agents.ShardKeys is not None
    assert beam_agents.shard_key is not None
    assert beam_agents.unshard_key is not None
    assert set(beam_agents.__all__) == {
        "AdkAgent",
        "AgentConfig",
        "Deny",
        "Drop",
        "Escalate",
        "FallbackContext",
        "HitlPolicy",
        "LangGraphAgent",
        "PydanticAIAgent",
        "RunAgent",
        "RunAgentOutputs",
        "ShardKeys",
        "StreamAgent",
        "shard_key",
        "tool",
        "unshard_key",
    }


def test_the_names_project_md_promises_resolve_eagerly() -> None:
    # A user following the constitution runs `from beam_agents import tool,
    # StreamAgent`; before the freeze that raised AttributeError. Neither name
    # drags in an optional extra, so both resolve without the lazy
    # `__getattr__` path the adapter classes need.
    assert beam_agents.tool is canonical_tool
    assert beam_agents.StreamAgent is CanonicalStreamAgent


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
