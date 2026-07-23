import ast
from pathlib import Path

import beam_agents


def test_import_succeeds() -> None:
    assert beam_agents is not None


def test_public_surface_is_empty() -> None:
    # `beam_agents/__init__.py` must re-export nothing. Inspect the source
    # rather than `dir(beam_agents)`: the runtime namespace picks up submodules
    # once imported (e.g. `beam_agents.core`) and instrumentation artifacts
    # (mutmut injects a `MutantDict`), none of which are re-exports. A module
    # docstring is allowed; no imports, assignments, or definitions.
    assert beam_agents.__file__ is not None
    source = Path(beam_agents.__file__).read_text()
    body = ast.parse(source).body
    offenders = [
        node
        for node in body
        if not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant))
    ]
    assert offenders == [], "beam_agents/__init__.py must not define or re-export any names"
