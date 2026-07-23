from types import ModuleType

import beam_agents


def test_import_succeeds() -> None:
    assert beam_agents is not None


def test_public_surface_is_empty() -> None:
    # `beam_agents/__init__.py` must re-export nothing. Submodules (e.g.
    # `beam_agents.core`) may surface as attributes once imported elsewhere in
    # the test session; those are not re-exports, so exclude module objects.
    public_names = [
        name
        for name in dir(beam_agents)
        if not name.startswith("_") and not isinstance(getattr(beam_agents, name), ModuleType)
    ]
    assert public_names == []
