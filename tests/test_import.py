import beam_agents


def test_import_succeeds() -> None:
    assert beam_agents is not None


def test_public_surface_is_empty() -> None:
    public_names = [name for name in dir(beam_agents) if not name.startswith("_")]
    assert public_names == []
