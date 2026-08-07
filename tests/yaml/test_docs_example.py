"""Tests that `docs/yaml.md` documents only the surface the code actually ships.

Requirement: "An end-to-end YAML pipeline runs RunAgent offline with FakeLLM",
scenario "The documented example matches the shipped surface". Every YAML block
in the page is parsed, and every transform name, fully-qualified constructor
path, and config key it uses is checked against the shipped constructor and the
packaged provider listing.
"""

from __future__ import annotations

import inspect
import re
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pytest

import yaml
from beam_agents.yaml import PROVIDER_LISTING, run_agent
from beam_agents.yaml._config import CONFIG_KEYS, HITL_KEYS

DOC = Path(__file__).resolve().parents[2] / "docs" / "yaml.md"
CONSTRUCTOR_PATH = "beam_agents.yaml.run_agent"
_FENCE = re.compile(r"```yaml\n(?P<body>.*?)```", re.DOTALL)


def _blocks() -> list[Any]:
    assert DOC.is_file(), f"{DOC} is missing"
    bodies = [match["body"] for match in _FENCE.finditer(DOC.read_text(encoding="utf-8"))]
    assert bodies, "docs/yaml.md publishes no YAML blocks"
    return [yaml.safe_load(body) for body in bodies]


def _run_agent_configs(block: Any) -> list[dict[str, Any]]:
    if not isinstance(block, dict):
        return []
    pipeline = block.get("pipeline", block)
    transforms = pipeline.get("transforms", []) if isinstance(pipeline, dict) else []
    return [
        t.get("config", {}) or {}
        for t in transforms
        if isinstance(t, dict) and t.get("type") == "RunAgent"
    ]


def test_every_yaml_block_in_the_page_parses() -> None:
    assert all(block is not None for block in _blocks())


def test_the_documented_provider_block_names_the_shipped_constructor() -> None:
    declarations = [
        spec
        for block in _blocks()
        if isinstance(block, dict)
        for spec in block.get("providers", [])
    ]
    assert declarations, "docs/yaml.md shows no `providers:` block"
    for spec in declarations:
        if "include" in spec:  # the packaged-listing form, checked below
            continue
        assert spec["type"] == "python"
        for name, path in spec["transforms"].items():
            assert name == "RunAgent"
            assert path == CONSTRUCTOR_PATH


def test_the_documented_package_pin_matches_the_shipped_version() -> None:
    text = DOC.read_text(encoding="utf-8")
    # The pin has to admit PEP 440 pre-releases: an alpha/beta/rc ships the
    # same documented providers block, and a bare `\d+\.\d+\.\d+` would capture
    # `1.0.0` out of `1.0.0a1` and then compare it against the real version.
    pinned = re.findall(r"beam-agents==(\d+\.\d+\.\d+(?:(?:a|b|rc)\d+)?)", text)
    assert pinned, "docs/yaml.md must pin the installable package in its providers block"
    assert set(pinned) == {version("beam-agents")}


def test_every_documented_config_key_is_one_the_constructor_accepts() -> None:
    configs = [config for block in _blocks() for config in _run_agent_configs(block)]
    assert configs, "docs/yaml.md shows no RunAgent transform"
    for config in configs:
        unknown = set(config) - set(CONFIG_KEYS)
        assert not unknown, f"docs/yaml.md uses unknown config key(s): {sorted(unknown)}"
        hitl = config.get("hitl", {})
        assert not set(hitl) - set(HITL_KEYS), f"unknown hitl key(s) in {sorted(hitl)}"


def test_the_documented_key_table_covers_the_whole_constructor_surface() -> None:
    # Every accepted key is documented, so the page cannot fall behind the code.
    text = DOC.read_text(encoding="utf-8")
    for key in (*CONFIG_KEYS, *HITL_KEYS):
        assert f"`{key}`" in text, f"docs/yaml.md does not document the `{key}` config key"


def test_the_packaged_provider_listing_is_shown_in_the_page() -> None:
    listing = yaml.safe_load(Path(PROVIDER_LISTING).read_text(encoding="utf-8"))
    assert [spec["transforms"] for spec in listing] == [{"RunAgent": CONSTRUCTOR_PATH}]
    included = [
        spec
        for block in _blocks()
        if isinstance(block, dict)
        for spec in block.get("providers", [])
        if "include" in spec
    ]
    assert included, "docs/yaml.md must show the `include:` form of the packaged listing"
    assert all(spec["include"].endswith(Path(PROVIDER_LISTING).name) for spec in included)


@pytest.mark.parametrize("key", sorted(CONFIG_KEYS))
def test_config_keys_are_exactly_the_constructor_keyword_parameters(key: str) -> None:
    parameters = inspect.signature(run_agent).parameters
    assert key in parameters
    assert parameters[key].kind is inspect.Parameter.KEYWORD_ONLY
