"""`AgentConfig`'s numeric runtime knobs: the positivity boundary, runner-free.

Split out of `test_transform.py` rather than duplicated. `test_transform.py`
drives `TestPipeline`/`TestStream` and is therefore deselected under mutmut (see
the `[tool.mutmut]` comment in `pyproject.toml`), so while these scenarios lived
there nothing could kill a mutant in `transform._require_positive` — the shared
validator every knob goes through. The `AgentConfig` construction tests need no
runner at all, so they belong in a file the mutation selection can reach; the
sink-URI validation tests deliberately stay behind, because reaching
`DefaultSinkResolver.validate`/`resolve`/`_parse` from inside the selection
reclassifies their cross-language writer arms from "no tests" to "survived"
(recorded in `mutation-baseline.toml`).

Beam-free by construction: no pipeline, no sink URI, no store URI.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from beam_agents.core.transform import AgentConfig
from tests.core._dofn_helpers import make_pong_provider

# --- Requirement: AgentConfig bundles runtime configuration and validates ------

_KNOBS = ["activation_timeout_s", "ttl_ms", "cancel_grace_s"]


def test_valid_config_constructs_and_is_immutable() -> None:
    # Scenario: Valid config constructs.
    config = AgentConfig(provider_factory=make_pong_provider)
    assert config.activation_timeout_s == 30.0
    assert config.ttl_ms == 3_600_000
    assert config.cancel_grace_s == 5.0
    with pytest.raises(FrozenInstanceError):
        config.ttl_ms = 1  # type: ignore[misc]


@pytest.mark.parametrize("knob", _KNOBS)
@pytest.mark.parametrize("bad_value", [0, -1])
def test_non_positive_knob_is_rejected(knob: str, bad_value: float) -> None:
    # Scenario: Non-positive runtime knob is rejected. Zero is the boundary the
    # spec pins ("zero or negative"), so it must be rejected on its own and not
    # merely inherit a negative case's verdict.
    kwargs: dict[str, Any] = {knob: bad_value}
    with pytest.raises(ValueError, match=knob):
        AgentConfig(provider_factory=make_pong_provider, **kwargs)


@pytest.mark.parametrize("knob", _KNOBS)
@pytest.mark.parametrize("bad_value", [0, -1])
def test_the_rejection_names_the_knob_and_the_offending_value(knob: str, bad_value: float) -> None:
    # "an actionable message" is the requirement's word: the operator reading a
    # construction-time traceback has to see which knob and which value, not
    # just that something was out of range.
    kwargs: dict[str, Any] = {knob: bad_value}
    with pytest.raises(ValueError) as exc_info:
        AgentConfig(provider_factory=make_pong_provider, **kwargs)

    assert str(exc_info.value) == f"AgentConfig.{knob} must be positive, got {bad_value!r}"


@pytest.mark.parametrize("knob", _KNOBS)
def test_the_smallest_positive_knob_is_accepted(knob: str) -> None:
    # The other side of the same boundary. "Positive" means `> 0`, so 1 is
    # inside the range: a validator that refused it would reject a legitimate
    # (if aggressive) configuration at the construction site, and nothing in
    # the non-positive cases above can tell the two thresholds apart.
    kwargs: dict[str, Any] = {knob: 1}

    config = AgentConfig(provider_factory=make_pong_provider, **kwargs)

    assert getattr(config, knob) == 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
