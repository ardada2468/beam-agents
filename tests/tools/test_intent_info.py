"""IntentInfo and opt-in intent-parameter recognition for the tool-registry
capability.

Covers the "IntentInfo carries intent identity to opt-in tools" and
"Registration recognizes an opt-in keyword-only intent parameter" requirements
(change ``add-intent-info-for-tools``).

This module deliberately keeps ``from __future__ import annotations`` so every
inline ``@tool`` definition exercises the string-annotation recognition path;
the evaluated-annotation path is exercised via ``exec`` without the future
import.
"""

from __future__ import annotations

import ast
import dataclasses
from pathlib import Path

import pytest
from pydantic import ValidationError

import beam_agents.tools
from beam_agents.tools import IntentInfo, ToolDefinitionError, tool


def _intent_info(**overrides: object) -> IntentInfo:
    fields: dict[str, object] = {
        "intent_id": "id-1",
        "entity_key": b"k",
        "seq": 0,
        "step_index": 1,
        "attempt": 0,
    }
    fields.update(overrides)
    return IntentInfo(**fields)  # type: ignore[arg-type]


def test_intent_info_is_frozen_and_hashable() -> None:
    # Scenario: IntentInfo is frozen and hashable.
    info = _intent_info()
    with pytest.raises(dataclasses.FrozenInstanceError):
        info.intent_id = "other"  # type: ignore[misc]
    assert info == _intent_info()
    assert {info: "seen"}[_intent_info()] == "seen"


def test_intent_info_imports_standalone() -> None:
    # Scenario: IntentInfo imports standalone. Pinned statically over the whole
    # `tools/` package (the pattern of the effector boundary test): none of its
    # modules may import Beam, the effector, or generated protobuf modules.
    tools_dir = Path(beam_agents.tools.__file__).parent
    offenders: list[str] = []
    for path in sorted(tools_dir.glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                names = [node.module]
            for name in names:
                root = name.split(".")[0]
                if (
                    root == "apache_beam"
                    or name.startswith("beam_agents.effector")
                    or "_protos" in name
                    or "_pb2" in name
                ):
                    offenders.append(f"{path.name}: {name}")
    assert offenders == [], f"tools/ must stay importable standalone: {offenders}"


def test_declaring_side_effect_tool_is_marked_and_schema_excludes_intent() -> None:
    # Scenario: A declaring side-effect tool is marked and its schema excludes
    # intent.
    @tool(side_effect=True)
    def charge(key: str, *, intent: IntentInfo) -> str:
        return f"{key}:{intent.intent_id}"

    assert charge.accepts_intent is True
    validated = charge.argument_model.model_validate({"key": "k"})
    assert validated.model_dump() == {"key": "k"}
    parameters = charge.schema["parameters"]
    assert isinstance(parameters, dict)
    assert set(parameters["properties"]) == {"key"}
    assert parameters["required"] == ["key"]


def test_non_declaring_tool_is_untouched() -> None:
    # Scenario: A non-declaring tool is untouched.
    @tool(side_effect=True)
    def send(address: str, amount_cents: int = 5) -> str:
        return address

    assert send.accepts_intent is False
    parameters = send.schema["parameters"]
    assert isinstance(parameters, dict)
    assert set(parameters["properties"]) == {"address", "amount_cents"}
    assert parameters["required"] == ["address"]
    assert send.argument_model.model_validate({"address": "a"}).model_dump() == {
        "address": "a",
        "amount_cents": 5,
    }


def test_positional_intent_info_parameter_is_rejected() -> None:
    # Scenario: A positional IntentInfo parameter is rejected at decoration
    # time.
    with pytest.raises(ToolDefinitionError, match="intent"):

        @tool(side_effect=True)
        def charge(intent: IntentInfo, key: str) -> str:
            return key


def test_misnamed_intent_info_parameter_is_rejected() -> None:
    # Scenario: A misnamed IntentInfo parameter is rejected at decoration time.
    with pytest.raises(ToolDefinitionError, match="identity"):

        @tool(side_effect=True)
        def charge(key: str, *, identity: IntentInfo) -> str:
            return key


def test_read_only_tool_declaring_intent_is_rejected() -> None:
    # Scenario: A read-only tool declaring intent is rejected at decoration
    # time.
    with pytest.raises(ToolDefinitionError, match="side_effect"):

        @tool
        def lookup(key: str, *, intent: IntentInfo) -> str:
            return key


def test_intent_parameter_with_other_annotation_stays_ordinary() -> None:
    # Scenario: An intent parameter with a different annotation stays an
    # ordinary argument.
    @tool(side_effect=True)
    def notify(intent: str) -> str:
        return intent

    assert notify.accepts_intent is False
    parameters = notify.schema["parameters"]
    assert isinstance(parameters, dict)
    assert parameters["properties"]["intent"]["type"] == "string"
    assert parameters["required"] == ["intent"]


def test_evaluated_annotations_are_recognized() -> None:
    # Scenario: String annotations are recognized — the inverse direction: the
    # rest of this module uses `from __future__ import annotations`, so this
    # test compiles a tool WITHOUT it, making the annotations live class
    # objects, and recognition must behave identically.
    namespace: dict[str, object] = {"IntentInfo": IntentInfo}
    exec(
        "def charge(key: str, *, intent: IntentInfo) -> str:\n    return key",
        namespace,
    )
    charge = tool(side_effect=True)(namespace["charge"])  # type: ignore[arg-type]
    assert charge.accepts_intent is True
    parameters = charge.schema["parameters"]
    assert isinstance(parameters, dict)
    assert set(parameters["properties"]) == {"key"}


def test_string_annotation_is_recognized_when_hints_cannot_be_evaluated() -> None:
    # Scenario: String annotations are recognized — the fallback path: the
    # tool's module never imported IntentInfo and its return annotation is
    # unresolvable, so full hint evaluation fails, and the literal-string
    # comparison must still recognize `intent`.
    namespace: dict[str, object] = {}
    exec(
        "from __future__ import annotations\n"
        "def charge(key: str, *, intent: IntentInfo) -> UnresolvableRef:\n"
        "    return 'x'",
        namespace,
    )
    charge = tool(side_effect=True)(namespace["charge"])  # type: ignore[arg-type]
    assert charge.accepts_intent is True
    parameters = charge.schema["parameters"]
    assert isinstance(parameters, dict)
    assert set(parameters["properties"]) == {"key"}


def test_intent_key_in_arguments_is_rejected_not_shadowed() -> None:
    # Scenario (effector-execution): An intent key inside args_json is
    # rejected, not shadowed — enforced by the argument model itself.
    @tool(side_effect=True)
    def charge(key: str, *, intent: IntentInfo) -> str:
        return key

    with pytest.raises(ValidationError, match="intent"):
        charge.argument_model.model_validate({"key": "k", "intent": {"intent_id": "spoof"}})
