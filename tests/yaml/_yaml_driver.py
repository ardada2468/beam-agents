"""A deliberately small Beam YAML driver for the offline end-to-end suite.

Beam's own ``apache_beam.yaml`` package is **not importable in the offline unit
lane**: it needs Beam's ``yaml`` extra (``jinja2``, ``pandas``,
``virtualenv-clone``, ``js2py``, ...), which the locked test environment does
not carry — see the change's ``tasks.md`` Revision 1. This harness therefore
executes *the same document the docs publish*, mirroring the three Beam YAML
mechanisms this change actually couples to. Each was read out of the installed
Beam source (2.72.0) and is pinned against drift by
``test_yaml_e2e.py::test_beam_yaml_contract_still_matches_this_driver``:

1. ``providers: [{type: python, transforms: {Name: "pkg.mod.constructor"}}]``
   **with no** ``packages:`` resolves each fully-qualified constructor
   in-process through
   :meth:`apache_beam.utils.python_callable.PythonCallableWithSource.load_from_source`
   (``apache_beam/yaml/yaml_provider.py::python`` → ``InlineProvider``).
2. A provider transform is constructed as ``factory(**config)``
   (``apache_beam/yaml/yaml_provider.py::InlineProvider.create_transform``).
3. A transform whose ``expand`` returns a ``dict[str, PCollection]`` exposes
   those keys as named outputs, addressed downstream as
   ``TransformName.output_name``
   (``apache_beam/yaml/yaml_transform.py::expand_leaf_transform`` and
   ``Scope.get_pcollection``).

Only the transform vocabulary the shipped example uses is supported: ``Create``
(the offline stand-in for a real source) plus whatever the document's
``providers:`` block declares. Anything else raises, so the harness can never
quietly paper over a document the real expander would reject.
"""

from __future__ import annotations

from typing import Any

import apache_beam as beam
from apache_beam.pvalue import Row
from apache_beam.utils.python_callable import PythonCallableWithSource

import yaml

# The mechanisms above, as source-level fingerprints the drift guard asserts on.
BEAM_PYTHON_PROVIDER_TYPE = "python"
BEAM_INLINE_CONSTRUCTION = "self._transform_factories[type](**args)"
BEAM_DICT_OUTPUT_RETURN = "if isinstance(outputs, dict):"

Outputs = dict[str, beam.pvalue.PCollection]


def load_document(document: str) -> dict[str, Any]:
    """Parse a Beam YAML pipeline document with PyYAML, as Beam itself does."""
    parsed = yaml.safe_load(document)
    if not isinstance(parsed, dict) or "pipeline" not in parsed:
        raise ValueError("document must be a mapping with a `pipeline:` block")
    return parsed


def build_providers(document: dict[str, Any]) -> dict[str, Any]:
    """Resolve the document's ``providers:`` block to ``{name: constructor}``."""
    factories: dict[str, Any] = {}
    for spec in document.get("providers", []):
        if spec.get("type") != BEAM_PYTHON_PROVIDER_TYPE:
            raise ValueError(f"unsupported provider type {spec.get('type')!r}")
        if spec.get("config", {}).get("packages"):
            raise ValueError(
                "a `packages:` provider installs into a fresh venv, which the "
                "offline lane cannot do; the in-repo document declares none"
            )
        for name, path in spec["transforms"].items():
            factories[name] = PythonCallableWithSource.load_from_source(path)
    return factories


def expand_document(pipeline: beam.Pipeline, document: str) -> dict[str, Outputs]:
    """Expand `document` onto `pipeline`, returning ``{name: {output: pcoll}}``."""
    parsed = load_document(document)
    factories = build_providers(parsed)
    produced: dict[str, Outputs] = {}
    for spec in parsed["pipeline"]["transforms"]:
        name = spec.get("name", spec["type"])
        config = spec.get("config", {}) or {}
        if spec["type"] == "Create":
            result = pipeline | name >> beam.Create(
                [Row(**element) for element in config["elements"]]
            )
        else:
            if spec["type"] not in factories:
                raise ValueError(f"no provider declares transform type {spec['type']!r}")
            result = _resolve_input(produced, spec["input"]) | name >> factories[spec["type"]](
                **config
            )
        produced[name] = _named_outputs(result)
    return produced


def _named_outputs(result: object) -> Outputs:
    """Mirror ``expand_leaf_transform``'s output-naming rules (mechanism 3)."""
    if isinstance(result, dict):
        return dict(result)
    if isinstance(result, beam.pvalue.PCollection):
        return {"out": result}
    raise ValueError(f"transform returned an unexpected type {type(result)}")


def _resolve_input(produced: dict[str, Outputs], reference: str) -> beam.pvalue.PCollection:
    """Mirror ``Scope.get_pcollection``'s ``Name`` / ``Name.output`` addressing."""
    if "." in reference:
        name, _, output = reference.rpartition(".")
        outputs = produced[name]
        if output not in outputs:
            raise ValueError(f"unknown output {output!r}; {name} has {sorted(outputs)}")
        return outputs[output]
    outputs = produced[reference]
    if len(outputs) != 1:
        raise ValueError(f"ambiguous output: {reference} has outputs {sorted(outputs)}")
    return next(iter(outputs.values()))
