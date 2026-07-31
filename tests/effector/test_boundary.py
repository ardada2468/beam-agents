"""The effector's import boundary for the effector-service capability.

Covers the "The effector is a standalone service with no pipeline dependency"
requirement: nothing in the effector's own import closure touches Beam or
``beam_agents.core``, the optional client libraries stay unimported until an
adapter is constructed, and no effector symbol reaches the public API.

The closure is what is testable and what matters. Importing
``beam_agents.effector`` through the normal machinery also executes the parent
``beam_agents/__init__.py``, which re-exports the pipeline surface and *does*
import Beam — a property of the package layout, not of the effector. So the
runtime checks below stub the parent package (exactly as a standalone
deployment of these modules would see it) and the static check pins the closure
directly.
"""

from __future__ import annotations

import ast
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

import beam_agents

_EFFECTOR_DIR = Path(beam_agents.__file__).parent / "effector"

# Roots the effector must never import: Beam and the pipeline runtime (it runs
# outside both), plus the four optional transport/dedup clients (they belong to
# the `effector` extra and are absent from the offline unit lane).
_BLOCKED = (
    "apache_beam",
    "aiokafka",
    "redis",
    "google.cloud.pubsub_v1",
    "google.cloud.bigtable",
)

# Modules the effector may import from its own package. `core` is absent by
# design: it is the Beam-facing half of the runtime. `intent_signing` is
# shared with `actions/` on purpose — the signer and the verifier are two ends
# of one definition — and is Beam-free and stdlib-only for exactly that reason.
_ALLOWED_INTERNAL = (
    "beam_agents._protos",
    "beam_agents.effector",
    "beam_agents.hitl",
    "beam_agents.intent_signing",
    "beam_agents.tools",
)

_PREAMBLE = textwrap.dedent(
    f"""
    import sys, types

    BLOCKED = {_BLOCKED!r}

    class _Blocker:
        # Raising (rather than returning None) makes the block absolute: a
        # module that is installed in this environment is still unimportable.
        def find_spec(self, fullname, path=None, target=None):
            for blocked in BLOCKED:
                if fullname == blocked or fullname.startswith(blocked + "."):
                    raise ImportError(f"blocked by the effector boundary test: {{fullname}}")
            return None

    sys.meta_path.insert(0, _Blocker())

    # Stand in for `beam_agents/__init__.py`, which re-exports the Beam-facing
    # pipeline surface. A standalone effector deployment imports these modules
    # without ever evaluating it; this is that view.
    _pkg = types.ModuleType("beam_agents")
    _pkg.__path__ = [{str(_EFFECTOR_DIR.parent)!r}]
    sys.modules["beam_agents"] = _pkg
    """
)


def _run_with_blocked_imports(body: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", _PREAMBLE + textwrap.dedent(body)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def _effector_modules() -> list[Path]:
    return sorted(_EFFECTOR_DIR.glob("*.py"))


def test_no_effector_module_imports_beam_or_the_pipeline_runtime() -> None:
    # Scenario: The package imports with Beam unavailable — pinned statically,
    # so the closure cannot regress even where an import is lazy or guarded.
    offenders: list[str] = []
    for path in _effector_modules():
        for node in ast.walk(ast.parse(path.read_text())):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                names = [node.module]
            for name in names:
                root = name.split(".")[0]
                if root == "apache_beam" or name.startswith("beam_agents.core"):
                    offenders.append(f"{path.name}: {name}")
                if root == "beam_agents" and not name.startswith(_ALLOWED_INTERNAL):
                    offenders.append(f"{path.name}: {name}")

    assert offenders == [], f"effector modules must not import Beam or the runtime: {offenders}"


def test_the_package_imports_with_beam_unavailable() -> None:
    # Scenario: The package imports with Beam unavailable.
    result = _run_with_blocked_imports(
        """
        import importlib
        import pkgutil

        import beam_agents.effector as pkg

        imported = []
        for info in pkgutil.iter_modules(pkg.__path__):
            importlib.import_module(f"{pkg.__name__}.{info.name}")
            imported.append(info.name)

        assert imported, "no effector submodules were discovered"
        assert "apache_beam" not in sys.modules, "the effector imported Beam"
        assert "beam_agents.core" not in sys.modules, "the effector imported the runtime"
        print("OK", sorted(imported))
        """
    )
    assert result.returncode == 0, result.stderr


def test_the_package_imports_with_no_optional_client_libraries_installed() -> None:
    # Scenario: The package imports with no optional client libraries installed.
    result = _run_with_blocked_imports(
        """
        from beam_agents.effector import EffectorConfig

        config = EffectorConfig(
            intents_from="kafka://localhost:9092/intents",
            results_to="kafka://localhost:9092/results",
            approvals_to="kafka://localhost:9092/approvals",
            dedup="redis://localhost:6379",
            consumer_group="effector",
        )
        config.validate()

        for name in ("aiokafka", "redis"):
            assert name not in sys.modules, f"{name} was imported"
        print("OK")
        """
    )
    assert result.returncode == 0, result.stderr


def test_a_missing_client_surfaces_only_when_its_adapter_is_constructed() -> None:
    # Scenario: The package imports with no optional client libraries installed
    # — the ImportError is deferred to adapter construction, not import time.
    result = _run_with_blocked_imports(
        """
        from beam_agents.effector.dedup import RedisDedupStore

        try:
            RedisDedupStore("redis://localhost:6379")
        except ImportError:
            print("OK")
        else:
            raise AssertionError("expected ImportError from the adapter constructor")
        """
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    ("module", "symbol", "args"),
    [
        ("sources", "KafkaIntentSource", ("localhost:9092", "t", "g")),
        ("sinks", "KafkaMessageSink", ("localhost:9092", "t")),
    ],
)
def test_every_transport_adapter_defers_its_client_import(
    module: str, symbol: str, args: tuple[str, ...]
) -> None:
    result = _run_with_blocked_imports(
        f"""
        from beam_agents.effector.{module} import {symbol}

        try:
            {symbol}(*{args!r})
        except ImportError:
            print("OK")
        else:
            raise AssertionError("expected ImportError from the adapter constructor")
        """
    )
    assert result.returncode == 0, result.stderr


def test_the_effector_is_absent_from_the_public_api() -> None:
    # Scenario: The effector is absent from the public API.
    assert not [name for name in beam_agents.__all__ if "effector" in name.lower()]
    assert not [name for name in beam_agents.__all__ if name.startswith("Effector")]
