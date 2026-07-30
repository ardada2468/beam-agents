"""Spec: adk-adapter / Requirement: AdkAgent runs an ADK agent as an activation
— the import-isolation scenarios.

The unit environment installs ADK (the adapter's own tests need it), so absence
is simulated in a subprocess with a meta-path blocker rather than by
uninstalling: the boundary under proof is *what core imports*, not what the
environment happens to contain.

The blocker matches full dotted prefixes (`google.adk`, `google.genai`), not the
top-level name: `google` is a namespace package core installs already provide
(google-cloud dependencies), so blocking it wholesale would prove nothing about
the ADK boundary and would break `apache-beam[gcp]`.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest

_BLOCKER = textwrap.dedent(
    """
    import sys

    class _BlockAdk:
        _blocked = ("google.adk", "google.genai")

        def find_spec(self, fullname, path=None, target=None):
            if any(fullname == p or fullname.startswith(p + ".") for p in self._blocked):
                raise ModuleNotFoundError(
                    f"import of {fullname!r} blocked for test", name=fullname
                )
            return None

    sys.meta_path.insert(0, _BlockAdk())
    """
)


def _run(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", _BLOCKER + textwrap.dedent(script)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_core_import_works_without_the_extra() -> None:
    # Scenario: Core import works without the extra (first half): `import
    # beam_agents` and the whole non-adapter public surface work with the ADK
    # distributions unimportable.
    result = _run(
        """
        import beam_agents

        assert beam_agents.RunAgent is not None
        assert beam_agents.AgentConfig is not None
        print("core-import-ok")
        """
    )
    assert result.returncode == 0, result.stderr
    assert "core-import-ok" in result.stdout


def test_adk_agent_access_without_extra_names_the_extra() -> None:
    # Scenario: Core import works without the extra (second half): accessing
    # the lazy adapter export raises ImportError naming `beam-agents[adk]`.
    result = _run(
        """
        import beam_agents

        try:
            beam_agents.AdkAgent
        except ImportError as exc:
            assert "beam-agents[adk]" in str(exc), str(exc)
            print("names-the-extra")
        else:
            raise SystemExit("expected ImportError for AdkAgent without the extra")
        """
    )
    assert result.returncode == 0, result.stderr
    assert "names-the-extra" in result.stdout


def test_the_google_namespace_package_still_works_without_adk() -> None:
    # The namespace-package wrinkle: blocking `google.adk` must leave the rest
    # of the `google` namespace (which core depends on) importable, so the
    # ImportError branch is genuinely about the extra.
    result = _run(
        """
        import google.protobuf

        assert google.protobuf is not None
        print("namespace-intact")
        """
    )
    assert result.returncode == 0, result.stderr
    assert "namespace-intact" in result.stdout


def test_adk_agent_resolves_with_extra_installed() -> None:
    # With the extra present (as in this environment), the lazy export resolves
    # to the adapter class and unknown attributes still raise AttributeError.
    pytest.importorskip("google.adk")
    import beam_agents  # noqa: PLC0415 - imported here so the blocker tests above stay pure

    assert beam_agents.AdkAgent is not None
    with pytest.raises(AttributeError):
        _ = beam_agents.definitely_not_an_export
