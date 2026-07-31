"""Which two versions collide, and how each one gets an interpreter.

Design D3: the previous release is genuinely installed *from PyPI* into its own
venv (not checked out at its tag — that tests the source, not the artifact
users install, and packaging regressions in `_protos/` bindings are exactly a
plausible failure mode), while head is genuinely built from the checkout with
`uv build`. One launcher module then runs under both interpreters.

Design D7: until a release exists on PyPI there is no "previous version", so
resolution falls back to a **self-update** leg — head launched, head updated —
labelled in capitals everywhere it is reported, so a green bootstrap night is
never mistaken for cross-version evidence. Resolution failing outright (a PyPI
outage) takes the same fallback, with the error named in the report.

One refinement the design leaves implicit: the candidate set is restricted to
releases **at or below** head's own version. The promise is forward-only
(design D1), so a PyPI release newer than the checkout would make the gate test
a downgrade — explicitly unsupported — rather than the upgrade path.

Every subprocess goes through an injected `run` seam so the offline unit tests
can assert what this module would execute without executing anything.
"""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from importlib.metadata import version as _distribution_version
from pathlib import Path
from typing import Any, Protocol

import httpx

DISTRIBUTION = "beam-agents"
PYPI_JSON_URL = f"https://pypi.org/pypi/{DISTRIBUTION}/json"

#: Capitals, deliberately (design D7): this string is what stops a bootstrap
#: night from reading like a cross-version result.
BOOTSTRAP_LABEL = "SELF-UPDATE (BOOTSTRAP)"
CROSS_VERSION_LABEL = "CROSS-VERSION"


class Runner(Protocol):
    """Runs a command and returns its stdout. The one subprocess seam."""

    def __call__(self, command: list[str], **kwargs: object) -> str: ...


def run_command(command: list[str], **kwargs: object) -> str:
    """Default `Runner`: run to completion, fail loudly, return stdout."""
    cwd = kwargs.get("cwd")
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        cwd=str(cwd) if cwd is not None else None,
    )
    return completed.stdout


# -- resolution -----------------------------------------------------------------


def parse_version(text: str) -> tuple[int, ...] | None:
    """Numeric release tuple, or `None` for anything that is not a plain release.

    Deliberately strict and dependency-free: prereleases, dev releases, local
    versions and epochs all return `None`, so the gate only ever picks a
    version a user could plausibly be running in production.
    """
    parts = text.split(".")
    if not parts or not all(part.isdigit() for part in parts):
        return None
    return tuple(int(part) for part in parts)


def _has_installable_file(files: object) -> bool:
    """True when a PyPI release still has at least one un-yanked file."""
    if not isinstance(files, Sequence) or isinstance(files, str | bytes):
        return False
    return any(isinstance(entry, Mapping) and not entry.get("yanked", False) for entry in files)


def latest_release(payload: Mapping[str, Any], *, head_version: str) -> str | None:
    """The highest plain release at or below `head_version`, or `None`.

    `payload` is PyPI's JSON API response for the distribution.
    """
    head = parse_version(head_version)
    releases = payload.get("releases")
    if not isinstance(releases, Mapping):
        return None
    candidates: list[tuple[tuple[int, ...], str]] = []
    for version, files in releases.items():
        parsed = parse_version(version)
        if parsed is None or not _has_installable_file(files):
            continue
        if head is not None and parsed > head:
            continue
        candidates.append((parsed, version))
    if not candidates:
        return None
    return max(candidates)[1]


@dataclass(frozen=True)
class VersionPlan:
    """Which two versions this run collides, and why."""

    head_version: str
    previous_version: str | None
    reason: str

    @property
    def is_bootstrap(self) -> bool:
        return self.previous_version is None

    @property
    def label(self) -> str:
        return BOOTSTRAP_LABEL if self.is_bootstrap else CROSS_VERSION_LABEL

    @property
    def launch_version(self) -> str:
        """The version the first job runs — head itself on the bootstrap leg."""
        return self.previous_version if self.previous_version is not None else self.head_version

    def report(self) -> str:
        """The banner every run prints, and every failure message embeds."""
        lines = [
            f"[{self.label}] beam-agents --update compatibility gate",
            f"  launch version: {self.launch_version}",
            f"  update version: {self.head_version} (head, built from the checkout)",
        ]
        if self.reason:
            lines.append(f"  resolution: {self.reason}")
        if self.is_bootstrap:
            lines.append(
                "  NOTE: this is a SELF-UPDATE run. It proves the harness, the "
                "Dataflow update mechanics, and that head's job graph is "
                "update-compatible with itself. It is NOT cross-version evidence."
            )
        return "\n".join(lines)


def fetch_pypi_metadata(*, url: str = PYPI_JSON_URL, timeout_s: float = 30.0) -> dict[str, Any]:
    """The default resolver fetch: PyPI's JSON API, over the runtime's own httpx."""
    response = httpx.get(url, timeout=timeout_s, follow_redirects=True)
    response.raise_for_status()
    payload: dict[str, Any] = response.json()
    return payload


def head_version() -> str:
    """The checkout's own version, read from the installed distribution."""
    return _distribution_version(DISTRIBUTION)


def resolve_plan(
    *,
    head_version: str,
    fetch: Callable[[], Mapping[str, Any]] = fetch_pypi_metadata,
) -> VersionPlan:
    """Decide the run's two versions. Never raises: an unresolvable PyPI is a
    loudly-labelled bootstrap run, not a red gate (design D7 / the PyPI risk).
    """
    try:
        payload = fetch()
    except Exception as exc:  # any failure at all means "no previous version"
        return VersionPlan(
            head_version=head_version,
            previous_version=None,
            reason=f"PyPI resolution failed ({type(exc).__name__}: {exc})",
        )
    previous = latest_release(payload, head_version=head_version)
    if previous is None:
        return VersionPlan(
            head_version=head_version,
            previous_version=None,
            reason=(
                f"no released {DISTRIBUTION} version at or below {head_version} exists on PyPI"
            ),
        )
    return VersionPlan(
        head_version=head_version,
        previous_version=previous,
        reason=f"resolved {previous} as the latest release at or below head",
    )


# -- provisioning ---------------------------------------------------------------


def download_wheel_command(version: str, dest: Path) -> list[str]:
    return [
        sys.executable,
        "-m",
        "pip",
        "download",
        f"{DISTRIBUTION}=={version}",
        "--no-deps",
        "--only-binary",
        ":all:",
        "--dest",
        str(dest),
    ]


def create_venv_command(venv_dir: Path) -> list[str]:
    # Same interpreter minor version the job already uses: `sys.executable`.
    return [sys.executable, "-m", "venv", str(venv_dir)]


def venv_python(venv_dir: Path) -> Path:
    return venv_dir / "bin" / "python"


def install_wheel_command(python: Path, wheel: Path) -> list[str]:
    # No `--no-deps`: the previous release must pull *its own* pinned
    # `apache-beam[gcp]` resolution. That Beam skew is the real user upgrade
    # path, so the harness does not force the two legs equal (design D3).
    return [str(python), "-m", "pip", "install", "--quiet", str(wheel)]


def build_head_wheel_command(out_dir: Path) -> list[str]:
    return ["uv", "build", "--wheel", "--out-dir", str(out_dir)]


def freeze_command(python: Path) -> list[str]:
    # `uv pip freeze --python`, NOT `<python> -m pip freeze`: the head leg's
    # interpreter is the job's own uv-managed venv, and `uv sync` does not
    # install `pip` into it. The nightly Dataflow job provisions with
    # `uv sync --locked --group test --group integration --group bench`, so
    # `-m pip` raised `No module named pip` and the gate died during
    # provisioning — before launching any job, on both the bootstrap and
    # cross-version paths. `uv build` is already used for the head wheel just
    # above; this keeps the harness consistently uv-driven.
    #
    # `--python` makes this correct for the cross-version leg too, whose venv
    # comes from `python -m venv` (and therefore does have pip): uv reads the
    # target interpreter's environment either way.
    return ["uv", "pip", "freeze", "--python", str(python)]


def find_wheel(directory: Path, *, version: str) -> Path:
    """The single wheel for `version` in `directory`."""
    normalized = version.replace("-", "_")
    present = sorted(path.name for path in directory.glob("*.whl")) if directory.is_dir() else []
    matches = sorted(directory.glob(f"*-{normalized}-*.whl")) if directory.is_dir() else []
    if not matches:
        raise FileNotFoundError(
            f"no {DISTRIBUTION} wheel for version {version} in {directory} (found: {present})"
        )
    return matches[0]


@dataclass(frozen=True)
class Leg:
    """One version's interpreter and the wheel its Dataflow workers install."""

    version: str
    python: Path
    wheel: Path
    label: str

    def freeze(self, run: Runner) -> str:
        return run(freeze_command(self.python))


@dataclass(frozen=True)
class Provisioned:
    """Both legs of a run. On the bootstrap leg they are the same object."""

    plan: VersionPlan
    launch: Leg
    head: Leg


def provision(
    plan: VersionPlan,
    *,
    workdir: Path,
    run: Runner = run_command,
    repo_root: Path | None = None,
) -> Provisioned:
    """Build head's wheel, and (cross-version only) install the previous release.

    Both legs' full `pip freeze` is captured: a compat failure is meaningless
    without knowing which two environments collided, and the Beam skew inside
    the update is the first thing a triager asks about.
    """
    head_dist = workdir / "head-dist"
    run(build_head_wheel_command(head_dist), cwd=repo_root)
    head_leg = Leg(
        version=plan.head_version,
        python=Path(sys.executable),
        wheel=find_wheel(head_dist, version=plan.head_version),
        label=CROSS_VERSION_LABEL if not plan.is_bootstrap else BOOTSTRAP_LABEL,
    )

    if plan.is_bootstrap:
        # One environment, used for both legs: head → head.
        head_leg.freeze(run)
        return Provisioned(plan=plan, launch=head_leg, head=head_leg)

    previous = plan.previous_version
    assert previous is not None  # narrowed by `is_bootstrap`
    prev_dist = workdir / "prev-dist"
    prev_venv = workdir / "prev-venv"
    run(download_wheel_command(previous, prev_dist))
    prev_wheel = find_wheel(prev_dist, version=previous)
    run(create_venv_command(prev_venv))
    prev_python = venv_python(prev_venv)
    run(install_wheel_command(prev_python, prev_wheel))
    prev_leg = Leg(
        version=previous, python=prev_python, wheel=prev_wheel, label=CROSS_VERSION_LABEL
    )

    prev_leg.freeze(run)
    head_leg.freeze(run)
    return Provisioned(plan=plan, launch=prev_leg, head=head_leg)


def describe(provisioned: Provisioned) -> str:
    """A machine-greppable one-liner for the CI log, beside the banner."""
    return json.dumps(
        {
            "mode": provisioned.plan.label,
            "launch_version": provisioned.launch.version,
            "head_version": provisioned.head.version,
            "launch_wheel": provisioned.launch.wheel.name,
            "head_wheel": provisioned.head.wheel.name,
        },
        sort_keys=True,
    )
