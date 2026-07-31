"""The ``beam-agents-console`` startup contract.

Covers the scenarios under "The console is started by a documented command with
an offline default": a database path alone starts the service, an unusable
configuration exits ``2`` naming the value it rejected, and a clean shutdown
exits ``0``. Every flag's environment-variable fallback is exercised here too,
because the fallback *is* the deployment interface — a container sets variables,
not a command line.

Nothing here starts a server. ``_app.serve`` is replaced by a recorder, so what
these tests assert on is exactly what the CLI is responsible for: parsing,
fallback, validation, exit codes, and what it hands to ``serve``. The server
itself is ``_app``'s contract, tested there.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any

import pytest

from beam_agents.console import __main__ as cli
from beam_agents.console._app import DEFAULT_HOST, DEFAULT_PORT, STATIC_DIR_ENV

# Every variable the CLI reads. Cleared before each test: a developer's shell (or
# a previous test) must not decide what a parse produces.
_ENV_VARS = (
    "BEAM_AGENTS_CONSOLE_DB",
    "BEAM_AGENTS_CONSOLE_HOST",
    "BEAM_AGENTS_CONSOLE_PORT",
    "BEAM_AGENTS_CONSOLE_RETENTION_HOURS",
    "BEAM_AGENTS_CONSOLE_KAFKA_TRACES_FROM",
    "BEAM_AGENTS_CONSOLE_KAFKA_FROM_BEGINNING",
    "BEAM_AGENTS_CONSOLE_BIGQUERY_TRACES_FROM",
    "BEAM_AGENTS_CONSOLE_IMPORT_TRACES",
    "BEAM_AGENTS_CONSOLE_IMPORT_SNAPSHOT",
    "BEAM_AGENTS_CONSOLE_CORS_ORIGIN",
    "BEAM_AGENTS_CONSOLE_LOG_LEVEL",
    STATIC_DIR_ENV,
)


class _RecordingServe:
    """Stands in for ``_app.serve``, recording the keywords it was handed.

    ``raises`` lets a test drive the shutdown paths: a signalled console reaches
    ``main`` as ``KeyboardInterrupt``.
    """

    def __init__(self, raises: BaseException | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._raises = raises

    def __call__(self, **options: Any) -> None:
        self.calls.append(options)
        if self._raises is not None:
            raise self._raises

    @property
    def options(self) -> dict[str, Any]:
        """The single call's keywords, asserting there was exactly one."""
        assert len(self.calls) == 1
        return self.calls[0]


@pytest.fixture(autouse=True)
def _clean_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    # `main` sets the root logger's level, as an entry point that owns the
    # process should. Restored here so it stays in the test that asked for it.
    root = logging.getLogger()
    monkeypatch.setattr(root, "level", root.level)


@pytest.fixture
def served(monkeypatch: pytest.MonkeyPatch) -> _RecordingServe:
    recorder = _RecordingServe()
    monkeypatch.setattr(cli, "serve", recorder)
    return recorder


# --- Scenario: The service starts with only a database path -------------------


def test_the_service_starts_with_only_a_database_path(
    served: _RecordingServe, tmp_path: Path
) -> None:
    database = tmp_path / "console.db"

    assert cli.main(["--db", str(database)]) == 0

    assert Path(served.options["database"]) == database
    assert served.options["host"] == DEFAULT_HOST
    assert served.options["port"] == DEFAULT_PORT


def test_the_default_database_makes_the_bare_command_usable(
    served: _RecordingServe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # "Start it and look at it" is the shortest possible command: no flags at
    # all, writing beside the working directory.
    monkeypatch.chdir(tmp_path)

    assert cli.main([]) == 0

    assert Path(served.options["database"]).name == cli.DEFAULT_DATABASE


def test_no_ingest_source_means_no_broker_no_project_and_no_egress(
    served: _RecordingServe, tmp_path: Path
) -> None:
    # The offline default: every ingest source beyond the HTTP endpoints is
    # opt-in, and the listener is loopback.
    assert cli.main(["--db", str(tmp_path / "console.db")]) == 0

    options = served.options
    assert options["kafka_traces_from"] is None
    assert options["bigquery_traces_from"] is None
    assert options["import_traces"] is None
    assert options["import_snapshot"] is None
    assert options["cors_origins"] == ()
    assert options["retention_hours"] is None
    assert options["host"] == "127.0.0.1"


def test_every_configured_source_reaches_serve(served: _RecordingServe, tmp_path: Path) -> None:
    traces = tmp_path / "traces.bin"
    traces.write_bytes(b"")
    snapshot = tmp_path / "snapshot.bin"
    snapshot.write_bytes(b"")
    static = tmp_path / "static"
    static.mkdir()

    assert (
        cli.main(
            [
                "--db",
                str(tmp_path / "console.db"),
                "--host",
                "0.0.0.0",
                "--port",
                "9999",
                "--static-dir",
                str(static),
                "--retention-hours",
                "48",
                "--kafka-traces-from",
                "kafka://localhost:9092/traces",
                "--kafka-from-beginning",
                "--bigquery-traces-from",
                "bigquery://my-project/telemetry/traces",
                "--import-traces",
                str(traces),
                "--import-snapshot",
                str(snapshot),
                "--cors-origin",
                "http://localhost:5173",
                "--cors-origin",
                "http://localhost:4173",
                "--log-level",
                "debug",
            ]
        )
        == 0
    )

    options = served.options
    assert options["host"] == "0.0.0.0"
    assert options["port"] == 9999
    assert Path(options["static_dir"]) == static
    assert options["retention_hours"] == 48.0
    assert options["kafka_traces_from"] == "kafka://localhost:9092/traces"
    assert options["kafka_from_beginning"] is True
    assert options["bigquery_traces_from"] == "bigquery://my-project/telemetry/traces"
    assert Path(options["import_traces"]) == traces
    assert Path(options["import_snapshot"]) == snapshot
    assert options["cors_origins"] == ("http://localhost:5173", "http://localhost:4173")
    assert options["log_level"] == "DEBUG"


def test_the_parser_exposes_every_documented_flag() -> None:
    help_text = cli.build_parser().format_help()

    for flag in (
        "--db",
        "--host",
        "--port",
        "--static-dir",
        "--retention-hours",
        "--kafka-traces-from",
        "--kafka-from-beginning",
        "--bigquery-traces-from",
        "--import-traces",
        "--import-snapshot",
        "--cors-origin",
        "--log-level",
    ):
        assert flag in help_text


# --- Scenario: An unusable configuration exits with a named cause -------------


@pytest.mark.parametrize(
    "uri",
    [
        "not-a-uri",
        "kafka://",
        "kafka://localhost:9092",
        "kafka://localhost:9092/traces/extra",
        "pubsub://project/traces",
    ],
)
def test_a_malformed_kafka_uri_exits_two_naming_the_rejected_value(
    served: _RecordingServe, tmp_path: Path, capsys: pytest.CaptureFixture[str], uri: str
) -> None:
    code = cli.main(["--db", str(tmp_path / "console.db"), "--kafka-traces-from", uri])

    assert code == 2
    error = capsys.readouterr().err
    assert "--kafka-traces-from" in error
    assert uri in error
    assert served.calls == []


@pytest.mark.parametrize(
    "uri",
    [
        "bigquery://",
        "bigquery://project/traces",
        "bigquery://project/dataset/table/extra",
        "kafka://localhost:9092/traces",
    ],
)
def test_a_malformed_bigquery_uri_exits_two_naming_the_rejected_value(
    served: _RecordingServe, tmp_path: Path, capsys: pytest.CaptureFixture[str], uri: str
) -> None:
    code = cli.main(["--db", str(tmp_path / "console.db"), "--bigquery-traces-from", uri])

    assert code == 2
    error = capsys.readouterr().err
    assert "--bigquery-traces-from" in error
    assert uri in error
    assert served.calls == []


def test_a_malformed_uri_is_rejected_without_importing_the_client(
    served: _RecordingServe,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # The same discipline `SinkResolver.validate` follows: URI validation imports
    # no client library, so a typo is caught in an environment that has none of
    # the ingest extras installed. `None` in sys.modules makes `import aiokafka`
    # raise, which is what an absent client looks like from inside the CLI.
    monkeypatch.setitem(sys.modules, "aiokafka", None)

    code = cli.main(["--db", str(tmp_path / "console.db"), "--kafka-traces-from", "kafka://"])

    assert code == 2
    assert "kafka" in capsys.readouterr().err
    assert served.calls == []


def test_an_unwritable_database_path_exits_two_naming_the_path(
    served: _RecordingServe, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    missing_parent = tmp_path / "no-such-directory" / "console.db"

    code = cli.main(["--db", str(missing_parent)])

    assert code == 2
    error = capsys.readouterr().err
    assert str(missing_parent) in error
    assert served.calls == []


def test_a_database_path_under_a_file_exits_two_naming_the_path(
    served: _RecordingServe, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Unwritable regardless of the process's uid, which a chmod-based case is
    # not: this suite runs as root in CI containers.
    blocker = tmp_path / "not-a-directory"
    blocker.write_text("")

    code = cli.main(["--db", str(blocker / "console.db")])

    assert code == 2
    assert str(blocker) in capsys.readouterr().err
    assert served.calls == []


def test_a_database_path_that_is_a_directory_exits_two(
    served: _RecordingServe, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = cli.main(["--db", str(tmp_path)])

    assert code == 2
    assert str(tmp_path) in capsys.readouterr().err
    assert served.calls == []


@pytest.mark.skipif(os.geteuid() == 0, reason="root bypasses the permission bits this asserts on")
def test_an_unwritable_existing_database_exits_two(
    served: _RecordingServe, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = tmp_path / "console.db"
    database.write_bytes(b"")
    database.chmod(0o400)

    code = cli.main(["--db", str(database)])

    assert code == 2
    assert str(database) in capsys.readouterr().err
    assert served.calls == []


@pytest.mark.parametrize("flag", ["--import-traces", "--import-snapshot"])
def test_a_missing_import_file_exits_two_naming_the_path(
    served: _RecordingServe, tmp_path: Path, capsys: pytest.CaptureFixture[str], flag: str
) -> None:
    missing = tmp_path / "capture.bin"

    code = cli.main(["--db", str(tmp_path / "console.db"), flag, str(missing)])

    assert code == 2
    error = capsys.readouterr().err
    assert flag in error
    assert str(missing) in error
    assert served.calls == []


@pytest.mark.parametrize("port", ["0", "65536", "-1"])
def test_a_port_outside_the_valid_range_exits_two(
    served: _RecordingServe, tmp_path: Path, capsys: pytest.CaptureFixture[str], port: str
) -> None:
    code = cli.main(["--db", str(tmp_path / "console.db"), "--port", port])

    assert code == 2
    assert port in capsys.readouterr().err
    assert served.calls == []


@pytest.mark.parametrize("hours", ["0", "-4"])
def test_a_non_positive_retention_window_exits_two(
    served: _RecordingServe, tmp_path: Path, capsys: pytest.CaptureFixture[str], hours: str
) -> None:
    code = cli.main(["--db", str(tmp_path / "console.db"), "--retention-hours", hours])

    assert code == 2
    assert "--retention-hours" in capsys.readouterr().err
    assert served.calls == []


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("BEAM_AGENTS_CONSOLE_PORT", "eight-thousand"),
        ("BEAM_AGENTS_CONSOLE_RETENTION_HOURS", "a-while"),
    ],
)
def test_a_numeric_environment_variable_that_is_not_a_number_exits_two(
    served: _RecordingServe,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    variable: str,
    value: str,
) -> None:
    # argparse converts a string default with the flag's `type`, so this exits
    # through `parser.error` rather than through `main`'s handler — still 2,
    # still naming the value, which is what the contract promises.
    monkeypatch.setenv(variable, value)

    with pytest.raises(SystemExit) as excinfo:
        cli.main(["--db", str(tmp_path / "console.db")])

    assert excinfo.value.code == 2
    assert value in capsys.readouterr().err
    assert served.calls == []


def test_an_unknown_log_level_exits_two(
    served: _RecordingServe, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = cli.main(["--db", str(tmp_path / "console.db"), "--log-level", "chatty"])

    assert code == 2
    assert "chatty" in capsys.readouterr().err
    assert served.calls == []


def test_an_unparseable_boolean_environment_variable_exits_two(
    served: _RecordingServe,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("BEAM_AGENTS_CONSOLE_KAFKA_FROM_BEGINNING", "flase")

    code = cli.main(["--db", str(tmp_path / "console.db")])

    assert code == 2
    error = capsys.readouterr().err
    assert "BEAM_AGENTS_CONSOLE_KAFKA_FROM_BEGINNING" in error
    assert "flase" in error
    assert served.calls == []


def test_credentials_in_a_rejected_uri_are_redacted(
    served: _RecordingServe, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Startup failure is the single most likely moment for a credentialed URI to
    # reach a terminal or a CI log.
    code = cli.main(
        [
            "--db",
            str(tmp_path / "console.db"),
            "--kafka-traces-from",
            "kafka://admin:hunter2@broker:9092",
        ]
    )

    assert code == 2
    error = capsys.readouterr().err
    assert "hunter2" not in error
    assert "admin" not in error
    assert "***@broker:9092" in error


# --- Environment-variable fallback --------------------------------------------


@pytest.mark.parametrize(
    ("variable", "value", "option", "expected"),
    [
        ("BEAM_AGENTS_CONSOLE_HOST", "0.0.0.0", "host", "0.0.0.0"),
        ("BEAM_AGENTS_CONSOLE_PORT", "9123", "port", 9123),
        ("BEAM_AGENTS_CONSOLE_RETENTION_HOURS", "12.5", "retention_hours", 12.5),
        (
            "BEAM_AGENTS_CONSOLE_KAFKA_TRACES_FROM",
            "kafka://localhost:9092/traces",
            "kafka_traces_from",
            "kafka://localhost:9092/traces",
        ),
        (
            "BEAM_AGENTS_CONSOLE_BIGQUERY_TRACES_FROM",
            "bigquery://p/d/t",
            "bigquery_traces_from",
            "bigquery://p/d/t",
        ),
        ("BEAM_AGENTS_CONSOLE_KAFKA_FROM_BEGINNING", "true", "kafka_from_beginning", True),
        ("BEAM_AGENTS_CONSOLE_KAFKA_FROM_BEGINNING", "0", "kafka_from_beginning", False),
        # `FOO=` in a compose file means "unset it", not "unparseable".
        ("BEAM_AGENTS_CONSOLE_KAFKA_FROM_BEGINNING", "", "kafka_from_beginning", False),
        ("BEAM_AGENTS_CONSOLE_LOG_LEVEL", "warning", "log_level", "WARNING"),
        (
            "BEAM_AGENTS_CONSOLE_CORS_ORIGIN",
            "http://a.test, http://b.test",
            "cors_origins",
            ("http://a.test", "http://b.test"),
        ),
    ],
)
def test_every_flag_falls_back_to_its_environment_variable(
    served: _RecordingServe,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    variable: str,
    value: str,
    option: str,
    expected: object,
) -> None:
    monkeypatch.setenv(variable, value)

    assert cli.main(["--db", str(tmp_path / "console.db")]) == 0

    assert served.options[option] == expected


def test_the_database_path_falls_back_to_its_environment_variable(
    served: _RecordingServe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "from-env.db"
    monkeypatch.setenv("BEAM_AGENTS_CONSOLE_DB", str(database))

    assert cli.main([]) == 0

    assert Path(served.options["database"]) == database


def test_the_static_directory_falls_back_to_the_documented_variable(
    served: _RecordingServe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # One variable, not two: `_app.resolve_static_dir` already documents
    # BEAM_AGENTS_CONSOLE_STATIC as the static-bundle override.
    static = tmp_path / "static"
    static.mkdir()
    monkeypatch.setenv(STATIC_DIR_ENV, str(static))

    assert cli.main(["--db", str(tmp_path / "console.db")]) == 0

    assert Path(served.options["static_dir"]) == static


@pytest.mark.parametrize("flag", ["--import-traces", "--import-snapshot"])
def test_the_import_paths_fall_back_to_their_environment_variables(
    served: _RecordingServe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, flag: str
) -> None:
    capture = tmp_path / "capture.bin"
    capture.write_bytes(b"")
    variable = f"BEAM_AGENTS_CONSOLE_{flag.removeprefix('--').replace('-', '_').upper()}"
    monkeypatch.setenv(variable, str(capture))

    assert cli.main(["--db", str(tmp_path / "console.db")]) == 0

    option = flag.removeprefix("--").replace("-", "_")
    assert Path(served.options[option]) == capture


def test_a_flag_overrides_its_environment_variable(
    served: _RecordingServe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BEAM_AGENTS_CONSOLE_PORT", "9123")

    assert cli.main(["--db", str(tmp_path / "console.db"), "--port", "8080"]) == 0

    assert served.options["port"] == 8080


def test_repeated_cors_flags_override_the_environment_variable(
    served: _RecordingServe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A repeatable flag must replace the variable rather than extend it, or a
    # container's default origin can never be removed from a command line.
    monkeypatch.setenv("BEAM_AGENTS_CONSOLE_CORS_ORIGIN", "http://from-env.test")

    assert (
        cli.main(["--db", str(tmp_path / "console.db"), "--cors-origin", "http://from-flag.test"])
        == 0
    )

    assert served.options["cors_origins"] == ("http://from-flag.test",)


def test_the_log_level_reaches_the_root_logger(
    served: _RecordingServe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Regression: `logging.basicConfig` is a no-op once the root logger has a
    # handler, and importing this package imports Beam, which logs at import
    # time and installs one — so `--log-level` silently did nothing.
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [*root.handlers, logging.NullHandler()])

    assert cli.main(["--db", str(tmp_path / "console.db"), "--log-level", "debug"]) == 0

    assert root.level == logging.DEBUG


# --- Shutdown -----------------------------------------------------------------


def test_a_clean_shutdown_exits_zero(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(cli, "serve", _RecordingServe())

    assert cli.main(["--db", str(tmp_path / "console.db")]) == 0


def test_a_signalled_shutdown_exits_zero(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # SIGINT reaches a blocking `serve` as KeyboardInterrupt; a supervisor must
    # see a clean exit rather than a crash.
    monkeypatch.setattr(cli, "serve", _RecordingServe(raises=KeyboardInterrupt()))

    assert cli.main(["--db", str(tmp_path / "console.db")]) == 0
