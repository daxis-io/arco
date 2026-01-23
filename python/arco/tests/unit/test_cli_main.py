"""Tests for CLI main entry point."""

from __future__ import annotations

import re

from typer.testing import CliRunner

runner = CliRunner()

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _plain_output(result) -> str:
    # Typer's rich help formatter can inject ANSI codes between
    # characters (e.g. "-" + style + "-" + style + "dry-run"), which
    # breaks naive substring checks.
    return _ANSI_RE.sub("", result.stdout + result.stderr)


class TestCLIMain:
    """Tests for CLI main commands."""

    def test_version_flag(self) -> None:
        """--version shows version and exits."""
        from arco_flow.cli.main import app

        result = runner.invoke(app, ["--version"])

        assert result.exit_code == 0
        assert "arco-flow" in _plain_output(result).lower()

    def test_help_flag(self) -> None:
        """--help shows help text."""
        from arco_flow.cli.main import app

        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        output = _plain_output(result)
        assert "deploy" in output
        assert "run" in output
        assert "status" in output

    def test_deploy_help(self) -> None:
        """deploy --help shows deploy options."""
        from arco_flow.cli.main import app

        result = runner.invoke(app, ["deploy", "--help"])

        assert result.exit_code == 0
        assert "--dry-run" in _plain_output(result)

    def test_run_help(self) -> None:
        """run --help shows run options."""
        from arco_flow.cli.main import app

        result = runner.invoke(app, ["run", "--help"])

        assert result.exit_code == 0
        assert "asset" in _plain_output(result).lower()

    def test_status_help(self) -> None:
        """status --help shows status options."""
        from arco_flow.cli.main import app

        result = runner.invoke(app, ["status", "--help"])

        assert result.exit_code == 0
        assert "--watch" in _plain_output(result)
