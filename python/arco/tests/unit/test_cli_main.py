"""Tests for CLI main entry point."""
from __future__ import annotations

from typer.testing import CliRunner

import pytest

runner = CliRunner()


class TestCLIMain:
    """Tests for CLI main commands."""

    def test_version_flag(self) -> None:
        """--version shows version and exits."""
        from servo.cli.main import app

        result = runner.invoke(app, ["--version"])

        assert result.exit_code == 0
        assert "servo" in result.stdout.lower()

    def test_help_flag(self) -> None:
        """--help shows help text."""
        from servo.cli.main import app

        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "deploy" in result.stdout
        assert "run" in result.stdout
        assert "status" in result.stdout

    def test_deploy_help(self) -> None:
        """deploy --help shows deploy options."""
        from servo.cli.main import app

        result = runner.invoke(app, ["deploy", "--help"])

        assert result.exit_code == 0
        assert "--dry-run" in result.stdout

    def test_run_help(self) -> None:
        """run --help shows run options."""
        from servo.cli.main import app

        result = runner.invoke(app, ["run", "--help"])

        assert result.exit_code == 0
        assert "asset" in result.stdout.lower()

    def test_status_help(self) -> None:
        """status --help shows status options."""
        from servo.cli.main import app

        result = runner.invoke(app, ["status", "--help"])

        assert result.exit_code == 0
        assert "--watch" in result.stdout
