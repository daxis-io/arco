"""Tests for CLI commands."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest  # noqa: F401 - needed for pytest fixtures
from typer.testing import CliRunner

runner = CliRunner()


class TestDeployCommand:
    """Tests for deploy command."""

    def test_deploy_dry_run_no_assets(self, tmp_path: Path) -> None:
        """Deploy dry-run with no assets shows message."""
        from arco_flow.cli.main import app

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(app, ["deploy", "--dry-run"])

        assert result.exit_code == 0
        assert "No assets found" in result.stdout or "0" in result.stdout

    def test_deploy_dry_run_with_assets(self, tmp_path: Path) -> None:
        """Deploy dry-run discovers and displays assets."""
        from arco_flow.cli.main import app

        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create asset file
            Path("assets").mkdir()
            Path("assets/__init__.py").write_text("")
            Path("assets/raw.py").write_text(
                dedent("""
                from arco_flow import asset
                from arco_flow.context import AssetContext

                @asset(namespace="raw")
                def events(ctx: AssetContext) -> None:
                    pass
            """)
            )

            result = runner.invoke(app, ["deploy", "--dry-run"])

        assert result.exit_code == 0

    def test_deploy_writes_manifest_file(self, tmp_path: Path) -> None:
        """Deploy --output writes manifest to file."""
        from arco_flow.cli.main import app

        with runner.isolated_filesystem(temp_dir=tmp_path):
            Path("assets").mkdir()
            Path("assets/__init__.py").write_text("")
            Path("assets/raw.py").write_text(
                dedent("""
                from arco_flow import asset
                from arco_flow.context import AssetContext

                @asset(namespace="raw")
                def events(ctx: AssetContext) -> None:
                    pass
            """)
            )

            result = runner.invoke(app, ["deploy", "--dry-run", "--output", "manifest.json"])
            # Check inside the context - the file is written to current directory
            manifest_exists = Path("manifest.json").exists()

        assert result.exit_code == 0
        assert manifest_exists


class TestValidateCommand:
    """Tests for validate command."""

    def test_validate_no_assets(self, tmp_path: Path) -> None:
        """Validate with no assets passes."""
        from arco_flow.cli.main import app

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(app, ["validate"])

        assert result.exit_code == 0

    def test_validate_valid_assets(self, tmp_path: Path) -> None:
        """Validate with valid assets passes."""
        from arco_flow.cli.main import app

        with runner.isolated_filesystem(temp_dir=tmp_path):
            Path("assets").mkdir()
            Path("assets/__init__.py").write_text("")
            Path("assets/raw.py").write_text(
                dedent("""
                from arco_flow import asset
                from arco_flow.context import AssetContext

                @asset(namespace="raw")
                def events(ctx: AssetContext) -> None:
                    pass
            """)
            )

            result = runner.invoke(app, ["validate"])

        assert result.exit_code == 0


class TestInitCommand:
    """Tests for init command."""

    def test_init_creates_project(self, tmp_path: Path) -> None:
        """Init creates project structure."""
        from arco_flow.cli.main import app

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(app, ["init", "my-project"])
            # Check inside the context - project is created in current directory
            project_exists = Path("my-project").exists()
            assets_dir_exists = Path("my-project/assets").exists()

        assert result.exit_code == 0
        assert project_exists
        assert assets_dir_exists
