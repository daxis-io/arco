"""Tests for asset discovery."""
from __future__ import annotations

from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    pass


class TestAssetDiscovery:
    """Tests for AssetDiscovery."""

    def test_discover_no_assets(self, tmp_path: Path) -> None:
        """Discovery returns empty list when no assets found."""
        from servo.manifest.discovery import AssetDiscovery

        discovery = AssetDiscovery(root_path=tmp_path)
        assets = discovery.discover()

        assert assets == []

    def test_discover_single_asset(self, tmp_path: Path) -> None:
        """Discovery finds single @asset decorated function."""
        from servo.manifest.discovery import AssetDiscovery

        # Create a Python file with an asset
        (tmp_path / "assets").mkdir()
        (tmp_path / "assets" / "__init__.py").write_text("")
        (tmp_path / "assets" / "raw.py").write_text(dedent("""
            from servo import asset
            from servo.context import AssetContext

            @asset(namespace="raw")
            def events(ctx: AssetContext) -> None:
                pass
        """))

        discovery = AssetDiscovery(root_path=tmp_path)
        assets = discovery.discover()

        assert len(assets) == 1
        assert str(assets[0].key) == "raw.events"

    def test_discover_multiple_assets(self, tmp_path: Path) -> None:
        """Discovery finds multiple assets across files."""
        from servo.manifest.discovery import AssetDiscovery

        # Create multiple asset files
        (tmp_path / "assets").mkdir()
        (tmp_path / "assets" / "__init__.py").write_text("")

        (tmp_path / "assets" / "raw.py").write_text(dedent("""
            from servo import asset
            from servo.context import AssetContext

            @asset(namespace="raw")
            def events(ctx: AssetContext) -> None:
                pass

            @asset(namespace="raw")
            def users(ctx: AssetContext) -> None:
                pass
        """))

        (tmp_path / "assets" / "staging.py").write_text(dedent("""
            from servo import asset
            from servo.context import AssetContext
            from servo.types import AssetIn

            @asset(namespace="staging")
            def cleaned_events(ctx: AssetContext, raw: AssetIn["raw.events"]) -> None:
                pass
        """))

        discovery = AssetDiscovery(root_path=tmp_path)
        assets = discovery.discover()

        assert len(assets) == 3
        keys = {str(a.key) for a in assets}
        assert keys == {"raw.events", "raw.users", "staging.cleaned_events"}

    def test_skips_test_files(self, tmp_path: Path) -> None:
        """Discovery skips test files."""
        from servo.manifest.discovery import AssetDiscovery

        (tmp_path / "assets").mkdir()
        (tmp_path / "assets" / "__init__.py").write_text("")
        (tmp_path / "assets" / "test_raw.py").write_text(dedent("""
            from servo import asset
            from servo.context import AssetContext

            @asset(namespace="raw")
            def test_events(ctx: AssetContext) -> None:
                pass
        """))

        discovery = AssetDiscovery(root_path=tmp_path)
        assets = discovery.discover()

        assert len(assets) == 0

    def test_skips_venv(self, tmp_path: Path) -> None:
        """Discovery skips virtual environment directories."""
        from servo.manifest.discovery import AssetDiscovery

        (tmp_path / ".venv" / "lib").mkdir(parents=True)
        (tmp_path / ".venv" / "lib" / "module.py").write_text(dedent("""
            from servo import asset
            from servo.context import AssetContext

            @asset(namespace="raw")
            def events(ctx: AssetContext) -> None:
                pass
        """))

        discovery = AssetDiscovery(root_path=tmp_path)
        assets = discovery.discover()

        assert len(assets) == 0

    def test_returns_sorted_by_key(self, tmp_path: Path) -> None:
        """Discovery returns assets sorted by key."""
        from servo.manifest.discovery import AssetDiscovery

        (tmp_path / "assets").mkdir()
        (tmp_path / "assets" / "__init__.py").write_text("")
        (tmp_path / "assets" / "mixed.py").write_text(dedent("""
            from servo import asset
            from servo.context import AssetContext

            @asset(namespace="staging")
            def zebra(ctx: AssetContext) -> None:
                pass

            @asset(namespace="raw")
            def alpha(ctx: AssetContext) -> None:
                pass

            @asset(namespace="mart")
            def beta(ctx: AssetContext) -> None:
                pass
        """))

        discovery = AssetDiscovery(root_path=tmp_path)
        assets = discovery.discover()

        keys = [str(a.key) for a in assets]
        assert keys == sorted(keys)


class TestFindPythonFiles:
    """Tests for _find_python_files helper."""

    def test_finds_py_files(self, tmp_path: Path) -> None:
        """Finds .py files in assets directory."""
        from servo.manifest.discovery import AssetDiscovery

        (tmp_path / "assets").mkdir()
        (tmp_path / "assets" / "raw.py").write_text("")
        (tmp_path / "assets" / "staging.py").write_text("")
        (tmp_path / "assets" / "__init__.py").write_text("")

        discovery = AssetDiscovery(root_path=tmp_path)
        files = discovery._find_python_files()

        names = {f.name for f in files}
        assert "raw.py" in names
        assert "staging.py" in names
