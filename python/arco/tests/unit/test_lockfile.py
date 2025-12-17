"""Tests for asset ID lockfile persistence."""
from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


class TestLockfile:
    """Tests for Lockfile persistence."""

    def test_load_empty_when_missing(self, tmp_path: Path) -> None:
        """Missing lockfile returns empty state."""
        from servo.manifest.lockfile import Lockfile

        lf = Lockfile.load(tmp_path / ".servo" / "state.json")
        assert lf.asset_ids == {}

    def test_save_and_load_roundtrip(self, tmp_path: Path) -> None:
        """Lockfile persists and loads correctly."""
        from servo.manifest.lockfile import Lockfile

        path = tmp_path / ".servo" / "state.json"

        lf = Lockfile()
        lf.asset_ids["raw.events"] = "01ABCDEF"
        lf.asset_ids["staging.users"] = "02GHIJKL"
        lf.save(path)

        loaded = Lockfile.load(path)
        assert loaded.asset_ids == lf.asset_ids

    def test_get_or_create_returns_existing(self) -> None:
        """get_or_create returns existing ID if present."""
        from servo.manifest.lockfile import Lockfile
        from servo.types.asset import AssetKey

        lf = Lockfile()
        lf.asset_ids["raw.events"] = "01EXISTING"

        key = AssetKey(namespace="raw", name="events")
        asset_id, created = lf.get_or_create(key)

        assert str(asset_id) == "01EXISTING"
        assert created is False

    def test_get_or_create_generates_new(self) -> None:
        """get_or_create generates new ID if not present."""
        from servo.manifest.lockfile import Lockfile
        from servo.types.asset import AssetKey

        lf = Lockfile()
        key = AssetKey(namespace="raw", name="events")

        asset_id, created = lf.get_or_create(key)

        assert asset_id is not None
        assert len(str(asset_id)) == 26  # ULID length
        assert created is True
        assert "raw.events" in lf.asset_ids

    def test_lockfile_format_valid_json(self, tmp_path: Path) -> None:
        """Lockfile is valid JSON with expected structure."""
        from servo.manifest.lockfile import Lockfile

        path = tmp_path / ".servo" / "state.json"

        lf = Lockfile()
        lf.asset_ids["raw.events"] = "01ABCDEF"
        lf.save(path)

        with path.open() as f:
            data = json.load(f)

        assert "version" in data
        assert data["version"] == "1.0"
        assert "assetIds" in data
        assert data["assetIds"]["raw.events"] == "01ABCDEF"

    def test_update_from_server(self) -> None:
        """update_from_server merges server-assigned IDs."""
        from servo.manifest.lockfile import Lockfile

        lf = Lockfile()
        lf.asset_ids["raw.events"] = "01LOCAL"

        server_ids = {
            "raw.events": "01SERVER",  # Server may reassign
            "staging.users": "02SERVER",  # New from server
        }
        lf.update_from_server(server_ids)

        assert lf.asset_ids["raw.events"] == "01SERVER"
        assert lf.asset_ids["staging.users"] == "02SERVER"
