"""Tests for manifest builder."""
from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

if TYPE_CHECKING:
    from pathlib import Path


class TestManifestBuilder:
    """Tests for ManifestBuilder."""

    def test_build_empty_manifest(self, tmp_path: Path) -> None:
        """Building with no assets creates empty manifest."""
        from servo.manifest.builder import ManifestBuilder

        builder = ManifestBuilder(
            tenant_id="test",
            workspace_id="dev",
            lockfile_path=tmp_path / ".servo" / "state.json",
        )
        manifest = builder.build([])

        assert manifest.tenant_id == "test"
        assert manifest.workspace_id == "dev"
        assert manifest.assets == []

    def test_build_with_assets(self, tmp_path: Path) -> None:
        """Building with assets includes them in manifest."""
        from servo.asset import RegisteredAsset
        from servo.manifest.builder import ManifestBuilder
        from servo.types import AssetDefinition, AssetId, AssetKey, CodeLocation

        builder = ManifestBuilder(
            tenant_id="test",
            workspace_id="dev",
            lockfile_path=tmp_path / ".servo" / "state.json",
        )

        definition = AssetDefinition(
            key=AssetKey("raw", "events"),
            id=AssetId.generate(),
            code=CodeLocation(module="test", function="events"),
        )
        asset = RegisteredAsset(key=definition.key, definition=definition, func=lambda: None)

        manifest = builder.build([asset])

        assert len(manifest.assets) == 1
        assert manifest.assets[0].key["namespace"] == "raw"
        assert manifest.assets[0].key["name"] == "events"

    def test_build_generates_code_version_id(self, tmp_path: Path) -> None:
        """Build generates a code version ID."""
        from servo.manifest.builder import ManifestBuilder

        builder = ManifestBuilder(
            tenant_id="test",
            workspace_id="dev",
            lockfile_path=tmp_path / ".servo" / "state.json",
        )
        manifest = builder.build([])

        assert manifest.code_version_id != ""
        assert len(manifest.code_version_id) == 26  # ULID format

    def test_build_sets_deployed_by(self, tmp_path: Path) -> None:
        """Build sets deployed_by from environment or default."""
        from servo.manifest.builder import ManifestBuilder

        builder = ManifestBuilder(
            tenant_id="test",
            workspace_id="dev",
            deployed_by="user@example.com",
            lockfile_path=tmp_path / ".servo" / "state.json",
        )
        manifest = builder.build([])

        assert manifest.deployed_by == "user@example.com"

    def test_build_persists_assigned_asset_ids(self, tmp_path: Path) -> None:
        """Build assigns stable asset IDs and persists them to the lockfile."""
        from servo.asset import RegisteredAsset
        from servo.manifest.builder import ManifestBuilder
        from servo.manifest.lockfile import Lockfile
        from servo.types import AssetDefinition, AssetId, AssetKey, CodeLocation

        lockfile_path = tmp_path / ".servo" / "state.json"
        builder = ManifestBuilder(
            tenant_id="test",
            workspace_id="dev",
            lockfile_path=lockfile_path,
        )

        definition = AssetDefinition(
            key=AssetKey("raw", "events"),
            id=AssetId.generate(),
            code=CodeLocation(module="test", function="events"),
        )
        asset = RegisteredAsset(key=definition.key, definition=definition, func=lambda: None)

        manifest = builder.build([asset])

        lf = Lockfile.load(lockfile_path)
        assert lf.asset_ids["raw.events"] == manifest.assets[0].id

    def test_build_reuses_existing_lockfile_id(self, tmp_path: Path) -> None:
        """Existing lockfile IDs are reused for stable deploy identity."""
        from servo.asset import RegisteredAsset
        from servo.manifest.builder import ManifestBuilder
        from servo.manifest.lockfile import Lockfile
        from servo.types import AssetDefinition, AssetId, AssetKey, CodeLocation

        lockfile_path = tmp_path / ".servo" / "state.json"
        lf = Lockfile()
        lf.asset_ids["raw.events"] = "01EXISTING"
        lf.save(lockfile_path)

        builder = ManifestBuilder(
            tenant_id="test",
            workspace_id="dev",
            lockfile_path=lockfile_path,
        )

        definition = AssetDefinition(
            key=AssetKey("raw", "events"),
            id=AssetId.generate(),
            code=CodeLocation(module="test", function="events"),
        )
        asset = RegisteredAsset(key=definition.key, definition=definition, func=lambda: None)

        manifest = builder.build([asset])
        assert manifest.assets[0].id == "01EXISTING"


class TestGitContextExtraction:
    """Tests for Git context extraction."""

    @patch("servo.manifest.builder._run_git_command")
    def test_extracts_git_info(self, mock_run: Any) -> None:
        """Extracts Git information from git command outputs."""
        from servo.manifest.builder import extract_git_context

        def mock_git(cmd: list[str]) -> str:
            outputs: dict[tuple[str, ...], str] = {
                ("remote", "get-url", "origin"): "https://example.com/repo.git",
                ("branch", "--show-current"): "main",
                ("rev-parse", "HEAD"): "abc123",
                ("log", "-1", "--format=%s"): "feat: test",
                ("log", "-1", "--format=%ae"): "dev@example.com",
                ("status", "--porcelain"): "",
            }

            if cmd[:3] == ["remote", "get-url", "origin"]:
                key = ("remote", "get-url", "origin")
            elif cmd[:2] == ["branch", "--show-current"]:
                key = ("branch", "--show-current")
            elif cmd[:2] == ["rev-parse", "HEAD"]:
                key = ("rev-parse", "HEAD")
            elif cmd[:2] == ["log", "-1"] and "--format=%s" in cmd:
                key = ("log", "-1", "--format=%s")
            elif cmd[:2] == ["log", "-1"] and "--format=%ae" in cmd:
                key = ("log", "-1", "--format=%ae")
            elif cmd[:2] == ["status", "--porcelain"]:
                key = ("status", "--porcelain")
            else:
                key = tuple(cmd)

            return outputs.get(key, "")

        mock_run.side_effect = mock_git

        git_ctx = extract_git_context()

        assert git_ctx.repository == "https://example.com/repo.git"
        assert git_ctx.branch == "main"
        assert git_ctx.commit_sha == "abc123"
        assert git_ctx.commit_message == "feat: test"
        assert git_ctx.author == "dev@example.com"
        assert git_ctx.dirty is False

    @patch("servo.manifest.builder._run_git_command")
    def test_handles_git_errors_gracefully(self, mock_run: Any) -> None:
        """Returns empty GitContext on git errors."""
        from servo.manifest.builder import extract_git_context

        mock_run.side_effect = subprocess.CalledProcessError(1, "git")

        git_ctx = extract_git_context()

        assert git_ctx.repository == ""
        assert git_ctx.branch == ""

    @patch("servo.manifest.builder._run_git_command")
    def test_detects_dirty_repo(self, mock_run: Any) -> None:
        """Detects when working directory has uncommitted changes."""
        from servo.manifest.builder import extract_git_context

        def mock_git(cmd: list[str]) -> str:
            if "status" in cmd:
                return "M some_file.py"  # Modified file
            if "rev-parse" in cmd and "HEAD" in cmd:
                return "abc123"
            if "branch" in cmd:
                return "* main"
            return ""

        mock_run.side_effect = mock_git

        git_ctx = extract_git_context()
        assert git_ctx.dirty is True
