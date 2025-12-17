"""Tests for manifest builder."""
from __future__ import annotations

import subprocess
from typing import Any
from unittest.mock import patch


class TestManifestBuilder:
    """Tests for ManifestBuilder."""

    def test_build_empty_manifest(self) -> None:
        """Building with no assets creates empty manifest."""
        from servo.manifest.builder import ManifestBuilder

        builder = ManifestBuilder(tenant_id="test", workspace_id="dev")
        manifest = builder.build([])

        assert manifest.tenant_id == "test"
        assert manifest.workspace_id == "dev"
        assert manifest.assets == []

    def test_build_with_assets(self) -> None:
        """Building with assets includes them in manifest."""
        from servo.asset import RegisteredAsset
        from servo.manifest.builder import ManifestBuilder
        from servo.types import AssetDefinition, AssetId, AssetKey, CodeLocation

        builder = ManifestBuilder(tenant_id="test", workspace_id="dev")

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

    def test_build_generates_code_version_id(self) -> None:
        """Build generates a code version ID."""
        from servo.manifest.builder import ManifestBuilder

        builder = ManifestBuilder(tenant_id="test", workspace_id="dev")
        manifest = builder.build([])

        assert manifest.code_version_id != ""
        assert len(manifest.code_version_id) == 26  # ULID format

    def test_build_sets_deployed_by(self) -> None:
        """Build sets deployed_by from environment or default."""
        from servo.manifest.builder import ManifestBuilder

        builder = ManifestBuilder(
            tenant_id="test",
            workspace_id="dev",
            deployed_by="user@example.com",
        )
        manifest = builder.build([])

        assert manifest.deployed_by == "user@example.com"


class TestGitContextExtraction:
    """Tests for Git context extraction."""

    def test_extracts_git_info(self) -> None:
        """Extracts Git information when in a git repo."""
        from servo.manifest.builder import extract_git_context

        git_ctx = extract_git_context()

        # Should have some values (we're in a git repo during tests)
        assert git_ctx.branch != "" or git_ctx.commit_sha != ""

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
