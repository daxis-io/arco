"""Manifest builder for creating deployment manifests."""
from __future__ import annotations

import os
import subprocess
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from servo.manifest.model import AssetEntry, AssetManifest, GitContext
from servo.types.ids import MaterializationId

if TYPE_CHECKING:
    from servo.asset import RegisteredAsset


def _run_git_command(args: list[str]) -> str:
    """Run a git command and return output.

    Args:
        args: Git command arguments.

    Returns:
        Command output stripped of whitespace.

    Raises:
        subprocess.CalledProcessError: If git command fails.
    """
    result = subprocess.run(  # noqa: S603
        ["git", *args],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def extract_git_context() -> GitContext:
    """Extract Git context from current repository.

    Returns:
        GitContext with repository information, or empty context on error.
    """
    try:
        # Get remote URL
        try:
            repository = _run_git_command(["remote", "get-url", "origin"])
        except subprocess.CalledProcessError:
            repository = ""

        # Get current branch
        try:
            branch = _run_git_command(["branch", "--show-current"])
            if not branch:
                # Detached HEAD - get commit instead
                branch = _run_git_command(["rev-parse", "--short", "HEAD"])
        except subprocess.CalledProcessError:
            branch = ""

        # Get current commit SHA
        try:
            commit_sha = _run_git_command(["rev-parse", "HEAD"])
        except subprocess.CalledProcessError:
            commit_sha = ""

        # Get commit message
        try:
            commit_message = _run_git_command(["log", "-1", "--format=%s"])
        except subprocess.CalledProcessError:
            commit_message = ""

        # Get author
        try:
            author = _run_git_command(["log", "-1", "--format=%ae"])
        except subprocess.CalledProcessError:
            author = ""

        # Check if working directory is dirty
        try:
            status = _run_git_command(["status", "--porcelain"])
            dirty = bool(status)
        except subprocess.CalledProcessError:
            dirty = False

        return GitContext(
            repository=repository,
            branch=branch,
            commit_sha=commit_sha,
            commit_message=commit_message,
            author=author,
            dirty=dirty,
        )

    except Exception:
        # Return empty context on any error
        return GitContext()


class ManifestBuilder:
    """Builder for creating deployment manifests.

    Example:
        >>> builder = ManifestBuilder(tenant_id="acme", workspace_id="prod")
        >>> manifest = builder.build(discovered_assets)
    """

    def __init__(
        self,
        tenant_id: str,
        workspace_id: str,
        *,
        deployed_by: str | None = None,
        include_git: bool = True,
    ) -> None:
        """Initialize the manifest builder.

        Args:
            tenant_id: Tenant identifier.
            workspace_id: Workspace identifier.
            deployed_by: User or system deploying (defaults to environment user).
            include_git: Whether to include Git context.
        """
        self.tenant_id = tenant_id
        self.workspace_id = workspace_id
        self.deployed_by = deployed_by or os.environ.get("USER", "unknown")
        self.include_git = include_git

    def build(self, assets: list[RegisteredAsset]) -> AssetManifest:
        """Build a manifest from discovered assets.

        Args:
            assets: List of registered assets from discovery.

        Returns:
            Complete AssetManifest ready for deployment.
        """
        # Convert assets to entries
        asset_entries = [
            AssetEntry.from_definition(asset.definition)
            for asset in sorted(assets, key=lambda a: str(a.key))
        ]

        # Extract Git context if enabled
        git_context = extract_git_context() if self.include_git else GitContext()

        # Generate code version ID (ULID)
        code_version_id = str(MaterializationId.generate())

        return AssetManifest(
            manifest_version="1.0",
            tenant_id=self.tenant_id,
            workspace_id=self.workspace_id,
            code_version_id=code_version_id,
            git=git_context,
            assets=asset_entries,
            deployed_at=datetime.now(UTC),
            deployed_by=self.deployed_by,
        )
