"""Tests for manifest model."""

from __future__ import annotations

import json


class TestGitContext:
    """Tests for GitContext."""

    def test_default_values(self) -> None:
        """Default values are empty strings and False."""
        from arco_flow.manifest.model import GitContext

        git = GitContext()
        assert git.repository == ""
        assert git.branch == ""
        assert git.commit_sha == ""
        assert git.dirty is False

    def test_with_values(self) -> None:
        """Can create with actual values."""
        from arco_flow.manifest.model import GitContext

        git = GitContext(
            repository="https://github.com/arco/arco",
            branch="main",
            commit_sha="abc123",
            dirty=True,
        )
        assert git.repository == "https://github.com/arco/arco"
        assert git.dirty is True


class TestAssetManifest:
    """Tests for AssetManifest."""

    def test_default_values(self) -> None:
        """Default values are sensible."""
        from arco_flow.manifest.model import AssetManifest

        manifest = AssetManifest()
        assert manifest.manifest_version == "1.0"
        assert manifest.tenant_id == ""
        assert manifest.assets == []

    def test_with_values(self) -> None:
        """Can create with actual values."""
        from arco_flow.manifest.model import AssetManifest, GitContext

        manifest = AssetManifest(
            tenant_id="acme",
            workspace_id="production",
            git=GitContext(branch="main"),
        )
        assert manifest.tenant_id == "acme"
        assert manifest.git.branch == "main"

    def test_to_canonical_json_uses_camel_case(self) -> None:
        """Canonical JSON uses camelCase keys."""
        from arco_flow.manifest.model import AssetManifest

        manifest = AssetManifest(
            tenant_id="test",
            workspace_id="dev",
        )
        json_str = manifest.to_canonical_json()
        parsed = json.loads(json_str)

        assert "tenantId" in parsed
        assert "workspaceId" in parsed
        assert "manifestVersion" in parsed
        assert "tenant_id" not in json_str

    def test_to_canonical_json_is_deterministic(self) -> None:
        """Canonical JSON output is deterministic."""
        from arco_flow.manifest.model import AssetManifest, GitContext

        manifest = AssetManifest(
            tenant_id="test",
            workspace_id="dev",
            git=GitContext(branch="main"),
        )

        results = [manifest.to_canonical_json() for _ in range(5)]
        assert all(r == results[0] for r in results)

    def test_to_dict_converts_nested(self) -> None:
        """to_dict converts nested objects."""
        from arco_flow.manifest.model import AssetManifest, GitContext

        manifest = AssetManifest(
            tenant_id="test",
            git=GitContext(branch="main"),
        )
        d = manifest.to_dict()

        assert isinstance(d["git"], dict)
        assert d["git"]["branch"] == "main"

    def test_fingerprint_is_stable(self) -> None:
        """Fingerprint is stable for same content."""
        from arco_flow.manifest.model import AssetManifest

        manifest = AssetManifest(tenant_id="test")
        fp1 = manifest.fingerprint()
        fp2 = manifest.fingerprint()

        assert fp1 == fp2
        assert len(fp1) == 64  # SHA-256 hex


class TestAssetEntry:
    """Tests for AssetEntry in manifest."""

    def test_from_definition(self) -> None:
        """Can create AssetEntry from AssetDefinition."""
        from arco_flow.manifest.model import AssetEntry
        from arco_flow.types import AssetDefinition, AssetId, AssetKey, CodeLocation

        definition = AssetDefinition(
            key=AssetKey("raw", "events"),
            id=AssetId.generate(),
            description="Raw events",
            code=CodeLocation(module="assets.raw", function="events"),
        )

        entry = AssetEntry.from_definition(definition)
        assert entry.key["namespace"] == "raw"
        assert entry.key["name"] == "events"
        assert entry.description == "Raw events"
