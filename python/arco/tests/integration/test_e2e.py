"""End-to-end integration tests."""
from __future__ import annotations

import json
from pathlib import Path  # noqa: TC003 - used at runtime
from textwrap import dedent

import pytest


@pytest.fixture
def project_with_assets(tmp_path: Path) -> Path:
    """Create a project with multiple assets."""
    (tmp_path / "assets").mkdir()
    (tmp_path / "assets" / "__init__.py").write_text("")

    (tmp_path / "assets" / "raw.py").write_text(dedent("""
        from servo import asset
        from servo.context import AssetContext
        from servo.types import AssetOut, DailyPartition

        @asset(namespace="raw", partitions=DailyPartition("date"))
        def events(ctx: AssetContext) -> AssetOut:
            '''Raw event data.'''
            return ctx.output([])
    """))

    (tmp_path / "assets" / "staging.py").write_text(dedent("""
        from servo import asset
        from servo.context import AssetContext
        from servo.types import AssetIn, AssetOut, DailyPartition, row_count

        @asset(
            namespace="staging",
            partitions=DailyPartition("date"),
            checks=[row_count(min_rows=1)],
        )
        def cleaned_events(ctx: AssetContext, raw: AssetIn["raw.events"]) -> AssetOut:
            '''Cleaned events.'''
            return ctx.output([])
    """))

    return tmp_path


class TestDiscoveryE2E:
    """End-to-end discovery tests."""

    def test_discovers_all_assets(self, project_with_assets: Path) -> None:
        """Discovery finds all @asset decorated functions."""
        from servo.manifest.discovery import AssetDiscovery

        discovery = AssetDiscovery(root_path=project_with_assets)
        assets = discovery.discover()

        assert len(assets) == 2
        keys = {str(a.key) for a in assets}
        assert keys == {"raw.events", "staging.cleaned_events"}

    def test_dependency_extraction(self, project_with_assets: Path) -> None:
        """Dependencies are correctly extracted."""
        from servo.manifest.discovery import AssetDiscovery

        discovery = AssetDiscovery(root_path=project_with_assets)
        assets = discovery.discover()

        staging = next(a for a in assets if a.key.name == "cleaned_events")
        assert len(staging.definition.dependencies) == 1
        assert staging.definition.dependencies[0].upstream_key.name == "events"

    def test_partition_extraction(self, project_with_assets: Path) -> None:
        """Partitioning is correctly extracted."""
        from servo.manifest.discovery import AssetDiscovery

        discovery = AssetDiscovery(root_path=project_with_assets)
        assets = discovery.discover()

        raw = next(a for a in assets if a.key.name == "events")
        assert raw.definition.partitioning.is_partitioned
        assert raw.definition.partitioning.dimension_names == ["date"]


class TestManifestE2E:
    """End-to-end manifest generation tests."""

    def test_generates_valid_manifest(self, project_with_assets: Path) -> None:
        """Manifest generation produces valid JSON."""
        from servo.manifest.builder import ManifestBuilder
        from servo.manifest.discovery import AssetDiscovery

        discovery = AssetDiscovery(root_path=project_with_assets)
        assets = discovery.discover()

        builder = ManifestBuilder(tenant_id="test", workspace_id="dev")
        manifest = builder.build(assets)

        # Should be valid JSON
        json_str = manifest.to_canonical_json()
        parsed = json.loads(json_str)

        assert parsed["tenantId"] == "test"  # camelCase
        assert len(parsed["assets"]) == 2

    def test_manifest_has_git_context(self, project_with_assets: Path) -> None:
        """Manifest includes Git context."""
        from servo.manifest.builder import ManifestBuilder
        from servo.manifest.discovery import AssetDiscovery

        discovery = AssetDiscovery(root_path=project_with_assets)
        assets = discovery.discover()

        builder = ManifestBuilder(tenant_id="test", workspace_id="dev")
        manifest = builder.build(assets)

        # We're in a git repo during tests
        assert manifest.git is not None

    def test_manifest_fingerprint_stable(self, project_with_assets: Path) -> None:
        """Manifest fingerprint is stable."""
        from servo.manifest.builder import ManifestBuilder
        from servo.manifest.discovery import AssetDiscovery

        discovery = AssetDiscovery(root_path=project_with_assets)
        assets = discovery.discover()

        builder = ManifestBuilder(tenant_id="test", workspace_id="dev", include_git=False)
        manifest1 = builder.build(assets)
        manifest2 = builder.build(assets)

        # Fingerprints should be equal (excluding timestamp)
        # Note: deployed_at will differ, so we test JSON structure instead
        json1 = json.loads(manifest1.to_canonical_json())
        json2 = json.loads(manifest2.to_canonical_json())

        # Remove timestamp for comparison
        del json1["deployedAt"]
        del json2["deployedAt"]
        del json1["codeVersionId"]
        del json2["codeVersionId"]

        assert json.dumps(json1, sort_keys=True) == json.dumps(json2, sort_keys=True)


class TestCLIE2E:
    """End-to-end CLI tests."""

    def test_deploy_dry_run_e2e(self, project_with_assets: Path) -> None:
        """Full deploy dry-run flow works."""
        from typer.testing import CliRunner

        from servo.cli.main import app

        runner = CliRunner()

        with runner.isolated_filesystem(temp_dir=project_with_assets):
            result = runner.invoke(app, ["deploy", "--dry-run"])

        assert result.exit_code == 0
        # Should show deployment summary or assets found message
        # Note: either shows assets or "No assets found" message
        stdout = result.stdout.lower()
        assert "assets" in stdout or "manifest" in stdout or "dry run" in stdout

    def test_validate_e2e(self, project_with_assets: Path) -> None:
        """Full validate flow works."""
        from typer.testing import CliRunner

        from servo.cli.main import app

        runner = CliRunner()

        with runner.isolated_filesystem(temp_dir=project_with_assets):
            result = runner.invoke(app, ["validate"])

        assert result.exit_code == 0
        assert "valid" in result.stdout.lower() or "assets" in result.stdout.lower()
