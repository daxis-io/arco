"""Deploy command implementation."""
from __future__ import annotations

from collections.abc import Sequence  # noqa: TC003
from pathlib import Path
from typing import TYPE_CHECKING

from rich.console import Console
from rich.table import Table
from rich.tree import Tree

from servo.cli.config import get_config
from servo.client import ApiError, ServoApiClient
from servo.manifest.builder import ManifestBuilder
from servo.manifest.discovery import AssetDiscovery, AssetDiscoveryError

if TYPE_CHECKING:
    from servo._internal.registry import RegisteredAssetProtocol

console = Console()
err_console = Console(stderr=True)


def run_deploy(
    *,
    dry_run: bool,
    output: Path | None,
    workspace: str | None,
    json_output: bool,
) -> None:
    """Execute deploy command.

    Args:
        dry_run: Generate manifest without deploying.
        output: Path to write manifest file.
        workspace: Override workspace ID.
        json_output: Output raw JSON.
    """
    config = get_config()
    workspace_id = workspace or config.workspace_id

    # Validate for non-dry-run
    if not dry_run:
        errors = config.validate_for_deploy()
        if errors:
            for err in errors:
                err_console.print(f"[red]✗[/red] {err}")
            raise SystemExit(1)

    # Discover assets
    if not json_output:
        console.print(f"[blue]i[/blue] Discovering assets in {Path.cwd()}...")

    discovery = AssetDiscovery()
    try:
        assets = discovery.discover(strict=True)
    except AssetDiscoveryError as e:
        err_console.print("[red]✗[/red] Asset discovery failed")
        for failure in e.failures:
            err_console.print(f"  - {failure.file_path}: {failure.error}")
        raise SystemExit(1) from None

    if not assets:
        console.print("[yellow]![/yellow] No assets found. Use @asset decorator to define assets.")
        return

    # Build manifest
    builder = ManifestBuilder(
        tenant_id=config.tenant_id or "local",
        workspace_id=workspace_id,
    )
    manifest = builder.build(assets)

    # Output
    if json_output:
        console.print(manifest.to_canonical_json())
    else:
        _print_summary(manifest, assets)

    # Write to file
    if output:
        output.write_text(manifest.to_canonical_json())
        console.print(f"[green]✓[/green] Manifest written to {output}")

    if dry_run:
        console.print("[blue]i[/blue] Dry run complete. Use without --dry-run to deploy.")
    else:
        try:
            with ServoApiClient(config) as client:
                response = client.deploy_manifest(
                    workspace_id=workspace_id,
                    manifest_json=manifest.to_canonical_json(),
                    idempotency_key=manifest.fingerprint(),
                ).payload
        except ApiError as err:
            err_console.print(f"[red]✗[/red] Deploy failed: {err}")
            raise SystemExit(1) from None

        console.print(
            f"[green]✓[/green] Deployed {response.get('assetCount')} assets "
            f"to {workspace_id}"
        )
        console.print(f"  Manifest ID: {response.get('manifestId')}")
        console.print(f"  Fingerprint: {response.get('fingerprint')}")


def _print_summary(manifest: object, assets: Sequence[RegisteredAssetProtocol]) -> None:
    """Print manifest summary.

    Args:
        manifest: The generated manifest.
        assets: List of discovered assets.
    """
    from servo.manifest.model import AssetManifest  # noqa: PLC0415

    if not isinstance(manifest, AssetManifest):
        return

    table = Table(title="Manifest Summary")
    table.add_column("Property", style="cyan")
    table.add_column("Value")

    table.add_row("Tenant", manifest.tenant_id)
    table.add_row("Workspace", manifest.workspace_id)
    table.add_row("Code Version", manifest.code_version_id[:12] + "...")
    table.add_row("Assets", str(len(assets)))

    if manifest.git.branch:
        table.add_row("Git Branch", manifest.git.branch)
    if manifest.git.commit_sha:
        table.add_row("Git Commit", manifest.git.commit_sha[:12])
    if manifest.git.dirty:
        table.add_row("Git Status", "[yellow]dirty[/yellow]")

    console.print(table)

    # Asset tree
    tree = Tree("[bold]Assets[/bold]")
    by_ns: dict[str, list[RegisteredAssetProtocol]] = {}
    for a in assets:
        ns = a.key.namespace
        by_ns.setdefault(ns, []).append(a)

    for ns, ns_assets in sorted(by_ns.items()):
        branch = tree.add(f"[cyan]{ns}/[/cyan]")
        for a in ns_assets:
            deps = len(a.definition.dependencies)
            name = a.key.name
            branch.add(f"{name} [dim]({deps} deps)[/dim]")

    console.print(tree)
