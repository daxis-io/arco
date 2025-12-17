"""Validate command implementation."""
from __future__ import annotations

from pathlib import Path

from rich.console import Console

from servo.manifest.discovery import AssetDiscovery

console = Console()
err_console = Console(stderr=True)


def run_validate() -> None:
    """Execute validate command."""
    console.print(f"[blue]i[/blue] Validating assets in {Path.cwd()}...")

    # Discover assets
    discovery = AssetDiscovery()

    try:
        assets = discovery.discover()
    except Exception as e:
        err_console.print(f"[red]✗[/red] Discovery failed: {e}")
        raise SystemExit(1) from None

    if not assets:
        console.print("[yellow]![/yellow] No assets found.")
        return

    console.print(f"[green]✓[/green] Found {len(assets)} assets")

    # Validate dependencies
    errors: list[str] = []
    asset_keys = {str(a.key) for a in assets}

    for asset in assets:
        for dep in asset.definition.dependencies:
            dep_key = str(dep.upstream_key)
            if dep_key not in asset_keys:
                errors.append(
                    f"Asset {asset.key} depends on {dep_key} which is not defined"
                )

    # Report results
    if errors:
        err_console.print(f"\n[red]✗[/red] Found {len(errors)} validation errors:")
        for err in errors:
            err_console.print(f"  • {err}")
        raise SystemExit(1)

    console.print("[green]✓[/green] All assets valid")
