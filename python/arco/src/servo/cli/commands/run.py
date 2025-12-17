"""Run command implementation."""
from __future__ import annotations

from rich.console import Console

from servo.cli.config import get_config

console = Console()
err_console = Console(stderr=True)


def run_asset(
    *,
    asset: str,
    partitions: list[str],
    wait: bool,
    timeout: int,
) -> None:
    """Execute run command.

    Args:
        asset: Asset key to run.
        partitions: Partition values (key=value format).
        wait: Whether to wait for completion.
        timeout: Timeout in seconds.
    """
    config = get_config()

    # Validate config
    errors = config.validate_for_run()
    if errors:
        for err in errors:
            err_console.print(f"[red]✗[/red] {err}")
        raise SystemExit(1)

    # Parse partitions
    partition_dict: dict[str, str] = {}
    for p in partitions:
        if "=" not in p:
            err_console.print(f"[red]✗[/red] Invalid partition format: {p}. Use key=value.")
            raise SystemExit(1)
        key, value = p.split("=", 1)
        partition_dict[key] = value

    console.print(f"[blue]i[/blue] Triggering run for [cyan]{asset}[/cyan]...")

    if partition_dict:
        console.print(f"  Partitions: {partition_dict}")

    # TODO: Implement actual run trigger via API
    console.print("[yellow]![/yellow] Run triggering not yet implemented.")
    console.print("  This will call the Servo API to trigger the asset run.")

    if wait:
        console.print(f"  Would wait up to {timeout}s for completion.")
