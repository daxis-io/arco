"""Status command implementation."""
from __future__ import annotations

from rich.console import Console
from rich.table import Table

from servo.cli.config import get_config

console = Console()
err_console = Console(stderr=True)


def show_status(
    *,
    run_id: str | None,
    watch: bool,
    limit: int,
) -> None:
    """Execute status command.

    Args:
        run_id: Specific run ID to check.
        watch: Watch status in real-time.
        limit: Number of recent runs to show.
    """
    _ = get_config()  # Ensure config is valid

    if run_id:
        _show_run_status(run_id, watch)
    else:
        _show_recent_runs(limit)


def _show_run_status(run_id: str, watch: bool) -> None:
    """Show status of a specific run.

    Args:
        run_id: The run ID to check.
        watch: Whether to watch for updates.
    """
    console.print(f"[blue]i[/blue] Checking status for run [cyan]{run_id}[/cyan]...")

    # TODO: Implement actual status check via API
    console.print("[yellow]![/yellow] Status checking not yet implemented.")

    if watch:
        console.print("  Would watch for status updates...")


def _show_recent_runs(limit: int) -> None:
    """Show recent runs.

    Args:
        limit: Number of runs to show.
    """
    console.print(f"[blue]i[/blue] Showing {limit} most recent runs...")

    # TODO: Implement actual run listing via API
    table = Table(title="Recent Runs")
    table.add_column("Run ID", style="cyan")
    table.add_column("Asset")
    table.add_column("Status")
    table.add_column("Started")
    table.add_column("Duration")

    # Placeholder data
    console.print("[yellow]![/yellow] Run listing not yet implemented.")
    console.print(table)
