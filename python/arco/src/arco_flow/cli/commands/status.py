"""Status command implementation."""

from __future__ import annotations

import time

from rich.console import Console
from rich.live import Live
from rich.table import Table

from arco_flow.cli.config import ArcoFlowConfig, get_config
from arco_flow.client import ApiError, ArcoFlowApiClient

console = Console()
err_console = Console(stderr=True)

_TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"}


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
    config = get_config()

    if run_id:
        _show_run_status(config, run_id, watch)
    else:
        _show_recent_runs(config, limit)


def _show_run_status(config: ArcoFlowConfig, run_id: str, watch: bool) -> None:
    """Show status of a specific run.

    Args:
        run_id: The run ID to check.
        watch: Whether to watch for updates.
    """
    console.print(f"[blue]i[/blue] Checking status for run [cyan]{run_id}[/cyan]...")

    try:
        with ArcoFlowApiClient(config) as client:
            run = client.get_run(
                workspace_id=config.workspace_id,
                run_id=run_id,
            ).payload
            if not watch:
                console.print(_render_run_table(run))
                return

            with Live(_render_run_table(run), refresh_per_second=1) as live:
                while True:
                    state = run.get("state")
                    if state in _TERMINAL_STATES:
                        break
                    time.sleep(2)
                    run = client.get_run(
                        workspace_id=config.workspace_id,
                        run_id=run_id,
                    ).payload
                    live.update(_render_run_table(run))
    except ApiError as err:
        err_console.print(f"[red]✗[/red] Status check failed: {err}")
        raise SystemExit(1) from None


def _show_recent_runs(config: ArcoFlowConfig, limit: int) -> None:
    """Show recent runs.

    Args:
        limit: Number of runs to show.
    """
    console.print(f"[blue]i[/blue] Showing {limit} most recent runs...")

    try:
        with ArcoFlowApiClient(config) as client:
            response = client.list_runs(
                workspace_id=config.workspace_id,
                limit=limit,
            ).payload
    except ApiError as err:
        err_console.print(f"[red]✗[/red] Run listing failed: {err}")
        raise SystemExit(1) from None

    runs = response.get("runs", [])
    if not runs:
        console.print("[yellow]![/yellow] No runs found.")
        return

    table = Table(title="Recent Runs")
    table.add_column("Run ID", style="cyan")
    table.add_column("State")
    table.add_column("Created")
    table.add_column("Completed")
    table.add_column("Tasks")
    table.add_column("Failed")

    for run in runs:
        table.add_row(
            run.get("runId", ""),
            run.get("state", ""),
            run.get("createdAt", ""),
            run.get("completedAt", "") or "-",
            str(run.get("taskCount", "")),
            str(run.get("tasksFailed", "")),
        )

    console.print(table)


def _render_run_table(run: dict[str, object]) -> Table:
    run_id = run.get("runId", "")
    state = run.get("state", "")
    title = f"Run {run_id} ({state})"

    parent_run_id = run.get("parentRunId")
    rerun_kind = run.get("rerunKind")
    if parent_run_id:
        title = f"{title} parent={parent_run_id}"
    if rerun_kind:
        title = f"{title} rerun={rerun_kind}"

    table = Table(title=title, show_lines=True)
    table.add_column("Task", style="cyan")
    table.add_column("State")
    table.add_column("Attempt")
    table.add_column("Started")
    table.add_column("Completed")
    table.add_column("Error")

    tasks = run.get("tasks", []) or []
    if not tasks:
        table.add_row("-", "-", "-", "-", "-", "-")
        return table

    for task in tasks:
        table.add_row(
            str(task.get("taskKey", "")),
            str(task.get("state", "")),
            str(task.get("attempt", "")),
            str(task.get("startedAt", "") or "-"),
            str(task.get("completedAt", "") or "-"),
            str(task.get("errorMessage", "") or "-"),
        )

    return table
