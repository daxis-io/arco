"""Logs command implementation."""
from __future__ import annotations

from rich.console import Console

from servo.cli.config import get_config
from servo.client import ApiError, ServoApiClient

console = Console()
err_console = Console(stderr=True)


def show_logs(*, run_id: str, task_key: str | None) -> None:
    """Fetch and print logs for a run.

    Args:
        run_id: Run ID to fetch logs for.
        task_key: Optional task key filter.
    """
    config = get_config()

    try:
        with ServoApiClient(config) as client:
            logs_text = client.get_logs(
                workspace_id=config.workspace_id,
                run_id=run_id,
                task_key=task_key,
            )
    except ApiError as err:
        err_console.print(f"[red]✗[/red] Logs fetch failed: {err}")
        raise SystemExit(1) from None

    console.print(logs_text)
