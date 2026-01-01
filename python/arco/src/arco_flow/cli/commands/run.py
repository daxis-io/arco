"""Run command implementation."""

from __future__ import annotations

import time

from rich.console import Console

from arco_flow.cli.config import get_config
from arco_flow.client import ApiError, ArcoFlowApiClient

console = Console()
err_console = Console(stderr=True)

_TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"}


def run_asset(
    *,
    asset: str,
    partitions: list[str],
    wait: bool,
    timeout: int,
    run_key: str | None,
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

    partition_payload = [{"key": key, "value": value} for key, value in partition_dict.items()]

    try:
        with ArcoFlowApiClient(config) as client:
            response = client.trigger_run(
                workspace_id=config.workspace_id,
                selection=[asset],
                partitions=partition_payload,
                run_key=run_key,
            ).payload
    except ApiError as err:
        err_console.print(f"[red]✗[/red] Run trigger failed: {err}")
        raise SystemExit(1) from None

    run_id = response.get("runId")
    plan_id = response.get("planId")
    state = response.get("state")
    if not run_id:
        err_console.print("[red]✗[/red] Run response missing runId.")
        raise SystemExit(1)
    console.print(f"[green]✓[/green] Run created: {run_id}")
    console.print(f"  Plan ID: {plan_id}")
    console.print(f"  State: {state}")

    if not wait:
        return

    deadline = time.time() + timeout
    console.print(f"[blue]i[/blue] Waiting for completion (timeout {timeout}s)...")

    try:
        with ArcoFlowApiClient(config) as client:
            while time.time() < deadline:
                run = client.get_run(
                    workspace_id=config.workspace_id,
                    run_id=run_id,
                ).payload
                state = run.get("state")
                if state in _TERMINAL_STATES:
                    console.print(f"[green]✓[/green] Run finished: {state}")
                    if state != "SUCCEEDED":
                        raise SystemExit(1)
                    return
                time.sleep(2)
    except ApiError as err:
        err_console.print(f"[red]✗[/red] Failed to fetch run status: {err}")
        raise SystemExit(1) from None

    err_console.print("[red]✗[/red] Timed out waiting for run completion.")
    raise SystemExit(1)
