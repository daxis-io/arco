"""Run command implementation."""

from __future__ import annotations

import time

from rich.console import Console

from arco_flow.cli.config import get_config
from arco_flow.client import ApiError, ArcoFlowApiClient
from arco_flow.types import PartitionKey
from arco_flow.types.scalar import PartitionDimensionValue

console = Console()
err_console = Console(stderr=True)

_TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"}


def run_asset(
    *,
    asset: str | None,
    assets: list[str],
    rerun: str | None,
    from_failure: bool,
    include_upstream: bool,
    include_downstream: bool,
    partitions: list[str],
    wait: bool,
    timeout: int,
    run_key: str | None,
) -> None:
    """Execute run command.

    Args:
        asset: Optional positional asset key.
        assets: Optional list of asset keys from `--asset` flags.
        rerun: Optional parent run ID for reruns.
        from_failure: Whether to rerun from failure.
        include_upstream: Whether to include upstream dependencies.
        include_downstream: Whether to include downstream dependents.
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

    selection: list[str] = []
    if asset:
        selection.append(_normalize_asset_key(asset))
    selection.extend(_normalize_asset_key(value) for value in assets)

    # Parse partitions
    partition_dict: dict[str, str] = {}
    for p in partitions:
        if "=" not in p:
            err_console.print(f"[red]✗[/red] Invalid partition format: {p}. Use key=value.")
            raise SystemExit(1)
        key, value = p.split("=", 1)
        partition_dict[key] = value

    partition_key = None
    if partition_dict:
        partition_dims: dict[str, PartitionDimensionValue] = {
            key: value for key, value in partition_dict.items()
        }
        partition_key = PartitionKey(partition_dims).canonical_string()

    if rerun:
        _rerun_run(
            config=config,
            parent_run_id=rerun,
            from_failure=from_failure,
            selection=selection,
            include_upstream=include_upstream,
            include_downstream=include_downstream,
            run_key=run_key,
            wait=wait,
            timeout=timeout,
        )
        return

    if not selection:
        err_console.print(
            "[red]✗[/red] Must specify an asset (positional) or one or more --asset flags."
        )
        raise SystemExit(1)

    console.print(f"[blue]i[/blue] Triggering run for [cyan]{', '.join(selection)}[/cyan]...")

    if partition_dict:
        console.print(f"  Partitions: {partition_dict}")

    try:
        with ArcoFlowApiClient(config) as client:
            response = client.trigger_run(
                workspace_id=config.workspace_id,
                selection=selection,
                partition_key=partition_key,
                run_key=run_key,
                include_upstream=include_upstream,
                include_downstream=include_downstream,
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

    created = response.get("created")
    if created is False:
        console.print(f"[yellow]![/yellow] Existing run found (run_key match): {run_id}")
    else:
        console.print(f"[green]✓[/green] Run created: {run_id}")

    console.print(f"  Plan ID: {plan_id}")
    console.print(f"  State: {state}")

    if not wait:
        return

    _wait_for_completion(config=config, run_id=run_id, timeout=timeout)


def _rerun_run(
    *,
    config,
    parent_run_id: str,
    from_failure: bool,
    selection: list[str],
    include_upstream: bool,
    include_downstream: bool,
    run_key: str | None,
    wait: bool,
    timeout: int,
) -> None:
    if from_failure:
        if selection:
            err_console.print("[red]✗[/red] --from-failure cannot be combined with assets.")
            raise SystemExit(1)
        if include_upstream or include_downstream:
            err_console.print(
                "[red]✗[/red] --from-failure cannot be combined with --include-upstream/--include-downstream."
            )
            raise SystemExit(1)
    else:
        if not selection:
            err_console.print(
                "[red]✗[/red] Subset rerun requires assets (positional or --asset), or use --from-failure."
            )
            raise SystemExit(1)

    mode = "fromFailure" if from_failure else "subset"

    if from_failure:
        console.print(f"[blue]i[/blue] Rerunning from failure: [cyan]{parent_run_id}[/cyan]")
    else:
        console.print(
            f"[blue]i[/blue] Rerunning subset from [cyan]{parent_run_id}[/cyan]: [cyan]{', '.join(selection)}[/cyan]"
        )

    try:
        with ArcoFlowApiClient(config) as client:
            response = client.rerun_run(
                workspace_id=config.workspace_id,
                run_id=parent_run_id,
                mode=mode,
                selection=selection,
                include_upstream=include_upstream,
                include_downstream=include_downstream,
                run_key=run_key,
                labels=None,
            ).payload
    except ApiError as err:
        err_console.print(f"[red]✗[/red] Rerun failed: {err}")
        raise SystemExit(1) from None

    run_id = response.get("runId")
    if not run_id:
        err_console.print("[red]✗[/red] Rerun response missing runId.")
        raise SystemExit(1)

    created = response.get("created")
    if created is False:
        console.print("[yellow]![/yellow] Existing rerun found (run_key match)")
    else:
        console.print("[green]✓[/green] Rerun created!")

    console.print(f"  Run ID:     {run_id}")
    console.print(f"  Parent:     {response.get('parentRunId')}")
    console.print(f"  Rerun Kind: {response.get('rerunKind')}")
    console.print(f"  State:      {response.get('state')}")

    if not wait:
        return

    _wait_for_completion(config=config, run_id=run_id, timeout=timeout)


def _wait_for_completion(*, config, run_id: str, timeout: int) -> None:
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


def _normalize_asset_key(value: str) -> str:
    return value.strip().replace("/", ".")
