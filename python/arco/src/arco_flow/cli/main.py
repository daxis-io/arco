"""Main CLI entry point using Typer.

This module defines the top-level CLI commands:
- arco-flow deploy: Deploy assets to Arco Flow
- arco-flow run: Trigger asset runs
- arco-flow status: Check run status
- arco-flow logs: Fetch run logs
- arco-flow validate: Validate asset definitions
- arco-flow init: Initialize a new project
- arco-flow worker: Run a local worker process
"""

from __future__ import annotations

from pathlib import Path  # noqa: TC003 - Typer requires runtime access
from typing import Annotated

import typer
from rich.console import Console

from arco_flow import __version__

app = typer.Typer(
    name="arco-flow",
    help="Arco Flow - Data orchestration CLI",
    no_args_is_help=True,
)
console = Console()
err_console = Console(stderr=True)


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        console.print(f"arco-flow {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[  # noqa: ARG001
        bool | None,
        typer.Option(
            "--version",
            "-V",
            callback=version_callback,
            is_eager=True,
            help="Show version and exit.",
        ),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable verbose output."),
    ] = False,
) -> None:
    """Arco Flow - Data orchestration CLI.

    Use 'arco-flow COMMAND --help' for information on specific commands.
    """
    if verbose:
        import structlog  # noqa: PLC0415

        structlog.configure(
            wrapper_class=structlog.make_filtering_bound_logger(10),  # DEBUG
        )


@app.command()
def deploy(
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Generate manifest without deploying."),
    ] = False,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Write manifest to file."),
    ] = None,
    workspace: Annotated[
        str | None,
        typer.Option("--workspace", "-w", help="Override workspace ID."),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output raw JSON."),
    ] = False,
) -> None:
    """Deploy assets to Arco Flow.

    Scans the current project for @asset-decorated functions and deploys
    them to the specified workspace.

    Examples:
        arco-flow deploy --dry-run

        arco-flow deploy --output manifest.json

        arco-flow deploy --workspace production
    """
    from arco_flow.cli.commands.deploy import run_deploy  # noqa: PLC0415

    run_deploy(
        dry_run=dry_run,
        output=output,
        workspace=workspace,
        json_output=json_output,
    )


@app.command()
def run(
    asset: Annotated[str, typer.Argument(help="Asset key to run (namespace.name).")],
    partition: Annotated[
        list[str] | None,
        typer.Option("--partition", "-p", help="Partition value (key=value)."),
    ] = None,
    run_key: Annotated[
        str | None,
        typer.Option("--run-key", help="Idempotency key for the run."),
    ] = None,
    wait: Annotated[
        bool,
        typer.Option("--wait/--no-wait", help="Wait for completion."),
    ] = False,
    timeout: Annotated[
        int,
        typer.Option("--timeout", "-t", help="Timeout in seconds (with --wait)."),
    ] = 3600,
) -> None:
    """Trigger an asset run.

    Examples:
        arco-flow run raw.events

        arco-flow run staging.users --partition date=2025-01-15

        arco-flow run mart.metrics --wait
    """
    from arco_flow.cli.commands.run import run_asset  # noqa: PLC0415

    run_asset(
        asset=asset,
        partitions=partition or [],
        wait=wait,
        timeout=timeout,
        run_key=run_key,
    )


@app.command()
def status(
    run_id: Annotated[
        str | None,
        typer.Argument(help="Run ID to check."),
    ] = None,
    watch: Annotated[
        bool,
        typer.Option("--watch", help="Watch status in real-time."),
    ] = False,
    limit: Annotated[
        int,
        typer.Option("--limit", "-n", help="Number of recent runs to show."),
    ] = 10,
) -> None:
    """Check run status.

    Without arguments, shows recent runs.

    Examples:
        arco-flow status

        arco-flow status 01HX9ABC... --watch

        arco-flow status --limit 20
    """
    from arco_flow.cli.commands.status import show_status  # noqa: PLC0415

    show_status(run_id=run_id, watch=watch, limit=limit)


@app.command()
def logs(
    run_id: Annotated[str, typer.Argument(help="Run ID to fetch logs for.")],
    task: Annotated[
        str | None,
        typer.Option("--task", "-t", help="Filter logs to a task key."),
    ] = None,
) -> None:
    """Fetch logs for a run.

    Examples:
        arco-flow logs 01HX9ABC...

        arco-flow logs 01HX9ABC... --task raw.events
    """
    from arco_flow.cli.commands.logs import show_logs  # noqa: PLC0415

    show_logs(run_id=run_id, task_key=task)


@app.command()
def validate() -> None:
    """Validate asset definitions.

    Discovers assets and validates:
    - No duplicate asset keys
    - All dependencies exist
    - Partition configurations are valid
    - Check definitions are valid

    Examples:
        arco-flow validate
    """
    from arco_flow.cli.commands.validate import run_validate  # noqa: PLC0415

    run_validate()


@app.command()
def init(
    name: Annotated[str, typer.Argument(help="Project name.")],
    template: Annotated[
        str,
        typer.Option("--template", "-t", help="Template to use."),
    ] = "basic",
) -> None:
    """Initialize a new Arco Flow project.

    Creates project structure with example assets.

    Examples:
        arco-flow init my-project

        arco-flow init my-project --template advanced
    """
    from arco_flow.cli.commands.init import run_init  # noqa: PLC0415

    run_init(name=name, template=template)


@app.command()
def worker(
    host: Annotated[
        str,
        typer.Option("--host", help="Host interface to bind."),
    ] = "0.0.0.0",
    port: Annotated[
        int,
        typer.Option("--port", help="Port to bind."),
    ] = 8081,
    root: Annotated[
        Path | None,
        typer.Option("--root", help="Project root for asset discovery."),
    ] = None,
    worker_id: Annotated[
        str | None,
        typer.Option("--worker-id", help="Explicit worker identifier."),
    ] = None,
) -> None:
    """Run a local worker HTTP server.

    Examples:
        arco-flow worker --port 8081

        arco-flow worker --root ./my-project
    """
    from arco_flow.worker.server import run_worker  # noqa: PLC0415

    run_worker(host=host, port=port, root_path=root, worker_id=worker_id)


if __name__ == "__main__":
    app()
