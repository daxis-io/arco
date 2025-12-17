"""Servo CLI entrypoint."""
from __future__ import annotations

import typer

from servo import __version__

app = typer.Typer(help="Arco Servo CLI", no_args_is_help=True)


@app.callback()
def main() -> None:
    """Arco Servo CLI."""


@app.command()
def version() -> None:
    """Print the installed SDK version."""
    typer.echo(__version__)
