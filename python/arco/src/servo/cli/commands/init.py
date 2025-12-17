"""Init command implementation."""
from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from rich.console import Console

console = Console()


def run_init(*, name: str, template: str) -> None:  # noqa: ARG001
    """Execute init command.

    Args:
        name: Project name.
        template: Template to use.
    """
    project_dir = Path(name)

    if project_dir.exists():
        console.print(f"[red]✗[/red] Directory {name} already exists")
        raise SystemExit(1)

    console.print(f"[blue]i[/blue] Creating project {name}...")

    # Create directory structure
    project_dir.mkdir()
    (project_dir / "assets").mkdir()
    (project_dir / "tests").mkdir()

    # Create __init__.py files
    (project_dir / "assets" / "__init__.py").write_text("")
    (project_dir / "tests" / "__init__.py").write_text("")

    # Create example asset
    (project_dir / "assets" / "example.py").write_text(dedent('''
        """Example asset definitions."""
        from servo import asset
        from servo.context import AssetContext
        from servo.types import AssetOut, DailyPartition


        @asset(
            namespace="raw",
            description="Example raw data asset",
            partitions=DailyPartition("date"),
        )
        def example_events(ctx: AssetContext) -> AssetOut:
            """Load example events for a given date.

            This is a placeholder - replace with your actual data loading logic.
            """
            # Example: Load data for the partition date
            # partition_date = ctx.partition_key.dimensions["date"].value

            data = {"message": "Hello, Servo!"}
            return ctx.output(data)
    ''').strip() + "\n")

    # Create pyproject.toml
    (project_dir / "pyproject.toml").write_text(dedent(f'''
        [project]
        name = "{name}"
        version = "0.1.0"
        requires-python = ">=3.11"
        dependencies = [
            "arco-servo>=0.1.0",
        ]

        [project.optional-dependencies]
        dev = [
            "pytest>=7.4.0",
            "ruff>=0.1.8",
        ]
    ''').strip() + "\n")

    # Create .env.example
    (project_dir / ".env.example").write_text(dedent('''
        # Servo configuration
        SERVO_TENANT_ID=your-tenant-id
        SERVO_WORKSPACE_ID=development
        SERVO_API_KEY=your-api-key

        # Optional
        # SERVO_API_URL=https://api.servo.dev
    ''').strip() + "\n")

    console.print(f"[green]✓[/green] Created project {name}")
    console.print()
    console.print("Next steps:")
    console.print(f"  cd {name}")
    console.print("  pip install -e .[dev]")
    console.print("  cp .env.example .env")
    console.print("  servo validate")
    console.print("  servo deploy --dry-run")
