"""Deploy command implementation."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def run_deploy(
    *,
    dry_run: bool,
    output: Path | None,
    workspace: str | None,
    json_output: bool,
) -> None:
    """Execute deploy command (stub)."""
    pass
