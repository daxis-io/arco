"""CLI worker defaults."""

from __future__ import annotations

import inspect

from arco_flow.cli.main import worker


def test_worker_command_binds_loopback_by_default() -> None:
    signature = inspect.signature(worker)

    assert signature.parameters["host"].default == "127.0.0.1"
