"""Asset execution context."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from servo.types import AssetOut, PartitionKey


@dataclass
class AssetContext:
    """Context provided to asset functions during execution.

    The context provides access to:
    - Partition key for the current execution
    - Methods to create outputs with metadata
    - Progress reporting for long-running tasks
    - Custom metric reporting
    - Configuration and secrets

    Example:
        @asset(namespace="staging")
        def my_asset(ctx: AssetContext) -> AssetOut:
            ctx.report_progress("Loading data", percentage=10)
            data = load_data()
            ctx.report_progress("Transforming", percentage=50)
            result = transform(data)
            ctx.report_metric("rows_processed", len(result))
            return ctx.output(result)
    """

    partition_key: PartitionKey = field(default_factory=PartitionKey)
    run_id: str = ""
    task_id: str = ""
    tenant_id: str = ""
    workspace_id: str = ""

    # Internal tracking for progress and metrics
    _progress_messages: list[dict[str, Any]] = field(default_factory=list)
    _metrics: dict[str, int | float] = field(default_factory=dict)
    _progress_callback: Any = None  # Set by worker for streaming

    def output(
        self,
        data: object,
        *,
        schema: dict[str, Any] | None = None,
        row_count: int | None = None,
    ) -> AssetOut:
        """Create an output wrapper for the asset's result.

        Args:
            data: The output data (DataFrame, path, etc.)
            schema: Optional schema metadata
            row_count: Optional row count (auto-detected if possible)

        Returns:
            AssetOut wrapper with metadata
        """
        return AssetOut(data=data, schema=schema, row_count=row_count)

    def report_progress(
        self,
        message: str,
        *,
        percentage: int | None = None,
    ) -> None:
        """Report progress for long-running tasks.

        Progress messages are tracked and can be streamed to the
        control plane for UI updates.

        Args:
            message: Human-readable progress message
            percentage: Optional completion percentage (0-100)
        """
        progress: dict[str, Any] = {
            "message": message,
            "timestamp": time.time(),
        }
        if percentage is not None:
            if not 0 <= percentage <= 100:  # noqa: PLR2004
                msg = f"percentage must be 0-100, got {percentage}"
                raise ValueError(msg)
            progress["percentage"] = percentage

        self._progress_messages.append(progress)

        # Stream to control plane if callback set
        if self._progress_callback is not None:
            self._progress_callback(progress)

    def report_metric(
        self,
        name: str,
        value: int | float,
    ) -> None:
        """Report a custom metric.

        Metrics are collected and included in task completion
        for observability dashboards.

        Args:
            name: Metric name (e.g., 'rows_processed')
            value: Numeric metric value
        """
        self._metrics[name] = value
