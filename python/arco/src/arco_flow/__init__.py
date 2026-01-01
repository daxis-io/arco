"""Arco Flow Python SDK.

A Pythonic interface for defining data assets and orchestrating pipelines.

Example:
    >>> from arco_flow import asset, AssetIn, AssetOut, AssetContext
    >>> from arco_flow.types import DailyPartition
    >>>
    >>> @asset(
    ...     namespace="staging",
    ...     description="Daily user metrics",
    ...     partitions=DailyPartition("date"),
    ... )
    ... def user_metrics(
    ...     ctx: AssetContext,
    ...     raw_events: AssetIn["raw.events"],
    ... ) -> AssetOut:
    ...     events = raw_events.read()
    ...     return ctx.output(events.group_by("user_id").agg(...))
"""

from __future__ import annotations

from arco_flow.asset import RegisteredAsset, asset
from arco_flow.context import AssetContext
from arco_flow.types import (
    AssetDefinition,
    AssetDependency,
    AssetId,
    AssetIn,
    AssetKey,
    AssetOut,
    Check,
    CheckPhase,
    CheckSeverity,
    DailyPartition,
    DependencyMapping,
    ExecutionPolicy,
    HourlyPartition,
    MaterializationId,
    PartitionKey,
    PartitionStrategy,
    ResourceRequirements,
    RunId,
    TaskId,
    not_null,
    row_count,
    unique,
)

__version__ = "0.1.0-alpha"

__all__ = [
    "AssetContext",
    "AssetDefinition",
    "AssetDependency",
    "AssetId",
    "AssetIn",
    "AssetKey",
    "AssetOut",
    "Check",
    "CheckPhase",
    "CheckSeverity",
    "DailyPartition",
    "DependencyMapping",
    "ExecutionPolicy",
    "HourlyPartition",
    "MaterializationId",
    "PartitionKey",
    "PartitionStrategy",
    "RegisteredAsset",
    "ResourceRequirements",
    "RunId",
    "TaskId",
    "__version__",
    "asset",
    "not_null",
    "row_count",
    "unique",
]
