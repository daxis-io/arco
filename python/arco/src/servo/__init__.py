"""Arco Servo Python SDK.

A Pythonic interface for defining data assets and orchestrating pipelines.

Example:
    >>> from servo import AssetContext, asset
    >>> from servo.types import AssetIn, AssetOut, DailyPartition
    >>>
    >>> @asset(
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

__version__ = "0.1.0-alpha"

from servo.asset import asset
from servo.context import AssetContext

__all__ = ["AssetContext", "__version__", "asset"]
