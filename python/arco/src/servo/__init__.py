"""Arco Servo Python SDK.

A Pythonic interface for defining data assets and orchestrating pipelines.

Example:
    >>> from servo import asset, AssetIn, AssetOut, AssetContext
    >>> from servo.types import DailyPartition
    >>>
    >>> @asset(
    ...     description="Daily user metrics",
    ...     partitions=DailyPartition("date"),
    ... )
    ... def user_metrics(
    ...     ctx: AssetContext,
    ...     raw_events: AssetIn["raw_events"],
    ... ) -> AssetOut:
    ...     events = raw_events.read()
    ...     return ctx.output(events.group_by("user_id").agg(...))
"""
from __future__ import annotations

__version__ = "0.1.0-alpha"

__all__ = ["__version__"]
