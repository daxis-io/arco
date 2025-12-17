"""The @asset decorator for defining data assets."""
from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import TYPE_CHECKING, ParamSpec, TypeVar, overload

from servo._internal.frozen import FrozenDict
from servo._internal.introspection import (
    compute_transform_fingerprint,
    extract_code_location,
    extract_dependencies,
    infer_asset_key,
    validate_asset_function,
)
from servo._internal.registry import get_registry
from servo.types import (
    AssetDefinition,
    AssetId,
    AssetKey,
    Check,
    ExecutionPolicy,
    IoConfig,
    PartitionDimension,
    PartitionStrategy,
    ResourceRequirements,
)

if TYPE_CHECKING:
    from collections.abc import Callable

P = ParamSpec("P")
R = TypeVar("R")


@dataclass
class RegisteredAsset:
    """Runtime representation of a registered @asset."""

    key: AssetKey
    definition: AssetDefinition
    func: object


@overload
def asset(func: Callable[P, R]) -> Callable[P, R]: ...


@overload
def asset(
    *,
    name: str | None = None,
    namespace: str | None = None,
    description: str = "",
    partitions: PartitionDimension | list[PartitionDimension] | None = None,
    owners: list[str] | None = None,
    tags: dict[str, str] | None = None,
    checks: list[Check] | None = None,
    max_retries: int = 3,
    timeout_seconds: int = 3600,
    cpu_millicores: int = 1000,
    memory_mib: int = 1024,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def asset(
    func: Callable[P, R] | None = None,
    *,
    name: str | None = None,
    namespace: str | None = None,
    description: str = "",
    partitions: PartitionDimension | list[PartitionDimension] | None = None,
    owners: list[str] | None = None,
    tags: dict[str, str] | None = None,
    checks: list[Check] | None = None,
    max_retries: int = 3,
    timeout_seconds: int = 3600,
    cpu_millicores: int = 1000,
    memory_mib: int = 1024,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator to define a data asset.

    An asset represents a logical data artifact in your pipeline. The decorator
    captures metadata about the asset and registers it for discovery.

    Args:
        func: The asset function (when used without parentheses).
        name: Asset name. Defaults to function name.
        namespace: Asset namespace. Required.
        description: Human-readable description.
        partitions: Partition dimensions (e.g., DailyPartition("date")).
        owners: List of owner emails.
        tags: Key-value tags for categorization.
        checks: List of quality checks to run (full Check objects).
        max_retries: Maximum retry attempts on failure.
        timeout_seconds: Execution timeout.
        cpu_millicores: CPU allocation (1000 = 1 core).
        memory_mib: Memory allocation in MiB.

    Returns:
        The decorated function.

    Example:
        @asset(namespace="staging", description="Daily user metrics")
        def user_metrics(ctx: AssetContext) -> AssetOut:
            return ctx.output(compute_metrics())
    """

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        # Validate function signature
        validate_asset_function(fn)

        # Build partition strategy
        partition_dims: list[PartitionDimension] = (
            partitions if isinstance(partitions, list) else [partitions]
        ) if partitions is not None else []

        # Build asset key
        asset_key = infer_asset_key(fn, namespace)
        if name is not None:
            asset_key = AssetKey(namespace=asset_key.namespace, name=name)

        # Extract metadata
        code_location = extract_code_location(fn)
        dependencies = tuple(extract_dependencies(fn))
        fingerprint = compute_transform_fingerprint(fn)

        # Get description from docstring if not provided
        asset_description = description
        if not asset_description and fn.__doc__:
            asset_description = fn.__doc__.strip().split("\n")[0]

        # Build definition
        # CRITICAL: checks stores full Check objects per contract, NOT names
        definition = AssetDefinition(
            key=asset_key,
            id=AssetId.generate(),
            description=asset_description,
            owners=tuple(owners) if owners is not None else (),
            tags=FrozenDict(tags) if tags is not None else FrozenDict(),
            partitioning=PartitionStrategy(dimensions=tuple(partition_dims)),
            dependencies=dependencies,
            code=code_location,
            checks=tuple(checks) if checks is not None else (),  # Full Check objects, not names
            execution=ExecutionPolicy(
                max_retries=max_retries,
                timeout_seconds=timeout_seconds,
            ),
            resources=ResourceRequirements(
                cpu_millicores=cpu_millicores,
                memory_mib=memory_mib,
            ),
            io=IoConfig(),  # Default I/O config
            transform_fingerprint=fingerprint,
        )

        # Create registered asset
        registered = RegisteredAsset(func=fn, key=asset_key, definition=definition)

        # Register with global registry
        registry = get_registry()
        registry.register(registered)

        # Wrap function
        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return fn(*args, **kwargs)

        # Attach metadata
        wrapper.__servo_asset__ = True  # type: ignore[attr-defined]
        wrapper.__servo_key__ = asset_key  # type: ignore[attr-defined]
        wrapper.__servo_definition__ = definition  # type: ignore[attr-defined]

        return wrapper

    # Handle @asset vs @asset()
    if func is not None:
        return decorator(func)
    return decorator
