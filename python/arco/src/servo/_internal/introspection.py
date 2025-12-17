"""Introspection utilities for extracting metadata from decorated functions."""
from __future__ import annotations

import hashlib
import inspect
from collections.abc import Callable
from typing import get_type_hints

from servo.types.asset import AssetDependency, AssetIn, AssetKey, CodeLocation, get_asset_in_key

_ASSET_MODULE_PATH_MIN_PARTS = 2


def extract_code_location(func: Callable[..., object]) -> CodeLocation:
    """Extract code location from a function."""
    try:
        file_path = inspect.getfile(func)
        _, line_number = inspect.getsourcelines(func)
    except (TypeError, OSError):
        file_path = "<unknown>"
        line_number = 0

    return CodeLocation(
        module=func.__module__,
        function=func.__name__,
        file_path=file_path,
        line_number=line_number,
    )


def extract_dependencies(func: Callable[..., object]) -> list[AssetDependency]:
    """Extract dependencies from function signature type hints.

    Looks for parameters annotated with AssetIn["namespace.name"].
    Skips the 'ctx' parameter.
    """
    dependencies: list[AssetDependency] = []
    sig = inspect.signature(func)

    try:
        hints = get_type_hints(func, globalns=func.__globals__, localns={"AssetIn": AssetIn})
    except (TypeError, NameError, AttributeError):
        hints = getattr(func, "__annotations__", {})

    for param_name in sig.parameters:
        if param_name == "ctx":
            continue

        hint = hints.get(param_name)
        if hint is None:
            continue

        asset_key = get_asset_in_key(hint)
        if asset_key is None:
            continue

        dependencies.append(
            AssetDependency(
                upstream_key=asset_key,
                parameter_name=param_name,
            )
        )

    return dependencies


def compute_transform_fingerprint(func: Callable[..., object]) -> str:
    """Compute a fingerprint of the function's code (truncated SHA-256)."""
    try:
        source = inspect.getsource(func)
    except (TypeError, OSError):
        source = func.__name__

    return hashlib.sha256(source.encode("utf-8")).hexdigest()[:16]


def infer_asset_key(func: Callable[..., object], namespace: str | None) -> AssetKey:
    """Infer an AssetKey from a function name and optional namespace."""
    name = func.__name__

    if namespace is None:
        module_parts = func.__module__.split(".")
        if len(module_parts) >= _ASSET_MODULE_PATH_MIN_PARTS and module_parts[-2] == "assets":
            namespace = module_parts[-1]
        else:
            namespace = "default"

    return AssetKey(namespace=namespace, name=name)


def validate_asset_function(func: Callable[..., object]) -> None:
    """Validate that a function has correct signature for @asset."""
    sig = inspect.signature(func)
    params = list(sig.parameters.keys())

    if not params:
        msg = (
            f"Asset function {func.__name__!r} must have at least one parameter 'ctx'.\n"
            f"Example: def {func.__name__}(ctx: AssetContext) -> AssetOut: ..."
        )
        raise TypeError(msg)

    if params[0] != "ctx":
        msg = (
            f"Asset function {func.__name__!r} first parameter must be named 'ctx'.\n"
            f"Found: {params[0]!r}\n"
            f"Example: def {func.__name__}(ctx: AssetContext, ...) -> AssetOut: ..."
        )
        raise TypeError(msg)

