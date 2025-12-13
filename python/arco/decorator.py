"""Asset decorator for defining data assets."""

from functools import wraps
from typing import Callable, TypeVar

F = TypeVar("F", bound=Callable[..., object])


def asset(
    name: str | None = None,
    deps: list[str] | None = None,
) -> Callable[[F], F]:
    """Decorator to define a data asset.

    Args:
        name: Asset name (defaults to function name)
        deps: List of upstream asset dependencies

    Example:
        @asset(deps=["raw_events"])
        def cleaned_events():
            ...
    """

    def decorator(fn: F) -> F:
        asset_name = name or fn.__name__
        dependencies = deps or []

        @wraps(fn)
        def wrapper(*args: object, **kwargs: object) -> object:
            # TODO: Register with Arco runtime
            return fn(*args, **kwargs)

        # Attach metadata
        wrapper._arco_asset = True  # type: ignore[attr-defined]
        wrapper._arco_name = asset_name  # type: ignore[attr-defined]
        wrapper._arco_deps = dependencies  # type: ignore[attr-defined]

        return wrapper  # type: ignore[return-value]

    return decorator
