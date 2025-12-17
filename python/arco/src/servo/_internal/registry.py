"""Thread-safe registry for @asset-decorated functions."""
from __future__ import annotations

from threading import Lock
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from servo.types import AssetDefinition, AssetKey


class RegisteredAssetProtocol(Protocol):
    """Protocol for registered assets."""

    key: AssetKey
    definition: AssetDefinition
    func: object


class AssetRegistry:
    """Thread-safe registry of discovered assets.

    This is a singleton that collects all @asset-decorated functions
    when modules are imported.
    """

    _instance: AssetRegistry | None = None
    _lock: Lock = Lock()
    _assets: dict[str, RegisteredAssetProtocol]
    _by_function: dict[int, RegisteredAssetProtocol]

    def __new__(cls) -> AssetRegistry:
        """Ensure singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._assets = {}
                    instance._by_function = {}
                    cls._instance = instance
        return cls._instance

    def register(self, asset: RegisteredAssetProtocol) -> None:
        """Register an asset.

        Args:
            asset: The registered asset to add.

        Raises:
            ValueError: If an asset with the same key is already registered.
        """
        key = str(asset.key)
        with self._lock:
            if key in self._assets:
                existing = self._assets[key]
                msg = (
                    f"Duplicate asset key: {key!r}\n"
                    f"  First defined at: {existing.definition.code.file_path}:"
                    f"{existing.definition.code.line_number}\n"
                    f"  Duplicate at: {asset.definition.code.file_path}:"
                    f"{asset.definition.code.line_number}"
                )
                raise ValueError(msg)
            self._assets[key] = asset
            self._by_function[id(asset.func)] = asset

    def get(self, key: AssetKey | str) -> RegisteredAssetProtocol | None:
        """Get asset by key."""
        key_str = str(key) if not isinstance(key, str) else key
        return self._assets.get(key_str)

    def get_by_function(self, func: object) -> RegisteredAssetProtocol | None:
        """Get asset by its decorated function."""
        return self._by_function.get(id(func))

    def all(self) -> list[RegisteredAssetProtocol]:
        """Get all registered assets, sorted by key."""
        return [self._assets[k] for k in sorted(self._assets.keys())]

    def clear(self) -> None:
        """Clear all registered assets (for testing)."""
        with self._lock:
            self._assets.clear()
            self._by_function.clear()

    def __len__(self) -> int:
        """Return number of registered assets."""
        return len(self._assets)

    def __contains__(self, key: str) -> bool:
        """Check if asset is registered."""
        return key in self._assets


# Global singleton instance
_registry: AssetRegistry | None = None


def get_registry() -> AssetRegistry:
    """Get the global asset registry."""
    global _registry  # noqa: PLW0603
    if _registry is None:
        _registry = AssetRegistry()
    return _registry


def clear_registry() -> None:
    """Clear the global registry (for testing)."""
    if _registry is not None:
        _registry.clear()
