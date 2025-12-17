"""Tests for asset registry."""
from __future__ import annotations

import pytest


class TestAssetRegistry:
    """Tests for AssetRegistry."""

    def test_singleton(self) -> None:
        """Registry is a singleton."""
        from servo._internal.registry import get_registry

        reg1 = get_registry()
        reg2 = get_registry()
        assert reg1 is reg2

    def test_register_and_get(self) -> None:
        """Assets can be registered and retrieved."""
        from servo._internal.registry import get_registry
        from servo.types import AssetDefinition, AssetId, AssetKey, CodeLocation

        reg = get_registry()
        reg.clear()

        key = AssetKey(namespace="raw", name="events")
        definition = AssetDefinition(
            key=key,
            id=AssetId.generate(),
            description="Test asset",
            code=CodeLocation(module="test", function="events"),
        )

        # Create a mock registered asset
        class MockRegisteredAsset:
            def __init__(self, key: AssetKey, definition: AssetDefinition) -> None:
                self.key = key
                self.definition = definition
                self.func = lambda: None

        asset = MockRegisteredAsset(key, definition)
        reg.register(asset)

        assert len(reg) == 1
        assert reg.get(key) is asset

    def test_get_by_function_supports_decorated_wrapper(self) -> None:
        """get_by_function should work for the decorated function object."""
        from servo._internal.registry import get_registry
        from servo.asset import asset
        from servo.context import AssetContext

        reg = get_registry()
        reg.clear()

        @asset(namespace="raw")
        def my_events(ctx: AssetContext) -> None:
            del ctx

        registered = reg.get_by_function(my_events)
        assert registered is not None

        original = getattr(my_events, "__wrapped__", None)
        assert original is not None
        assert reg.get_by_function(original) is registered

    def test_rejects_duplicate(self) -> None:
        """Duplicate asset keys are rejected with helpful error."""
        from servo._internal.registry import get_registry
        from servo.types import AssetDefinition, AssetId, AssetKey, CodeLocation

        reg = get_registry()
        reg.clear()

        key = AssetKey(namespace="raw", name="events")

        class MockRegisteredAsset:
            def __init__(self, definition: AssetDefinition) -> None:
                self.key = key
                self.definition = definition
                self.func = lambda: None

        def1 = AssetDefinition(
            key=key,
            id=AssetId.generate(),
            code=CodeLocation(module="m1", function="f1", file_path="a.py", line_number=10),
        )
        def2 = AssetDefinition(
            key=key,
            id=AssetId.generate(),
            code=CodeLocation(module="m2", function="f2", file_path="b.py", line_number=20),
        )

        reg.register(MockRegisteredAsset(def1))

        with pytest.raises(ValueError, match="Duplicate asset key"):
            reg.register(MockRegisteredAsset(def2))

    def test_list_all_sorted(self) -> None:
        """All registered assets are returned sorted by key."""
        from servo._internal.registry import get_registry
        from servo.types import AssetDefinition, AssetId, AssetKey, CodeLocation

        reg = get_registry()
        reg.clear()

        class MockRegisteredAsset:
            def __init__(self, key: AssetKey) -> None:
                self.key = key
                self.definition = AssetDefinition(
                    key=key,
                    id=AssetId.generate(),
                    code=CodeLocation(module="m", function="f"),
                )
                self.func = lambda: None

        # Register in non-sorted order
        reg.register(MockRegisteredAsset(AssetKey("staging", "c")))
        reg.register(MockRegisteredAsset(AssetKey("raw", "a")))
        reg.register(MockRegisteredAsset(AssetKey("raw", "b")))

        all_assets = reg.all()
        keys = [str(a.key) for a in all_assets]
        assert keys == sorted(keys)

    def test_clear(self) -> None:
        """Clear removes all registered assets."""
        from servo._internal.registry import get_registry
        from servo.types import AssetDefinition, AssetId, AssetKey, CodeLocation

        reg = get_registry()
        reg.clear()

        class MockRegisteredAsset:
            def __init__(self) -> None:
                self.key = AssetKey("raw", "events")
                self.definition = AssetDefinition(
                    key=self.key,
                    id=AssetId.generate(),
                    code=CodeLocation(module="m", function="f"),
                )
                self.func = lambda: None

        reg.register(MockRegisteredAsset())
        assert len(reg) == 1

        reg.clear()
        assert len(reg) == 0
