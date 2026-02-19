"""Tests for @asset decorator."""

from __future__ import annotations

import pytest

from arco_flow._internal.registry import get_registry
from arco_flow.types import AssetIn, AssetKey, DailyPartition, not_null, row_count


class TestAssetDecorator:
    """Tests for @asset decorator."""

    def test_registers_function(self) -> None:
        """Basic @asset decorator registers the function."""
        from arco_flow.asset import asset
        from arco_flow.context import AssetContext

        get_registry().clear()

        @asset(namespace="raw")
        def my_events(ctx: AssetContext) -> None:
            pass

        reg = get_registry()
        assert len(reg) == 1

        registered = reg.get(AssetKey("raw", "my_events"))
        assert registered is not None
        assert registered.definition.key == AssetKey("raw", "my_events")

    def test_explicit_name(self) -> None:
        """Asset name can be explicitly specified."""
        from arco_flow.asset import asset
        from arco_flow.context import AssetContext

        get_registry().clear()

        @asset(namespace="staging", name="user_metrics")
        def compute_users(ctx: AssetContext) -> None:
            pass

        reg = get_registry()
        registered = reg.get(AssetKey("staging", "user_metrics"))
        assert registered is not None
        assert registered.definition.code.function == "compute_users"

    def test_with_metadata(self) -> None:
        """Decorator accepts owners and tags."""
        from arco_flow.asset import asset
        from arco_flow.context import AssetContext

        get_registry().clear()

        @asset(
            namespace="mart",
            owners=["analytics@example.com"],
            tags={"tier": "gold"},
        )
        def revenue(ctx: AssetContext) -> None:
            pass

        reg = get_registry()
        registered = reg.get(AssetKey("mart", "revenue"))
        assert registered.definition.owners == ("analytics@example.com",)
        assert registered.definition.tags == {"tier": "gold"}

    def test_preserves_function(self) -> None:
        """Decorated function remains callable."""
        from arco_flow.asset import asset
        from arco_flow.context import AssetContext

        get_registry().clear()

        @asset(namespace="raw")
        def adder(ctx: AssetContext, x: int = 0, y: int = 0) -> int:
            del ctx
            return x + y

        # Function should still work
        result = adder(None, x=2, y=3)  # type: ignore[arg-type]
        assert result == 5

    def test_infers_dependencies(self) -> None:
        """Dependencies inferred from AssetIn type hints."""
        from arco_flow.asset import asset
        from arco_flow.context import AssetContext

        get_registry().clear()

        @asset(namespace="staging")
        def cleaned_events(ctx: AssetContext, raw: AssetIn["raw.events"]) -> None:  # noqa: UP037
            pass

        reg = get_registry()
        registered = reg.get(AssetKey("staging", "cleaned_events"))

        assert len(registered.definition.dependencies) == 1
        assert registered.definition.dependencies[0].upstream_key == AssetKey("raw", "events")
        assert registered.definition.dependencies[0].parameter_name == "raw"

    def test_with_partitions(self) -> None:
        """Decorator accepts partition configuration."""
        from arco_flow.asset import asset
        from arco_flow.context import AssetContext

        get_registry().clear()

        @asset(namespace="staging", partitions=DailyPartition("date"))
        def daily_events(ctx: AssetContext) -> None:
            pass

        reg = get_registry()
        registered = reg.get(AssetKey("staging", "daily_events"))
        assert registered.definition.partitioning.is_partitioned
        assert registered.definition.partitioning.dimension_names == ["date"]

    def test_with_checks(self) -> None:
        """Decorator accepts quality checks."""
        from arco_flow.asset import asset
        from arco_flow.context import AssetContext

        get_registry().clear()

        @asset(
            namespace="mart",
            checks=[row_count(min_rows=1), not_null("user_id")],
        )
        def users(ctx: AssetContext) -> None:
            pass

        reg = get_registry()
        registered = reg.get(AssetKey("mart", "users"))
        assert len(registered.definition.checks) == 2
        # CRITICAL: checks are full Check objects, not names
        assert registered.definition.checks[0].check_type == "row_count"
        assert registered.definition.checks[1].check_type == "not_null"

    def test_default_io_format_is_delta(self) -> None:
        """Decorator should use delta as the default IO format."""
        from arco_flow.asset import asset
        from arco_flow.context import AssetContext

        get_registry().clear()

        @asset(namespace="raw")
        def daily_events(ctx: AssetContext) -> None:
            pass

        reg = get_registry()
        registered = reg.get(AssetKey("raw", "daily_events"))
        assert registered is not None
        assert registered.definition.io.format == "delta"

    def test_validates_signature(self) -> None:
        """Decorator validates function signature."""
        from arco_flow.asset import asset

        with pytest.raises(TypeError, match="ctx"):

            @asset(namespace="raw")
            def bad_asset() -> None:  # Missing ctx parameter
                pass
