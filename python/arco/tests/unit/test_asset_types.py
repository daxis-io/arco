"""Tests for asset types."""
from __future__ import annotations

from typing import get_type_hints

import pytest

from servo.types.asset import AssetIn, AssetKey, DependencyMapping


class TestAssetKey:
    """Tests for AssetKey."""

    def test_str_representation(self) -> None:
        """String representation should be dotted notation."""
        key = AssetKey(namespace="staging", name="events")
        assert str(key) == "staging.events"

    def test_parse_dotted(self) -> None:
        """Should parse dotted notation."""
        key = AssetKey.parse("raw.user_events")
        assert key.namespace == "raw"
        assert key.name == "user_events"

    def test_validates_pattern(self) -> None:
        """Namespace and name must match pattern."""
        with pytest.raises(ValueError, match="namespace"):
            AssetKey(namespace="Raw", name="events")  # Capital letter

        with pytest.raises(ValueError, match="name"):
            AssetKey(namespace="raw", name="User-Events")  # Hyphen

    def test_equality(self) -> None:
        """AssetKeys with same namespace/name are equal."""
        ak1 = AssetKey(namespace="raw", name="events")
        ak2 = AssetKey.parse("raw.events")
        assert ak1 == ak2
        assert hash(ak1) == hash(ak2)


class TestAssetIn:
    """Tests for AssetIn."""

    def test_subscript_syntax(self) -> None:
        """AssetIn supports subscript syntax."""
        hint = AssetIn["raw.events"]
        assert hasattr(hint, "__asset_key__")
        assert hint.__asset_key__ == "raw.events"

    def test_subscript_returns_type(self) -> None:
        """AssetIn['key'] returns a type, not an instance."""
        hint = AssetIn["raw.events"]
        # Must be a type (class) for typing.get_type_hints() to work
        assert isinstance(hint, type)

    def test_different_keys_return_different_types(self) -> None:
        """Each key creates a distinct type."""
        type_a = AssetIn["raw.events"]
        type_b = AssetIn["staging.users"]

        assert type_a is not type_b
        assert type_a.__asset_key__ != type_b.__asset_key__

    def test_same_key_returns_same_type(self) -> None:
        """Same key returns cached type (identity)."""
        type_a = AssetIn["raw.events"]
        type_b = AssetIn["raw.events"]

        assert type_a is type_b

    def test_asset_key_property(self) -> None:
        """AssetIn can return parsed AssetKey."""
        hint = AssetIn["staging.users"]
        key = hint.get_asset_key()
        assert key.namespace == "staging"
        assert key.name == "users"

    def test_typing_get_type_hints_compatibility(self) -> None:
        """AssetIn works with typing.get_type_hints()."""
        def fn(ctx: object, upstream: AssetIn["raw.events"]) -> None:  # noqa: UP037, F821
            pass

        hints = get_type_hints(fn)
        # Should have 'upstream' with __asset_key__ attribute
        assert hasattr(hints["upstream"], "__asset_key__")
        assert hints["upstream"].__asset_key__ == "raw.events"


class TestDependencyMapping:
    """Tests for DependencyMapping."""

    def test_enum_values(self) -> None:
        """DependencyMapping has expected values."""
        assert DependencyMapping.IDENTITY.value == "identity"
        assert DependencyMapping.ALL_UPSTREAM.value == "all"
        assert DependencyMapping.LATEST.value == "latest"
        assert DependencyMapping.WINDOW.value == "window"
