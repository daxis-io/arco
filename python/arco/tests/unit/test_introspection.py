"""Tests for introspection utilities."""
from __future__ import annotations

import pytest

from servo._internal.introspection import (
    compute_transform_fingerprint,
    extract_code_location,
    extract_dependencies,
    infer_asset_key,
    validate_asset_function,
)
from servo.types import AssetIn


class TestExtractCodeLocation:
    """Tests for extract_code_location."""

    def test_extracts_module_and_function(self) -> None:
        """Should extract module and function name."""
        def my_func() -> None:
            pass

        loc = extract_code_location(my_func)
        # Module might have prefix (e.g., "arco.tests.unit.test_introspection")
        assert loc.module.endswith("test_introspection")
        assert loc.function == "my_func"

    def test_extracts_file_and_line(self) -> None:
        """Should extract file path and line number."""
        def my_func() -> None:
            pass

        loc = extract_code_location(my_func)
        assert loc.file_path is not None
        assert "test_introspection.py" in loc.file_path
        assert loc.line_number is not None
        assert loc.line_number > 0


class TestExtractDependencies:
    """Tests for extract_dependencies."""

    def test_extracts_asset_in_hints(self) -> None:
        """Should extract AssetIn type hints."""
        def my_asset(ctx: object, raw: AssetIn["raw.events"]) -> None:  # noqa: UP037
            pass

        deps = extract_dependencies(my_asset)
        assert len(deps) == 1
        assert deps[0].upstream_key.namespace == "raw"
        assert deps[0].upstream_key.name == "events"
        assert deps[0].parameter_name == "raw"

    def test_multiple_dependencies(self) -> None:
        """Should extract multiple dependencies."""
        def my_asset(
            ctx: object,
            events: AssetIn["raw.events"],  # noqa: UP037
            users: AssetIn["staging.users"],  # noqa: UP037
        ) -> None:
            pass

        deps = extract_dependencies(my_asset)
        assert len(deps) == 2

    def test_skips_ctx_parameter(self) -> None:
        """Should skip the ctx parameter."""
        def my_asset(ctx: object) -> None:
            pass

        deps = extract_dependencies(my_asset)
        assert len(deps) == 0

    def test_skips_non_asset_params(self) -> None:
        """Should skip non-AssetIn parameters."""
        def my_asset(
            ctx: object,
            raw: AssetIn["raw.events"],  # noqa: UP037
            limit: int,
            name: str = "default",
        ) -> None:
            pass

        deps = extract_dependencies(my_asset)
        assert len(deps) == 1


class TestComputeTransformFingerprint:
    """Tests for compute_transform_fingerprint."""

    def test_deterministic(self) -> None:
        """Same function should produce same fingerprint."""
        def my_func() -> None:
            return None

        fp1 = compute_transform_fingerprint(my_func)
        fp2 = compute_transform_fingerprint(my_func)
        assert fp1 == fp2

    def test_different_for_different_code(self) -> None:
        """Different functions should produce different fingerprints."""
        def func_a() -> None:
            return None

        def func_b() -> int:
            return 42

        fp_a = compute_transform_fingerprint(func_a)
        fp_b = compute_transform_fingerprint(func_b)
        assert fp_a != fp_b


class TestInferAssetKey:
    """Tests for infer_asset_key."""

    def test_uses_function_name(self) -> None:
        """Should use function name as asset name."""
        def user_events() -> None:
            pass

        key = infer_asset_key(user_events, namespace="raw")
        assert key.namespace == "raw"
        assert key.name == "user_events"

    def test_infers_namespace_from_module(self) -> None:
        """Should infer namespace from module path."""
        def events() -> None:
            pass

        key = infer_asset_key(events, namespace=None)
        assert key.namespace == "default"  # Falls back to default
        assert key.name == "events"


class TestValidateAssetFunction:
    """Tests for validate_asset_function."""

    def test_requires_ctx_parameter(self) -> None:
        """Function must have ctx as first parameter."""
        def bad_func() -> None:
            pass

        with pytest.raises(TypeError, match="ctx"):
            validate_asset_function(bad_func)

    def test_ctx_must_be_first(self) -> None:
        """ctx must be the first parameter."""
        def bad_func(other: int, ctx: object) -> None:
            pass

        with pytest.raises(TypeError, match="first parameter"):
            validate_asset_function(bad_func)

    def test_valid_function_passes(self) -> None:
        """Valid function should pass validation."""
        def good_func(ctx: object) -> None:
            pass

        # Should not raise
        validate_asset_function(good_func)
