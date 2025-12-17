"""Tests for ID types."""
from __future__ import annotations

import pytest


class TestAssetId:
    """Tests for AssetId."""

    def test_generate_unique(self) -> None:
        """Generated IDs should be unique."""
        from servo.types.ids import AssetId

        ids = {AssetId.generate() for _ in range(100)}
        assert len(ids) == 100

    def test_format_is_ulid(self) -> None:
        """IDs should be valid ULID format (26 alphanumeric chars)."""
        from servo.types.ids import AssetId

        asset_id = AssetId.generate()
        assert len(asset_id) == 26
        assert asset_id.isalnum()
        assert asset_id.isupper()

    def test_validation_rejects_invalid(self) -> None:
        """Invalid IDs should be rejected."""
        from servo.types.ids import AssetId

        with pytest.raises(ValueError, match="Invalid ULID"):
            AssetId.validate("not-valid")

    def test_validation_normalizes_case(self) -> None:
        """IDs should be normalized to uppercase."""
        from servo.types.ids import AssetId

        lower = "01hx9abcdefghijklmnopqrstu"
        validated = AssetId.validate(lower)
        assert validated.isupper()


class TestRunId:
    """Tests for RunId."""

    def test_generate_unique(self) -> None:
        """Generated IDs should be unique."""
        from servo.types.ids import RunId

        ids = {RunId.generate() for _ in range(100)}
        assert len(ids) == 100


class TestTaskId:
    """Tests for TaskId."""

    def test_generate_unique(self) -> None:
        """Generated IDs should be unique."""
        from servo.types.ids import TaskId

        ids = {TaskId.generate() for _ in range(100)}
        assert len(ids) == 100
