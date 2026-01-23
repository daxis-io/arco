"""Tests for ID types."""

from __future__ import annotations

import uuid

import pytest

from arco_flow.types.ids import AssetId, RunId, TaskId


class TestAssetId:
    """Tests for AssetId."""

    def test_generate_unique(self) -> None:
        """Generated IDs should be unique."""
        ids = {AssetId.generate() for _ in range(100)}
        assert len(ids) == 100

    def test_format_is_uuid(self) -> None:
        """IDs should be valid UUID format (canonical hyphenated string)."""
        asset_id = AssetId.generate()
        parsed = uuid.UUID(asset_id)
        assert parsed.version == 7
        assert str(parsed) == asset_id
        assert len(asset_id) == 36

    def test_validation_rejects_invalid(self) -> None:
        """Invalid IDs should be rejected."""
        with pytest.raises(ValueError, match="Invalid UUID"):
            AssetId.validate("not-valid")

    def test_validation_normalizes_case(self) -> None:
        """IDs should be normalized to canonical lowercase."""
        upper = "01930A0B-1234-7ABC-9DEF-0123456789AB"
        validated = AssetId.validate(upper)
        assert validated == upper.lower()


class TestRunId:
    """Tests for RunId."""

    def test_generate_unique(self) -> None:
        """Generated IDs should be unique."""
        ids = {RunId.generate() for _ in range(100)}
        assert len(ids) == 100


class TestTaskId:
    """Tests for TaskId."""

    def test_generate_unique(self) -> None:
        """Generated IDs should be unique."""
        ids = {TaskId.generate() for _ in range(100)}
        assert len(ids) == 100
