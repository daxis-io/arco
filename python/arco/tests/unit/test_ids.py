"""Tests for ID types."""

from __future__ import annotations

import pytest

from arco_flow.types.ids import AssetId, RunId, TaskId


class TestAssetId:
    """Tests for AssetId."""

    def test_generate_unique(self) -> None:
        """Generated IDs should be unique."""
        ids = {AssetId.generate() for _ in range(100)}
        assert len(ids) == 100

    def test_format_is_ulid(self) -> None:
        """IDs should be valid ULID format (26 alphanumeric chars)."""
        asset_id = AssetId.generate()
        assert len(asset_id) == 26
        assert asset_id.isalnum()
        assert asset_id.isupper()

    def test_validation_rejects_invalid(self) -> None:
        """Invalid IDs should be rejected."""
        with pytest.raises(ValueError, match="Invalid ULID"):
            AssetId.validate("not-valid")

    def test_validation_rejects_invalid_base32(self) -> None:
        """IDs must use ULID Crockford base32 alphabet."""
        with pytest.raises(ValueError, match="Invalid ULID"):
            AssetId.validate("01ARZ3NDEKTSV4RRFFQ69G5FAU")  # U is invalid in ULIDs

    def test_validation_normalizes_case(self) -> None:
        """IDs should be normalized to uppercase."""
        lower = "01arz3ndektsv4rrffq69g5fav"
        validated = AssetId.validate(lower)
        assert validated.isupper()


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
