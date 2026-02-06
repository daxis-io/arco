"""Tests for check types."""

from __future__ import annotations

import pytest

from arco_flow.types.check import (
    CheckPhase,
    CheckSeverity,
    FreshnessCheck,
    NotNullCheck,
    RowCountCheck,
    UniqueCheck,
    not_null,
    row_count,
    unique,
)


class TestCheckTypes:
    """Tests for check types."""

    def test_row_count_check(self) -> None:
        """RowCountCheck with min/max bounds."""
        check = RowCountCheck(name="row_count", min_rows=1, max_rows=1000)
        assert check.check_type == "row_count"
        assert check.min_rows == 1
        assert check.max_rows == 1000
        assert check.severity == CheckSeverity.ERROR

    def test_not_null_check(self) -> None:
        """NotNullCheck with column list."""
        check = NotNullCheck(name="not_null", columns=["user_id", "email"])
        assert check.check_type == "not_null"
        assert check.columns == ("user_id", "email")

    def test_unique_check(self) -> None:
        """UniqueCheck with column list."""
        check = UniqueCheck(name="unique_pk", columns=["id"])
        assert check.check_type == "unique"

    def test_freshness_check(self) -> None:
        """FreshnessCheck with timestamp column."""
        check = FreshnessCheck(
            name="freshness",
            timestamp_column="created_at",
            max_age_hours=24,
        )
        assert check.check_type == "freshness"

    def test_severity_levels(self) -> None:
        """CheckSeverity has expected values."""
        assert CheckSeverity.INFO.value == "info"
        assert CheckSeverity.WARNING.value == "warning"
        assert CheckSeverity.ERROR.value == "error"
        assert CheckSeverity.CRITICAL.value == "critical"

    def test_check_phases(self) -> None:
        """CheckPhase has pre and post."""
        assert CheckPhase.PRE.value == "pre"
        assert CheckPhase.POST.value == "post"


class TestCheckHelpers:
    """Tests for check helper functions."""

    def test_row_count_helper(self) -> None:
        """row_count() creates RowCountCheck."""
        check = row_count(min_rows=1, max_rows=1000)
        assert check.check_type == "row_count"
        assert check.min_rows == 1

    def test_not_null_helper(self) -> None:
        """not_null() creates NotNullCheck."""
        check = not_null("user_id", "email")
        assert check.check_type == "not_null"
        assert check.columns == ("user_id", "email")

    def test_not_null_requires_columns(self) -> None:
        """not_null() requires at least one column."""
        with pytest.raises(ValueError, match="requires at least one column"):
            not_null()

    def test_unique_helper(self) -> None:
        """unique() creates UniqueCheck."""
        check = unique("id")
        assert check.check_type == "unique"
        assert check.columns == ("id",)

    def test_unique_requires_columns(self) -> None:
        """unique() requires at least one column."""
        with pytest.raises(ValueError, match="requires at least one column"):
            unique()
