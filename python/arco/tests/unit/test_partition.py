"""Tests for partition types."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from arco_flow.types.partition import DailyPartition, PartitionKey, PartitionStrategy
from arco_flow.types.scalar import ScalarValue


class TestScalarValue:
    """Tests for ScalarValue."""

    def test_string_value(self) -> None:
        """String scalars serialize correctly."""
        sv = ScalarValue.from_value("hello")
        assert sv.kind == "string"
        assert sv.value == "hello"

    def test_int_value(self) -> None:
        """Integer scalars serialize correctly."""
        sv = ScalarValue.from_value(42)
        assert sv.kind == "int64"
        assert sv.value == 42

    def test_date_value(self) -> None:
        """Date scalars use YYYY-MM-DD format."""
        sv = ScalarValue.from_value(date(2025, 1, 15))
        assert sv.kind == "date"
        assert sv.value == "2025-01-15"

    def test_bool_value(self) -> None:
        """Boolean scalars serialize correctly."""
        sv = ScalarValue.from_value(True)
        assert sv.kind == "bool"
        assert sv.value is True

    def test_rejects_float(self) -> None:
        """Floats are prohibited in partition keys."""
        with pytest.raises(ValueError, match="float"):
            ScalarValue.from_value(3.14)

    def test_timestamp_value_normalized_to_utc(self) -> None:
        """Datetime scalars are normalized to UTC."""
        sv = ScalarValue.from_value(
            datetime(2025, 1, 15, 1, 2, 3, 456000, tzinfo=timezone(timedelta(hours=2)))
        )
        assert sv.kind == "timestamp"
        assert sv.value == "2025-01-14T23:02:03.456Z"


class TestPartitionKey:
    """Tests for PartitionKey."""

    def test_canonical_order(self) -> None:
        """Partition keys serialize with sorted dimension names."""
        pk1 = PartitionKey({"tenant": "acme", "date": "2025-01-15"})
        pk2 = PartitionKey({"date": "2025-01-15", "tenant": "acme"})

        assert pk1.to_canonical() == pk2.to_canonical()
        # Type-tagged format: strings get "s:" prefix
        assert pk1.to_canonical() == '{"date":"s:2025-01-15","tenant":"s:acme"}'

    def test_fingerprint_stable(self) -> None:
        """Fingerprint is stable SHA-256 of canonical form."""
        pk = PartitionKey({"date": "2025-01-15"})
        fp = pk.fingerprint()

        assert len(fp) == 64  # SHA-256 hex
        assert pk.fingerprint() == fp  # Deterministic

    def test_rejects_float_in_dict(self) -> None:
        """Float values rejected even when passed via dict."""
        with pytest.raises(ValueError, match="float"):
            PartitionKey({"rate": 0.5})

    def test_from_dict_mixed_types(self) -> None:
        """PartitionKey accepts dict with mixed value types."""
        pk = PartitionKey(
            {
                "date": date(2025, 1, 15),
                "region": "us-east-1",
                "version": 42,
            }
        )

        assert len(pk.dimensions) == 3

    def test_to_proto_dict_returns_string_values(self) -> None:
        """to_proto_dict() returns dict with all string values (proto compliance)."""
        pk = PartitionKey(
            {
                "date": date(2025, 1, 15),
                "region": "us-east-1",
                "version": 42,
            }
        )

        proto_dict = pk.to_proto_dict()

        # All values must be strings
        assert all(isinstance(v, str) for v in proto_dict.values())
        # Values are type-tagged
        assert proto_dict["date"] == "d:2025-01-15"
        assert proto_dict["region"] == "s:us-east-1"
        assert proto_dict["version"] == "i:42"

    def test_from_proto_dict_roundtrip(self) -> None:
        """Can roundtrip through proto dict format."""
        original = PartitionKey(
            {
                "date": date(2025, 1, 15),
                "region": "us-east-1",
                "version": 42,
                "active": True,
            }
        )

        proto_dict = original.to_proto_dict()
        restored = PartitionKey.from_proto_dict(proto_dict)

        assert original.fingerprint() == restored.fingerprint()


class TestPartitionStrategy:
    """Tests for PartitionStrategy."""

    def test_daily_partition(self) -> None:
        """DailyPartition creates day granularity."""
        strategy = PartitionStrategy(dimensions=[DailyPartition("date")])
        assert strategy.is_partitioned
        assert strategy.dimension_names == ["date"]

    def test_empty_strategy(self) -> None:
        """Empty strategy is not partitioned."""
        strategy = PartitionStrategy()
        assert not strategy.is_partitioned
