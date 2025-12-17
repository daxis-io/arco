"""Partition strategy types for multi-dimensional partitioning.

Partitions enable:
- Parallel execution across partition values
- Incremental processing (only new partitions)
- Intelligent backfills (date ranges, specific tenants)
"""
from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import Enum
from typing import Literal

from servo.types.scalar import PartitionDimensionValue, ScalarValue


def _canonicalize_date(value: date, granularity: Literal["year", "month", "day", "hour"]) -> str:
    if granularity == "month":
        return value.strftime("%Y-%m")
    if granularity == "year":
        return str(value.year)
    return value.isoformat()


class DimensionKind(str, Enum):
    """Type of partition dimension."""

    TIME = "time"
    STATIC = "static"
    TENANT = "tenant"


@dataclass(frozen=True)
class PartitionDimension(ABC):
    """Base class for partition dimensions."""

    name: str
    kind: DimensionKind

    @abstractmethod
    def canonical_value(self, value: PartitionDimensionValue) -> str:
        """Convert a value to its canonical string representation."""
        ...


@dataclass(frozen=True)
class TimeDimension(PartitionDimension):
    """Time-based partition dimension (date, hour, etc.).

    Example:
        >>> dim = TimeDimension(name="date", granularity="day")
        >>> dim.canonical_value(date(2024, 1, 15))
        "2024-01-15"
    """

    granularity: Literal["year", "month", "day", "hour"] = "day"
    kind: DimensionKind = field(default=DimensionKind.TIME, init=False)

    def canonical_value(self, value: PartitionDimensionValue) -> str:
        """Convert to ISO format string."""
        if isinstance(value, str):
            return value
        if isinstance(value, datetime):
            value = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
            if self.granularity == "hour":
                return value.strftime("%Y-%m-%dT%H:00:00Z")
            value = value.date()
        if isinstance(value, date):
            return _canonicalize_date(value, self.granularity)
        return str(value)


@dataclass(frozen=True)
class StaticDimension(PartitionDimension):
    """Static partition dimension with predefined values.

    Example:
        >>> dim = StaticDimension(name="region", values=["us", "eu", "apac"])
    """

    values: tuple[str, ...] = ()
    kind: DimensionKind = field(default=DimensionKind.STATIC, init=False)

    def canonical_value(self, value: PartitionDimensionValue) -> str:
        """Validate against allowed values."""
        str_value = str(value) if value is not None else ""
        if self.values and str_value not in self.values:
            msg = f"Invalid value {str_value!r} for dimension {self.name!r}. Allowed: {self.values}"
            raise ValueError(msg)
        return str_value


@dataclass(frozen=True)
class TenantDimension(PartitionDimension):
    """Tenant partition dimension for multi-tenant isolation.

    Example:
        >>> dim = TenantDimension(name="tenant_id")
    """

    kind: DimensionKind = field(default=DimensionKind.TENANT, init=False)

    def canonical_value(self, value: PartitionDimensionValue) -> str:
        """Return tenant ID as-is."""
        return str(value) if value is not None else ""


# Convenience constructors
class DailyPartition(TimeDimension):
    """Daily partition by date.

    Example:
        >>> partitions = DailyPartition("date")
    """

    def __init__(self, name: str = "date") -> None:
        """Create a daily partition dimension."""
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "granularity", "day")
        object.__setattr__(self, "kind", DimensionKind.TIME)


class HourlyPartition(TimeDimension):
    """Hourly partition by timestamp.

    Example:
        >>> partitions = HourlyPartition("hour")
    """

    def __init__(self, name: str = "hour") -> None:
        """Create an hourly partition dimension."""
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "granularity", "hour")
        object.__setattr__(self, "kind", DimensionKind.TIME)


@dataclass(frozen=True)
class PartitionKey:
    """Multi-dimensional partition key with canonical serialization.

    Dimensions are stored as ScalarValue internally but serialize to
    proto-compatible map<string,string> using type-tagged encoding.

    Example:
        >>> key = PartitionKey({"date": date(2024, 1, 15), "tenant": "acme"})
        >>> key.to_proto_dict()
        {'date': 'd:2024-01-15', 'tenant': 's:acme'}
    """

    dimensions: dict[str, ScalarValue] = field(default_factory=dict)

    def __init__(self, dims: dict[str, PartitionDimensionValue] | None = None) -> None:
        """Create partition key from dimension values."""
        if dims is None:
            dims = {}
        converted = {k: ScalarValue.from_value(v) for k, v in dims.items()}
        object.__setattr__(self, "dimensions", converted)

    def to_proto_dict(self) -> dict[str, str]:
        """Convert to proto-compatible map<string,string>.

        Returns:
            Dict with type-tagged string values.
        """
        return {k: v.to_tagged_string() for k, v in sorted(self.dimensions.items())}

    @classmethod
    def from_proto_dict(cls, proto_dict: dict[str, str]) -> PartitionKey:
        """Create from proto-format dict with tagged strings.

        Args:
            proto_dict: Dict with type-tagged string values.

        Returns:
            PartitionKey with parsed dimensions.
        """
        instance = cls.__new__(cls)
        dims = {}
        for k, tagged in proto_dict.items():
            dims[k] = ScalarValue.from_tagged_string(tagged)
        object.__setattr__(instance, "dimensions", dims)
        return instance

    def to_canonical(self) -> str:
        """Serialize to canonical JSON using type-tagged strings.

        Uses type-tagged strings for cross-language determinism.
        Keys are sorted alphabetically.

        Returns:
            Canonical JSON string with no whitespace.
        """
        proto_dict = self.to_proto_dict()
        return json.dumps(proto_dict, separators=(",", ":"), sort_keys=True)

    def fingerprint(self) -> str:
        """Compute SHA-256 fingerprint of canonical form."""
        canonical = self.to_canonical()
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, str]:
        """Convert to proto-compatible dict (alias for to_proto_dict)."""
        return self.to_proto_dict()


@dataclass(frozen=True)
class PartitionStrategy:
    """Complete partition strategy for an asset.

    Example:
        >>> strategy = PartitionStrategy(dimensions=[DailyPartition("date")])
    """

    dimensions: list[PartitionDimension] = field(default_factory=list)

    @property
    def is_partitioned(self) -> bool:
        """Check if the asset is partitioned."""
        return len(self.dimensions) > 0

    @property
    def dimension_names(self) -> list[str]:
        """Get list of dimension names."""
        return [d.name for d in self.dimensions]
