"""Partition strategy types for multi-dimensional partitioning."""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import Enum
from typing import Literal

from arco_flow._internal.frozen import FrozenDict
from arco_flow.types.scalar import PartitionDimensionValue, ScalarValue


def _canonicalize_date(value: date, granularity: Literal["year", "month", "day", "hour"]) -> str:
    if granularity == "month":
        return value.strftime("%Y-%m")
    if granularity == "year":
        return str(value.year)
    return value.isoformat()


def _is_valid_dimension_key(key: str) -> bool:
    if not key:
        return False
    first = key[0]
    if not ("a" <= first <= "z"):
        return False
    for c in key[1:]:
        if not ("a" <= c <= "z" or "0" <= c <= "9" or c == "_"):
            return False
    return True


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
    """Time-based partition dimension (date, hour, etc.)."""

    granularity: Literal["year", "month", "day", "hour"] = "day"
    kind: DimensionKind = field(default=DimensionKind.TIME, init=False)

    def canonical_value(self, value: PartitionDimensionValue) -> str:
        if isinstance(value, str):
            return value
        if isinstance(value, datetime):
            value = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
            if self.granularity == "hour":
                value = value.replace(minute=0, second=0, microsecond=0)
                return value.strftime("%Y-%m-%dT%H:00:00Z")
            value = value.date()
        if isinstance(value, date):
            return _canonicalize_date(value, self.granularity)
        return str(value)


@dataclass(frozen=True)
class StaticDimension(PartitionDimension):
    """Static partition dimension with predefined values."""

    values: tuple[str, ...] = ()
    kind: DimensionKind = field(default=DimensionKind.STATIC, init=False)

    def canonical_value(self, value: PartitionDimensionValue) -> str:
        str_value = str(value) if value is not None else ""
        if self.values and str_value not in self.values:
            msg = f"Invalid value {str_value!r} for dimension {self.name!r}. Allowed: {self.values}"
            raise ValueError(msg)
        return str_value


@dataclass(frozen=True)
class TenantDimension(PartitionDimension):
    """Tenant partition dimension for multi-tenant isolation."""

    kind: DimensionKind = field(default=DimensionKind.TENANT, init=False)

    def canonical_value(self, value: PartitionDimensionValue) -> str:
        return str(value) if value is not None else ""


class DailyPartition(TimeDimension):
    """Daily partition by date."""

    def __init__(self, name: str = "date") -> None:
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "granularity", "day")
        object.__setattr__(self, "kind", DimensionKind.TIME)


class HourlyPartition(TimeDimension):
    """Hourly partition by timestamp."""

    def __init__(self, name: str = "hour") -> None:
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "granularity", "hour")
        object.__setattr__(self, "kind", DimensionKind.TIME)


@dataclass(frozen=True)
class PartitionKey:
    """Multi-dimensional partition key with ADR-011 canonical encoding."""

    dimensions: FrozenDict[str, ScalarValue] = field(default_factory=FrozenDict)

    def __init__(self, dims: dict[str, PartitionDimensionValue] | None = None) -> None:
        if dims is None:
            dims = {}

        converted: dict[str, ScalarValue] = {}
        for key, raw_value in dims.items():
            if not _is_valid_dimension_key(key):
                msg = f"Invalid partition dimension key: {key!r}"
                raise ValueError(msg)
            converted[key] = ScalarValue.from_value(raw_value)

        object.__setattr__(self, "dimensions", FrozenDict(converted))

    def to_proto_dict(self) -> dict[str, str]:
        return {k: v.to_tagged_string() for k, v in sorted(self.dimensions.items())}

    @classmethod
    def from_proto_dict(cls, proto_dict: dict[str, str]) -> PartitionKey:
        instance = cls.__new__(cls)
        dims: dict[str, ScalarValue] = {}
        for key, tagged in proto_dict.items():
            if not _is_valid_dimension_key(key):
                msg = f"Invalid partition dimension key: {key!r}"
                raise ValueError(msg)
            dims[key] = ScalarValue.from_tagged_string(tagged)
        object.__setattr__(instance, "dimensions", FrozenDict(dims))
        return instance

    def canonical_string(self) -> str:
        if not self.dimensions:
            return ""
        return ",".join(
            f"{key}={value.to_tagged_string()}" for key, value in sorted(self.dimensions.items())
        )

    def to_canonical(self) -> str:
        return self.canonical_string()

    def fingerprint(self) -> str:
        canonical = self.canonical_string()
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, str]:
        return self.to_proto_dict()


@dataclass(frozen=True)
class PartitionStrategy:
    """Complete partition strategy for an asset."""

    dimensions: tuple[PartitionDimension, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "dimensions", tuple(self.dimensions))

    @property
    def is_partitioned(self) -> bool:
        return len(self.dimensions) > 0

    @property
    def dimension_names(self) -> list[str]:
        return [d.name for d in self.dimensions]
