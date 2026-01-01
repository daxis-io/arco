"""Quality check types for data validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Literal

from arco_flow._internal.frozen import FrozenDict


class CheckPhase(str, Enum):
    """When the check runs."""

    PRE = "pre"  # Before execution (validate inputs)
    POST = "post"  # After execution (validate outputs)


class CheckSeverity(str, Enum):
    """Severity level of check failures."""

    INFO = "info"  # Log only, no action
    WARNING = "warning"  # Log and alert, but don't block
    ERROR = "error"  # Fail the task
    CRITICAL = "critical"  # Fail the task and halt run


@dataclass(frozen=True)
class CheckResult:
    """Result of a quality check execution."""

    check_name: str
    passed: bool
    severity: CheckSeverity
    message: str | None = None
    details: FrozenDict[str, str | int | float | bool] = field(default_factory=FrozenDict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "details", FrozenDict(self.details))


@dataclass(frozen=True)
class BaseCheck:
    """Base class for check definitions."""

    name: str
    phase: CheckPhase = CheckPhase.POST
    severity: CheckSeverity = CheckSeverity.ERROR


@dataclass(frozen=True)
class RowCountCheck(BaseCheck):
    """Validate row count is within bounds.

    Example:
        >>> check = RowCountCheck(name="row_count", min_rows=1)
    """

    check_type: Literal["row_count"] = "row_count"
    min_rows: int | None = None
    max_rows: int | None = None
    min_ratio: float | None = None  # vs previous materialization


@dataclass(frozen=True)
class NotNullCheck(BaseCheck):
    """Validate columns have no null values.

    Example:
        >>> check = NotNullCheck(name="not_null_user_id", columns=["user_id"])
    """

    check_type: Literal["not_null"] = "not_null"
    columns: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        columns = tuple(self.columns)
        if not columns:
            raise ValueError("NotNullCheck requires at least one column")
        object.__setattr__(self, "columns", columns)


@dataclass(frozen=True)
class UniqueCheck(BaseCheck):
    """Validate column combinations are unique.

    Example:
        >>> check = UniqueCheck(name="unique_pk", columns=["id"])
    """

    check_type: Literal["unique"] = "unique"
    columns: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        columns = tuple(self.columns)
        if not columns:
            raise ValueError("UniqueCheck requires at least one column")
        object.__setattr__(self, "columns", columns)


@dataclass(frozen=True)
class FreshnessCheck(BaseCheck):
    """Validate data freshness.

    Example:
        >>> check = FreshnessCheck(
        ...     name="freshness",
        ...     timestamp_column="created_at",
        ...     max_age_hours=24,
        ... )
    """

    check_type: Literal["freshness"] = "freshness"
    timestamp_column: str = ""
    max_age_hours: int = 24


@dataclass(frozen=True)
class CustomSqlCheck(BaseCheck):
    """Custom SQL-based check.

    Example:
        >>> check = CustomSqlCheck(
        ...     name="positive_amounts",
        ...     sql="SELECT COUNT(*) = 0 FROM {table} WHERE amount < 0",
        ... )
    """

    check_type: Literal["custom_sql"] = "custom_sql"
    sql: str = ""


# Type alias for any check
Check = RowCountCheck | NotNullCheck | UniqueCheck | FreshnessCheck | CustomSqlCheck


# Convenience constructors
def row_count(
    *,
    min_rows: int | None = None,
    max_rows: int | None = None,
    severity: CheckSeverity = CheckSeverity.ERROR,
) -> RowCountCheck:
    """Create a row count check.

    Example:
        >>> @asset(checks=[row_count(min_rows=1)])
        ... def my_asset(ctx): ...
    """
    return RowCountCheck(
        name="row_count",
        min_rows=min_rows,
        max_rows=max_rows,
        severity=severity,
    )


def not_null(*columns: str, severity: CheckSeverity = CheckSeverity.ERROR) -> NotNullCheck:
    """Create a not-null check.

    Example:
        >>> @asset(checks=[not_null("user_id", "email")])
        ... def my_asset(ctx): ...
    """
    if not columns:
        raise ValueError("not_null() requires at least one column name")
    return NotNullCheck(
        name=f"not_null_{'-'.join(columns)}",
        columns=tuple(columns),
        severity=severity,
    )


def unique(*columns: str, severity: CheckSeverity = CheckSeverity.ERROR) -> UniqueCheck:
    """Create a uniqueness check.

    Example:
        >>> @asset(checks=[unique("id")])
        ... def my_asset(ctx): ...
    """
    if not columns:
        raise ValueError("unique() requires at least one column name")
    return UniqueCheck(
        name=f"unique_{'-'.join(columns)}",
        columns=tuple(columns),
        severity=severity,
    )
