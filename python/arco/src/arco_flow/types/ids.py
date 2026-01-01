"""Strongly-typed identifier types using ULID format.

All IDs use ULID format for:
- Lexicographic ordering = chronological ordering
- URL-safe (alphanumeric only)
- Collision-resistant (128-bit)
"""
from __future__ import annotations

import ulid

_ULID_LENGTH = 26


def _validate_ulid(value: str) -> str:
    """Validate ULID format and normalize to uppercase."""
    if len(value) != _ULID_LENGTH:
        msg = f"Invalid ULID format: {value!r} (must be {_ULID_LENGTH} characters)"
        raise ValueError(msg)
    try:
        parsed = ulid.from_str(value)
    except ValueError as exc:
        msg = f"Invalid ULID format: {value!r}"
        raise ValueError(msg) from exc
    return str(parsed)


def _generate_ulid() -> str:
    """Generate a new ULID."""
    return str(ulid.new()).upper()


class AssetId(str):
    """Unique identifier for an asset definition.

    Format: 26-character ULID (uppercase alphanumeric).

    Example:
        >>> asset_id = AssetId.generate()
        >>> print(asset_id)  # e.g., "01HX9ABC..."
    """

    @classmethod
    def generate(cls) -> AssetId:
        """Generate a new unique AssetId."""
        return cls(_generate_ulid())

    @classmethod
    def validate(cls, value: str) -> AssetId:
        """Validate and normalize the ID."""
        return cls(_validate_ulid(value))


class RunId(str):
    """Unique identifier for a run (execution instance).

    Format: 26-character ULID (uppercase alphanumeric).
    """

    @classmethod
    def generate(cls) -> RunId:
        """Generate a new unique RunId."""
        return cls(_generate_ulid())

    @classmethod
    def validate(cls, value: str) -> RunId:
        """Validate and normalize the ID."""
        return cls(_validate_ulid(value))


class TaskId(str):
    """Unique identifier for a task within a run.

    Format: 26-character ULID (uppercase alphanumeric).
    """

    @classmethod
    def generate(cls) -> TaskId:
        """Generate a new unique TaskId."""
        return cls(_generate_ulid())

    @classmethod
    def validate(cls, value: str) -> TaskId:
        """Validate and normalize the ID."""
        return cls(_validate_ulid(value))


class MaterializationId(str):
    """Unique identifier for a materialization (asset partition output).

    Format: 26-character ULID (uppercase alphanumeric).
    """

    @classmethod
    def generate(cls) -> MaterializationId:
        """Generate a new unique MaterializationId."""
        return cls(_generate_ulid())

    @classmethod
    def validate(cls, value: str) -> MaterializationId:
        """Validate and normalize the ID."""
        return cls(_validate_ulid(value))
