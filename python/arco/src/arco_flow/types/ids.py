"""Strongly-typed identifier types.

Canonical Rust implementation (`crates/arco-core/src/id.rs`) uses:
- `AssetId`: UUID (generated as UUIDv7)
- `RunId`, `TaskId`, `MaterializationId`: ULID

Wire format is plain strings:
- UUID: hyphenated 36-character canonical form
- ULID: 26-character Crockford base32
"""

from __future__ import annotations

import threading
import uuid

import ulid
import uuid6

_ULID_LENGTH = 26
_UUIDV7_LOCK = threading.Lock()


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
    return str(parsed).upper()


def _generate_ulid() -> str:
    """Generate a new ULID (uppercase)."""
    return str(ulid.new()).upper()


def _validate_uuid(value: str) -> str:
    """Validate UUID format and normalize to canonical string."""
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        msg = f"Invalid UUID format: {value!r}"
        raise ValueError(msg) from exc
    return str(parsed)


def _generate_uuidv7() -> str:
    """Generate a new UUIDv7 in canonical string form."""
    with _UUIDV7_LOCK:
        return str(uuid6.uuid7())


class AssetId(str):
    """Unique identifier for an asset definition.

    Format: UUID string (generated as UUIDv7).

    Example:
        >>> asset_id = AssetId.generate()
        >>> print(asset_id)  # e.g., "01930a0b-1234-7abc-9def-0123456789ab"
    """

    @classmethod
    def generate(cls) -> AssetId:
        """Generate a new unique AssetId."""
        return cls(_generate_uuidv7())

    @classmethod
    def validate(cls, value: str) -> AssetId:
        """Validate and normalize the ID."""
        return cls(_validate_uuid(value))


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
