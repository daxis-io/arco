"""Scalar value types for partition keys.

Floats are explicitly prohibited to ensure cross-language determinism.

Type-tagged string format matches ADR-011 and Rust `ScalarValue::canonical_repr()`:
- s:<base64url_no_pad(utf8)>  - string
- i:<decimal>                 - int64
- b:true|b:false              - bool
- d:YYYY-MM-DD                - date
- t:YYYY-MM-DDTHH:MM:SS.ffffffZ - timestamp (UTC, microseconds)
- n:null                      - null
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import ClassVar, Literal

# Type alias for valid partition key dimension values
PartitionDimensionValue = str | int | bool | date | datetime | None

ScalarKind = Literal["string", "int64", "bool", "date", "timestamp", "null"]

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

_BASE64URL_NOPAD_RE = re.compile(r"^[A-Za-z0-9_-]*\Z")


def _is_valid_date_format(value: str) -> bool:
    if len(value) != 10:
        return False
    if value[4] != "-" or value[7] != "-":
        return False

    try:
        year = int(value[0:4])
        month = int(value[5:7])
        day = int(value[8:10])
    except ValueError:
        return False

    if year < 1970:
        return False
    if month < 1 or month > 12:
        return False
    if day < 1 or day > 31:
        return False

    if month == 2:
        return day <= 29
    if month in (4, 6, 9, 11):
        return day <= 30
    return True


def _is_valid_timestamp_format(value: str) -> bool:
    if len(value) != 27 or not value.endswith("Z"):
        return False

    if (
        value[4] != "-"
        or value[7] != "-"
        or value[10] != "T"
        or value[13] != ":"
        or value[16] != ":"
        or value[19] != "."
    ):
        return False

    if not _is_valid_date_format(value[0:10]):
        return False

    try:
        hour = int(value[11:13])
        minute = int(value[14:16])
        second = int(value[17:19])
    except ValueError:
        return False

    if hour < 0 or hour > 23:
        return False
    if minute < 0 or minute > 59:
        return False
    if second < 0 or second > 59:
        return False

    micros = value[20:26]
    return len(micros) == 6 and micros.isdigit()


def _b64url_no_pad_encode(value: str) -> str:
    encoded = base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii")
    return encoded.rstrip("=")


def _b64url_no_pad_decode(value: str) -> str:
    if "=" in value or not _BASE64URL_NOPAD_RE.match(value):
        msg = f"Invalid base64url (no-pad) encoding: {value!r}"
        raise ValueError(msg)

    padding = "=" * (-len(value) % 4)
    try:
        decoded = base64.urlsafe_b64decode(value + padding)
        return decoded.decode("utf-8")
    except (binascii.Error, ValueError, UnicodeDecodeError) as exc:
        msg = f"Invalid base64url (no-pad) encoding: {value!r}"
        raise ValueError(msg) from exc


@dataclass(frozen=True)
class ScalarValue:
    """Scalar value with explicit type for canonical encoding."""

    kind: ScalarKind
    value: str | int | bool | None

    _TYPE_TAGS: ClassVar[dict[ScalarKind, str]] = {
        "string": "s:",
        "int64": "i:",
        "bool": "b:",
        "date": "d:",
        "timestamp": "t:",
        "null": "n:",
    }

    @classmethod
    def from_value(cls, v: PartitionDimensionValue | float | object) -> ScalarValue:
        """Create ScalarValue from Python value, inferring type.

        Raises:
            ValueError: If value is a float (prohibited).
            TypeError: If value type is not supported.
        """
        if v is None:
            return cls(kind="null", value=None)
        if isinstance(v, bool):
            return cls(kind="bool", value=v)
        if isinstance(v, int):
            if v < _INT64_MIN or v > _INT64_MAX:
                msg = f"int64 out of range: {v}"
                raise ValueError(msg)
            return cls(kind="int64", value=v)
        if isinstance(v, float):
            raise ValueError(
                "float values are prohibited in partition keys. "
                "Use int, str, date, or datetime instead. "
                "See: ADR-011."
            )
        if isinstance(v, datetime):
            v = v.replace(tzinfo=UTC) if v.tzinfo is None else v.astimezone(UTC)
            iso = v.strftime("%Y-%m-%dT%H:%M:%S.") + f"{v.microsecond:06d}Z"
            if not _is_valid_timestamp_format(iso):
                msg = f"Invalid timestamp value: {iso!r}"
                raise ValueError(msg)
            return cls(kind="timestamp", value=iso)
        if isinstance(v, date):
            date_str = v.strftime("%Y-%m-%d")
            if not _is_valid_date_format(date_str):
                msg = f"Invalid date value: {date_str!r}"
                raise ValueError(msg)
            return cls(kind="date", value=date_str)
        if isinstance(v, str):
            return cls(kind="string", value=v)
        raise TypeError(f"Unsupported partition value type: {type(v)}")

    def to_tagged_string(self) -> str:
        """Convert to canonical tagged string per ADR-011."""
        tag = self._TYPE_TAGS[self.kind]

        if self.kind == "null":
            return f"{tag}null"
        if self.kind == "bool":
            return f"{tag}{str(self.value).lower()}"
        if self.kind == "int64":
            return f"{tag}{self.value}"
        if self.kind == "string":
            return f"{tag}{_b64url_no_pad_encode(str(self.value))}"
        # date/timestamp are already canonical strings
        return f"{tag}{self.value}"

    @classmethod
    def from_tagged_string(cls, tagged: str) -> ScalarValue:
        """Parse tagged string back to ScalarValue."""
        if ":" not in tagged:
            msg = f"Invalid tagged string (no colon): {tagged!r}"
            raise ValueError(msg)

        tag, value = tagged.split(":", 1)
        tag_with_colon = f"{tag}:"

        kind: ScalarKind | None = None
        for k, t in cls._TYPE_TAGS.items():
            if t == tag_with_colon:
                kind = k
                break

        if kind is None:
            msg = f"Unknown type tag: {tag!r}"
            raise ValueError(msg)

        if kind == "null":
            if value != "null":
                msg = f"Invalid null value: {tagged!r}"
                raise ValueError(msg)
            return cls(kind="null", value=None)
        if kind == "bool":
            if value not in ("true", "false"):
                msg = f"Invalid bool value: {tagged!r}"
                raise ValueError(msg)
            return cls(kind="bool", value=value == "true")
        if kind == "int64":
            if not re.fullmatch(r"-?\d+", value):
                msg = f"Invalid int64 value: {tagged!r}"
                raise ValueError(msg)
            parsed = int(value)
            if parsed < _INT64_MIN or parsed > _INT64_MAX:
                msg = f"int64 out of range: {tagged!r}"
                raise ValueError(msg)
            return cls(kind="int64", value=parsed)
        if kind == "string":
            return cls(kind="string", value=_b64url_no_pad_decode(value))
        if kind == "date":
            if not _is_valid_date_format(value):
                msg = f"Invalid date value: {tagged!r}"
                raise ValueError(msg)
            return cls(kind="date", value=value)

        if kind == "timestamp":
            if not _is_valid_timestamp_format(value):
                msg = f"Invalid timestamp value: {tagged!r}"
                raise ValueError(msg)
            return cls(kind="timestamp", value=value)

        msg = f"Unexpected kind: {kind}"
        raise ValueError(msg)

    def to_canonical(self) -> str:
        """Serialize to JSON literal representation (not PartitionKey canonical string)."""
        if self.kind == "null":
            return "null"
        if self.kind == "bool":
            return "true" if self.value else "false"
        if self.kind == "int64":
            return str(self.value)
        return json.dumps(self.value)
