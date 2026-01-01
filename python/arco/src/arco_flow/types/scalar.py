"""Scalar value types for partition keys.

Floats are explicitly prohibited to ensure cross-language determinism.

Type-tagged string format ensures proto map<string,string> compliance:
- s:value - string
- i:value - int64
- b:true/b:false - bool
- d:YYYY-MM-DD - date
- t:ISO8601 - timestamp
- n: - null
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import ClassVar, Literal

# Type alias for valid partition key dimension values
PartitionDimensionValue = str | int | bool | date | datetime | None

ScalarKind = Literal["string", "int64", "bool", "date", "timestamp", "null"]


@dataclass(frozen=True)
class ScalarValue:
    """Scalar value with explicit type for canonical encoding.

    Maps to proto ScalarValue message. Floats are prohibited
    to ensure cross-language determinism in partition keys.

    Type-tagged string format ensures proto map<string,string> compliance.
    """

    kind: ScalarKind
    value: str | int | bool | None

    # Type tag prefixes for proto-compatible string encoding
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

        Args:
            v: Python value to convert. Floats are explicitly rejected.

        Returns:
            ScalarValue with appropriate kind.

        Raises:
            ValueError: If value is a float (prohibited).
            TypeError: If value type is not supported.
        """
        if v is None:
            return cls(kind="null", value=None)
        if isinstance(v, bool):  # Must check before int
            return cls(kind="bool", value=v)
        if isinstance(v, int):
            return cls(kind="int64", value=v)
        if isinstance(v, float):
            raise ValueError(
                "float values are prohibited in partition keys. "
                "Use int, str, date, or datetime instead. "
                "See: canonical serialization rules in design docs."
            )
        if isinstance(v, datetime):
            v = v.replace(tzinfo=UTC) if v.tzinfo is None else v.astimezone(UTC)
            # ISO 8601 with milliseconds and Z suffix
            iso = v.strftime("%Y-%m-%dT%H:%M:%S.") + f"{v.microsecond // 1000:03d}Z"
            return cls(kind="timestamp", value=iso)
        if isinstance(v, date):
            return cls(kind="date", value=v.strftime("%Y-%m-%d"))
        if isinstance(v, str):
            return cls(kind="string", value=v)
        raise TypeError(f"Unsupported partition value type: {type(v)}")

    def to_tagged_string(self) -> str:
        """Convert to type-tagged string for proto map<string,string>.

        Returns:
            String in format '<type_tag>:<value>'.
        """
        tag = self._TYPE_TAGS[self.kind]
        if self.kind == "null":
            return f"{tag}"
        if self.kind == "bool":
            return f"{tag}{str(self.value).lower()}"
        return f"{tag}{self.value}"

    @classmethod
    def from_tagged_string(cls, tagged: str) -> ScalarValue:
        """Parse type-tagged string back to ScalarValue.

        Args:
            tagged: String in format '<type_tag>:<value>'.

        Returns:
            ScalarValue with parsed kind and value.

        Raises:
            ValueError: If format is invalid or tag unknown.
        """
        if ":" not in tagged:
            msg = f"Invalid tagged string (no colon): {tagged!r}"
            raise ValueError(msg)

        tag, value = tagged.split(":", 1)
        tag_with_colon = f"{tag}:"

        # Find kind by tag
        kind: ScalarKind | None = None
        for k, t in cls._TYPE_TAGS.items():
            if t == tag_with_colon:
                kind = k
                break

        if kind is None:
            msg = f"Unknown type tag: {tag!r}"
            raise ValueError(msg)

        if kind == "null":
            return cls(kind="null", value=None)
        if kind == "bool":
            return cls(kind="bool", value=value == "true")
        if kind == "int64":
            return cls(kind="int64", value=int(value))
        if kind in ("string", "date", "timestamp"):
            return cls(kind=kind, value=value)

        msg = f"Unexpected kind: {kind}"
        raise ValueError(msg)

    def to_canonical(self) -> str:
        """Serialize to canonical JSON representation (for internal use)."""
        if self.kind == "null":
            return "null"
        if self.kind == "bool":
            return "true" if self.value else "false"
        if self.kind == "int64":
            return str(self.value)
        return json.dumps(self.value)
