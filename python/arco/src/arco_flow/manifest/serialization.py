"""Canonical JSON serialization matching Protobuf schema.

This module provides serialization that:
- Converts snake_case field names to camelCase (Protobuf JSON convention)
- Produces strict canonical JSON (ADR-010): sorted keys, no whitespace, UTF-8
- Rejects floats to avoid cross-language drift

Note: Protobuf JSON casing applies to *field names*, not to map keys.
"""

from __future__ import annotations

import json
from typing import Any, cast

from arco_flow.canonical_json import to_canonical_json

JsonValue = dict[str, Any] | list[Any] | str | int | bool | None

# Fields whose values are map/struct-like and must preserve user keys exactly.
_MAP_FIELD_NAMES = frozenset({"metadata", "tags"})


def to_camel_case(snake_str: str) -> str:
    if not snake_str:
        return snake_str
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def _convert_keys(obj: JsonValue, *, convert_keys: bool = True) -> JsonValue:
    """Recursively convert dictionary *field* names to camelCase."""
    if isinstance(obj, dict):
        converted: dict[str, Any] = {}
        for key, value in sorted(obj.items()):
            if not isinstance(key, str):
                msg = f"JSON object keys must be strings, got {type(key).__name__}"
                raise TypeError(msg)

            out_key = to_camel_case(key) if convert_keys else key
            if out_key in converted:
                msg = (
                    "Key collision during camelCase conversion: "
                    f"{key!r} -> {out_key!r} conflicts with an existing key. "
                    "This usually indicates mixed casing or invalid input."
                )
                raise ValueError(msg)

            next_convert_keys = convert_keys and key not in _MAP_FIELD_NAMES
            converted[out_key] = _convert_keys(value, convert_keys=next_convert_keys)
        return converted
    if isinstance(obj, list):
        return [_convert_keys(item, convert_keys=convert_keys) for item in obj]
    return obj


def serialize_to_manifest_json(obj: JsonValue | object) -> str:
    """Serialize object to canonical JSON with camelCase keys."""
    json_obj: JsonValue
    if hasattr(obj, "__dict__") and not isinstance(obj, dict):
        json_obj = cast("JsonValue", obj.__dict__)
    else:
        json_obj = cast("JsonValue", obj)

    converted = _convert_keys(json_obj)
    return to_canonical_json(converted)


def serialize_dict_for_fingerprint(data: dict[str, Any]) -> str:
    """Serialize a dict for fingerprinting (no camelCase conversion)."""
    if not isinstance(data, dict):
        raise TypeError("data must be a dict")
    return to_canonical_json(data)


def dumps_json_pretty(data: Any) -> str:
    """Pretty-print JSON for debugging output only."""
    return json.dumps(data, indent=2, ensure_ascii=False)
