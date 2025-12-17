"""Canonical JSON serialization matching Protobuf schema.

This module provides serialization that:
- Converts snake_case keys to camelCase (matching protobuf JSON convention)
- Sorts keys alphabetically for deterministic output
- Removes whitespace for compact representation
- Produces identical output for identical input (fingerprint-safe)
"""
from __future__ import annotations

import json
from typing import Any, cast

# Type alias for JSON-serializable values (Any is appropriate here)
JsonValue = dict[str, Any] | list[Any] | str | int | float | bool | None


def to_camel_case(snake_str: str) -> str:
    """Convert snake_case string to camelCase.

    Args:
        snake_str: String in snake_case format.

    Returns:
        String in camelCase format.

    Example:
        >>> to_camel_case("asset_key")
        "assetKey"
    """
    if not snake_str:
        return snake_str
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def _convert_keys(obj: JsonValue) -> JsonValue:
    """Recursively convert dictionary keys to camelCase.

    Args:
        obj: Any JSON-serializable object.

    Returns:
        Object with all dict keys converted to camelCase.
    """
    if isinstance(obj, dict):
        return {to_camel_case(k): _convert_keys(v) for k, v in sorted(obj.items())}
    if isinstance(obj, list):
        return [_convert_keys(item) for item in obj]
    return obj


def serialize_to_manifest_json(obj: JsonValue | object) -> str:
    """Serialize object to canonical JSON with camelCase keys.

    This function produces canonical JSON that:
    - Uses camelCase keys (matching Protobuf JSON convention)
    - Sorts keys alphabetically
    - Has no unnecessary whitespace
    - Is deterministic (same input = same output)

    Args:
        obj: A dict, dataclass (via __dict__), or JSON-serializable object.

    Returns:
        Canonical JSON string.

    Example:
        >>> data = {"asset_key": {"namespace": "raw"}}
        >>> serialize_to_manifest_json(data)
        '{"assetKey":{"namespace":"raw"}}'
    """
    # Convert dataclass to dict if needed
    json_obj: JsonValue
    if hasattr(obj, "__dict__") and not isinstance(obj, dict):
        json_obj = cast("JsonValue", obj.__dict__)
    else:
        json_obj = cast("JsonValue", obj)

    converted = _convert_keys(json_obj)
    return json.dumps(converted, separators=(",", ":"), sort_keys=True)


def serialize_dict_for_fingerprint(data: dict[str, Any]) -> str:
    """Serialize a dict for fingerprinting (no camelCase conversion).

    Used for internal hashing where Python names are retained.

    Args:
        data: Dictionary to serialize.

    Returns:
        Canonical JSON string with sorted keys and no whitespace.
    """
    return json.dumps(data, separators=(",", ":"), sort_keys=True)
