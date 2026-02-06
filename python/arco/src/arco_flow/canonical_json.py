"""Strict canonical JSON serialization (ADR-010).

Properties:
- Object keys sorted lexicographically by UTF-8 bytes
- No whitespace
- UTF-8 output (ensure_ascii=False)
- Floats rejected (including NaN/Infinity)
"""

from __future__ import annotations

import json
from typing import Any


class CanonicalJsonError(ValueError):
    """Raised when canonical JSON cannot be produced."""


def _reject_floats(value: Any) -> None:
    if isinstance(value, float):
        raise CanonicalJsonError("float values are not allowed in canonical JSON (use integers)")
    if isinstance(value, dict):
        for v in value.values():
            _reject_floats(v)
        return
    if isinstance(value, list):
        for v in value:
            _reject_floats(v)
        return


def _sort_keys_utf8(value: Any) -> Any:
    if isinstance(value, dict):
        items: list[tuple[str, Any]] = []
        for k, v in value.items():
            if not isinstance(k, str):
                raise TypeError(f"JSON object keys must be strings, got {type(k).__name__}")
            items.append((k, v))
        items.sort(key=lambda kv: kv[0].encode("utf-8"))
        return {k: _sort_keys_utf8(v) for k, v in items}
    if isinstance(value, list):
        return [_sort_keys_utf8(v) for v in value]
    return value


def to_canonical_json(value: Any) -> str:
    """Serialize a JSON-compatible value into strict canonical JSON."""
    _reject_floats(value)
    normalized = _sort_keys_utf8(value)
    return json.dumps(
        normalized,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
