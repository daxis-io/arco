"""Cross-language PartitionKey fixture parity tests.

These tests consume the Rust fixture vectors to ensure Python produces identical
canonical partition key strings.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from arco_flow.types.partition import PartitionKey


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _parse_dimension_value(dim_type: str, value: str) -> object:
    if dim_type == "string":
        return value
    if dim_type == "date":
        return date.fromisoformat(value)
    if dim_type == "timestamp":
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(UTC)
    if dim_type == "int64":
        return int(value)
    if dim_type == "bool":
        if value not in ("true", "false"):
            raise ValueError(f"invalid bool literal: {value!r}")
        return value == "true"
    if dim_type == "null":
        if value != "null":
            raise ValueError(f"invalid null literal: {value!r}")
        return None
    raise ValueError(f"unknown fixture dimension type: {dim_type!r}")


def test_partition_key_fixture_parity() -> None:
    fixture_path = _repo_root() / "crates/arco-core/tests/fixtures/partition_key_cases.json"
    data = json.loads(fixture_path.read_text(encoding="utf-8"))

    assert isinstance(data, list)
    assert len(data) >= 10

    for case in data:
        dims: dict[str, object] = {}
        for key, dim in case["dimensions"].items():
            dims[key] = _parse_dimension_value(dim["type"], dim["value"])

        pk = PartitionKey(dims)
        assert pk.canonical_string() == case["expected_canonical"], case["name"]


def test_partition_key_fixture_rejects_invalid_key() -> None:
    with pytest.raises(ValueError, match="Invalid partition dimension key"):
        PartitionKey({"Date": "2025-01-01"})
