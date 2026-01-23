"""Cross-language canonical JSON golden vector tests.

These tests consume the Rust golden vectors to ensure Python produces identical
canonical JSON output.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from arco_flow.canonical_json import CanonicalJsonError, to_canonical_json


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def test_canonical_json_golden_vectors() -> None:
    vectors_path = _repo_root() / "crates/arco-core/tests/golden/canonical_json_vectors.json"
    data = json.loads(vectors_path.read_text(encoding="utf-8"))

    for vector in data["vectors"]:
        actual = to_canonical_json(vector["input"])
        assert actual == vector["expected_canonical"], vector["name"]


def test_canonical_json_rejects_floats() -> None:
    with pytest.raises(CanonicalJsonError, match="float"):
        to_canonical_json({"x": 1.25})

    with pytest.raises(CanonicalJsonError, match="float"):
        to_canonical_json([math.nan])

    with pytest.raises(CanonicalJsonError, match="float"):
        to_canonical_json([math.inf])
