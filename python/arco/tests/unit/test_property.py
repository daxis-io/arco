"""Property-based tests using Hypothesis."""
from __future__ import annotations

import time

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st


class TestPartitionKeyProperties:
    """Property tests for PartitionKey."""

    @given(
        dims=st.dictionaries(
            keys=st.from_regex(r"[a-z][a-z0-9_]{0,20}", fullmatch=True),
            values=st.one_of(
                st.text(min_size=1, max_size=50, alphabet=st.characters(
                    whitelist_categories=("Lu", "Ll", "Nd"),
                    min_codepoint=32, max_codepoint=126,
                )),
                st.integers(min_value=-2**31, max_value=2**31),
                st.booleans(),
            ),
            min_size=0,
            max_size=5,
        )
    )
    def test_canonical_deterministic(self, dims: dict[str, str | int | bool]) -> None:
        """Canonical form is deterministic regardless of insertion order."""
        from servo.types.partition import PartitionKey

        # Create with original order
        pk1 = PartitionKey(dims)

        # Create with reversed order
        reversed_dims = dict(reversed(list(dims.items())))
        pk2 = PartitionKey(reversed_dims)

        assert pk1.to_canonical() == pk2.to_canonical()

    @given(
        dims=st.dictionaries(
            keys=st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True),
            values=st.text(min_size=1, max_size=20, alphabet=st.characters(
                whitelist_categories=("Lu", "Ll", "Nd"),
                min_codepoint=32, max_codepoint=126,
            )),
            min_size=1,
            max_size=3,
        )
    )
    def test_fingerprint_stable(self, dims: dict[str, str]) -> None:
        """Fingerprint is stable across multiple calls."""
        from servo.types.partition import PartitionKey

        pk = PartitionKey(dims)
        fp1 = pk.fingerprint()
        fp2 = pk.fingerprint()

        assert fp1 == fp2
        assert len(fp1) == 64  # SHA-256 hex


class TestAssetKeyProperties:
    """Property tests for AssetKey."""

    @given(
        namespace=st.from_regex(r"[a-z][a-z0-9_]{0,20}", fullmatch=True),
        name=st.from_regex(r"[a-z][a-z0-9_]{0,20}", fullmatch=True),
    )
    def test_roundtrip_through_string(self, namespace: str, name: str) -> None:
        """AssetKey survives string roundtrip."""
        from servo.types.asset import AssetKey

        original = AssetKey(namespace=namespace, name=name)
        parsed = AssetKey.parse(str(original))

        assert parsed == original


class TestIdProperties:
    """Property tests for ID types."""

    @given(count=st.integers(min_value=1, max_value=100))
    def test_ulid_uniqueness(self, count: int) -> None:
        """Generated ULIDs are unique."""
        from servo.types.ids import AssetId

        ids = {AssetId.generate() for _ in range(count)}
        assert len(ids) == count

    @settings(max_examples=10)  # Reduce examples due to sleep
    @given(count=st.integers(min_value=2, max_value=10))
    def test_ulid_lexicographic_order(self, count: int) -> None:
        """Sequential ULIDs are lexicographically ordered."""
        from servo.types.ids import AssetId

        ids = []
        for _ in range(count):
            ids.append(AssetId.generate())
            time.sleep(0.002)  # Small delay to ensure different timestamps

        assert ids == sorted(ids)


class TestSerializationProperties:
    """Property tests for JSON serialization."""

    @given(
        data=st.dictionaries(
            keys=st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True),
            values=st.one_of(
                st.text(min_size=0, max_size=50, alphabet=st.characters(
                    whitelist_categories=("Lu", "Ll", "Nd"),
                    min_codepoint=32, max_codepoint=126,
                )),
                st.integers(min_value=-1000, max_value=1000),
                st.booleans(),
                st.none(),
            ),
            min_size=0,
            max_size=5,
        )
    )
    def test_serialization_deterministic(self, data: dict[str, object]) -> None:
        """Serialization is deterministic."""
        from servo.manifest.serialization import serialize_to_manifest_json

        try:
            result1 = serialize_to_manifest_json(data)
        except (TypeError, ValueError) as err:
            with pytest.raises(type(err)):
                serialize_to_manifest_json(data)
        else:
            result2 = serialize_to_manifest_json(data)
            assert result1 == result2

    @given(
        key=st.from_regex(r"[a-z][a-z0-9_]+", fullmatch=True),
    )
    def test_camel_case_idempotent_for_single_word(self, key: str) -> None:
        """Single word keys without underscores remain unchanged."""
        from servo.manifest.serialization import to_camel_case

        if "_" not in key:
            assert to_camel_case(key) == key
