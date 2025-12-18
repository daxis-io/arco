"""Tests for canonical JSON serialization."""
from __future__ import annotations

import json


class TestToCamelCase:
    """Tests for snake_case to camelCase conversion."""

    def test_simple_conversion(self) -> None:
        """Simple snake_case converts to camelCase."""
        from servo.manifest.serialization import to_camel_case

        assert to_camel_case("asset_key") == "assetKey"
        assert to_camel_case("tenant_id") == "tenantId"

    def test_single_word(self) -> None:
        """Single words remain unchanged."""
        from servo.manifest.serialization import to_camel_case

        assert to_camel_case("name") == "name"
        assert to_camel_case("id") == "id"

    def test_multiple_underscores(self) -> None:
        """Multiple underscores handled correctly."""
        from servo.manifest.serialization import to_camel_case

        assert to_camel_case("code_version_id") == "codeVersionId"
        assert to_camel_case("max_retry_delay_seconds") == "maxRetryDelaySeconds"

    def test_empty_string(self) -> None:
        """Empty string returns empty string."""
        from servo.manifest.serialization import to_camel_case

        assert to_camel_case("") == ""


class TestSerializeToManifestJson:
    """Tests for canonical JSON serialization."""

    def test_dict_keys_converted_to_camel_case(self) -> None:
        """Dictionary keys are converted to camelCase."""
        from servo.manifest.serialization import serialize_to_manifest_json

        data = {"asset_key": {"namespace": "raw", "name": "events"}}
        result = serialize_to_manifest_json(data)
        parsed = json.loads(result)

        assert "assetKey" in parsed
        assert "asset_key" not in result

    def test_keys_sorted(self) -> None:
        """Keys are sorted alphabetically."""
        from servo.manifest.serialization import serialize_to_manifest_json

        data = {"zebra": 1, "alpha": 2, "beta": 3}
        result = serialize_to_manifest_json(data)

        # Verify order by checking positions
        assert result.index("alpha") < result.index("beta") < result.index("zebra")

    def test_no_whitespace(self) -> None:
        """Output has no unnecessary whitespace."""
        from servo.manifest.serialization import serialize_to_manifest_json

        data = {"key": "value", "nested": {"a": 1}}
        result = serialize_to_manifest_json(data)

        assert " " not in result
        assert "\n" not in result
        assert "\t" not in result

    def test_nested_dict_conversion(self) -> None:
        """Nested dictionaries have keys converted."""
        from servo.manifest.serialization import serialize_to_manifest_json

        data = {
            "outer_key": {
                "inner_key": {
                    "deep_key": "value"
                }
            }
        }
        result = serialize_to_manifest_json(data)
        parsed = json.loads(result)

        assert "outerKey" in parsed
        assert "innerKey" in parsed["outerKey"]
        assert "deepKey" in parsed["outerKey"]["innerKey"]

    def test_list_items_converted(self) -> None:
        """List items (if dicts) have keys converted."""
        from servo.manifest.serialization import serialize_to_manifest_json

        data = {
            "items": [
                {"item_name": "a"},
                {"item_name": "b"},
            ]
        }
        result = serialize_to_manifest_json(data)
        parsed = json.loads(result)

        assert "itemName" in parsed["items"][0]

    def test_map_keys_preserved_for_tags(self) -> None:
        """Map keys (e.g., tags) are not camelCased."""
        from servo.manifest.serialization import serialize_to_manifest_json

        data = {"tags": {"foo_bar": "x", "foo__bar": "y"}}
        result = serialize_to_manifest_json(data)
        parsed = json.loads(result)

        # Map keys must be preserved exactly; conversion must not drop/merge keys.
        assert parsed["tags"] == {"foo_bar": "x", "foo__bar": "y"}

    def test_struct_keys_preserved_for_metadata(self) -> None:
        """Struct-like metadata must preserve keys at all nesting levels."""
        from servo.manifest.serialization import serialize_to_manifest_json

        data = {"metadata": {"foo_bar": {"inner_key": 1}}}
        result = serialize_to_manifest_json(data)
        parsed = json.loads(result)

        assert parsed["metadata"]["foo_bar"]["inner_key"] == 1

    def test_deterministic_output(self) -> None:
        """Same input produces identical output every time."""
        from servo.manifest.serialization import serialize_to_manifest_json

        data = {"b_key": 1, "a_key": 2, "c_key": {"nested": True}}

        results = [serialize_to_manifest_json(data) for _ in range(10)]
        assert all(r == results[0] for r in results)

    def test_primitives_unchanged(self) -> None:
        """Primitive values are serialized correctly."""
        from servo.manifest.serialization import serialize_to_manifest_json

        data = {
            "string_val": "hello",
            "int_val": 42,
            "float_val": 3.14,
            "bool_val": True,
            "null_val": None,
        }
        result = serialize_to_manifest_json(data)
        parsed = json.loads(result)

        assert parsed["stringVal"] == "hello"
        assert parsed["intVal"] == 42
        assert parsed["floatVal"] == 3.14
        assert parsed["boolVal"] is True
        assert parsed["nullVal"] is None
