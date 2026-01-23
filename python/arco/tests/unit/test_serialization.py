"""Tests for canonical JSON serialization."""

from __future__ import annotations

import json

import pytest


class TestToCamelCase:
    """Tests for snake_case to camelCase conversion."""

    def test_simple_conversion(self) -> None:
        from arco_flow.manifest.serialization import to_camel_case

        assert to_camel_case("asset_key") == "assetKey"
        assert to_camel_case("tenant_id") == "tenantId"

    def test_single_word(self) -> None:
        from arco_flow.manifest.serialization import to_camel_case

        assert to_camel_case("name") == "name"
        assert to_camel_case("id") == "id"

    def test_multiple_underscores(self) -> None:
        from arco_flow.manifest.serialization import to_camel_case

        assert to_camel_case("code_version_id") == "codeVersionId"
        assert to_camel_case("max_retry_delay_seconds") == "maxRetryDelaySeconds"

    def test_empty_string(self) -> None:
        from arco_flow.manifest.serialization import to_camel_case

        assert to_camel_case("") == ""


class TestSerializeToManifestJson:
    """Tests for canonical JSON serialization."""

    def test_dict_keys_converted_to_camel_case(self) -> None:
        from arco_flow.manifest.serialization import serialize_to_manifest_json

        data = {"asset_key": {"namespace": "raw", "name": "events"}}
        result = serialize_to_manifest_json(data)
        parsed = json.loads(result)

        assert "assetKey" in parsed
        assert "asset_key" not in result

    def test_keys_sorted(self) -> None:
        from arco_flow.manifest.serialization import serialize_to_manifest_json

        data = {"zebra": 1, "alpha": 2, "beta": 3}
        result = serialize_to_manifest_json(data)

        assert result.index("alpha") < result.index("beta") < result.index("zebra")

    def test_no_whitespace(self) -> None:
        from arco_flow.manifest.serialization import serialize_to_manifest_json

        data = {"key": "value", "nested": {"a": 1}}
        result = serialize_to_manifest_json(data)

        assert " " not in result
        assert "\n" not in result
        assert "\t" not in result

    def test_nested_dict_conversion(self) -> None:
        from arco_flow.manifest.serialization import serialize_to_manifest_json

        data = {"outer_key": {"inner_key": {"deep_key": "value"}}}
        result = serialize_to_manifest_json(data)
        parsed = json.loads(result)

        assert "outerKey" in parsed
        assert "innerKey" in parsed["outerKey"]
        assert "deepKey" in parsed["outerKey"]["innerKey"]

    def test_list_items_converted(self) -> None:
        from arco_flow.manifest.serialization import serialize_to_manifest_json

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
        from arco_flow.manifest.serialization import serialize_to_manifest_json

        data = {"tags": {"foo_bar": "x", "foo__bar": "y"}}
        result = serialize_to_manifest_json(data)
        parsed = json.loads(result)

        assert parsed["tags"] == {"foo_bar": "x", "foo__bar": "y"}

    def test_struct_keys_preserved_for_metadata(self) -> None:
        from arco_flow.manifest.serialization import serialize_to_manifest_json

        data = {"metadata": {"foo_bar": {"inner_key": 1}}}
        result = serialize_to_manifest_json(data)
        parsed = json.loads(result)

        assert parsed["metadata"]["foo_bar"]["inner_key"] == 1

    def test_deterministic_output(self) -> None:
        from arco_flow.manifest.serialization import serialize_to_manifest_json

        data = {"b_key": 1, "a_key": 2, "c_key": {"nested": True}}

        results = [serialize_to_manifest_json(data) for _ in range(10)]
        assert all(r == results[0] for r in results)

    def test_primitives_unchanged(self) -> None:
        from arco_flow.manifest.serialization import serialize_to_manifest_json

        data = {
            "string_val": "hello",
            "int_val": 42,
            "bool_val": True,
            "null_val": None,
        }
        result = serialize_to_manifest_json(data)
        parsed = json.loads(result)

        assert parsed["stringVal"] == "hello"
        assert parsed["intVal"] == 42
        assert parsed["boolVal"] is True
        assert parsed["nullVal"] is None

    def test_rejects_floats(self) -> None:
        from arco_flow.manifest.serialization import serialize_to_manifest_json

        with pytest.raises(ValueError, match="float"):
            serialize_to_manifest_json({"float_val": 3.14})
