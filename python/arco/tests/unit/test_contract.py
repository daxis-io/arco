"""Contract tests for JSON serialization matching Protobuf schema.

These tests ensure that Python types serialize to JSON that matches
the Protobuf schema exactly, including:
- Field names (camelCase)
- Required fields
- Value formats
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest


class TestAssetDefinitionContract:
    """Contract tests for AssetDefinition serialization."""

    def test_required_fields_present(self) -> None:
        """Serialized AssetDefinition has all required fields."""
        from arco_flow.manifest.model import AssetEntry
        from arco_flow.manifest.serialization import serialize_to_manifest_json
        from arco_flow.types import AssetDefinition, AssetId, AssetKey, CodeLocation

        definition = AssetDefinition(
            key=AssetKey("raw", "events"),
            id=AssetId.generate(),
            description="Test",
            code=CodeLocation(module="test", function="events"),
        )

        entry = AssetEntry.from_definition(definition)
        json_str = serialize_to_manifest_json(entry.to_dict())
        data = json.loads(json_str)

        # Required fields per proto/arco/v1/asset.proto
        required_fields = [
            "key",
            "id",
            "description",
            "owners",
            "tags",
            "partitioning",
            "dependencies",
            "code",
            "checks",
            "execution",
            "resources",
            "transformFingerprint",  # camelCase!
        ]

        for field in required_fields:
            assert field in data, f"Missing required field: {field}"

    def test_key_structure(self) -> None:
        """AssetKey serializes with namespace and name fields."""
        from arco_flow.manifest.model import AssetEntry
        from arco_flow.manifest.serialization import serialize_to_manifest_json
        from arco_flow.types import AssetDefinition, AssetId, AssetKey, CodeLocation

        definition = AssetDefinition(
            key=AssetKey("staging", "users"),
            id=AssetId.generate(),
            code=CodeLocation(module="m", function="f"),
        )

        entry = AssetEntry.from_definition(definition)
        json_str = serialize_to_manifest_json(entry.to_dict())
        data = json.loads(json_str)

        assert data["key"]["namespace"] == "staging"
        assert data["key"]["name"] == "users"

    def test_camel_case_fields(self) -> None:
        """All fields use camelCase naming."""
        from arco_flow.manifest.model import AssetEntry
        from arco_flow.manifest.serialization import serialize_to_manifest_json
        from arco_flow.types import AssetDefinition, AssetId, AssetKey, CodeLocation

        definition = AssetDefinition(
            key=AssetKey("raw", "events"),
            id=AssetId.generate(),
            code=CodeLocation(
                module="test",
                function="events",
                file_path="/path/to/file.py",
                line_number=42,
            ),
        )

        entry = AssetEntry.from_definition(definition)
        json_str = serialize_to_manifest_json(entry.to_dict())

        # Verify snake_case is NOT in output
        assert "transform_fingerprint" not in json_str
        assert "file_path" not in json_str
        assert "line_number" not in json_str

        # Verify camelCase IS in output
        assert "transformFingerprint" in json_str
        assert "filePath" in json_str
        assert "lineNumber" in json_str


class TestPartitionKeyContract:
    """Contract tests for PartitionKey serialization."""

    def test_canonical_format_matches_spec(self) -> None:
        """PartitionKey canonical format matches specification.

        Per design docs:
        - Keys sorted alphabetically
        - No whitespace
        - JSON format with type prefixes (s: for strings, i: for integers, etc.)
        """
        from arco_flow.types.partition import PartitionKey

        pk = PartitionKey({"date": "2025-01-15", "tenant": "acme"})
        canonical = pk.to_canonical()

        # Must be: sorted keys, no whitespace, proper JSON with type prefixes
        # String values are prefixed with "s:" for type safety
        expected = '{"date":"s:2025-01-15","tenant":"s:acme"}'
        assert canonical == expected


class TestScalarValueContract:
    """Contract tests for ScalarValue types."""

    def test_type_tags_match_proto(self) -> None:
        """ScalarValue kinds match proto ScalarValue oneof cases."""
        from arco_flow.types.scalar import ScalarValue

        # String
        sv_str = ScalarValue.from_value("hello")
        assert sv_str.kind == "string"

        # Int
        sv_int = ScalarValue.from_value(42)
        assert sv_int.kind == "int64"

        # Date (YYYY-MM-DD per proto)
        sv_date = ScalarValue.from_value(date(2025, 1, 15))
        assert sv_date.kind == "date"
        assert sv_date.value == "2025-01-15"

        # Timestamp (ISO 8601 with micros per proto)
        sv_ts = ScalarValue.from_value(datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC))
        assert sv_ts.kind == "timestamp"
        assert "2025-01-15T10:30:00" in str(sv_ts.value)

        # Bool
        sv_bool = ScalarValue.from_value(True)
        assert sv_bool.kind == "bool"

        # Null
        sv_null = ScalarValue.from_value(None)
        assert sv_null.kind == "null"

    def test_float_prohibited(self) -> None:
        """Float values are prohibited per canonical serialization rules."""
        from arco_flow.types.scalar import ScalarValue

        with pytest.raises(ValueError, match=r"[Ff]loat"):
            ScalarValue.from_value(3.14)


class TestManifestContract:
    """Contract tests for AssetManifest serialization."""

    def test_manifest_structure(self) -> None:
        """Manifest JSON has correct structure."""
        from arco_flow.manifest.model import AssetManifest, GitContext

        manifest = AssetManifest(
            tenant_id="acme",
            workspace_id="prod",
            git=GitContext(branch="main"),
        )

        json_str = manifest.to_canonical_json()
        data = json.loads(json_str)

        # Required top-level fields
        assert "manifestVersion" in data
        assert "tenantId" in data
        assert "workspaceId" in data
        assert "codeVersionId" in data
        assert "git" in data
        assert "assets" in data
        assert "deployedAt" in data

    def test_git_context_fields(self) -> None:
        """Git context has expected fields."""
        from arco_flow.manifest.model import AssetManifest, GitContext

        manifest = AssetManifest(
            git=GitContext(
                repository="https://github.com/org/repo",
                branch="main",
                commit_sha="abc123",
                dirty=True,
            ),
        )

        json_str = manifest.to_canonical_json()
        data = json.loads(json_str)

        assert data["git"]["repository"] == "https://github.com/org/repo"
        assert data["git"]["branch"] == "main"
        assert data["git"]["commitSha"] == "abc123"  # camelCase
        assert data["git"]["dirty"] is True
