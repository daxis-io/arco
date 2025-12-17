"""Manifest model matching Protobuf AssetManifest schema.

The manifest captures all asset definitions and metadata for deployment.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from servo.manifest.serialization import serialize_to_manifest_json

if TYPE_CHECKING:
    from servo.types import AssetDefinition


@dataclass(frozen=True)
class GitContext:
    """Git metadata for deployment provenance.

    Captures the state of the git repository at deployment time.
    """

    repository: str = ""
    branch: str = ""
    commit_sha: str = ""
    commit_message: str = ""
    author: str = ""
    dirty: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "repository": self.repository,
            "branch": self.branch,
            "commit_sha": self.commit_sha,
            "commit_message": self.commit_message,
            "author": self.author,
            "dirty": self.dirty,
        }


@dataclass
class AssetEntry:
    """Asset entry in manifest (serializable form of AssetDefinition).

    This is the wire format that gets serialized to JSON.
    """

    key: dict[str, str]
    id: str
    description: str = ""
    owners: list[str] = field(default_factory=list)
    tags: dict[str, str] = field(default_factory=dict)
    partitioning: dict[str, Any] = field(default_factory=dict)
    dependencies: list[dict[str, Any]] = field(default_factory=list)
    code: dict[str, Any] = field(default_factory=dict)
    checks: list[dict[str, Any]] = field(default_factory=list)
    execution: dict[str, Any] = field(default_factory=dict)
    resources: dict[str, Any] = field(default_factory=dict)
    io: dict[str, Any] = field(default_factory=dict)
    transform_fingerprint: str = ""

    @classmethod
    def from_definition(cls, definition: AssetDefinition) -> AssetEntry:
        """Create AssetEntry from an AssetDefinition.

        Args:
            definition: The asset definition to convert.

        Returns:
            AssetEntry ready for JSON serialization.
        """
        # Serialize checks as dicts with their properties
        checks_serialized = []
        for check in definition.checks:
            check_dict: dict[str, Any] = {
                "name": check.name,
                "check_type": check.check_type,
                "severity": check.severity.value,
                "phase": check.phase.value,
            }
            # Add type-specific fields
            if hasattr(check, "columns"):
                check_dict["columns"] = list(check.columns)
            if hasattr(check, "min_rows"):
                check_dict["min_rows"] = check.min_rows
            if hasattr(check, "max_rows"):
                check_dict["max_rows"] = check.max_rows
            if hasattr(check, "timestamp_column"):
                check_dict["timestamp_column"] = check.timestamp_column
            if hasattr(check, "max_age_hours"):
                check_dict["max_age_hours"] = check.max_age_hours
            checks_serialized.append(check_dict)

        return cls(
            key={
                "namespace": definition.key.namespace,
                "name": definition.key.name,
            },
            id=str(definition.id),
            description=definition.description,
            owners=list(definition.owners),
            tags=dict(definition.tags),
            partitioning={
                "is_partitioned": definition.partitioning.is_partitioned,
                "dimensions": [
                    {
                        "name": d.name,
                        "kind": d.kind.value,
                    }
                    for d in definition.partitioning.dimensions
                ],
            },
            dependencies=[
                {
                    "upstream_key": {
                        "namespace": d.upstream_key.namespace,
                        "name": d.upstream_key.name,
                    },
                    "parameter_name": d.parameter_name,
                    "mapping": d.mapping.value,
                }
                for d in definition.dependencies
            ],
            code={
                "module": definition.code.module,
                "function": definition.code.function,
                "file_path": definition.code.file_path,
                "line_number": definition.code.line_number,
            },
            checks=checks_serialized,
            execution={
                "max_retries": definition.execution.max_retries,
                "retry_delay_seconds": definition.execution.retry_delay_seconds,
                "timeout_seconds": definition.execution.timeout_seconds,
            },
            resources={
                "cpu_millicores": definition.resources.cpu_millicores,
                "memory_mib": definition.resources.memory_mib,
            },
            io={
                "format": definition.io.format,
                "compression": definition.io.compression,
                "partition_by": list(definition.io.partition_by),
            },
            transform_fingerprint=definition.transform_fingerprint,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "key": self.key,
            "id": self.id,
            "description": self.description,
            "owners": self.owners,
            "tags": self.tags,
            "partitioning": self.partitioning,
            "dependencies": self.dependencies,
            "code": self.code,
            "checks": self.checks,
            "execution": self.execution,
            "resources": self.resources,
            "io": self.io,
            "transform_fingerprint": self.transform_fingerprint,
        }


@dataclass
class AssetManifest:
    """Complete manifest for deployment.

    Matches proto/arco/v1/asset.proto AssetManifest message.
    """

    manifest_version: str = "1.0"
    tenant_id: str = ""
    workspace_id: str = ""
    code_version_id: str = ""

    git: GitContext = field(default_factory=GitContext)
    assets: list[AssetEntry] = field(default_factory=list)
    schedules: list[dict[str, Any]] = field(default_factory=list)

    deployed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    deployed_by: str = ""

    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "manifest_version": self.manifest_version,
            "tenant_id": self.tenant_id,
            "workspace_id": self.workspace_id,
            "code_version_id": self.code_version_id,
            "git": self.git.to_dict(),
            "assets": [a.to_dict() for a in self.assets],
            "schedules": self.schedules,
            "deployed_at": self.deployed_at.isoformat(),
            "deployed_by": self.deployed_by,
            "metadata": self.metadata,
        }

    def to_canonical_json(self) -> str:
        """Serialize to canonical JSON (camelCase, sorted keys, no whitespace).

        Returns:
            Canonical JSON string matching Protobuf JSON convention.
        """
        return serialize_to_manifest_json(self.to_dict())

    def fingerprint(self) -> str:
        """Compute SHA-256 fingerprint of canonical form.

        Returns:
            64-character hex string.
        """
        canonical = self.to_canonical_json()
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
