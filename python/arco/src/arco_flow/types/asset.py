"""Asset definition types matching Protobuf schema."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, TypeVar

from arco_flow._internal.frozen import FrozenDict
from arco_flow.types.partition import PartitionStrategy

if TYPE_CHECKING:
    from arco_flow.types.check import Check
    from arco_flow.types.ids import AssetId

T = TypeVar("T")

# Valid identifier pattern per proto/arco/v1/common.proto
_IDENTIFIER_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class AssetKey:
    """Asset identifier (namespace + name).

    Both namespace and name must match pattern ^[a-z][a-z0-9_]*$.

    Example:
        >>> key = AssetKey(namespace="staging", name="user_events")
        >>> str(key)
        "staging.user_events"
    """

    namespace: str
    name: str

    def __post_init__(self) -> None:
        """Validate namespace and name patterns."""
        if not _IDENTIFIER_PATTERN.match(self.namespace):
            msg = f"Invalid namespace '{self.namespace}': must match ^[a-z][a-z0-9_]*$"
            raise ValueError(msg)
        if not _IDENTIFIER_PATTERN.match(self.name):
            msg = f"Invalid name '{self.name}': must match ^[a-z][a-z0-9_]*$"
            raise ValueError(msg)

    def __str__(self) -> str:
        """Return dotted notation."""
        return f"{self.namespace}.{self.name}"

    @classmethod
    def parse(cls, s: str) -> AssetKey:
        """Parse from dotted notation."""
        if "." not in s:
            return cls(namespace="default", name=s)
        namespace, name = s.rsplit(".", 1)
        return cls(namespace=namespace, name=name)


class AssetIn(Generic[T]):
    """Type hint for asset inputs (dependencies).

    Usage:
        >>> def my_asset(
        ...     ctx: AssetContext,
        ...     upstream: AssetIn["raw.events"],
        ... ) -> AssetOut:
        ...     data = upstream.read()
        ...     ...

    The string in brackets is the upstream asset's key (namespace.name).

    Implementation: __class_getitem__ returns a dynamically created subclass
    with __asset_key__ as a class attribute. This ensures:
    - typing.get_type_hints() works correctly (returns a type, not instance)
    - Each key gets a unique type for proper introspection
    - Same key returns cached type (identity preserved)
    """

    __asset_key__: ClassVar[str] = ""
    _cache: ClassVar[dict[str, type[AssetIn[Any]]]] = {}

    def __init__(self) -> None:
        """Should not be instantiated directly."""
        raise TypeError(
            "AssetIn should not be instantiated directly. "
            "Use as type hint: AssetIn['namespace.name']"
        )

    def __class_getitem__(cls, key: str) -> type[AssetIn[Any]]:
        """Create or return cached type for the given asset key.

        Args:
            key: Asset key in 'namespace.name' format.

        Returns:
            A dynamically created subclass with __asset_key__ set.
        """
        if not isinstance(key, str):
            raise TypeError("AssetIn[...] key must be a string in 'namespace.name' format")
        if "." not in key:
            raise ValueError("AssetIn[...] key must be in 'namespace.name' format")
        # Validate format early (also enforces identifier pattern).
        AssetKey.parse(key)
        if key not in cls._cache:
            # Create a new type dynamically
            new_type: type[AssetIn[Any]] = type(
                f"AssetIn[{key!r}]",  # Type name for repr
                (cls,),  # Inherit from AssetIn
                {
                    "__asset_key__": key,
                    "__module__": cls.__module__,
                },
            )
            cls._cache[key] = new_type
        return cls._cache[key]

    @classmethod
    def get_asset_key(cls) -> AssetKey:
        """Parse the asset key from __asset_key__ class attribute.

        Returns:
            Parsed AssetKey.

        Raises:
            ValueError: If __asset_key__ is not set (base class).
        """
        if not cls.__asset_key__:
            msg = (
                "get_asset_key() called on base AssetIn class. "
                "Use a subscripted type like AssetIn['namespace.name']."
            )
            raise ValueError(msg)
        return AssetKey.parse(cls.__asset_key__)


class AssetOut:
    """Wrapper for asset output data.

    Created via AssetContext.output() to capture metadata.
    """

    def __init__(
        self,
        data: object,
        *,
        schema: dict[str, str] | None = None,
        row_count: int | None = None,
    ) -> None:
        """Initialize output wrapper."""
        self.data: object = data
        self.schema = schema
        self.row_count = row_count


class DependencyMapping(str, Enum):
    """How upstream partitions map to downstream."""

    IDENTITY = "identity"  # Same partition key
    ALL_UPSTREAM = "all"  # All partitions of upstream
    LATEST = "latest"  # Latest partition only
    WINDOW = "window"  # Time window (e.g., last 7 days)


@dataclass(frozen=True)
class AssetDependency:
    """Dependency on an upstream asset.

    Example:
        >>> dep = AssetDependency(
        ...     upstream_key=AssetKey.parse("raw.events"),
        ...     parameter_name="events",
        ... )
    """

    upstream_key: AssetKey
    parameter_name: str = ""
    mapping: DependencyMapping = DependencyMapping.IDENTITY
    window_start: int | None = None  # For WINDOW mapping
    window_end: int | None = None
    require_quality: bool = False


@dataclass(frozen=True)
class ExecutionPolicy:
    """Execution configuration for an asset."""

    max_retries: int = 3
    retry_delay_seconds: int = 60
    timeout_seconds: int = 3600
    max_parallelism: int | None = None


@dataclass(frozen=True)
class ResourceRequirements:
    """Resource requirements for task execution."""

    cpu_millicores: int = 1000  # 1000 = 1 core
    memory_mib: int = 1024
    ephemeral_storage_mib: int = 1024


@dataclass(frozen=True)
class CodeLocation:
    """Location of asset code."""

    module: str
    function: str
    file_path: str | None = None
    line_number: int | None = None


@dataclass(frozen=True)
class IoConfig:
    """I/O configuration for asset output.

    Maps to proto IoConfig message. Defines how asset outputs
    are written to storage.
    """

    format: Literal["parquet", "delta", "iceberg", "json", "csv"] = "parquet"
    compression: Literal["none", "snappy", "gzip", "zstd", "lz4"] = "snappy"
    partition_by: tuple[str, ...] = field(default_factory=tuple)
    row_group_size: int | None = None
    max_file_size_bytes: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "partition_by", tuple(self.partition_by))


@dataclass(frozen=True)
class AssetDefinition:
    """Complete asset definition for manifest.

    This is the serializable representation of an @asset-decorated function.
    """

    key: AssetKey
    id: AssetId
    description: str = ""

    owners: tuple[str, ...] = field(default_factory=tuple)
    tags: FrozenDict[str, str] = field(default_factory=FrozenDict)

    partitioning: PartitionStrategy = field(default_factory=PartitionStrategy)
    dependencies: tuple[AssetDependency, ...] = field(default_factory=tuple)

    code: CodeLocation = field(default_factory=lambda: CodeLocation(module="", function=""))
    checks: tuple[Check, ...] = field(default_factory=tuple)  # Full Check objects, not names
    execution: ExecutionPolicy = field(default_factory=ExecutionPolicy)
    resources: ResourceRequirements = field(default_factory=ResourceRequirements)
    io: IoConfig = field(default_factory=IoConfig)  # Output configuration

    transform_fingerprint: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "owners", tuple(self.owners))
        object.__setattr__(self, "tags", FrozenDict(self.tags))
        object.__setattr__(self, "dependencies", tuple(self.dependencies))
        object.__setattr__(self, "checks", tuple(self.checks))


def is_asset_in_type(annotation: type[Any]) -> bool:
    """Check if an annotation is an AssetIn type hint.

    Args:
        annotation: A type annotation to check.

    Returns:
        True if annotation is an AssetIn subclass with __asset_key__.
    """
    try:
        return (
            isinstance(annotation, type)
            and issubclass(annotation, AssetIn)
            and bool(getattr(annotation, "__asset_key__", ""))
        )
    except TypeError:
        # issubclass can fail for some generic types
        return False


def get_asset_in_key(annotation: type[Any]) -> AssetKey | None:
    """Extract AssetKey from an AssetIn annotation.

    Args:
        annotation: An AssetIn type hint.

    Returns:
        Parsed AssetKey, or None if not an AssetIn type.
    """
    if is_asset_in_type(annotation):
        return AssetKey.parse(annotation.__asset_key__)
    return None
