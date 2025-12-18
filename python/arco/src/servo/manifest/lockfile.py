"""Asset ID lockfile for stable deploy identity.

The lockfile (.servo/state.json) maps AssetKey -> AssetId persistently.
This ensures:
- Same asset keeps same ID across deploys
- Renames require migrating the mapping (e.g., server reconciliation or manual update)
- Deploy is idempotent (no random ID generation per import)
"""
from __future__ import annotations

import json
import warnings
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from servo.types.ids import AssetId

if TYPE_CHECKING:
    from pathlib import Path

    from servo.types.asset import AssetKey


@dataclass
class Lockfile:
    """Persistent asset ID mapping.

    Attributes:
        asset_ids: Map of 'namespace.name' -> AssetId string
        version: Lockfile format version
    """

    asset_ids: dict[str, str] = field(default_factory=dict)
    version: str = "1.0"

    @classmethod
    def load(cls, path: Path) -> Lockfile:
        """Load lockfile from path.

        Args:
            path: Path to .servo/state.json

        Returns:
            Lockfile instance (empty if file doesn't exist)
        """
        if not path.exists():
            return cls()

        try:
            with path.open() as f:
                data = json.load(f)

            return cls(
                asset_ids=data.get("assetIds", {}),
                version=data.get("version", "1.0"),
            )
        except (json.JSONDecodeError, KeyError) as e:
            # Corrupted lockfile - start fresh but warn
            warnings.warn(f"Corrupted lockfile at {path}, starting fresh: {e}", stacklevel=2)
            return cls()

    def save(self, path: Path) -> None:
        """Save lockfile to path.

        Args:
            path: Path to .servo/state.json
        """
        path.parent.mkdir(parents=True, exist_ok=True)

        data = {
            "version": self.version,
            "assetIds": self.asset_ids,
        }

        with path.open("w") as f:
            json.dump(data, f, indent=2, sort_keys=True)

    def get_or_create(self, key: AssetKey) -> tuple[AssetId, bool]:
        """Get existing ID or create new one.

        Args:
            key: Asset key to look up

        Returns:
            Tuple of (AssetId, created) where created is True if new
        """
        key_str = str(key)
        if key_str in self.asset_ids:
            return AssetId(self.asset_ids[key_str]), False

        # Generate new ID
        new_id = AssetId.generate()
        self.asset_ids[key_str] = str(new_id)
        return new_id, True

    def update_from_server(self, server_ids: dict[str, str]) -> None:
        """Update with server-assigned IDs.

        Server IDs take precedence over local IDs.

        Args:
            server_ids: Map of 'namespace.name' -> AssetId from server
        """
        self.asset_ids.update(server_ids)
