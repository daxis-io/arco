"""Manifest generation module."""

from __future__ import annotations

from arco_flow.manifest.builder import ManifestBuilder, extract_git_context
from arco_flow.manifest.discovery import AssetDiscovery
from arco_flow.manifest.lockfile import Lockfile
from arco_flow.manifest.model import AssetEntry, AssetManifest, GitContext
from arco_flow.manifest.serialization import serialize_to_manifest_json, to_camel_case

__all__ = [
    "AssetDiscovery",
    "AssetEntry",
    "AssetManifest",
    "GitContext",
    "Lockfile",
    "ManifestBuilder",
    "extract_git_context",
    "serialize_to_manifest_json",
    "to_camel_case",
]
