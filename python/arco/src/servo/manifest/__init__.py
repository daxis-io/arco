"""Manifest generation module."""
from __future__ import annotations

from servo.manifest.builder import ManifestBuilder, extract_git_context
from servo.manifest.discovery import AssetDiscovery
from servo.manifest.lockfile import Lockfile
from servo.manifest.model import AssetEntry, AssetManifest, GitContext
from servo.manifest.serialization import serialize_to_manifest_json, to_camel_case

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
