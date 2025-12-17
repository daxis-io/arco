"""Asset discovery from Python source files.

Scans Python files for @asset-decorated functions and imports them
to trigger registration with the global registry.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

from servo._internal.registry import clear_registry, get_registry

if TYPE_CHECKING:
    from servo.asset import RegisteredAsset

logger = structlog.get_logger()


class AssetDiscovery:
    """Discovers @asset-decorated functions in Python files.

    Example:
        >>> discovery = AssetDiscovery(root_path=Path.cwd())
        >>> assets = discovery.discover()
        >>> print(f"Found {len(assets)} assets")
    """

    # Directory names to skip (exact match in path parts)
    SKIP_DIRS = frozenset([
        "__pycache__",
        ".venv",
        "venv",
        ".git",
        "tests",
        ".mypy_cache",
        ".ruff_cache",
        ".pytest_cache",
        "site-packages",
    ])

    # Filename prefixes to skip
    SKIP_FILE_PREFIXES = ("test_", "conftest")
    # Filename suffixes to skip
    SKIP_FILE_SUFFIXES = ("_test.py",)

    def __init__(self, root_path: Path | None = None) -> None:
        """Initialize discovery.

        Args:
            root_path: Root directory to scan. Defaults to current directory.
        """
        self.root_path = root_path or Path.cwd()

    def discover(self, *, clear: bool = True) -> list[RegisteredAsset]:
        """Discover all assets in the project.

        This method:
        1. Clears the registry (optional)
        2. Finds all Python files in the project
        3. Imports each file to trigger @asset registration
        4. Returns all registered assets

        Args:
            clear: Whether to clear registry before discovery.

        Returns:
            List of discovered RegisteredAsset instances, sorted by key.
        """
        if clear:
            clear_registry()

        python_files = self._find_python_files()
        logger.info(
            "discovering_assets",
            file_count=len(python_files),
            root=str(self.root_path),
        )

        for file_path in python_files:
            self._import_file(file_path)

        assets = get_registry().all()
        logger.info("discovery_complete", asset_count=len(assets))
        return assets

    def _find_python_files(self) -> list[Path]:
        """Find all Python files in the project.

        Returns:
            List of Python file paths, excluding test files and venvs.
        """
        files: list[Path] = []

        # Search in common asset locations
        search_dirs = [
            self.root_path / "assets",
            self.root_path / "src" / "assets",
            self.root_path,
        ]

        for search_dir in search_dirs:
            if not search_dir.exists():
                continue

            for py_file in search_dir.rglob("*.py"):
                if not self._should_skip(py_file):
                    files.append(py_file)

        # Deduplicate and sort
        return sorted(set(files))

    def _should_skip(self, path: Path) -> bool:
        """Check if a file should be skipped.

        Args:
            path: File path to check.

        Returns:
            True if the file should be skipped.
        """
        # Check if any directory in path is a skip directory
        # Only check directories between root_path and the file
        try:
            relative = path.relative_to(self.root_path)
            for part in relative.parts[:-1]:  # Exclude filename
                if part in self.SKIP_DIRS:
                    return True
        except ValueError:
            # path is not relative to root_path, check all parts
            for part in path.parts[:-1]:
                if part in self.SKIP_DIRS:
                    return True

        # Check filename patterns
        filename = path.name
        if filename.startswith(self.SKIP_FILE_PREFIXES):
            return True
        return filename.endswith(self.SKIP_FILE_SUFFIXES)

    def _import_file(self, file_path: Path) -> None:
        """Import a Python file to trigger @asset registration.

        Args:
            file_path: Path to the Python file to import.
        """
        try:
            # Generate unique module name to avoid conflicts
            module_name = f"_servo_discovery_{file_path.stem}_{id(file_path)}"

            spec = importlib.util.spec_from_file_location(module_name, file_path)
            if spec is None or spec.loader is None:
                logger.warning("cannot_load_file", path=str(file_path))
                return

            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module

            try:
                spec.loader.exec_module(module)
                logger.debug("imported_file", path=str(file_path))
            finally:
                # Clean up to avoid memory leaks
                sys.modules.pop(module_name, None)

        except Exception as e:
            logger.warning(
                "import_error",
                path=str(file_path),
                error=str(e),
                exc_info=True,
            )
