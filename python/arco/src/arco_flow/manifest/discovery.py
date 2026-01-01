"""Asset discovery from Python source files.

Scans Python files for @asset-decorated functions and imports them
to trigger registration with the global registry.
"""

from __future__ import annotations

import importlib
import os
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

from arco_flow._internal.registry import (
    RegisteredAssetProtocol,
    clear_registry,
    get_registry,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = structlog.get_logger()

_MAX_FAILURES_IN_ERROR_MESSAGE = 10


@dataclass(frozen=True, slots=True)
class ImportFailure:
    """Details of a module import failure during discovery."""

    file_path: Path
    module_name: str
    error: BaseException
    traceback: str


class AssetDiscoveryError(Exception):
    """Discovery failed due to import errors."""

    def __init__(self, failures: Sequence[ImportFailure]) -> None:
        self.failures = list(failures)

        msg_lines = ["asset discovery failed:"]
        for failure in self.failures[:_MAX_FAILURES_IN_ERROR_MESSAGE]:
            msg_lines.append(f"- {failure.module_name} ({failure.file_path}): {failure.error}")
        if len(self.failures) > _MAX_FAILURES_IN_ERROR_MESSAGE:
            msg_lines.append(
                f"- ... and {len(self.failures) - _MAX_FAILURES_IN_ERROR_MESSAGE} more"
            )

        super().__init__("\n".join(msg_lines))


class AssetDiscovery:
    """Discovers @asset-decorated functions in Python files.

    Example:
        >>> discovery = AssetDiscovery(root_path=Path.cwd())
        >>> assets = discovery.discover()
        >>> print(f"Found {len(assets)} assets")
    """

    # Directory names to skip (exact match in path parts)
    SKIP_DIRS = frozenset(
        [
            "__pycache__",
            ".venv",
            "venv",
            ".git",
            "tests",
            ".mypy_cache",
            ".ruff_cache",
            ".pytest_cache",
            "site-packages",
        ]
    )

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

    def discover(
        self,
        *,
        clear: bool = True,
        strict: bool = False,
    ) -> list[RegisteredAssetProtocol]:
        """Discover all assets in the project.

        This method:
        1. Clears the registry (optional)
        2. Finds all Python files in the project
        3. Imports each file to trigger @asset registration
        4. Returns all registered assets

        Args:
            clear: Whether to clear registry before discovery.
            strict: If true, fail discovery if any file fails to import.

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

        preloaded_modules = set(sys.modules.keys())
        purged_packages: set[str] = set()

        module_plan: list[tuple[Path, str]] = []
        failures: list[ImportFailure] = []
        module_to_path: dict[str, Path] = {}
        for file_path in python_files:
            try:
                module_name = self._module_name_for_path(file_path)
            except Exception as err:
                failures.append(
                    ImportFailure(
                        file_path=file_path,
                        module_name="<unknown>",
                        error=err,
                        traceback=traceback.format_exc(),
                    )
                )
                logger.warning(
                    "invalid_module_path",
                    path=str(file_path),
                    error=str(err),
                    exc_info=True,
                )
                continue

            existing = module_to_path.get(module_name)
            if existing is not None and existing != file_path:
                dup_err = ValueError(
                    f"Duplicate module name {module_name!r} for {existing} and {file_path}"
                )
                failures.append(
                    ImportFailure(
                        file_path=file_path,
                        module_name=module_name,
                        error=dup_err,
                        traceback="",
                    )
                )
                logger.warning(
                    "duplicate_module_name",
                    module=module_name,
                    first=str(existing),
                    second=str(file_path),
                )
                continue

            module_to_path[module_name] = file_path
            module_plan.append((file_path, module_name))

        added_sys_path = self._add_sys_path()
        try:
            importlib.invalidate_caches()

            for file_path, module_name in module_plan:
                failure = self._import_module(
                    file_path,
                    module_name,
                    clear=clear,
                    preloaded_modules=preloaded_modules,
                    purged_packages=purged_packages,
                )
                if failure is not None:
                    failures.append(failure)
        finally:
            self._remove_sys_path(added_sys_path)

        if failures and strict:
            raise AssetDiscoveryError(failures)

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
        ]
        if not any(d.exists() for d in search_dirs):
            # Fallback for minimal projects without an assets directory.
            search_dirs = [self.root_path]

        for search_dir in search_dirs:
            if not search_dir.exists():
                continue

            for py_file in self._walk_python_files(search_dir):
                if not self._should_skip(py_file):
                    files.append(py_file)

        # Deduplicate and sort
        return sorted(set(files))

    def _walk_python_files(self, search_dir: Path) -> list[Path]:
        """Walk `search_dir` and yield candidate Python files.

        This prunes skip directories to avoid expensive traversal into virtualenvs,
        caches, etc.
        """
        found: list[Path] = []
        for dirpath, dirnames, filenames in os.walk(search_dir, topdown=True):
            dirnames[:] = [d for d in dirnames if d not in self.SKIP_DIRS]

            for filename in filenames:
                if filename.endswith(".py"):
                    found.append(Path(dirpath) / filename)

        return found

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

    def _module_name_for_path(self, file_path: Path) -> str:
        """Compute the importable module name for `file_path`.

        Uses src-layout rules:
        - Files under `<root>/src` are importable relative to that directory.
        - Otherwise, files are importable relative to `<root>`.
        """
        src_root = self.root_path / "src"
        try:
            relative = file_path.relative_to(src_root)
        except ValueError:
            relative = file_path.relative_to(self.root_path)

        parts = list(relative.with_suffix("").parts)
        if parts and parts[-1] == "__init__":
            parts.pop()

        if not parts:
            msg = f"Cannot derive a module name from {file_path}"
            raise ValueError(msg)

        for part in parts:
            if not part.isidentifier():
                msg = f"Path contains a non-importable module segment: {part!r}"
                raise ValueError(msg)
        return ".".join(parts)

    def _add_sys_path(self) -> list[str]:
        """Add project import roots to sys.path and return inserted entries."""
        roots: list[Path] = [self.root_path]
        src_root = self.root_path / "src"
        if src_root.exists():
            roots.insert(0, src_root)

        added: list[str] = []
        for root in roots:
            root_str = str(root)
            if root_str not in sys.path:
                sys.path.insert(0, root_str)
                added.append(root_str)
        return added

    def _remove_sys_path(self, added: Sequence[str]) -> None:
        """Remove sys.path entries previously added by `_add_sys_path`."""
        for root_str in reversed(list(added)):
            try:
                sys.path.remove(root_str)
            except ValueError:
                continue

    def _import_module(
        self,
        file_path: Path,
        module_name: str,
        *,
        clear: bool,
        preloaded_modules: set[str],
        purged_packages: set[str],
    ) -> ImportFailure | None:
        """Import `module_name` and return an ImportFailure on error."""
        try:
            purged = False
            top_level = module_name.split(".", 1)[0]
            if top_level in sys.modules and not self._module_is_from_root(sys.modules[top_level]):
                self._purge_module_tree(top_level)
                purged_packages.add(top_level)
                purged = True

            if module_name in sys.modules:
                loaded = sys.modules[module_name]
                loaded_file = getattr(loaded, "__file__", None)
                if loaded_file is not None and Path(loaded_file).resolve() != file_path.resolve():
                    self._purge_module_tree(module_name)
                    purged_packages.add(top_level)
                    purged = True

            module = importlib.import_module(module_name)

            module_file = getattr(module, "__file__", None)
            if module_file is not None and Path(module_file).resolve() != file_path.resolve():
                msg = f"Imported {module_name!r} from {module_file}, expected {file_path}"
                raise RuntimeError(msg)

            # Only reload a module if it was present at the start of discovery
            # and we did not purge it to resolve a root-path collision.
            if (
                clear
                and not purged
                and module_name in preloaded_modules
                and top_level not in purged_packages
            ):
                importlib.reload(module)

            logger.debug("imported_file", path=str(file_path), module=module_name)
            return None
        except Exception as e:
            # Avoid leaving partially-imported modules in the cache.
            sys.modules.pop(module_name, None)

            logger.warning(
                "import_error",
                path=str(file_path),
                module=module_name,
                error=str(e),
                exc_info=True,
            )
            return ImportFailure(
                file_path=file_path,
                module_name=module_name,
                error=e,
                traceback=traceback.format_exc(),
            )

    def _module_is_from_root(self, module: object) -> bool:
        """Return true if a module appears to come from the current discovery root."""
        file = getattr(module, "__file__", None)
        if file is not None:
            file_path = Path(file).resolve()
            return file_path.is_relative_to(self.root_path.resolve()) or file_path.is_relative_to(
                (self.root_path / "src").resolve()
            )

        search_locations = getattr(module, "__path__", None)
        if search_locations is None:
            return False

        root = self.root_path.resolve()
        src_root = (self.root_path / "src").resolve()
        for location in search_locations:
            try:
                location_path = Path(location).resolve()
            except TypeError:
                continue
            if location_path.is_relative_to(root) or location_path.is_relative_to(src_root):
                return True
        return False

    def _purge_module_tree(self, module_name: str) -> None:
        """Remove a module (and its top-level package) from sys.modules.

        Discovery often runs repeatedly in the same process (e.g., pytest).
        When two different roots contain the same package/module names (like
        `assets.*`), Python's import cache can point at the wrong root. This
        method clears the cached modules so we can import from the current root.
        """
        top_level = module_name.split(".", 1)[0]
        to_remove = [
            name for name in sys.modules if name == top_level or name.startswith(f"{top_level}.")
        ]
        for name in to_remove:
            sys.modules.pop(name, None)
