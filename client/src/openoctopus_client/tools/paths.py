from __future__ import annotations

import ntpath
import os
import stat
from pathlib import Path

from openoctopus_client.tools.common import ToolFailure


class WorkspacePaths:
    def __init__(self, workspace: Path, *, sandbox_mode: bool) -> None:
        # Resolve only after checking the configured root itself and every
        # existing parent.  Resolving first would silently accept a symlinked
        # workspace root and make the sandbox boundary point somewhere else.
        self._check_existing_components(workspace)
        if _is_reparse_or_symlink(workspace):
            raise ToolFailure("workspace_symlink_escape", "Workspace root must not be a link")
        self._root = workspace.resolve(strict=True)
        self._sandbox_mode = sandbox_mode

    @property
    def root(self) -> Path:
        return self._root

    def resolve(self, value: str, *, directory: bool | None = None) -> Path:
        if not value or "\x00" in value or ntpath.splitdrive(value)[0]:
            raise ToolFailure("tool_path_outside_workspace", "Path is invalid")
        supplied = Path(value)
        candidate = supplied if supplied.is_absolute() else self._root / supplied
        self._check_existing_components(candidate)
        resolved_parent, tail = self._closest_existing_parent(candidate)
        resolved = resolved_parent.joinpath(*tail)
        if self._sandbox_mode and not _is_within(resolved, self._root):
            raise ToolFailure("tool_path_outside_workspace", "Path is outside the workspace")
        if candidate.exists() or candidate.is_symlink():
            self._require_kind(candidate, directory)
        return resolved

    def prepare_parent(self, path: Path) -> None:
        parent = path.parent
        missing: list[Path] = []
        while not parent.exists():
            missing.append(parent)
            parent = parent.parent
        self._check_existing_components(parent)
        for item in reversed(missing):
            item.mkdir()
            self._check_existing_components(item)
        if self._sandbox_mode and not _is_within(path.parent.resolve(strict=True), self._root):
            raise ToolFailure("tool_path_outside_workspace", "Path is outside the workspace")

    def _closest_existing_parent(self, candidate: Path) -> tuple[Path, tuple[str, ...]]:
        tail: list[str] = []
        current = candidate
        while not current.exists() and not current.is_symlink():
            tail.append(current.name)
            if current.parent == current:
                raise ToolFailure("tool_path_outside_workspace", "Path is unavailable")
            current = current.parent
        self._check_existing_components(current)
        return current.resolve(strict=True), tuple(reversed(tail))

    def _check_existing_components(self, candidate: Path) -> None:
        current = Path(candidate.anchor) if candidate.is_absolute() else Path()
        for part in candidate.parts:
            if part in {candidate.anchor, "."}:
                continue
            current /= part
            try:
                mode = os.lstat(current).st_mode
            except FileNotFoundError:
                return
            except OSError as exc:
                raise ToolFailure(
                    "workspace_permission_denied", "Path could not be inspected"
                ) from exc
            # Transfer and tool paths share the same no-follow policy.  Trusted
            # mode allows absolute paths outside the workspace, but it does not
            # turn a symlink/reparse component into a regular-file guarantee.
            if _is_reparse_or_symlink(current, mode):
                raise ToolFailure("workspace_symlink_escape", "Path contains a symbolic link")

    @staticmethod
    def _require_kind(path: Path, directory: bool | None) -> None:
        try:
            mode = os.lstat(path).st_mode
        except OSError as exc:
            raise ToolFailure("workspace_permission_denied", "Path could not be inspected") from exc
        if directory is True and not stat.S_ISDIR(mode):
            raise ToolFailure("tool_not_a_directory", "Path is not a directory")
        if directory is False and not stat.S_ISREG(mode):
            if stat.S_ISDIR(mode):
                raise ToolFailure("tool_is_directory", "Path is a directory")
            raise ToolFailure("workspace_blocked_path", "Path is not a regular file")


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _is_reparse_or_symlink(path: Path, mode: int | None = None) -> bool:
    if mode is None:
        try:
            mode = os.lstat(path).st_mode
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise ToolFailure(
                "workspace_permission_denied", "Path could not be inspected"
            ) from exc
    if stat.S_ISLNK(mode):
        return True
    # Windows junctions and other reparse points are not necessarily reported
    # as POSIX symlinks.  ``st_file_attributes`` is absent on Unix.
    attributes = getattr(os.lstat(path), "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(attributes & reparse_flag)
