from __future__ import annotations

import ntpath
import os
import stat
from pathlib import Path

from openoctopus_client.tools.common import ToolFailure


class WorkspacePaths:
    def __init__(self, workspace: Path, *, restrict_to_workspace: bool) -> None:
        # Resolve only after checking the configured root itself and every
        # existing parent.  Resolving first would silently accept a symlinked
        # workspace root and make the restriction boundary point somewhere else.
        self._check_existing_components(workspace)
        if _is_reparse_or_symlink(workspace):
            raise ToolFailure("workspace_symlink_escape", "Workspace root must not be a link")
        self._root = workspace.resolve(strict=True)
        self._restrict_to_workspace = restrict_to_workspace

    @property
    def root(self) -> Path:
        return self._root

    def canonical(self, path: Path) -> str:
        """Return a reusable tool path without exposing the local home directory."""

        home = Path.home().resolve(strict=False)
        try:
            relative = path.relative_to(home)
        except ValueError:
            return str(path)
        suffix = relative.as_posix()
        return "~" if not suffix or suffix == "." else f"~/{suffix}"

    def resolve(self, value: str, *, directory: bool | None = None) -> Path:
        supplied = _expand_current_user_home(value)
        if (
            not value
            or len(value) > 4096
            or "\x00" in value
            or (os.name == "nt" and _is_ambiguous_windows_path(value))
            or (os.name != "nt" and _is_windows_path_on_posix(value))
        ):
            raise ToolFailure("tool_path_outside_workspace", "Path is invalid")
        candidate = supplied if supplied.is_absolute() else self._root / supplied
        candidate = Path(os.path.normpath(candidate))
        self._check_existing_components(candidate)
        resolved_parent, tail = self._closest_existing_parent(candidate)
        resolved = resolved_parent.joinpath(*tail)
        if self._restrict_to_workspace and not _is_within(resolved, self._root):
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
        if self._restrict_to_workspace and not _is_within(
            path.parent.resolve(strict=True), self._root
        ):
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


def _expand_current_user_home(value: str) -> Path:
    if value == "~":
        return Path.home()
    if value.startswith(("~/", "~\\")):
        return Path.home() / value[2:].lstrip("/\\")
    return Path(value)


def _is_ambiguous_windows_path(value: str) -> bool:
    drive, _ = ntpath.splitdrive(value)
    return (bool(drive) and not ntpath.isabs(value)) or (
        not drive and value.startswith(("/", "\\"))
    )


def _is_windows_path_on_posix(value: str) -> bool:
    drive, _ = ntpath.splitdrive(value)
    return bool(drive) and not value.startswith("/")


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
