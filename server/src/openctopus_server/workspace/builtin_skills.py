from __future__ import annotations

import hashlib
from dataclasses import replace
from functools import lru_cache
from importlib.resources import files
from importlib.resources.abc import Traversable
from types import MappingProxyType

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.fs import DirectoryEntry, DirectoryPage, FileMetadata
from openctopus_server.workspace.search import SearchObject
from openctopus_server.workspace.skills import SkillInfo, parse_skill_manifest
from openctopus_server.workspace.storage import STREAM_CHUNK_SIZE, ObjectStream, StoredObject

BUILTIN_ROOT = "/builtin"
_SKILL_NAMES = frozenset(
    {
        "create-skill",
        "connect-mcp",
        "pair-client",
        "connect-channel",
        "manage-cron",
        "manage-heartbeat",
    }
)


def builtin_relative_path(path: str) -> str | None:
    """Recognize only the reserved absolute Server namespace, including path aliases."""
    if not path.startswith("/"):
        return None
    parts = [part for part in path.split("/") if part not in {"", "."}]
    if not parts or parts[0] != "builtin":
        return None
    if ".." in parts or "\x00" in path:
        raise WorkspaceError(ErrorCode.WORKSPACE_BLOCKED_PATH, "Built-in path is invalid")
    return "/".join(parts[1:])


def reject_builtin_mutation(kind: str) -> None:
    if kind == "builtin":
        raise WorkspaceError(
            ErrorCode.WORKSPACE_BLOCKED_PATH,
            "Built-in skills are read-only and are updated by Server releases",
        )


class BuiltinSkillLibrary:
    """One validated immutable catalog backed by packaged SKILL.md files."""

    def __init__(self, root: Traversable) -> None:
        resources = {entry.name: entry for entry in root.iterdir()}
        if set(resources) != _SKILL_NAMES or any(
            not entry.is_dir() for entry in resources.values()
        ):
            raise ValueError("The packaged built-in skill library is incomplete")
        contents: dict[str, bytes] = {}
        skills: list[SkillInfo] = []
        for name, directory in sorted(resources.items()):
            entries = tuple(directory.iterdir())
            if len(entries) != 1 or entries[0].name != "SKILL.md" or not entries[0].is_file():
                raise ValueError(f"Built-in skill {name} must contain SKILL.md")
            path = f"skills/{name}/SKILL.md"
            content = entries[0].read_bytes()
            skill = parse_skill_manifest(path, content)
            if skill.always_on or not skill.body.strip():
                raise ValueError(f"Built-in skill {name} must have conditional instructions")
            contents[path] = content
            skills.append(replace(skill, body="", path=f"{BUILTIN_ROOT}/{path}", origin="builtin"))
        self._contents = MappingProxyType(contents)
        self.skills = tuple(skills)

    def stat(self, path: str) -> FileMetadata:
        content = self._content(path)
        return FileMetadata(size=len(content), etag=hashlib.sha256(content).hexdigest())

    def read(self, path: str, *, offset: int = 0, length: int = 0, max_bytes: int) -> bytes:
        content = self._content(path)
        size = min(length, max_bytes) if length else max_bytes
        return content[offset : offset + size]

    def read_with_metadata(self, path: str, *, max_bytes: int) -> StoredObject:
        content = self._content(path)
        return StoredObject(
            data=content[:max_bytes],
            etag=self.stat(path).etag,
            truncated=len(content) > max_bytes,
        )

    def open_stream(self, path: str) -> ObjectStream:
        return _BuiltinStream(self._content(path), self.stat(path).etag)

    def list_page(self, path: str, *, limit: int, offset: int, scan_limit: int) -> DirectoryPage:
        if path in self._contents:
            raise WorkspaceError(ErrorCode.TOOL_NOT_A_DIRECTORY, "Built-in path is not a directory")
        prefix = f"{path}/" if path else ""
        children: dict[str, DirectoryEntry] = {}
        for file_path, content in self._contents.items():
            if not file_path.startswith(prefix):
                continue
            name, separator, _ = file_path[len(prefix) :].partition("/")
            child_path = f"{prefix}{name}"
            children[child_path] = DirectoryEntry(
                path=child_path,
                is_directory=bool(separator),
                size=None if separator else len(content),
            )
        if not children:
            raise WorkspaceError(ErrorCode.WORKSPACE_NOT_FOUND, "Built-in folder was not found")
        entries = tuple(children[key] for key in sorted(children))
        scanned = entries[:scan_limit]
        has_more = len(scanned) > offset + limit
        return DirectoryPage(
            items=scanned[offset : offset + limit],
            next_offset=offset + limit if has_more else None,
            truncated=len(entries) > scan_limit,
        )

    def scan(self, path: str, *, scan_limit: int) -> tuple[tuple[SearchObject, ...], bool]:
        prefix = f"{path}/" if path else ""
        objects = tuple(
            SearchObject(path=file_path, size=len(content), content=content)
            for file_path, content in self._contents.items()
            if file_path == path or file_path.startswith(prefix)
        )
        if not objects:
            raise WorkspaceError(ErrorCode.WORKSPACE_NOT_FOUND, "Built-in path was not found")
        return objects[:scan_limit], len(objects) > scan_limit

    def _content(self, path: str) -> bytes:
        try:
            return self._contents[path]
        except KeyError as exc:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_NOT_FOUND, "Built-in file was not found"
            ) from exc


class _BuiltinStream(ObjectStream):
    def __init__(self, data: bytes, etag: str) -> None:
        self.size = len(data)
        self.etag = etag
        self._data = data
        self._cursor = 0

    async def read(self) -> bytes:
        chunk = self._data[self._cursor : self._cursor + STREAM_CHUNK_SIZE]
        self._cursor += len(chunk)
        return chunk

    async def aclose(self) -> None:
        self._cursor = self.size


@lru_cache(maxsize=1)
def get_builtin_skill_library() -> BuiltinSkillLibrary:
    return BuiltinSkillLibrary(files("openctopus_server").joinpath("assets", "builtin_skills"))


def validate_builtin_skills() -> None:
    """Fail startup before serving requests when packaged instructions are invalid."""
    get_builtin_skill_library()
