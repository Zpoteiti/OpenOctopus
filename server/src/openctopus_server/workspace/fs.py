from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import lru_cache
from pathlib import PurePosixPath
from typing import Annotated, Literal
from uuid import UUID

from fastapi import Depends

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.locks import KeyedLockManager
from openctopus_server.workspace.storage import (
    DirectoryObject,
    ObjectMetadata,
    ObjectStorage,
    ObjectStream,
    StoredObject,
    get_object_storage,
)

MAX_EDIT_BYTES = 8 * 1024 * 1024
MAX_READ_BYTES = 8 * 1024 * 1024
MAX_DIRECTORY_ENTRIES = 1000


@dataclass(frozen=True)
class FileMetadata:
    size: int
    etag: str
    created: bool = False


@dataclass(frozen=True)
class DirectoryEntry:
    path: str
    is_directory: bool
    size: int | None


@dataclass(frozen=True)
class DirectoryPage:
    items: tuple[DirectoryEntry, ...]
    next_offset: int | None
    truncated: bool


@dataclass(frozen=True)
class WorkspaceTarget:
    kind: Literal["personal", "shared"]
    id: UUID

    @classmethod
    def personal(cls, user_id: UUID) -> WorkspaceTarget:
        return cls(kind="personal", id=user_id)

    @classmethod
    def shared(cls, workspace_id: UUID) -> WorkspaceTarget:
        return cls(kind="shared", id=workspace_id)


class WorkspaceFS:
    """Quota-aware coordination for already-authorized workspace identities."""

    def __init__(
        self,
        storage: ObjectStorage,
        *,
        materialization_concurrency: int = 4,
        heavy_operation_concurrency: int = 4,
        file_operation_concurrency: int = 4,
    ) -> None:
        self._storage = storage
        self._materializations = asyncio.Semaphore(materialization_concurrency)
        self._heavy_operations = asyncio.Semaphore(heavy_operation_concurrency)
        self._file_operations = asyncio.Semaphore(file_operation_concurrency)
        self._mutation_locks = KeyedLockManager()
        self._retired_targets: set[WorkspaceTarget] = set()

    @property
    def mutation_lock_count(self) -> int:
        return self._mutation_locks.entry_count

    async def stat(self, target: WorkspaceTarget, relative_path: str) -> FileMetadata:
        metadata = await self._storage.stat(_object_key(target, relative_path))
        return FileMetadata(size=metadata.size, etag=metadata.etag)

    @asynccontextmanager
    async def materialization_slot(self) -> AsyncIterator[None]:
        async with self._materializations:
            yield

    @asynccontextmanager
    async def file_operation_slot(self) -> AsyncIterator[None]:
        async with self._file_operations:
            yield

    async def open_stream(
        self,
        target: WorkspaceTarget,
        relative_path: str,
    ) -> ObjectStream:
        self._ensure_active(target)
        return await self._storage.open_stream(_object_key(target, relative_path))

    @asynccontextmanager
    async def collect_upload(
        self,
        chunks: AsyncIterator[bytes],
        *,
        max_bytes: int,
    ) -> AsyncIterator[bytes]:
        async with self.materialization_slot():
            collected = bytearray()
            async for chunk in chunks:
                remaining = max_bytes + 1 - len(collected)
                collected.extend(chunk[:remaining])
                if len(collected) > max_bytes:
                    raise WorkspaceError(
                        ErrorCode.WORKSPACE_UPLOAD_TOO_LARGE,
                        "Workspace upload exceeds the REST upload limit",
                    )
            data = bytes(collected)
            del collected
            yield data

    async def read(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        *,
        offset: int = 0,
        length: int = 0,
        max_bytes: int = MAX_READ_BYTES,
    ) -> bytes:
        if offset < 0 or length < 0 or not 1 <= max_bytes <= MAX_READ_BYTES:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_BLOCKED_PATH,
                "Workspace byte range or limit is invalid",
            )
        read_limit = min(length, max_bytes) if length else max_bytes
        stored = await self._storage.read(
            _object_key(target, relative_path),
            max_bytes=read_limit,
            offset=offset,
            length=length,
        )
        return stored.data

    async def read_with_metadata(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        *,
        max_bytes: int = MAX_READ_BYTES,
    ) -> StoredObject:
        if not 1 <= max_bytes <= MAX_READ_BYTES:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_BLOCKED_PATH,
                "Workspace byte limit is invalid",
            )
        return await self._storage.read(
            _object_key(target, relative_path),
            max_bytes=max_bytes,
        )

    async def list_dir(
        self,
        target: WorkspaceTarget,
        relative_path: str = "",
    ) -> list[DirectoryEntry]:
        page = await self.list_dir_page(
            target,
            relative_path,
            limit=MAX_DIRECTORY_ENTRIES,
            offset=0,
        )
        if page.next_offset is not None or page.truncated:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_DIRECTORY_TOO_LARGE,
                "Workspace directory has too many entries to list",
            )
        return sorted(page.items, key=lambda entry: entry.path)

    async def list_dir_page(
        self,
        target: WorkspaceTarget,
        relative_path: str = "",
        *,
        limit: int,
        offset: int,
        scan_limit: int = 10_000,
    ) -> DirectoryPage:
        workspace_prefix = _workspace_prefix(target)
        if relative_path:
            object_name = _object_key(target, relative_path)
            directory_prefix = f"{object_name}/"
        else:
            directory_prefix = workspace_prefix

        seen: set[str] = set()
        items: list[DirectoryEntry] = []
        matched = 0
        scanned = 0
        start_after: str | None = None
        truncated = False
        while True:
            page = await self._storage.list_directory_page(
                directory_prefix,
                start_after=start_after,
            )
            for item in page.items:
                if scanned == scan_limit:
                    truncated = True
                    break
                scanned += 1
                if not item.object_name.startswith(directory_prefix):
                    continue
                remainder = item.object_name.removeprefix(directory_prefix)
                if not remainder:
                    continue
                name, separator, _ = remainder.rstrip("/").partition("/")
                public_path = f"{relative_path.rstrip('/')}/{name}" if relative_path else name
                if public_path in seen:
                    continue
                seen.add(public_path)
                if item.is_directory or separator:
                    entry = DirectoryEntry(
                        path=public_path,
                        is_directory=True,
                        size=None,
                    )
                else:
                    entry = DirectoryEntry(
                        path=public_path,
                        is_directory=False,
                        size=item.size,
                    )
                if matched >= offset:
                    items.append(entry)
                    if len(items) > limit:
                        return DirectoryPage(
                            items=tuple(items[:limit]),
                            next_offset=offset + limit,
                            truncated=False,
                        )
                matched += 1
            if truncated or page.next_start_after is None:
                break
            if scanned == scan_limit:
                truncated = True
                break
            start_after = page.next_start_after

        if relative_path and not seen:
            try:
                await self._storage.stat(object_name)
            except WorkspaceError as exc:
                if exc.code is not ErrorCode.WORKSPACE_NOT_FOUND:
                    raise
            else:
                raise WorkspaceError(
                    ErrorCode.TOOL_NOT_A_DIRECTORY,
                    "Workspace path is not a directory",
                )
            raise WorkspaceError(
                ErrorCode.WORKSPACE_NOT_FOUND,
                "Workspace folder was not found",
            )
        return DirectoryPage(
            items=tuple(items),
            next_offset=None,
            truncated=truncated,
        )

    async def usage(self, target: WorkspaceTarget) -> int:
        async with self._heavy_operations:
            usage = 0
            async for objects in _metadata_pages(self._storage, _workspace_prefix(target)):
                usage += sum(item.size for item in objects)
            return usage

    async def write(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        data: bytes,
        *,
        quota_bytes: int,
        if_match: str | None = None,
        if_none_match: bool = False,
    ) -> FileMetadata:
        async with self.materialization_slot():
            return await self._write(
                target,
                relative_path,
                data,
                quota_bytes=quota_bytes,
                if_match=if_match,
                if_none_match=if_none_match,
            )

    async def write_collected_upload(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        data: bytes,
        *,
        quota_bytes: int,
        if_match: str | None = None,
        if_none_match: bool = False,
    ) -> FileMetadata:
        return await self._write(
            target,
            relative_path,
            data,
            quota_bytes=quota_bytes,
            if_match=if_match,
            if_none_match=if_none_match,
        )

    async def _write(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        data: bytes,
        *,
        quota_bytes: int,
        if_match: str | None,
        if_none_match: bool,
    ) -> FileMetadata:
        object_name = _object_key(target, relative_path)
        async with self._mutation_locks.hold(target):
            self._ensure_active(target)
            return await self._write_locked(
                target,
                object_name,
                data,
                quota_bytes=quota_bytes,
                if_match=if_match,
                if_none_match=if_none_match,
                single_operation_bytes=len(data),
            )

    async def edit(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        transform: Callable[[bytes], bytes],
        *,
        quota_bytes: int,
        if_match: str | None = None,
    ) -> FileMetadata:
        async with self.materialization_slot():
            return await self.edit_materialized(
                target,
                relative_path,
                transform,
                quota_bytes=quota_bytes,
                if_match=if_match,
            )

    async def edit_materialized(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        transform: Callable[[bytes], bytes],
        *,
        quota_bytes: int,
        if_match: str | None = None,
    ) -> FileMetadata:
        object_name = _object_key(target, relative_path)
        async with self._mutation_locks.hold(target):
            self._ensure_active(target)
            metadata = await self._storage.stat(object_name)
            if if_match is not None and metadata.etag != if_match:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_FILE_CHANGED,
                    "Workspace file changed after it was read",
                )
            if metadata.size > MAX_EDIT_BYTES:
                raise _too_large_to_edit()
            current = await self._storage.read(
                object_name,
                max_bytes=MAX_EDIT_BYTES,
            )
            if current.truncated:
                raise _too_large_to_edit()
            updated = await _run_transform(transform, current.data)
            if len(updated) > MAX_EDIT_BYTES:
                raise _too_large_to_edit()
            return await self._write_locked(
                target,
                object_name,
                updated,
                quota_bytes=quota_bytes,
                if_match=if_match,
                if_none_match=False,
                single_operation_bytes=max(0, len(updated) - metadata.size),
            )

    async def _write_locked(
        self,
        target: WorkspaceTarget,
        object_name: str,
        data: bytes,
        *,
        quota_bytes: int,
        if_match: str | None,
        if_none_match: bool,
        single_operation_bytes: int,
    ) -> FileMetadata:
        prefix = _workspace_prefix(target)
        usage = 0
        existing = None
        parent_names = set(_parent_object_names(prefix, object_name))
        parent_is_file = False
        folder_prefix = f"{object_name}/"
        target_is_directory = False
        async for objects in _metadata_pages(self._storage, prefix):
            for item in objects:
                usage += item.size
                if item.object_name == object_name:
                    existing = item
                elif item.object_name in parent_names:
                    parent_is_file = True
                elif item.object_name.startswith(folder_prefix):
                    target_is_directory = True

        if parent_is_file:
            raise WorkspaceError(
                ErrorCode.TOOL_NOT_A_DIRECTORY,
                "A workspace path parent is a file",
            )
        if target_is_directory:
            raise WorkspaceError(
                ErrorCode.TOOL_IS_DIRECTORY,
                "Workspace path is a directory",
            )

        if usage > quota_bytes:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_SOFT_LOCKED,
                "Workspace is over quota; delete files before writing",
            )
        if single_operation_bytes * 5 > quota_bytes * 4:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_UPLOAD_TOO_LARGE,
                "Workspace operation exceeds the single-operation size limit",
            )
        if if_match is not None and (existing is None or existing.etag != if_match):
            raise WorkspaceError(
                ErrorCode.WORKSPACE_FILE_CHANGED,
                "Workspace file changed after it was read",
            )
        if if_none_match and existing is not None:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_FILE_CHANGED,
                "Workspace file already exists",
            )

        replaced_size = existing.size if existing is not None else 0
        if usage - replaced_size + len(data) > quota_bytes:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_QUOTA_EXCEEDED,
                "Workspace quota would be exceeded",
            )

        uploaded = await self._storage.write(object_name, data)
        return FileMetadata(
            size=len(data),
            etag=uploaded.etag,
            created=existing is None,
        )

    async def delete_file(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        *,
        if_match: str | None = None,
    ) -> None:
        object_name = _object_key(target, relative_path)
        async with self._mutation_locks.hold(target):
            self._ensure_active(target)
            try:
                existing = await self._storage.stat(object_name)
            except WorkspaceError as exc:
                if exc.code is not ErrorCode.WORKSPACE_NOT_FOUND:
                    raise
                folder = await self._storage.list_page(f"{object_name}/", limit=1)
                if folder.items:
                    raise WorkspaceError(
                        ErrorCode.TOOL_IS_DIRECTORY,
                        "Workspace path is a directory",
                    )
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_NOT_FOUND,
                    "Workspace file was not found",
                ) from exc
            if if_match is not None and existing.etag != if_match:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_FILE_CHANGED,
                    "Workspace file changed after it was read",
                )
            await self._storage.delete(object_name)

    async def delete_folder(self, target: WorkspaceTarget, relative_path: str) -> None:
        object_name = _object_key(target, relative_path)
        folder_prefix = f"{object_name}/"
        async with self._mutation_locks.hold(target):
            self._ensure_active(target)
            try:
                await self._storage.stat(object_name)
            except WorkspaceError as exc:
                if exc.code is not ErrorCode.WORKSPACE_NOT_FOUND:
                    raise
            else:
                raise WorkspaceError(
                    ErrorCode.TOOL_IS_FILE,
                    "Workspace path is a file",
                )
            if not await _delete_prefix(self._storage, folder_prefix):
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_NOT_FOUND,
                    "Workspace folder was not found",
                )

    async def purge_workspace(self, target: WorkspaceTarget) -> None:
        """Idempotently remove every object for an already-authorized lifecycle event."""
        async with self._mutation_locks.hold(target):
            self._retired_targets.add(target)
            await _delete_prefix(self._storage, _workspace_prefix(target))

    async def retire_workspace(self, target: WorkspaceTarget) -> None:
        """Fence mutations before committing durable lifecycle deletion state."""
        async with self._mutation_locks.hold(target):
            self._retired_targets.add(target)

    async def reactivate_workspace(self, target: WorkspaceTarget) -> None:
        """Undo retirement when the corresponding database deletion rolls back."""
        async with self._mutation_locks.hold(target):
            self._retired_targets.discard(target)

    async def forget_workspace(self, target: WorkspaceTarget) -> None:
        """Release a completed deletion tombstone after its durable job commits."""
        async with self._mutation_locks.hold(target):
            self._retired_targets.discard(target)

    def _ensure_active(self, target: WorkspaceTarget) -> None:
        if target in self._retired_targets:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_NOT_FOUND,
                "Workspace was not found",
            )


async def _run_transform(transform: Callable[[bytes], bytes], current: bytes) -> bytes:
    worker = asyncio.create_task(asyncio.to_thread(transform, current))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        try:
            await worker
        except Exception:
            pass
        raise


async def _metadata_pages(
    storage: ObjectStorage,
    prefix: str,
) -> AsyncIterator[tuple[ObjectMetadata, ...]]:
    start_after: str | None = None
    while True:
        page = await storage.list_page(prefix, start_after=start_after)
        yield page.items
        if page.next_start_after is None:
            return
        start_after = page.next_start_after


async def _directory_pages(
    storage: ObjectStorage,
    prefix: str,
) -> AsyncIterator[tuple[DirectoryObject, ...]]:
    start_after: str | None = None
    while True:
        page = await storage.list_directory_page(prefix, start_after=start_after)
        yield page.items
        if page.next_start_after is None:
            return
        start_after = page.next_start_after


async def _delete_prefix(storage: ObjectStorage, prefix: str) -> bool:
    deleted = False
    start_after: str | None = None
    while True:
        page = await storage.list_page(prefix, start_after=start_after)
        for item in page.items:
            await storage.delete(item.object_name)
            deleted = True
        if page.next_start_after is None:
            return deleted
        start_after = page.next_start_after


def _workspace_prefix(target: WorkspaceTarget) -> str:
    collection = "users" if target.kind == "personal" else "workspaces"
    return f"{collection}/{target.id}/"


def _object_key(target: WorkspaceTarget, relative_path: str) -> str:
    path = PurePosixPath(relative_path)
    if (
        not relative_path
        or relative_path.startswith("/")
        or "\x00" in relative_path
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise WorkspaceError(
            ErrorCode.WORKSPACE_BLOCKED_PATH,
            "Workspace path is invalid",
        )
    return f"{_workspace_prefix(target)}{path.as_posix()}"


def _parent_object_names(prefix: str, object_name: str) -> list[str]:
    relative_parts = object_name.removeprefix(prefix).split("/")
    return [
        f"{prefix}{'/'.join(relative_parts[:index])}" for index in range(1, len(relative_parts))
    ]


def _too_large_to_edit() -> WorkspaceError:
    return WorkspaceError(
        ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT,
        "Workspace file exceeds the 8 MiB server edit limit",
    )


@lru_cache
def _workspace_fs_for_storage(storage: ObjectStorage) -> WorkspaceFS:
    return WorkspaceFS(storage)


def get_workspace_fs(
    storage: Annotated[ObjectStorage, Depends(get_object_storage)],
) -> WorkspaceFS:
    return _workspace_fs_for_storage(storage)
