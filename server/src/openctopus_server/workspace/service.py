from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Annotated
from uuid import UUID

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.fs import (
    DirectoryEntry,
    DirectoryPage,
    FileMetadata,
    WorkspaceFS,
    WorkspaceTarget,
    get_workspace_fs,
)
from openctopus_server.workspace.resolver import ResolvedWorkspacePath, WorkspacePathResolver
from openctopus_server.workspace.storage import ObjectStream, StoredObject

REST_UPLOAD_MAX_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class DownloadTicket:
    target: WorkspaceTarget
    relative_path: str


class WorkspaceService:
    """Authorized virtual-path façade for REST handlers and agent tools."""

    def __init__(self, workspace_fs: WorkspaceFS) -> None:
        self._fs = workspace_fs
        self._resolver = WorkspacePathResolver()

    async def stat(self, db: AsyncSession, *, user_id: UUID, path: str) -> FileMetadata:
        await self._preflight(db, user_id=user_id, path=path)
        async with self._fs.file_operation_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
            return await self._fs.stat(resolved.target, resolved.relative_path)

    async def read(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        offset: int = 0,
        length: int = 0,
    ) -> bytes:
        await self._preflight(db, user_id=user_id, path=path)
        async with self._fs.materialization_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
            return await self._fs.read(
                resolved.target,
                resolved.relative_path,
                offset=offset,
                length=length,
            )

    async def read_with_metadata(
        self, db: AsyncSession, *, user_id: UUID, path: str
    ) -> StoredObject:
        await self._preflight(db, user_id=user_id, path=path)
        async with self._fs.materialization_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
            return await self._fs.read_with_metadata(resolved.target, resolved.relative_path)

    async def list_dir(self, db: AsyncSession, *, user_id: UUID, path: str) -> list[DirectoryEntry]:
        resolved = await self._preflight(db, user_id=user_id, path=path)
        async with self._fs.file_operation_slot():
            return await self._fs.list_dir(resolved.target, resolved.relative_path)

    async def usage(self, db: AsyncSession, *, user_id: UUID, path: str = "") -> int:
        resolved = await self._preflight(db, user_id=user_id, path=path)
        return await self._fs.usage(resolved.target)

    async def authorized_usage(self, target: WorkspaceTarget) -> int:
        return await self._fs.usage(target)

    async def authorize_download(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
    ) -> DownloadTicket:
        resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
        return DownloadTicket(
            target=resolved.target,
            relative_path=resolved.relative_path,
        )

    async def open_download(self, ticket: DownloadTicket) -> ObjectStream:
        return await self._fs.open_stream(ticket.target, ticket.relative_path)

    async def list_authorized(self, ticket: DownloadTicket) -> list[DirectoryEntry]:
        async with self._fs.file_operation_slot():
            return await self._fs.list_dir(ticket.target, ticket.relative_path)

    async def list_authorized_page(
        self,
        ticket: DownloadTicket,
        *,
        limit: int,
        offset: int,
    ) -> DirectoryPage:
        async with self._fs.file_operation_slot():
            return await self._fs.list_dir_page(
                ticket.target,
                ticket.relative_path,
                limit=limit,
                offset=offset,
            )

    async def upload_limit(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
    ) -> int:
        resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
        return min(REST_UPLOAD_MAX_BYTES, resolved.quota_bytes * 4 // 5)

    def collect_upload(
        self,
        chunks: AsyncIterator[bytes],
        *,
        max_bytes: int,
    ) -> AbstractAsyncContextManager[bytes]:
        return self._fs.collect_upload(chunks, max_bytes=max_bytes)

    async def write(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        data: bytes,
        if_match: str | None = None,
        if_none_match: bool = False,
    ) -> FileMetadata:
        await self._preflight(db, user_id=user_id, path=path)
        async with self._fs.materialization_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
            return await self._fs.write_collected_upload(
                resolved.target,
                resolved.relative_path,
                data,
                quota_bytes=resolved.quota_bytes,
                if_match=if_match,
                if_none_match=if_none_match,
            )

    async def write_collected_upload(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        data: bytes,
        if_match: str | None = None,
        if_none_match: bool = False,
    ) -> FileMetadata:
        resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
        return await self._fs.write_collected_upload(
            resolved.target,
            resolved.relative_path,
            data,
            quota_bytes=resolved.quota_bytes,
            if_match=if_match,
            if_none_match=if_none_match,
        )

    async def edit(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        transform: Callable[[bytes], bytes],
        if_match: str | None = None,
    ) -> FileMetadata:
        await self._preflight(db, user_id=user_id, path=path)
        async with self._fs.materialization_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
            return await self._fs.edit_materialized(
                resolved.target,
                resolved.relative_path,
                transform,
                quota_bytes=resolved.quota_bytes,
                if_match=if_match,
            )

    async def edit_text(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        old_text: str,
        new_text: str,
        occurrence: int | None,
        if_match: str | None = None,
    ) -> tuple[FileMetadata, int]:
        replacements = 0

        def transform(data: bytes) -> bytes:
            nonlocal replacements
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_INVALID_REQUEST,
                    "Workspace file is not UTF-8 text",
                ) from exc
            updated, replacements = _replace_exact(
                text,
                old_text=old_text,
                new_text=new_text,
                occurrence=occurrence,
            )
            return updated.encode("utf-8")

        metadata = await self.edit(
            db,
            user_id=user_id,
            path=path,
            transform=transform,
            if_match=if_match,
        )
        return metadata, replacements

    async def delete_file(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        if_match: str | None = None,
    ) -> None:
        await self._preflight(db, user_id=user_id, path=path)
        async with self._fs.file_operation_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
            await self._fs.delete_file(
                resolved.target,
                resolved.relative_path,
                if_match=if_match,
            )

    async def delete_folder(self, db: AsyncSession, *, user_id: UUID, path: str) -> None:
        await self._preflight(db, user_id=user_id, path=path)
        async with self._fs.file_operation_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
            await self._fs.delete_folder(resolved.target, resolved.relative_path)

    async def _preflight(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
    ) -> ResolvedWorkspacePath:
        resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
        await db.commit()
        return resolved


def get_workspace_service(
    workspace_fs: Annotated[WorkspaceFS, Depends(get_workspace_fs)],
) -> WorkspaceService:
    return WorkspaceService(workspace_fs)


def _replace_exact(
    text: str,
    *,
    old_text: str,
    new_text: str,
    occurrence: int | None,
) -> tuple[str, int]:
    if not old_text:
        raise WorkspaceError(
            ErrorCode.WORKSPACE_INVALID_REQUEST,
            "old_text must not be empty when editing an existing file",
        )
    count = text.count(old_text)
    if occurrence is None:
        if count == 0:
            raise WorkspaceError(ErrorCode.TOOL_NO_MATCH, "Text to replace was not found")
        if count > 1:
            raise WorkspaceError(
                ErrorCode.TOOL_AMBIGUOUS_EDIT,
                "Text to replace appears more than once",
            )
        return text.replace(old_text, new_text, 1), 1
    if count < occurrence:
        raise WorkspaceError(ErrorCode.TOOL_NO_MATCH, "Requested occurrence was not found")
    start = -1
    for _ in range(occurrence):
        start = text.find(old_text, start + 1)
    return f"{text[:start]}{new_text}{text[start + len(old_text) :]}", 1
