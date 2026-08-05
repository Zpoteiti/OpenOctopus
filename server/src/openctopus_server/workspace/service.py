from __future__ import annotations

from collections.abc import Callable
from typing import Annotated
from uuid import UUID

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.workspace.fs import (
    DirectoryEntry,
    FileMetadata,
    WorkspaceFS,
    get_workspace_fs,
)
from openctopus_server.workspace.resolver import WorkspacePathResolver
from openctopus_server.workspace.storage import StoredObject


class WorkspaceService:
    """Authorized virtual-path façade for REST handlers and agent tools."""

    def __init__(self, workspace_fs: WorkspaceFS) -> None:
        self._fs = workspace_fs
        self._resolver = WorkspacePathResolver()

    async def stat(self, db: AsyncSession, *, user_id: UUID, path: str) -> FileMetadata:
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
        resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
        return await self._fs.read_with_metadata(resolved.target, resolved.relative_path)

    async def list_dir(self, db: AsyncSession, *, user_id: UUID, path: str) -> list[DirectoryEntry]:
        resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
        return await self._fs.list_dir(resolved.target, resolved.relative_path)

    async def usage(self, db: AsyncSession, *, user_id: UUID, path: str = "") -> int:
        resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
        return await self._fs.usage(resolved.target)

    async def write(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        data: bytes,
        if_match: str | None = None,
    ) -> FileMetadata:
        resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
        return await self._fs.write(
            resolved.target,
            resolved.relative_path,
            data,
            quota_bytes=resolved.quota_bytes,
            if_match=if_match,
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
        resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
        return await self._fs.edit(
            resolved.target,
            resolved.relative_path,
            transform,
            quota_bytes=resolved.quota_bytes,
            if_match=if_match,
        )

    async def delete_file(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        if_match: str | None = None,
    ) -> None:
        resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
        await self._fs.delete_file(
            resolved.target,
            resolved.relative_path,
            if_match=if_match,
        )

    async def delete_folder(self, db: AsyncSession, *, user_id: UUID, path: str) -> None:
        resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
        await self._fs.delete_folder(resolved.target, resolved.relative_path)


def get_workspace_service(
    workspace_fs: Annotated[WorkspaceFS, Depends(get_workspace_fs)],
) -> WorkspaceService:
    return WorkspaceService(workspace_fs)
