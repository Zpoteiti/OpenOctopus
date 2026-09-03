from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.chat.attachments import expand_server_workspace_attachments
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.fs import (
    FileMetadata,
    UploadCommittedAfterCancellation,
)
from openctopus_server.workspace.service import WorkspaceService
from openctopus_server.workspace.storage import STREAM_CHUNK_SIZE

from .types import ChannelEvent, ExternalAttachmentDescriptor

MAX_OWNER_ATTACHMENTS = 10
MAX_OWNER_ATTACHMENT_BYTES = 64 * 1024 * 1024
OWNER_ATTACHMENT_FAILURE_NOTE = "Some attachments from the owner were not accepted."


class AuthenticatedAttachmentStream(Protocol):
    """Adapter-owned authenticated byte stream with no public URL surface."""

    size: int

    async def read(self, max_bytes: int) -> bytes: ...

    async def aclose(self) -> None: ...


class AuthenticatedAttachmentOpener(Protocol):
    async def __call__(
        self,
        user_id: UUID,
        event: ChannelEvent,
        attachment: ExternalAttachmentDescriptor,
    ) -> AuthenticatedAttachmentStream: ...


class OwnerAttachmentCleanup(Protocol):
    async def __call__(self) -> None: ...


@dataclass(frozen=True, slots=True)
class OwnerAttachmentResolution:
    content: tuple[dict[str, object], ...]
    attachment_refs: tuple[dict[str, object], ...]
    failed_count: int
    cleanup_unpublished: OwnerAttachmentCleanup | None = None


class OwnerAttachmentResolutionResolver(Protocol):
    async def __call__(
        self,
        *,
        user_id: UUID,
        event: ChannelEvent,
        message_id: UUID,
    ) -> OwnerAttachmentResolution: ...


@dataclass(frozen=True, slots=True)
class _ServerAttachmentRef:
    path: str
    openoctopus_device: str = "server"

    def persisted(self) -> dict[str, object]:
        return {
            "openoctopus_device": self.openoctopus_device,
            "path": self.path,
        }


@dataclass(frozen=True, slots=True)
class _CreatedAttachment:
    path: str
    etag: str


class _CreatedAttachmentCleanup:
    def __init__(
        self,
        *,
        workspace_service: WorkspaceService,
        user_id: UUID,
        attachments: tuple[_CreatedAttachment, ...],
    ) -> None:
        self._workspace_service = workspace_service
        self._user_id = user_id
        self._pending = list(attachments)
        self._lock = asyncio.Lock()

    async def __call__(self) -> None:
        async with self._lock:
            for attachment in tuple(self._pending):
                try:
                    await self._workspace_service.delete_channel_attachment(
                        user_id=self._user_id,
                        path=attachment.path,
                        if_match=attachment.etag,
                    )
                except WorkspaceError as exc:
                    if exc.code not in {
                        ErrorCode.WORKSPACE_NOT_FOUND,
                        ErrorCode.WORKSPACE_FILE_CHANGED,
                    }:
                        raise
                self._pending.remove(attachment)


class OwnerAttachmentResolver:
    def __init__(
        self,
        engine: AsyncEngine,
        *,
        workspace_service: WorkspaceService,
        open_authenticated: AuthenticatedAttachmentOpener,
    ) -> None:
        self._engine = engine
        self._workspace_service = workspace_service
        self._open_authenticated = open_authenticated

    async def __call__(
        self,
        *,
        user_id: UUID,
        event: ChannelEvent,
        message_id: UUID,
    ) -> OwnerAttachmentResolution:
        attachments = event.attachments
        if len(attachments) > MAX_OWNER_ATTACHMENTS:
            return _failure_resolution(event, failed_count=len(attachments))

        valid: list[tuple[int, ExternalAttachmentDescriptor, str]] = []
        failed_count = 0
        for index, attachment in enumerate(attachments):
            safe_filename = _safe_filename(attachment)
            if safe_filename is None:
                failed_count += 1
                continue
            valid.append((index, attachment, safe_filename))

        if sum(attachment.size or 0 for _, attachment, _ in valid) > MAX_OWNER_ATTACHMENT_BYTES:
            return _failure_resolution(event, failed_count=len(attachments))

        resolved: list[_ServerAttachmentRef] = []
        created: list[_CreatedAttachment] = []
        try:
            for index, attachment, safe_filename in valid:
                path = f".attachments/channels/{message_id}/{index}-{safe_filename}"
                try:
                    metadata = await self._write_one(
                        user_id=user_id,
                        event=event,
                        attachment=attachment,
                        path=path,
                    )
                except UploadCommittedAfterCancellation as exc:
                    if exc.metadata is not None and exc.metadata.created:
                        created.append(_CreatedAttachment(path, exc.metadata.etag))
                    raise
                except asyncio.CancelledError:
                    raise
                except Exception:
                    failed_count += 1
                else:
                    resolved.append(_ServerAttachmentRef(path=path))
                    if metadata.created:
                        created.append(_CreatedAttachment(path, metadata.etag))

            base_content = _base_content(event, failed_count=failed_count)
            if not resolved:
                return OwnerAttachmentResolution(
                    content=tuple(base_content),
                    attachment_refs=(),
                    failed_count=failed_count,
                )

            try:
                async with AsyncSession(self._engine) as db:
                    expanded = await expand_server_workspace_attachments(
                        db,
                        workspace_service=self._workspace_service,
                        user_id=user_id,
                        content=base_content,
                        attachments=tuple(resolved),
                    )
                    await db.rollback()
            except asyncio.CancelledError:
                raise
            except Exception:
                await _cleanup_cancellation_safe(self._created_cleanup(user_id, created))
                return _failure_resolution(
                    event,
                    failed_count=failed_count + len(resolved),
                )

            return OwnerAttachmentResolution(
                content=tuple(expanded),
                attachment_refs=tuple(item.persisted() for item in resolved),
                failed_count=failed_count,
                cleanup_unpublished=self._created_cleanup(user_id, created),
            )
        except BaseException:
            await _cleanup_cancellation_safe(self._created_cleanup(user_id, created))
            raise

    def _created_cleanup(
        self,
        user_id: UUID,
        created: list[_CreatedAttachment],
    ) -> OwnerAttachmentCleanup | None:
        if not created:
            return None
        return _CreatedAttachmentCleanup(
            workspace_service=self._workspace_service,
            user_id=user_id,
            attachments=tuple(created),
        )

    async def _write_one(
        self,
        *,
        user_id: UUID,
        event: ChannelEvent,
        attachment: ExternalAttachmentDescriptor,
        path: str,
    ) -> FileMetadata:
        assert attachment.size is not None

        async def chunks() -> AsyncIterator[bytes]:
            source = await self._open_authenticated(user_id, event, attachment)
            try:
                if source.size != attachment.size:
                    raise ValueError("Authenticated attachment size changed")
                transferred = 0
                while chunk := await source.read(STREAM_CHUNK_SIZE):
                    if not isinstance(chunk, bytes) or len(chunk) > STREAM_CHUNK_SIZE:
                        raise ValueError("Authenticated attachment chunk is invalid")
                    transferred += len(chunk)
                    if transferred > attachment.size:
                        raise ValueError("Authenticated attachment exceeds its metadata size")
                    yield chunk
                if transferred != attachment.size:
                    raise ValueError("Authenticated attachment is shorter than its metadata size")
            finally:
                cleanup = asyncio.create_task(source.aclose())
                try:
                    await await_future_cancellation_safe(cleanup)
                except Exception:
                    pass

        async with AsyncSession(self._engine) as db:
            return await self._workspace_service.write_bounded_stream(
                db,
                user_id=user_id,
                path=path,
                chunks=chunks(),
                expected_size=attachment.size,
                max_bytes=MAX_OWNER_ATTACHMENT_BYTES,
            )


async def _cleanup_cancellation_safe(
    cleanup: OwnerAttachmentCleanup | None,
) -> None:
    if cleanup is None:
        return
    task = asyncio.create_task(cleanup())
    await await_future_cancellation_safe(task)


def _safe_filename(attachment: ExternalAttachmentDescriptor) -> str | None:
    filename = attachment.filename
    size = attachment.size
    if (
        not isinstance(filename, str)
        or not filename
        or filename != filename.strip()
        or filename in {".", ".."}
        or filename != PurePosixPath(filename.replace("\\", "/")).name
        or "/" in filename
        or "\\" in filename
        or any(not character.isprintable() for character in filename)
        or len(filename.encode("utf-8")) > 255
    ):
        return None
    if isinstance(size, bool) or not isinstance(size, int):
        return None
    if not 0 <= size <= MAX_OWNER_ATTACHMENT_BYTES:
        return None
    if (
        not isinstance(attachment.source_id, str)
        or not attachment.source_id
        or len(attachment.source_id) > 512
        or "\x00" in attachment.source_id
    ):
        return None
    content_type = attachment.content_type
    if content_type is not None and (
        not isinstance(content_type, str)
        or not 1 <= len(content_type) <= 255
        or any(not character.isprintable() for character in content_type)
    ):
        return None
    return filename


def _base_content(
    event: ChannelEvent,
    *,
    failed_count: int,
) -> list[dict[str, object]]:
    content: list[dict[str, object]] = []
    if failed_count:
        content.append({"type": "text", "text": OWNER_ATTACHMENT_FAILURE_NOTE})
    if event.text.strip():
        content.append({"type": "text", "text": event.text})
    return content


def _failure_resolution(
    event: ChannelEvent,
    *,
    failed_count: int,
) -> OwnerAttachmentResolution:
    return OwnerAttachmentResolution(
        content=tuple(_base_content(event, failed_count=failed_count)),
        attachment_refs=(),
        failed_count=failed_count,
    )
