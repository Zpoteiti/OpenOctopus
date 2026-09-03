import asyncio
import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.channels.attachments import (
    MAX_OWNER_ATTACHMENT_BYTES,
    OWNER_ATTACHMENT_FAILURE_NOTE,
    AuthenticatedAttachmentStream,
    OwnerAttachmentResolver,
)
from openctopus_server.channels.types import (
    ChannelEvent,
    ExternalAttachmentDescriptor,
)
from openctopus_server.db.models import User
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.fs import (
    FileMetadata,
    UploadCommittedAfterCancellation,
    WorkspaceFS,
    WorkspaceTarget,
)
from openctopus_server.workspace.service import UploadTicket, WorkspaceService
from openctopus_server.workspace.storage import STREAM_CHUNK_SIZE, ObjectMetadata


class _Source:
    def __init__(self, data: bytes, *, size: int | None = None) -> None:
        self.data = data
        self.size = len(data) if size is None else size
        self.position = 0
        self.read_limits: list[int] = []
        self.closed = False

    async def read(self, max_bytes: int) -> bytes:
        self.read_limits.append(max_bytes)
        chunk = self.data[self.position : self.position + max_bytes]
        self.position += len(chunk)
        return chunk

    async def aclose(self) -> None:
        self.closed = True


class _Opener:
    def __init__(self, sources: dict[str, _Source]) -> None:
        self.sources = sources
        self.calls: list[str] = []
        self.user_ids: list[UUID] = []

    async def __call__(
        self,
        user_id: UUID,
        event: ChannelEvent,
        attachment: ExternalAttachmentDescriptor,
    ) -> AuthenticatedAttachmentStream:
        del event
        self.user_ids.append(user_id)
        self.calls.append(attachment.source_id)
        return self.sources[attachment.source_id]


class _Workspace:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}
        self.chunk_sizes: list[int] = []
        self.deleted: list[str] = []

    async def write_bounded_stream(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        chunks: AsyncIterator[bytes],
        expected_size: int,
        max_bytes: int,
    ) -> FileMetadata:
        del db, user_id
        assert expected_size <= max_bytes
        collected = bytearray()
        async for chunk in chunks:
            self.chunk_sizes.append(len(chunk))
            collected.extend(chunk)
        data = bytes(collected)
        assert len(data) == expected_size
        etag = hashlib.sha256(data).hexdigest()
        existing = self.objects.get(path)
        if existing is not None and existing != (data, etag):
            raise WorkspaceError(
                ErrorCode.WORKSPACE_FILE_CHANGED,
                "Workspace file changed",
            )
        self.objects[path] = (data, etag)
        return FileMetadata(size=len(data), etag=etag, created=existing is None)

    async def delete_channel_attachment(
        self,
        *,
        user_id: UUID,
        path: str,
        if_match: str,
    ) -> None:
        del user_id
        existing = self.objects.get(path)
        if existing is None or (if_match is not None and existing[1] != if_match):
            return
        self.deleted.append(path)
        self.objects.pop(path)


class _Upload:
    def __init__(self) -> None:
        self.object_name = "_openoctopus-transfers/0123456789abcdef0123456789abcdef"
        self.writes: list[bytes] = []
        self.write_entered = asyncio.Event()
        self.abort_calls = 0

    async def write(self, chunk: bytes) -> None:
        self.writes.append(chunk)
        self.write_entered.set()

    async def finish(self) -> ObjectMetadata:
        size = sum(len(chunk) for chunk in self.writes)
        return ObjectMetadata(self.object_name, size, "staged-etag")

    async def abort(self) -> None:
        self.abort_calls += 1


class _ReadStream:
    def __init__(self, data: bytes) -> None:
        self.size = len(data)
        self._data = data
        self._read = False
        self.closed = False

    async def read(self) -> bytes:
        if self._read:
            return b""
        self._read = True
        return self._data

    async def aclose(self) -> None:
        self.closed = True


def _event(
    attachments: tuple[ExternalAttachmentDescriptor, ...],
    *,
    text: str = "Please inspect",
) -> ChannelEvent:
    return ChannelEvent(
        platform="discord",
        binding_generation=uuid4(),
        runtime_generation=uuid4(),
        source_message_id="message-1",
        chat_id="chat-1",
        conversation_kind="dm",
        sender_id="owner-1",
        sender_display_name="Owner",
        sender_kind="human",
        explicitly_mentions_bot=False,
        text=text,
        attachments=attachments,
    )


async def _owner(engine: AsyncEngine) -> UUID:
    user_id = uuid4()
    async with AsyncSession(engine) as db:
        db.add(
            User(
                id=user_id,
                email=f"{user_id}@test.com",
                password_hash="hash",
                name="Owner",
                created_at=datetime.now(UTC),
            )
        )
        await db.commit()
    return user_id


async def test_resolver_streams_to_deterministic_refs_and_notes_partial_failure(
    pg_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id = await _owner(pg_engine)
    message_id = uuid4()
    data = b"x" * (STREAM_CHUNK_SIZE * 2 + 1)
    source = _Source(data)
    opener = _Opener({"valid": source})
    workspace = _Workspace()
    expanded_refs: list[str] = []

    async def expand(
        db: AsyncSession,
        *,
        workspace_service: object,
        user_id: UUID,
        content: list[dict[str, object]],
        attachments: tuple[object, ...],
    ) -> list[dict[str, object]]:
        del db, workspace_service, user_id
        expanded_refs.extend(str(getattr(item, "path")) for item in attachments)
        return [*content, {"type": "text", "text": "expanded attachment"}]

    monkeypatch.setattr(
        "openctopus_server.channels.attachments.expand_server_workspace_attachments",
        expand,
    )
    resolver = OwnerAttachmentResolver(
        pg_engine,
        workspace_service=workspace,  # type: ignore[arg-type]
        open_authenticated=opener,
    )
    event = _event(
        (
            ExternalAttachmentDescriptor(
                source_id="valid",
                filename="report.txt",
                content_type="text/plain",
                size=len(data),
            ),
            ExternalAttachmentDescriptor(
                source_id="unsafe",
                filename="../secret.txt",
                content_type="text/plain",
                size=1,
            ),
        )
    )

    result = await resolver(user_id=user_id, event=event, message_id=message_id)

    path = f".attachments/channels/{message_id}/0-report.txt"
    assert result.attachment_refs == ({"openoctopus_device": "server", "path": path},)
    assert result.failed_count == 1
    assert OWNER_ATTACHMENT_FAILURE_NOTE in str(result.content)
    assert "Please inspect" in str(result.content)
    assert expanded_refs == [path]
    assert opener.calls == ["valid"]
    assert opener.user_ids == [user_id]
    assert source.closed is True
    assert source.read_limits and set(source.read_limits) == {STREAM_CHUNK_SIZE}
    assert max(workspace.chunk_sizes) <= STREAM_CHUNK_SIZE


async def test_resolver_rejects_count_and_aggregate_limits_before_download(
    pg_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id = await _owner(pg_engine)
    opener = _Opener({})
    resolver = OwnerAttachmentResolver(
        pg_engine,
        workspace_service=_Workspace(),  # type: ignore[arg-type]
        open_authenticated=opener,
    )
    too_many = tuple(
        ExternalAttachmentDescriptor(
            source_id=str(index),
            filename=f"{index}.txt",
            content_type="text/plain",
            size=1,
        )
        for index in range(11)
    )

    count_result = await resolver(
        user_id=user_id,
        event=_event(too_many),
        message_id=uuid4(),
    )
    aggregate_result = await resolver(
        user_id=user_id,
        event=_event(
            (
                ExternalAttachmentDescriptor(
                    source_id="a",
                    filename="a.bin",
                    content_type="application/octet-stream",
                    size=MAX_OWNER_ATTACHMENT_BYTES // 2 + 1,
                ),
                ExternalAttachmentDescriptor(
                    source_id="b",
                    filename="b.bin",
                    content_type="application/octet-stream",
                    size=MAX_OWNER_ATTACHMENT_BYTES // 2 + 1,
                ),
            )
        ),
        message_id=uuid4(),
    )

    assert count_result.failed_count == 11
    assert aggregate_result.failed_count == 2
    assert count_result.attachment_refs == aggregate_result.attachment_refs == ()
    assert opener.calls == []


async def test_unpublished_cleanup_deletes_only_objects_created_by_this_resolution(
    pg_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id = await _owner(pg_engine)
    message_id = uuid4()
    reused_path = f".attachments/channels/{message_id}/0-existing.txt"
    created_path = f".attachments/channels/{message_id}/1-new.txt"
    workspace = _Workspace()
    reused_data = b"existing"
    workspace.objects[reused_path] = (
        reused_data,
        hashlib.sha256(reused_data).hexdigest(),
    )

    async def expand(
        db: AsyncSession,
        *,
        workspace_service: object,
        user_id: UUID,
        content: list[dict[str, object]],
        attachments: tuple[object, ...],
    ) -> list[dict[str, object]]:
        del db, workspace_service, user_id, attachments
        return content

    monkeypatch.setattr(
        "openctopus_server.channels.attachments.expand_server_workspace_attachments",
        expand,
    )
    resolver = OwnerAttachmentResolver(
        pg_engine,
        workspace_service=workspace,  # type: ignore[arg-type]
        open_authenticated=_Opener(
            {
                "existing": _Source(reused_data),
                "new": _Source(b"new"),
            }
        ),
    )
    result = await resolver(
        user_id=user_id,
        message_id=message_id,
        event=_event(
            (
                ExternalAttachmentDescriptor(
                    source_id="existing",
                    filename="existing.txt",
                    content_type="text/plain",
                    size=len(reused_data),
                ),
                ExternalAttachmentDescriptor(
                    source_id="new",
                    filename="new.txt",
                    content_type="text/plain",
                    size=3,
                ),
            )
        ),
    )

    assert result.cleanup_unpublished is not None
    await result.cleanup_unpublished()
    await result.cleanup_unpublished()

    assert reused_path in workspace.objects
    assert created_path not in workspace.objects
    assert workspace.deleted == [created_path]


async def test_workspace_bounded_stream_aborts_temporary_object_on_cancellation() -> None:
    fs = AsyncMock(spec=WorkspaceFS)
    upload = _Upload()
    fs.begin_idempotent_upload.return_value = upload
    service = WorkspaceService(fs)
    user_id = uuid4()
    ticket = UploadTicket(
        target=WorkspaceTarget.personal(user_id),
        relative_path=".attachments/channels/message/0-file.bin",
        quota_bytes=MAX_OWNER_ATTACHMENT_BYTES * 2,
        max_bytes=MAX_OWNER_ATTACHMENT_BYTES,
        user_id=user_id,
        display_path=".attachments/channels/message/0-file.bin",
    )
    release = asyncio.Event()

    async def chunks() -> AsyncIterator[bytes]:
        yield b"first"
        await release.wait()

    writing = asyncio.create_task(
        service.write_authorized_bounded_stream(
            ticket,
            chunks=chunks(),
            expected_size=10,
            max_bytes=MAX_OWNER_ATTACHMENT_BYTES,
        )
    )
    await upload.write_entered.wait()
    writing.cancel()

    with pytest.raises(asyncio.CancelledError):
        await writing
    assert upload.abort_calls == 1
    fs.commit_uploaded_object.assert_not_awaited()


async def test_workspace_bounded_stream_commits_with_staged_etag_for_reuse() -> None:
    fs = AsyncMock(spec=WorkspaceFS)
    upload = _Upload()
    fs.begin_idempotent_upload.return_value = upload
    fs.commit_uploaded_object.return_value = FileMetadata(
        size=6,
        etag="staged-etag",
        created=False,
    )
    service = WorkspaceService(fs)
    user_id = uuid4()
    ticket = UploadTicket(
        target=WorkspaceTarget.personal(user_id),
        relative_path=".attachments/channels/message/0-file.bin",
        quota_bytes=100,
        max_bytes=80,
        user_id=user_id,
        display_path=".attachments/channels/message/0-file.bin",
    )

    async def chunks() -> AsyncIterator[bytes]:
        yield b"abc"
        yield b"def"

    metadata = await service.write_authorized_bounded_stream(
        ticket,
        chunks=chunks(),
        expected_size=6,
        max_bytes=10,
    )

    assert metadata.created is False
    fs.commit_uploaded_object.assert_awaited_once_with(
        ticket.target,
        ticket.relative_path,
        upload.object_name,
        size=6,
        quota_bytes=ticket.quota_bytes,
        reuse_if_same_etag="staged-etag",
    )
    assert upload.abort_calls == 0


async def test_workspace_bounded_stream_aborts_incomplete_object() -> None:
    fs = AsyncMock(spec=WorkspaceFS)
    upload = _Upload()
    fs.begin_idempotent_upload.return_value = upload
    service = WorkspaceService(fs)
    user_id = uuid4()
    ticket = UploadTicket(
        target=WorkspaceTarget.personal(user_id),
        relative_path=".attachments/channels/message/0-file.bin",
        quota_bytes=100,
        max_bytes=80,
        user_id=user_id,
        display_path=".attachments/channels/message/0-file.bin",
    )

    async def incomplete_chunks() -> AsyncIterator[bytes]:
        yield b"partial"

    with pytest.raises(WorkspaceError) as caught:
        await service.write_authorized_bounded_stream(
            ticket,
            chunks=incomplete_chunks(),
            expected_size=8,
            max_bytes=10,
        )

    assert caught.value.code is ErrorCode.WORKSPACE_UPLOAD_TOO_LARGE
    assert upload.abort_calls == 1
    fs.commit_uploaded_object.assert_not_awaited()


async def test_workspace_bounded_stream_removes_destination_committed_during_cancel() -> None:
    fs = AsyncMock(spec=WorkspaceFS)
    upload = _Upload()
    fs.begin_idempotent_upload.return_value = upload
    fs.commit_uploaded_object.side_effect = UploadCommittedAfterCancellation(
        FileMetadata(size=3, etag="committed-etag", created=True)
    )
    service = WorkspaceService(fs)
    user_id = uuid4()
    ticket = UploadTicket(
        target=WorkspaceTarget.personal(user_id),
        relative_path=".attachments/channels/message/0-file.bin",
        quota_bytes=100,
        max_bytes=80,
        user_id=user_id,
        display_path=".attachments/channels/message/0-file.bin",
    )

    async def chunks() -> AsyncIterator[bytes]:
        yield b"new"

    with pytest.raises(asyncio.CancelledError):
        await service.write_authorized_bounded_stream(
            ticket,
            chunks=chunks(),
            expected_size=3,
            max_bytes=10,
        )

    fs.delete_file.assert_awaited_once_with(
        WorkspaceTarget.personal(user_id),
        ticket.relative_path,
        if_match="committed-etag",
    )
    assert upload.abort_calls == 1


async def test_workspace_fs_reuses_only_same_size_and_etag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = AsyncMock()
    target = WorkspaceTarget.personal(uuid4())
    path = ".attachments/channels/message/0-file.bin"
    object_name = f"users/{target.id}/{path}"
    existing = ObjectMetadata(object_name, 4, "same-etag")

    async def metadata_pages(*_args: object, **_kwargs: object):
        yield (existing,)

    monkeypatch.setattr(
        "openctopus_server.workspace.fs._metadata_pages",
        metadata_pages,
    )
    fs = WorkspaceFS(storage)

    reused = await fs.commit_uploaded_object(
        target,
        path,
        "_openoctopus-transfers/0123456789abcdef0123456789abcdef",
        size=4,
        quota_bytes=100,
        reuse_if_same_etag="same-etag",
    )

    assert reused == FileMetadata(size=4, etag="same-etag", created=False)
    storage.promote_if_absent.assert_not_awaited()
    await asyncio.sleep(0)
    storage.delete.assert_awaited_once()

    temporary_same = _ReadStream(b"same")
    existing_same = _ReadStream(b"same")
    storage.open_stream.side_effect = [temporary_same, existing_same]
    copied_reuse = await fs.commit_uploaded_object(
        target,
        path,
        "_openoctopus-transfers/abcdef0123456789abcdef0123456789",
        size=4,
        quota_bytes=100,
        reuse_if_same_etag="copy-produced-a-different-etag",
    )
    assert copied_reuse.created is False
    assert temporary_same.closed and existing_same.closed

    temporary_different = _ReadStream(b"diff")
    existing_original = _ReadStream(b"same")
    storage.open_stream.side_effect = [temporary_different, existing_original]
    with pytest.raises(WorkspaceError) as caught:
        await fs.commit_uploaded_object(
            target,
            path,
            "_openoctopus-transfers/fedcba9876543210fedcba9876543210",
            size=4,
            quota_bytes=100,
            reuse_if_same_etag="different-etag",
        )
    assert caught.value.code is ErrorCode.WORKSPACE_FILE_CHANGED
    storage.promote_if_absent.assert_not_awaited()
    assert temporary_different.closed and existing_original.closed
