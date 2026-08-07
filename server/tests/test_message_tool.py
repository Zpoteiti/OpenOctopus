import asyncio
from contextlib import asynccontextmanager
from unittest.mock import ANY, AsyncMock, Mock, call
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import Session, User, Workspace, WorkspaceMember
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.tools.base import (
    MessageDeliveryEffect,
    ToolContext,
    WorkspaceFileDeliveryRef,
)
from openctopus_server.tools.device_field import DEVICE_FIELD_MARKER
from openctopus_server.tools.message import MessageTool
from openctopus_server.tools.registry import ToolRegistry
from openctopus_server.workspace.fs import FileMetadata, WorkspaceFS, WorkspaceTarget
from openctopus_server.workspace.service import AuthorizedWorkspaceFile, WorkspaceService


def _ctx() -> ToolContext:
    return ToolContext(user_id=uuid4(), session_id=uuid4())


async def _web_ctx(
    engine: AsyncEngine,
    *,
    user_id: UUID | None = None,
    channel: str = "web",
) -> ToolContext:
    owner_id = user_id or uuid4()
    session_id = uuid4()
    async with AsyncSession(engine, expire_on_commit=False) as db:
        db.add(
            User(
                id=owner_id,
                email=f"message-{owner_id}@test.com",
                password_hash="x",
                name="Message User",
            )
        )
        await db.flush()
        db.add(
            Session(
                id=session_id,
                user_id=owner_id,
                session_key=f"{channel}:{session_id}",
                channel=channel,
                chat_id=str(session_id),
                title="Message session",
            )
        )
        await db.commit()
    return ToolContext(user_id=owner_id, session_id=session_id)


def test_message_schema_is_restricted_to_current_web_server_delivery(pg_engine) -> None:
    tool = MessageTool(pg_engine, AsyncMock(spec=WorkspaceService))

    schema = ToolRegistry((tool,)).get_tool_schemas()[0]

    assert schema["name"] == "message"
    assert schema["input_schema"] == {
        "type": "object",
        "properties": {
            "content": {
                "type": "string",
                "description": "Message text to deliver to the current web session.",
                "minLength": 1,
                "maxLength": 16_000,
            },
            "openoctopus_device": {
                "type": "string",
                "enum": ["server"],
                "description": "Workspace install site for media paths (default server).",
                DEVICE_FIELD_MARKER: True,
                "default": "server",
            },
            "media": {
                "type": "array",
                "description": "Optional workspace files to attach.",
                "items": {"type": "string", "minLength": 1},
                "maxItems": 10,
                "uniqueItems": True,
                "default": [],
            },
        },
        "required": ["content"],
        "additionalProperties": False,
    }


async def test_message_content_only_returns_internal_success_marker(pg_engine) -> None:
    service = AsyncMock(spec=WorkspaceService)
    registry = ToolRegistry((MessageTool(pg_engine, service),))

    result = await registry.execute(
        name="message",
        args={"content": "  Done.  "},
        ctx=await _web_ctx(pg_engine),
    )

    assert result.is_error is False
    assert result.side_effect == MessageDeliveryEffect(delivery_refs=())
    assert isinstance(result.content, list)
    assert result.content[-1] == {
        "type": "text",
        "text": "Message delivered to the current web session.",
    }
    service.resolve_delivery_file.assert_not_awaited()


async def test_message_builds_trusted_refs_without_reading_media(pg_engine) -> None:
    user_id = uuid4()
    workspace_id = uuid4()
    service = AsyncMock(spec=WorkspaceService)
    service.resolve_delivery_file.side_effect = (
        AuthorizedWorkspaceFile(
            target=WorkspaceTarget.personal(user_id),
            relative_path="large.bin",
            metadata=FileMetadata(size=128 * 1024 * 1024, etag="large"),
        ),
        AuthorizedWorkspaceFile(
            target=WorkspaceTarget.shared(workspace_id),
            relative_path="reports/report.pdf",
            metadata=FileMetadata(size=42, etag="pdf"),
        ),
    )
    registry = ToolRegistry((MessageTool(pg_engine, service),))
    ctx = await _web_ctx(pg_engine, user_id=user_id)

    result = await registry.execute(
        name="message",
        args={
            "content": "Files attached.",
            "openoctopus_device": "server",
            "media": ["large.bin", "/Reports@1234abcd/reports/report.pdf"],
        },
        ctx=ctx,
    )

    assert result.side_effect == MessageDeliveryEffect(
        delivery_refs=(
            WorkspaceFileDeliveryRef(
                path="large.bin",
                workspace_id=user_id,
                workspace_relative_path="large.bin",
                filename="large.bin",
                mime="application/octet-stream",
                size=128 * 1024 * 1024,
            ),
            WorkspaceFileDeliveryRef(
                path="/Reports@1234abcd/reports/report.pdf",
                workspace_id=workspace_id,
                workspace_relative_path="reports/report.pdf",
                filename="report.pdf",
                mime="application/pdf",
                size=42,
            ),
        )
    )
    assert service.resolve_delivery_file.await_args_list == [
        call(ANY, user_id=user_id, path="large.bin"),
        call(
            ANY,
            user_id=user_id,
            path="/Reports@1234abcd/reports/report.pdf",
        ),
    ]


async def test_message_rejects_invalid_inputs_before_workspace_access(pg_engine) -> None:
    service = AsyncMock(spec=WorkspaceService)
    registry = ToolRegistry((MessageTool(pg_engine, service),))
    invalid_args = (
        {"content": "   "},
        {"content": "x" * 16_001},
        {"content": "x" + " " * 16_000},
        {"content": "x", "openoctopus_device": "laptop"},
        {"content": "x", "media": ["a.txt", "a.txt"]},
        {"content": "x", "media": [f"{index}.txt" for index in range(11)]},
        {"content": "x", "delivery_refs": []},
    )

    for args in invalid_args:
        result = await registry.execute(name="message", args=args, ctx=_ctx())
        assert result.is_error is True
        assert result.code is ErrorCode.TOOL_INVALID_ARGS
        assert result.side_effect is None

    service.resolve_delivery_file.assert_not_awaited()


async def test_message_media_validation_is_all_or_nothing(pg_engine) -> None:
    user_id = uuid4()
    service = AsyncMock(spec=WorkspaceService)
    service.resolve_delivery_file.side_effect = (
        AuthorizedWorkspaceFile(
            target=WorkspaceTarget.personal(user_id),
            relative_path="exists.txt",
            metadata=FileMetadata(size=1, etag="one"),
        ),
        WorkspaceError(ErrorCode.WORKSPACE_NOT_FOUND, "Workspace file was not found"),
    )
    registry = ToolRegistry((MessageTool(pg_engine, service),))

    result = await registry.execute(
        name="message",
        args={"content": "Files", "media": ["exists.txt", "missing.txt"]},
        ctx=await _web_ctx(pg_engine, user_id=user_id),
    )

    assert result.is_error is True
    assert result.code is ErrorCode.WORKSPACE_NOT_FOUND
    assert result.side_effect is None


async def test_message_timeout_has_no_delivery_side_effect(pg_engine, monkeypatch) -> None:
    started = asyncio.Event()

    async def blocked(*args: object, **kwargs: object) -> None:
        del args, kwargs
        started.set()
        await asyncio.Event().wait()

    service = AsyncMock(spec=WorkspaceService)
    service.resolve_delivery_file.side_effect = blocked
    registry = ToolRegistry((MessageTool(pg_engine, service),))
    monkeypatch.setattr("openctopus_server.tools.message.MESSAGE_TOOL_TIMEOUT_SECONDS", 0.01)

    result = await registry.execute(
        name="message",
        args={"content": "File", "media": ["a.txt"]},
        ctx=await _web_ctx(pg_engine),
    )

    assert started.is_set()
    assert result.is_error is True
    assert result.code is ErrorCode.TOOL_EXEC_TIMEOUT
    assert result.side_effect is None


async def test_message_rejects_contexts_outside_the_current_web_session(pg_engine) -> None:
    service = AsyncMock(spec=WorkspaceService)
    registry = ToolRegistry((MessageTool(pg_engine, service),))
    non_web = await _web_ctx(pg_engine, channel="discord")
    wrong_owner_session = await _web_ctx(pg_engine)
    contexts = (
        non_web,
        ToolContext(user_id=uuid4(), session_id=wrong_owner_session.session_id),
        _ctx(),
    )

    for index, ctx in enumerate(contexts):
        result = await registry.execute(
            name="message",
            args={
                "content": "Do not deliver",
                "media": [] if index == 0 else ["secret.txt"],
            },
            ctx=ctx,
        )
        assert result.is_error is True
        assert result.code is ErrorCode.TOOL_CHANNEL_NOT_CONFIGURED
        assert result.side_effect is None

    service.resolve_delivery_file.assert_not_awaited()


async def test_message_rejects_two_paths_resolving_to_the_same_file(pg_engine) -> None:
    user_id = uuid4()
    service = AsyncMock(spec=WorkspaceService)
    resolved = AuthorizedWorkspaceFile(
        target=WorkspaceTarget.personal(user_id),
        relative_path="report.txt",
        metadata=FileMetadata(size=6, etag="same"),
    )
    service.resolve_delivery_file.side_effect = (resolved, resolved)
    registry = ToolRegistry((MessageTool(pg_engine, service),))

    result = await registry.execute(
        name="message",
        args={
            "content": "One file",
            "media": ["report.txt", f"/{user_id}/report.txt"],
        },
        ctx=await _web_ctx(pg_engine, user_id=user_id),
    )

    assert result.is_error is True
    assert result.code is ErrorCode.TOOL_INVALID_ARGS
    assert result.side_effect is None
    assert service.resolve_delivery_file.await_count == 2


@asynccontextmanager
async def _slot():
    yield


async def test_service_resolves_authorized_immutable_delivery_target(
    pg_engine: AsyncEngine,
) -> None:
    workspace_fs = AsyncMock(spec=WorkspaceFS)
    workspace_fs.file_operation_slot = Mock(side_effect=_slot)
    workspace_fs.stat.return_value = FileMetadata(size=81, etag="etag")
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = User(email="message-owner@test.com", password_hash="x", name="Owner")
        member = User(email="message-member@test.com", password_hash="x", name="Member")
        db.add_all((owner, member))
        await db.flush()
        workspace = Workspace(
            name="Messages",
            suffix="face1234",
            quota_bytes=1024,
            created_by=owner.id,
        )
        db.add(workspace)
        await db.flush()
        db.add_all(
            (
                WorkspaceMember(workspace_id=workspace.id, user_id=owner.id),
                WorkspaceMember(workspace_id=workspace.id, user_id=member.id),
            )
        )
        await db.commit()

        resolved = await service.resolve_delivery_file(
            db,
            user_id=member.id,
            path="/Messages@face1234/reports/final.pdf",
        )

    assert resolved == AuthorizedWorkspaceFile(
        target=WorkspaceTarget.shared(workspace.id),
        relative_path="reports/final.pdf",
        metadata=FileMetadata(size=81, etag="etag"),
    )
    workspace_fs.stat.assert_awaited_once_with(
        WorkspaceTarget.shared(workspace.id),
        "reports/final.pdf",
    )


async def test_delivery_stat_reauthorizes_after_waiting_for_file_admission(
    pg_engine: AsyncEngine,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    @asynccontextmanager
    async def blocked_slot():
        entered.set()
        await release.wait()
        yield

    workspace_fs = AsyncMock(spec=WorkspaceFS)
    workspace_fs.file_operation_slot = Mock(side_effect=blocked_slot)
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = User(email="delivery-owner@test.com", password_hash="x", name="Owner")
        member = User(email="delivery-member@test.com", password_hash="x", name="Member")
        db.add_all((owner, member))
        await db.flush()
        workspace = Workspace(
            name="Delivery",
            suffix="deadbeef",
            quota_bytes=1024,
            created_by=owner.id,
        )
        db.add(workspace)
        await db.flush()
        db.add_all(
            (
                WorkspaceMember(workspace_id=workspace.id, user_id=owner.id),
                WorkspaceMember(workspace_id=workspace.id, user_id=member.id),
            )
        )
        await db.commit()
        workspace_id = workspace.id
        member_id = member.id

    operation_db = AsyncSession(pg_engine, expire_on_commit=False)
    resolving = asyncio.create_task(
        service.resolve_delivery_file(
            operation_db,
            user_id=member_id,
            path="/Delivery@deadbeef/report.txt",
        )
    )
    await entered.wait()
    async with AsyncSession(pg_engine, expire_on_commit=False) as revoke_db:
        await revoke_db.execute(
            delete(WorkspaceMember).where(
                WorkspaceMember.workspace_id == workspace_id,
                WorkspaceMember.user_id == member_id,
            )
        )
        await revoke_db.commit()
    release.set()
    try:
        with pytest.raises(WorkspaceError) as caught:
            await resolving
    finally:
        await operation_db.close()

    assert caught.value.code is ErrorCode.WORKSPACE_NOT_FOUND
    workspace_fs.stat.assert_not_awaited()
