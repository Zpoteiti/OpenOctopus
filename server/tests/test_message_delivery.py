import json
from collections import deque
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.chat.compaction import commit_stage_two, stage_two_source_ids
from openctopus_server.chat.context import project_message_rows
from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.db.models import Device, Message, Session, SystemConfig, User
from openctopus_server.dto.message import MessageResponse
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.provider.anthropic import (
    DeltaCallback,
    ProviderResult,
    provider_fingerprint,
)
from openctopus_server.provider.config import ProviderConfig
from openctopus_server.provider.limiter import ProviderLimiter
from openctopus_server.provider.wire_types import Effort
from openctopus_server.tools.device_field import DEVICE_FIELD_NAME
from openctopus_server.tools.registry import build_py4_registry
from openctopus_server.workspace.fs import FileMetadata, WorkspaceFS
from openctopus_server.workspace.service import WorkspaceService


@dataclass(slots=True)
class _ProviderStep:
    content: list[dict[str, Any]]


class _ScriptedProvider:
    def __init__(self, steps: list[_ProviderStep]) -> None:
        self.steps = deque(steps)
        self.calls: list[dict[str, Any]] = []

    async def stream_turn(
        self,
        *,
        config: ProviderConfig,
        system: str,
        messages: list[dict[str, Any]],
        effort: Effort | None,
        limiter: ProviderLimiter,
        on_delta: DeltaCallback,
        tools: list[dict[str, Any]] | None = None,
    ) -> ProviderResult:
        del limiter, on_delta
        self.calls.append(
            {
                "system": system,
                "messages": deepcopy(messages),
                "effort": effort,
                "tools": deepcopy(tools),
            }
        )
        return ProviderResult(
            content=deepcopy(self.steps.popleft().content),
            fingerprint=provider_fingerprint(config),
        )

    async def count_tokens(
        self,
        *,
        config: ProviderConfig,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        effort: Effort | None,
        limiter: ProviderLimiter,
    ) -> int:
        del config, system, messages, tools, effort, limiter
        return 1

    async def close(self) -> None:
        return None


class _AsyncSlot:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        del args


def test_message_response_rejects_incomplete_workspace_delivery_refs() -> None:
    with pytest.raises(ValidationError):
        MessageResponse(
            id=uuid4(),
            session_id=uuid4(),
            role="assistant",
            message_kind="assistant",
            content=[],
            delivery_refs=[
                {
                    "tool_use_id": "message-incomplete",
                    "type": "workspace_file",
                    "openoctopus_device": "server",
                    "path": "report.pdf",
                    "filename": "report.pdf",
                    "online_only": False,
                }
            ],
            is_compacted=False,
            created_at=datetime.now(UTC),
        )


def test_message_response_accepts_device_delivery_ref_without_size() -> None:
    device_id = uuid4()
    response = MessageResponse(
        id=uuid4(),
        session_id=uuid4(),
        role="assistant",
        message_kind="assistant",
        content=[],
        delivery_refs=[
            {
                "tool_use_id": "message-device",
                "type": "device_file",
                "device_id": str(device_id),
                "openoctopus_device": "laptop",
                "path": "reports/final.pdf",
                "filename": "final.pdf",
                "mime": "application/pdf",
                "online_only": True,
            }
        ],
        is_compacted=False,
        created_at=datetime.now(UTC),
    )

    assert response.delivery_refs[0].type == "device_file"


def test_message_response_rejects_server_as_device_delivery_ref() -> None:
    with pytest.raises(ValidationError):
        MessageResponse(
            id=uuid4(),
            session_id=uuid4(),
            role="assistant",
            message_kind="assistant",
            content=[],
            delivery_refs=[
                {
                    "tool_use_id": "message-device",
                    "type": "device_file",
                    "openoctopus_device": "server",
                    "path": "report.pdf",
                    "filename": "report.pdf",
                    "mime": "application/pdf",
                    "online_only": True,
                }
            ],
            is_compacted=False,
            created_at=datetime.now(UTC),
        )


async def _configure_provider(engine: AsyncEngine) -> None:
    async with AsyncSession(engine, expire_on_commit=False) as db:
        db.add_all(
            (
                SystemConfig(key="llm_endpoint", value="http://fake.test"),
                SystemConfig(key="llm_api_key", value="fake-key"),
                SystemConfig(key="llm_model", value="fake-model"),
            )
        )
        await db.commit()


def _message_use(
    tool_use_id: str,
    content: str,
    media: list[str],
    *,
    device: str = "server",
) -> dict[str, Any]:
    return {
        "type": "tool_use",
        "id": tool_use_id,
        "name": "message",
        "input": {
            "content": content,
            "media": media,
            DEVICE_FIELD_NAME: device,
        },
    }


def _events(response) -> list[dict[str, Any]]:
    return [json.loads(line) for line in response.text.splitlines()]


async def _post(client, session_id: UUID):
    return await client.post(
        f"/api/sessions/{session_id}/messages",
        json={"content": [{"type": "text", "text": "send files"}], "attachments": []},
    )


async def _user_id(engine: AsyncEngine) -> UUID:
    async with AsyncSession(engine, expire_on_commit=False) as db:
        return (await db.execute(select(User.id).where(User.email == "user@test.com"))).scalar_one()


def _workspace_service(
    files: dict[str, FileMetadata],
) -> tuple[WorkspaceService, AsyncMock]:
    workspace_fs = AsyncMock(spec=WorkspaceFS)
    workspace_fs.materialization_slot = Mock(side_effect=_AsyncSlot)
    workspace_fs.file_operation_slot = Mock(side_effect=_AsyncSlot)

    async def stat(_target: object, relative_path: str) -> FileMetadata:
        try:
            return files[relative_path]
        except KeyError as exc:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_NOT_FOUND,
                "Workspace file was not found",
            ) from exc

    workspace_fs.stat.side_effect = stat
    workspace_fs.read.side_effect = WorkspaceError(
        ErrorCode.WORKSPACE_NOT_FOUND,
        "Optional prompt file was not found",
    )
    workspace_fs.list_dir_page.side_effect = WorkspaceError(
        ErrorCode.WORKSPACE_NOT_FOUND,
        "Optional skills directory was not found",
    )
    return WorkspaceService(workspace_fs), workspace_fs


def _install_runtime(
    test_app,
    engine: AsyncEngine,
    provider: _ScriptedProvider,
    workspace_service: WorkspaceService,
) -> ChatRuntime:
    runtime = ChatRuntime(
        engine,
        provider_factory=lambda config: provider,
        tool_registry=build_py4_registry(
            engine,
            workspace_service,
            Mock(spec=WorkspaceFS),
        ),
        workspace_service=workspace_service,
    )
    test_app.state.chat_runtime = runtime
    return runtime


async def test_two_message_calls_attach_correlated_refs_and_replay_only_provider_content(
    user_client,
    test_app,
    pg_engine: AsyncEngine,
) -> None:
    await _configure_provider(pg_engine)
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[
                    _message_use("message-1", "First file", ["reports/report.pdf"]),
                    _message_use("message-2", "Second file", ["images/chart.png"]),
                ]
            ),
            _ProviderStep(content=[{"type": "text", "text": "Delivered."}]),
        ]
    )
    workspace, _ = _workspace_service(
        {
            "reports/report.pdf": FileMetadata(size=101, etag="pdf-etag"),
            "images/chart.png": FileMetadata(size=202, etag="png-etag"),
        }
    )
    runtime = _install_runtime(test_app, pg_engine, provider, workspace)
    session_id = uuid4()
    try:
        response = await _post(user_client, session_id)

        assert response.status_code == 200
        assert len(provider.calls) == 2
        assert "message" in [schema["name"] for schema in provider.calls[0]["tools"]]
        assert "delivery_refs" not in json.dumps(provider.calls)

        replay = provider.calls[1]["messages"]
        assert replay[-2]["role"] == "assistant"
        assert [block["id"] for block in replay[-2]["content"]] == [
            "message-1",
            "message-2",
        ]
        assert replay[-1]["role"] == "user"
        assert [block["tool_use_id"] for block in replay[-1]["content"]] == [
            "message-1",
            "message-2",
        ]

        persisted_events = [
            event["message"] for event in _events(response) if event["type"] == "message_persisted"
        ]
        initial_assistant = persisted_events[0]
        assert initial_assistant["message_kind"] == "assistant"
        assistant_id = initial_assistant["id"]
        assert initial_assistant["delivery_refs"] == []
        assert [(message["message_kind"], message["id"]) for message in persisted_events[:-1]] == [
            ("assistant", assistant_id),
            ("assistant", assistant_id),
            ("tool_result", persisted_events[2]["id"]),
            ("assistant", assistant_id),
            ("tool_result", persisted_events[4]["id"]),
        ]
        assert [len(persisted_events[index]["delivery_refs"]) for index in (0, 1, 3)] == [0, 1, 2]

        history_response = await user_client.get(f"/api/sessions/{session_id}/messages")
        assert history_response.status_code == 200
        history = history_response.json()
        assistant = next(
            message for message in history["messages"] if message["id"] == assistant_id
        )
        user_id = await _user_id(pg_engine)
        assert assistant["delivery_refs"] == [
            {
                "tool_use_id": "message-1",
                "type": "workspace_file",
                "openoctopus_device": "server",
                "path": "reports/report.pdf",
                "workspace_id": str(user_id),
                "workspace_relative_path": "reports/report.pdf",
                "filename": "report.pdf",
                "mime": "application/pdf",
                "size": 101,
                "online_only": False,
            },
            {
                "tool_use_id": "message-2",
                "type": "workspace_file",
                "openoctopus_device": "server",
                "path": "images/chart.png",
                "workspace_id": str(user_id),
                "workspace_relative_path": "images/chart.png",
                "filename": "chart.png",
                "mime": "image/png",
                "size": 202,
                "online_only": False,
            },
        ]
    finally:
        await runtime.close()


async def test_device_message_ref_is_provider_hidden_and_does_not_open_workspace(
    user_client,
    test_app,
    pg_engine: AsyncEngine,
) -> None:
    await _configure_provider(pg_engine)
    user_id = await _user_id(pg_engine)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        device = Device(
                user_id=user_id,
                name="laptop",
                token_hash=b"d" * 32,
                token_hint="openoctopus_dev_...device",
                workspace_path="~/workspace",
                restrict_to_workspace=True,
                ssrf_denylist=[],
            )
        db.add(device)
        await db.commit()
        device_id = device.id
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[
                    _message_use(
                        "message-device",
                        "Device file",
                        ["reports/final.pdf"],
                        device="laptop",
                    )
                ]
            ),
            _ProviderStep(content=[{"type": "text", "text": "Delivered."}]),
        ]
    )
    workspace, workspace_fs = _workspace_service({})
    runtime = _install_runtime(test_app, pg_engine, provider, workspace)
    session_id = uuid4()
    try:
        response = await _post(user_client, session_id)

        assert response.status_code == 200
        message_schema = next(
            schema for schema in provider.calls[0]["tools"] if schema["name"] == "message"
        )
        assert message_schema["input_schema"]["properties"][DEVICE_FIELD_NAME]["enum"] == [
            "server",
            "laptop",
        ]
        assert "delivery_refs" not in json.dumps(provider.calls)
        workspace_fs.stat.assert_not_awaited()
        history = (await user_client.get(f"/api/sessions/{session_id}/messages")).json()
        assistant = next(
            message
            for message in history["messages"]
            if message["message_kind"] == "assistant" and message["delivery_refs"]
        )
        assert assistant["delivery_refs"] == [
            {
                "tool_use_id": "message-device",
                "type": "device_file",
                "device_id": str(device_id),
                "openoctopus_device": "laptop",
                "path": "reports/final.pdf",
                "filename": "final.pdf",
                "mime": "application/pdf",
                "online_only": True,
            }
        ]
    finally:
        await runtime.close()


async def test_message_media_validation_is_all_or_nothing(
    user_client,
    test_app,
    pg_engine: AsyncEngine,
) -> None:
    await _configure_provider(pg_engine)
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[
                    _message_use(
                        "message-invalid",
                        "These should not be delivered",
                        ["valid.txt", "missing.txt"],
                    )
                ]
            ),
            _ProviderStep(content=[{"type": "text", "text": "Could not deliver."}]),
        ]
    )
    workspace, workspace_fs = _workspace_service(
        {"valid.txt": FileMetadata(size=5, etag="valid-etag")}
    )
    runtime = _install_runtime(test_app, pg_engine, provider, workspace)
    session_id = uuid4()
    try:
        response = await _post(user_client, session_id)

        assert response.status_code == 200
        assert workspace_fs.stat.await_count == 2
        history = (await user_client.get(f"/api/sessions/{session_id}/messages")).json()
        tool_assistant = next(
            message
            for message in history["messages"]
            if message["message_kind"] == "assistant"
            and any(block.get("type") == "tool_use" for block in message["content"])
        )
        assert tool_assistant["delivery_refs"] == []
        tool_result = next(
            message for message in history["messages"] if message["message_kind"] == "tool_result"
        )
        assert tool_result["content"][0]["tool_use_id"] == "message-invalid"
        assert tool_result["content"][0]["is_error"] is True
        assert tool_result["content"][0]["code"] == ErrorCode.WORKSPACE_NOT_FOUND.value
    finally:
        await runtime.close()


async def test_delivery_ref_update_rolls_back_when_tool_result_insert_fails(
    user_client,
    test_app,
    pg_engine: AsyncEngine,
) -> None:
    await _configure_provider(pg_engine)
    provider = _ScriptedProvider(
        [_ProviderStep(content=[_message_use("message-atomic", "Atomic", ["atomic.txt"])])]
    )
    workspace, workspace_fs = _workspace_service(
        {"atomic.txt": FileMetadata(size=6, etag="atomic-etag")}
    )
    runtime = _install_runtime(test_app, pg_engine, provider, workspace)
    session_id = uuid4()
    fault_armed = True

    def fail_first_tool_result_insert(
        _conn: object,
        _cursor: object,
        statement: str,
        parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        nonlocal fault_armed
        if fault_armed and "INSERT INTO messages" in statement and "tool_result" in str(parameters):
            fault_armed = False
            raise RuntimeError("injected tool-result insert failure")

    event.listen(
        pg_engine.sync_engine,
        "before_cursor_execute",
        fail_first_tool_result_insert,
    )
    try:
        response = await _post(user_client, session_id)

        assert response.status_code == 200
        assert workspace_fs.stat.await_count == 1
        assert fault_armed is False
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            assistant = (
                await db.execute(
                    select(Message).where(
                        Message.session_id == session_id,
                        Message.message_kind == "assistant",
                    )
                )
            ).scalar_one()
            assert assistant.delivery_refs == []
            ordinary_result_count = len(
                list(
                    (
                        await db.execute(
                            select(Message).where(
                                Message.session_id == session_id,
                                Message.message_kind == "tool_result",
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
            )
            assert ordinary_result_count == 0
    finally:
        event.remove(
            pg_engine.sync_engine,
            "before_cursor_execute",
            fail_first_tool_result_insert,
        )
        await runtime.close()


async def test_compaction_keeps_canonical_delivery_refs_provider_hidden(
    pg_engine: AsyncEngine,
) -> None:
    fingerprint = "provider-fingerprint"
    user_id = uuid4()
    session_id = uuid4()
    tool_use_id = "message-compacted"
    created_at = datetime.now(UTC)
    delivery_ref = {
        "tool_use_id": tool_use_id,
        "type": "workspace_file",
        "openoctopus_device": "server",
        "path": "archive.txt",
        "workspace_id": str(user_id),
        "workspace_relative_path": "archive.txt",
        "filename": "archive.txt",
        "mime": "text/plain",
        "size": 7,
        "online_only": False,
    }
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = User(
            id=user_id,
            email="compaction-delivery@test.com",
            password_hash="x",
            name="Compaction",
        )
        session = Session(
            id=session_id,
            user_id=user_id,
            session_key=f"web:{session_id}",
            channel="web",
            chat_id=str(session_id),
            title="Delivery",
            created_at=datetime.now(UTC),
        )
        rows = [
            Message(
                id=uuid4(),
                session_id=session_id,
                message_kind="human",
                content=[{"type": "text", "text": "send it"}],
                delivery_refs=[],
                is_compacted=False,
                created_at=created_at,
            ),
            Message(
                id=uuid4(),
                session_id=session_id,
                message_kind="assistant",
                content=[_message_use(tool_use_id, "Here", ["archive.txt"])],
                delivery_refs=[delivery_ref],
                llm_fingerprint=fingerprint,
                is_compacted=False,
                created_at=created_at + timedelta(microseconds=1),
            ),
            Message(
                id=uuid4(),
                session_id=session_id,
                message_kind="tool_result",
                content=[
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_use_id,
                        "content": "Delivered.",
                        "is_error": False,
                    }
                ],
                delivery_refs=[],
                is_compacted=False,
                created_at=created_at + timedelta(microseconds=2),
            ),
        ]
        db.add(user)
        await db.flush()
        db.add(session)
        await db.flush()
        db.add_all(rows)
        await db.commit()

        projection = project_message_rows(rows, current_fingerprint=fingerprint)
        assert "delivery_refs" not in json.dumps(projection)
        source_ids = stage_two_source_ids(rows)
        summary = await commit_stage_two(
            db,
            session_id=session_id,
            source_ids=source_ids,
            summary_content=[{"type": "text", "text": "Delivery completed."}],
        )

        await db.refresh(rows[1])
        assert rows[1].is_compacted is True
        assert rows[1].delivery_refs == [delivery_ref]
        assert summary.delivery_refs == []
