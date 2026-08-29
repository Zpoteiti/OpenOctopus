import asyncio
import json
from collections import deque
from collections.abc import AsyncIterator
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from unittest.mock import Mock
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

import openctopus_server.chat.runner as chat_runner
from openctopus_server.admission import KeyedAdmission
from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.db.models import Device, Session, SystemConfig, TurnRun, User
from openctopus_server.devices.protocol import ToolResultFrame
from openctopus_server.devices.registry import DeviceRegistry
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.provider.anthropic import (
    DeltaCallback,
    ProviderInvocationError,
    ProviderResult,
    provider_fingerprint,
)
from openctopus_server.provider.config import ProviderConfig
from openctopus_server.provider.limiter import ProviderLimiter
from openctopus_server.provider.wire_types import Effort
from openctopus_server.services import messages as message_service
from openctopus_server.tools.base import Tool, ToolContext, ToolResult, ToolRoutingMode
from openctopus_server.tools.device_field import DEVICE_FIELD_NAME
from openctopus_server.tools.registry import ToolRegistry, build_py3_registry, build_py4_registry
from openctopus_server.tools.result import UNTRUSTED_TOOL_RESULT_WARNING
from openctopus_server.workspace.fs import (
    DirectoryEntry,
    DirectoryPage,
    FileMetadata,
    WorkspaceFS,
)
from openctopus_server.workspace.skills import SkillsCache


@dataclass(slots=True)
class _ProviderStep:
    content: list[dict[str, Any]]
    started: asyncio.Event | None = None
    release: asyncio.Event | None = None
    error: ProviderInvocationError | None = None


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
        step = self.steps.popleft()
        self.calls.append(
            {
                "system": system,
                "messages": deepcopy(messages),
                "effort": effort,
                "tools": deepcopy(tools),
            }
        )
        if step.started is not None:
            step.started.set()
        if step.release is not None:
            await step.release.wait()
        if step.error is not None:
            raise step.error
        return ProviderResult(
            content=deepcopy(step.content),
            fingerprint=provider_fingerprint(config),
        )

    async def estimate_tokens(
        self,
        *,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
    ) -> int:
        del system, messages, tools
        return 1

    async def close(self) -> None:
        return None


class _AsyncBytes(httpx.AsyncByteStream):
    def __init__(self, content: bytes) -> None:
        self._content = content

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self._content


@dataclass(slots=True)
class _ToolStep:
    result: ToolResult
    started: asyncio.Event | None = None
    release: asyncio.Event | None = None


class _ScriptedTool(Tool):
    def __init__(self, steps: list[_ToolStep]) -> None:
        self.steps = deque(steps)
        self.calls: list[str] = []
        self.active = 0
        self.max_active = 0

    def name(self) -> str:
        return "test_tool"

    def schema(self) -> dict[str, Any]:
        return {
            "name": self.name(),
            "description": "Deterministic Py3 acceptance-test tool",
            "input_schema": {
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
            },
        }

    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        del ctx
        value = str(args["value"])
        self.calls.append(value)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            step = self.steps.popleft()
            if step.started is not None:
                step.started.set()
            if step.release is not None:
                await step.release.wait()
            return step.result
        finally:
            self.active -= 1


class _ReadFileTool(_ScriptedTool):
    def name(self) -> str:
        return "read_file"


class _ExecTool(_ScriptedTool):
    routing_mode = ToolRoutingMode.CLIENT_ONLY

    def name(self) -> str:
        return "exec"


class _SelfCancellingTool(_ScriptedTool):
    def __init__(self, engine: AsyncEngine) -> None:
        super().__init__([])
        self.engine = engine

    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        self.calls.append(str(args["value"]))
        async with AsyncSession(self.engine, expire_on_commit=False) as db:
            session = await db.get(Session, ctx.session_id)
            assert session is not None and session.user_id == ctx.user_id
            session.cancel_requested = True
            await db.commit()
        asyncio.get_running_loop().call_soon(
            message_service._notify_cancel_waiters,
            ctx.session_id,
        )
        return ToolResult(content="known result")


@dataclass(slots=True)
class _DeviceTransport:
    sent: list[str] = field(default_factory=list)
    sent_event: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(self, payload: str) -> None:
        self.sent.append(payload)
        self.sent_event.set()

    async def send_binary(self, payload: bytes) -> None:
        del payload

    async def close(self, code: int, reason: str) -> None:
        del code, reason


class _CountingPromptWorkspace:
    def __init__(self) -> None:
        self.read_paths: list[str] = []
        self.list_calls = 0

    async def read_personal_for_prompt(self, *, user_id, path, offset=0, length=0) -> bytes:
        del user_id, offset, length
        self.read_paths.append(path)
        raise WorkspaceError(ErrorCode.WORKSPACE_NOT_FOUND, "missing")

    async def list_personal_for_prompt(
        self,
        *,
        user_id,
        path,
        limit,
        offset=0,
        include_noise_directories=False,
        scan_limit=10_000,
    ) -> DirectoryPage:
        del user_id, path, limit, offset, include_noise_directories, scan_limit
        self.list_calls += 1
        raise WorkspaceError(ErrorCode.WORKSPACE_NOT_FOUND, "missing")


class _Py4Workspace(_CountingPromptWorkspace):
    def __init__(self) -> None:
        super().__init__()
        self.writes: list[tuple[UUID, str, bytes]] = []

    async def write(self, db, *, user_id, path, data) -> FileMetadata:
        del db
        self.writes.append((user_id, path, data))
        return FileMetadata(size=len(data), etag="written", created=True)


class _SkillPromptWorkspace(_CountingPromptWorkspace):
    def __init__(self, files: dict[str, bytes]) -> None:
        super().__init__()
        self.files = files

    async def read_personal_for_prompt(self, *, user_id, path, offset=0, length=0) -> bytes:
        del user_id, offset
        self.read_paths.append(path)
        if path not in self.files:
            raise WorkspaceError(ErrorCode.WORKSPACE_NOT_FOUND, "missing")
        data = self.files[path]
        return data[:length] if length else data

    async def list_personal_for_prompt(
        self,
        *,
        user_id,
        path,
        limit,
        offset=0,
        include_noise_directories=False,
        scan_limit=10_000,
    ) -> DirectoryPage:
        del user_id, include_noise_directories, scan_limit
        assert path == "skills"
        names = sorted({file_path.split("/")[1] for file_path in self.files})
        entries = tuple(
            DirectoryEntry(path=f"skills/{name}", is_directory=True, size=None)
            for name in names[offset : offset + limit]
        )
        return DirectoryPage(items=entries, next_offset=None, truncated=False)


@pytest_asyncio.fixture
async def install_runtime(test_app, pg_engine):
    runtimes: list[ChatRuntime] = []

    def install(
        provider: _ScriptedProvider,
        tool: _ScriptedTool,
        workspace_service=None,
    ) -> ChatRuntime:
        async def estimate_tokens(**kwargs: Any) -> int:
            return await provider.estimate_tokens(**kwargs)

        runtime = ChatRuntime(
            pg_engine,
            provider_factory=lambda config: provider,
            tool_registry=ToolRegistry((tool,)),
            workspace_service=workspace_service,
            request_token_estimator=estimate_tokens,
        )
        test_app.state.chat_runtime = runtime
        runtimes.append(runtime)
        return runtime

    yield install

    await asyncio.gather(*(runtime.close() for runtime in runtimes))


async def _configure_provider(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add_all(
            [
                SystemConfig(key="llm_endpoint", value="http://fake.test"),
                SystemConfig(key="llm_api_key", value="fake-key"),
                SystemConfig(key="llm_model", value="fake-model"),
            ]
        )
        await db.commit()


async def _enable_compaction(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add_all(
            [
                SystemConfig(key="llm_max_context_tokens", value=100_000),
                SystemConfig(key="llm_compaction_threshold_tokens", value=5000),
            ]
        )
        await db.commit()


def _tool_use(tool_id: str, value: str) -> dict[str, Any]:
    return {
        "type": "tool_use",
        "id": tool_id,
        "name": "test_tool",
        "input": {"value": value, DEVICE_FIELD_NAME: "server"},
    }


def _events(response) -> list[dict[str, Any]]:
    return [json.loads(line) for line in response.text.splitlines()]


async def _post(client, session_id: UUID, text: str):
    return await client.post(
        f"/api/sessions/{session_id}/messages",
        json={"content": [{"type": "text", "text": text}], "attachments": []},
    )


async def _history(client, session_id: UUID) -> dict[str, Any]:
    response = await client.get(f"/api/sessions/{session_id}/messages")
    assert response.status_code == 200
    return response.json()


async def _wait_for_pending(client, session_id: UUID, expected: int) -> dict[str, Any]:
    for _ in range(200):
        history = await _history(client, session_id)
        if history["pending_count"] == expected:
            return history
        await asyncio.sleep(0.01)
    raise AssertionError(f"pending_count never reached {expected}")


async def _insert_idle_session(pg_engine, session_id: UUID) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = (await db.execute(select(User).where(User.email == "user@test.com"))).scalar_one()
        db.add(
            Session(
                id=session_id,
                user_id=user.id,
                session_key=f"web:{session_id}",
                channel="web",
                chat_id=str(session_id),
                title="New chat",
                created_at=datetime.now(UTC),
            )
        )
        await db.commit()


async def test_two_turn_react_uses_distinct_turn_ids_on_one_stream(
    user_client,
    pg_engine,
    install_runtime,
):
    await _configure_provider(pg_engine)
    provider = _ScriptedProvider(
        [
            _ProviderStep(content=[_tool_use("tool-1", "one")]),
            _ProviderStep(content=[{"type": "text", "text": "done"}]),
        ]
    )
    tool = _ScriptedTool([_ToolStep(result=ToolResult(content="result-one"))])
    install_runtime(provider, tool)
    session_id = uuid4()

    response = await _post(user_client, session_id, "start")

    assert response.status_code == 200
    events = _events(response)
    started = [event for event in events if event["type"] == "turn_started"]
    finished = [event for event in events if event["type"] == "turn_finished"]
    assert len(started) == len(finished) == 2
    assert started[0]["turn_id"] != started[1]["turn_id"]
    assert [event["turn_id"] for event in finished] == [
        started[0]["turn_id"],
        started[1]["turn_id"],
    ]
    assert started[1]["message_ids"] == []
    assert [event["status"] for event in finished] == ["completed", "completed"]

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        runs = list(
            (
                await db.execute(
                    select(TurnRun)
                    .where(TurnRun.session_id == session_id)
                    .order_by(TurnRun.started_at, TurnRun.id)
                )
            )
            .scalars()
            .all()
        )
    assert [str(run.id) for run in runs] == [event["turn_id"] for event in started]
    assert [run.status for run in runs] == ["completed", "completed"]


async def test_normal_agent_turn_builds_workspace_prompt_once(
    user_client,
    pg_engine,
    install_runtime,
) -> None:
    await _configure_provider(pg_engine)
    provider = _ScriptedProvider([_ProviderStep(content=[{"type": "text", "text": "done"}])])
    workspace = _CountingPromptWorkspace()
    install_runtime(provider, _ScriptedTool([]), workspace)

    response = await _post(user_client, uuid4(), "start")

    assert response.status_code == 200
    assert workspace.read_paths == ["SOUL.md", "MEMORY.md"]
    assert workspace.list_calls == 1


async def test_web_fetch_runs_end_to_end_through_the_agent_loop(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[
                    {
                        "type": "tool_use",
                        "id": "web-1",
                        "name": "web_fetch",
                        "input": {
                            "url": "https://example.com/weather",
                            DEVICE_FIELD_NAME: "server",
                        },
                    }
                ]
            ),
            _ProviderStep(content=[{"type": "text", "text": "Tokyo is 20 C."}]),
        ]
    )

    async def resolve(hostname: str, port: int) -> list[str]:
        assert (hostname, port) == ("example.com", 443)
        return ["93.184.216.34"]

    registry = build_py3_registry(
        resolver=resolve,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                stream=_AsyncBytes(b"Tokyo weather: 20 C"),
                headers={"content-type": "text/plain"},
            )
        ),
    )
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=registry,
    )
    test_app.state.chat_runtime = runtime
    session_id = uuid4()
    try:
        response = await _post(user_client, session_id, "Tokyo weather?")
        assert response.status_code == 200
        assert len(provider.calls) == 2
        provider_result = provider.calls[1]["messages"][-1]["content"][0]
        assert provider_result["tool_use_id"] == "web-1"
        assert provider_result["content"] == [
            {"type": "text", "text": UNTRUSTED_TOOL_RESULT_WARNING},
            {"type": "text", "text": "Tokyo weather: 20 C"},
        ]
        history = await _history(user_client, session_id)
        assert [message["message_kind"] for message in history["messages"]] == [
            "human",
            "assistant",
            "tool_result",
            "assistant",
        ]
    finally:
        await runtime.close()


async def test_py4_workspace_tool_and_prompt_run_end_to_end_through_agent_loop(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[
                    {
                        "type": "tool_use",
                        "id": "write-1",
                        "name": "write_file",
                        "input": {
                            "path": "notes/a.txt",
                            "content": "hello",
                            DEVICE_FIELD_NAME: "server",
                        },
                    }
                ]
            ),
            _ProviderStep(content=[{"type": "text", "text": "Saved."}]),
        ]
    )
    workspace = _Py4Workspace()
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=build_py4_registry(
            pg_engine,
            workspace,  # type: ignore[arg-type]
            Mock(spec=WorkspaceFS),
        ),
        workspace_service=workspace,  # type: ignore[arg-type]
    )
    test_app.state.chat_runtime = runtime
    try:
        response = await _post(user_client, uuid4(), "save a note")

        assert response.status_code == 200
        assert [schema["name"] for schema in provider.calls[0]["tools"]] == [
            "web_fetch",
            "message",
            "file_transfer",
            "read_file",
            "write_file",
            "edit_file",
            "apply_patch",
            "delete_file",
            "delete_folder",
            "list_dir",
            "find_files",
            "grep",
            "notebook_edit",
        ]
        assert workspace.writes and workspace.writes[0][1:] == ("notes/a.txt", b"hello")
        provider_result = provider.calls[1]["messages"][-1]["content"][0]
        assert provider_result["tool_use_id"] == "write-1"
        assert json.loads(provider_result["content"][1]["text"]) == {
            "ok": True,
            "operation": "write_file",
            "device": "server",
            "requested_path": "notes/a.txt",
            "canonical_path": "~/notes/a.txt",
            "bytes_written": 5,
        }
        assert workspace.read_paths == ["SOUL.md", "MEMORY.md"] * 2
        assert workspace.list_calls == 1
    finally:
        await runtime.close()


async def test_provider_schema_includes_only_the_session_owners_paired_devices(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await db.scalar(select(User).where(User.email == "user@test.com"))
        assert owner is not None
        other = User(email="other@test.com", password_hash="hash", name="Other")
        db.add(other)
        await db.flush()
        db.add_all(
            [
                Device(
                    user_id=owner.id,
                    name="owner-laptop",
                    token_hash=b"o" * 32,
                    token_hint="owner-token",
                ),
                Device(
                    user_id=other.id,
                    name="other-laptop",
                    token_hash=b"x" * 32,
                    token_hint="other-token",
                ),
            ]
        )
        await db.commit()

    provider = _ScriptedProvider([_ProviderStep(content=[{"type": "text", "text": "done"}])])
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=ToolRegistry((_ScriptedTool([]),)),
    )
    test_app.state.chat_runtime = runtime
    try:
        response = await _post(user_client, uuid4(), "show my tools")

        assert response.status_code == 200
        schema = provider.calls[0]["tools"][0]
        assert schema["input_schema"]["properties"][DEVICE_FIELD_NAME]["enum"] == [
            "server",
            "owner-laptop",
        ]
    finally:
        await runtime.close()


async def test_all_owned_devices_get_file_and_exec_tools(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await db.scalar(select(User).where(User.email == "user@test.com"))
        assert owner is not None
        db.add_all(
            [
                Device(
                    user_id=owner.id,
                    name="sandbox-laptop",
                    token_hash=b"s" * 32,
                    token_hint="sandbox-token",
                    restrict_to_workspace=True,
                ),
                Device(
                    user_id=owner.id,
                    name="trusted-laptop",
                    token_hash=b"t" * 32,
                    token_hint="trusted-token",
                    restrict_to_workspace=False,
                ),
            ]
        )
        await db.commit()

    provider = _ScriptedProvider([_ProviderStep(content=[{"type": "text", "text": "done"}])])
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=ToolRegistry((_ReadFileTool([]), _ExecTool([]))),
    )
    test_app.state.chat_runtime = runtime
    try:
        response = await _post(user_client, uuid4(), "show my tools")

        assert response.status_code == 200
        schemas = {schema["name"]: schema for schema in provider.calls[0]["tools"]}
        assert set(
            schemas["read_file"]["input_schema"]["properties"][DEVICE_FIELD_NAME]["enum"]
        ) == {
            "server",
            "sandbox-laptop",
            "trusted-laptop",
        }
        assert set(
            schemas["exec"]["input_schema"]["properties"][DEVICE_FIELD_NAME]["enum"]
        ) == {"sandbox-laptop", "trusted-laptop"}
    finally:
        await runtime.close()


async def test_two_tools_are_serial_and_collapse_into_next_provider_user_message(
    user_client,
    pg_engine,
    install_runtime,
):
    await _configure_provider(pg_engine)
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[
                    _tool_use("tool-1", "one"),
                    _tool_use("tool-2", "two"),
                ]
            ),
            _ProviderStep(content=[{"type": "text", "text": "done"}]),
        ]
    )
    tool = _ScriptedTool(
        [
            _ToolStep(
                result=ToolResult(content="result-one"),
                started=first_started,
                release=release_first,
            ),
            _ToolStep(result=ToolResult(content="result-two")),
        ]
    )
    install_runtime(provider, tool)
    session_id = uuid4()

    post_task = asyncio.create_task(_post(user_client, session_id, "start"))
    await asyncio.wait_for(first_started.wait(), timeout=2)
    assert tool.calls == ["one"]
    release_first.set()
    response = await asyncio.wait_for(post_task, timeout=2)

    assert response.status_code == 200
    assert tool.calls == ["one", "two"]
    assert tool.max_active == 1
    second_messages = provider.calls[1]["messages"]
    collapsed = [
        message
        for message in second_messages
        if any(block.get("type") == "tool_result" for block in message["content"])
    ]
    assert len(collapsed) == 1
    assert collapsed[0]["role"] == "user"
    assert [block["tool_use_id"] for block in collapsed[0]["content"]] == [
        "tool-1",
        "tool-2",
    ]


async def test_normalized_tool_failure_is_provider_data_and_loop_continues(
    user_client,
    pg_engine,
    install_runtime,
):
    await _configure_provider(pg_engine)
    provider = _ScriptedProvider(
        [
            _ProviderStep(content=[_tool_use("tool-1", "bad")]),
            _ProviderStep(content=[{"type": "text", "text": "recovered"}]),
        ]
    )
    tool = _ScriptedTool(
        [
            _ToolStep(
                result=ToolResult(
                    content="boom",
                    is_error=True,
                    code=ErrorCode.TOOL_DB_ERROR,
                )
            )
        ]
    )
    install_runtime(provider, tool)
    session_id = uuid4()

    response = await _post(user_client, session_id, "start")

    assert response.status_code == 200
    assert len(provider.calls) == 2
    provider_result = provider.calls[1]["messages"][-1]["content"][0]
    assert provider_result["type"] == "tool_result"
    assert provider_result["is_error"] is True
    assert "code" not in provider_result
    assert provider_result["content"] == [
        {"type": "text", "text": UNTRUSTED_TOOL_RESULT_WARNING},
        {"type": "text", "text": "boom"},
    ]
    history = await _history(user_client, session_id)
    persisted = next(
        message for message in history["messages"] if message["message_kind"] == "tool_result"
    )
    assert persisted["content"][0]["code"] == ErrorCode.TOOL_DB_ERROR.value
    assert history["messages"][-1]["content"] == [{"type": "text", "text": "recovered"}]


async def test_pending_during_tool_waits_for_complete_pairing(
    user_client,
    pg_engine,
    install_runtime,
):
    await _configure_provider(pg_engine)
    second_started = asyncio.Event()
    release_second = asyncio.Event()
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[
                    _tool_use("tool-1", "one"),
                    _tool_use("tool-2", "two"),
                ]
            ),
            _ProviderStep(content=[{"type": "text", "text": "done"}]),
        ]
    )
    tool = _ScriptedTool(
        [
            _ToolStep(result=ToolResult(content="result-one")),
            _ToolStep(
                result=ToolResult(content="result-two"),
                started=second_started,
                release=release_second,
            ),
        ]
    )
    install_runtime(provider, tool)
    session_id = uuid4()

    first_task = asyncio.create_task(_post(user_client, session_id, "start"))
    await asyncio.wait_for(second_started.wait(), timeout=2)
    followup_task = asyncio.create_task(_post(user_client, session_id, "follow up"))
    waiting = await _wait_for_pending(user_client, session_id, 1)
    pending_id = waiting["pending_messages"][0]["id"]
    assert [
        message["message_kind"]
        for message in waiting["messages"]
        if message["message_kind"] in {"tool_result", "synthetic_tool_result"}
    ] == ["tool_result"]

    release_second.set()
    first_response, followup_response = await asyncio.gather(first_task, followup_task)
    assert first_response.status_code == followup_response.status_code == 200
    followup_starts = [
        event for event in _events(followup_response) if event["type"] == "turn_started"
    ]
    assert followup_starts[0]["message_ids"] == [pending_id]

    messages = provider.calls[1]["messages"]
    assert [message["role"] for message in messages[-2:]] == ["user", "user"]
    assert [block["tool_use_id"] for block in messages[-2]["content"]] == [
        "tool-1",
        "tool-2",
    ]
    assert messages[-1]["content"][-1] == {"type": "text", "text": "follow up"}


async def test_post_boundary_subscriber_waits_for_its_captured_turn(
    user_client,
    pg_engine,
    install_runtime,
    monkeypatch,
):
    await _configure_provider(pg_engine)
    tool_started = asyncio.Event()
    release_tool = asyncio.Event()
    boundary_committed = asyncio.Event()
    release_handoff = asyncio.Event()
    provider = _ScriptedProvider(
        [
            _ProviderStep(content=[_tool_use("tool-1", "one")]),
            _ProviderStep(content=[{"type": "text", "text": "second done"}]),
            _ProviderStep(content=[{"type": "text", "text": "third done"}]),
        ]
    )
    runtime = install_runtime(
        provider,
        _ScriptedTool(
            [
                _ToolStep(
                    result=ToolResult(content="result"),
                    started=tool_started,
                    release=release_tool,
                )
            ]
        ),
    )
    registered: asyncio.Queue[UUID] = asyncio.Queue()
    original_register = runtime.register

    async def track_queued_registration(accepted):
        subscriber = await original_register(accepted)
        if accepted.turn is None:
            registered.put_nowait(accepted.message_id)
        return subscriber

    monkeypatch.setattr(runtime, "register", track_queued_registration)
    original_finish = chat_runner.finish_tool_batch_and_continue

    async def pause_after_boundary(*args: Any, **kwargs: Any):
        next_turn = await original_finish(*args, **kwargs)
        boundary_committed.set()
        await release_handoff.wait()
        return next_turn

    monkeypatch.setattr(chat_runner, "finish_tool_batch_and_continue", pause_after_boundary)
    session_id = uuid4()

    first_task = asyncio.create_task(_post(user_client, session_id, "first"))
    await asyncio.wait_for(tool_started.wait(), timeout=2)
    second_task = asyncio.create_task(_post(user_client, session_id, "second"))
    pending = await _wait_for_pending(user_client, session_id, 1)
    second_id = pending["pending_messages"][0]["id"]
    assert str(await asyncio.wait_for(registered.get(), timeout=2)) == second_id

    release_tool.set()
    await asyncio.wait_for(boundary_committed.wait(), timeout=2)
    third_task = asyncio.create_task(_post(user_client, session_id, "third"))
    pending = await _wait_for_pending(user_client, session_id, 2)
    third_id = next(
        row["id"]
        for row in pending["pending_messages"]
        if row["content"][-1] == {"type": "text", "text": "third"}
    )
    assert str(await asyncio.wait_for(registered.get(), timeout=2)) == third_id
    release_handoff.set()

    first_response, second_response, third_response = await asyncio.wait_for(
        asyncio.gather(first_task, second_task, third_task),
        timeout=2,
    )

    assert first_response.status_code == second_response.status_code == 200
    assert third_response.status_code == 200
    second_started = next(
        event for event in _events(second_response) if event["type"] == "turn_started"
    )
    third_started = next(
        event for event in _events(third_response) if event["type"] == "turn_started"
    )
    assert second_started["message_ids"] == [second_id]
    assert third_started["message_ids"] == [third_id]


async def test_pending_promoted_during_preflight_claims_newest_stream(
    user_client,
    pg_engine,
    install_runtime,
    monkeypatch,
):
    await _configure_provider(pg_engine)
    await _enable_compaction(pg_engine)

    first_provider_started = asyncio.Event()
    release_first_provider = asyncio.Event()
    second_count_started = asyncio.Event()
    release_second_count = asyncio.Event()
    release_queued_registration = asyncio.Event()
    queued_registration_started = asyncio.Event()
    queued_registration_finished = asyncio.Event()
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[{"type": "text", "text": "first done"}],
                started=first_provider_started,
                release=release_first_provider,
            ),
            _ProviderStep(content=[{"type": "text", "text": "second done"}]),
        ]
    )
    count_calls = 0

    async def blocking_count_tokens(**kwargs: Any) -> int:
        nonlocal count_calls
        del kwargs
        count_calls += 1
        if count_calls == 2:
            second_count_started.set()
            await release_second_count.wait()
        return 1

    monkeypatch.setattr(provider, "estimate_tokens", blocking_count_tokens)
    runtime = install_runtime(provider, _ScriptedTool([]))
    original_register = runtime.register

    async def delayed_queued_registration(accepted):
        if accepted.turn is None:
            queued_registration_started.set()
            await release_queued_registration.wait()
        subscriber = await original_register(accepted)
        if accepted.turn is None:
            queued_registration_finished.set()
        return subscriber

    monkeypatch.setattr(runtime, "register", delayed_queued_registration)
    session_id = uuid4()

    first_task = asyncio.create_task(_post(user_client, session_id, "first"))
    await asyncio.wait_for(first_provider_started.wait(), timeout=2)
    second_task = asyncio.create_task(_post(user_client, session_id, "second"))
    await asyncio.wait_for(queued_registration_started.wait(), timeout=2)
    pending = await _wait_for_pending(user_client, session_id, 1)
    pending_id = pending["pending_messages"][0]["id"]
    release_first_provider.set()
    await asyncio.wait_for(second_count_started.wait(), timeout=2)
    release_queued_registration.set()
    await asyncio.wait_for(queued_registration_finished.wait(), timeout=2)
    release_second_count.set()

    first_response, second_response = await asyncio.wait_for(
        asyncio.gather(first_task, second_task),
        timeout=2,
    )

    assert [
        event["status"] for event in _events(first_response) if event["type"] == "turn_finished"
    ] == ["completed"]
    second_events = _events(second_response)
    started = next(event for event in second_events if event["type"] == "turn_started")
    assert pending_id in started["message_ids"]
    assert [event["status"] for event in second_events if event["type"] == "turn_finished"] == [
        "completed"
    ]


async def test_stage_one_counts_and_promotes_only_captured_pending_prefix(
    user_client,
    pg_engine,
    install_runtime,
    monkeypatch,
):
    await _configure_provider(pg_engine)
    count_started = asyncio.Event()
    release_count = asyncio.Event()
    provider = _ScriptedProvider(
        [
            _ProviderStep(content=[{"type": "text", "text": "warm"}]),
            _ProviderStep(content=[{"type": "text", "text": "summary"}]),
            _ProviderStep(content=[{"type": "text", "text": "first done"}]),
            _ProviderStep(content=[{"type": "text", "text": "second done"}]),
        ]
    )
    install_runtime(provider, _ScriptedTool([]))
    session_id = uuid4()
    warmup = await _post(user_client, session_id, "warmup")
    assert warmup.status_code == 200
    await _enable_compaction(pg_engine)

    count_messages: list[list[dict[str, Any]]] = []

    async def count_tokens(**kwargs: Any) -> int:
        count_messages.append(deepcopy(kwargs["messages"]))
        if len(count_messages) == 1:
            count_started.set()
            await release_count.wait()
            return 99_000
        return 1

    monkeypatch.setattr(provider, "estimate_tokens", count_tokens)
    first_task = asyncio.create_task(_post(user_client, session_id, "first"))
    await asyncio.wait_for(count_started.wait(), timeout=2)
    second_task = asyncio.create_task(_post(user_client, session_id, "second"))
    pending = await _wait_for_pending(user_client, session_id, 2)
    second_id = next(
        row["id"]
        for row in pending["pending_messages"]
        if row["content"][-1] == {"type": "text", "text": "second"}
    )
    release_count.set()

    first_response, second_response = await asyncio.wait_for(
        asyncio.gather(first_task, second_task),
        timeout=2,
    )

    first_count_text = [
        block.get("text")
        for message in count_messages[0]
        for block in message["content"]
        if block.get("type") == "text"
    ]
    assert "first" in first_count_text
    assert "second" not in first_count_text
    first_started = next(
        event for event in _events(first_response) if event["type"] == "turn_started"
    )
    second_started = next(
        event for event in _events(second_response) if event["type"] == "turn_started"
    )
    assert second_id not in first_started["message_ids"]
    assert second_started["message_ids"] == [second_id]
    assert len(provider.calls) == 4


async def test_oversized_request_without_eligible_history_is_sent_once_to_provider(
    user_client,
    pg_engine,
    install_runtime,
    monkeypatch,
):
    await _configure_provider(pg_engine)
    await _enable_compaction(pg_engine)
    provider = _ScriptedProvider(
        [_ProviderStep(content=[{"type": "text", "text": "provider accepted"}])]
    )

    async def count_tokens(**kwargs: Any) -> int:
        del kwargs
        return 99_000

    monkeypatch.setattr(provider, "estimate_tokens", count_tokens)
    install_runtime(provider, _ScriptedTool([]))
    session_id = uuid4()

    response = await _post(user_client, session_id, "too large")

    assert response.status_code == 200
    assert [event["status"] for event in _events(response) if event["type"] == "turn_finished"] == [
        "completed"
    ]
    assert len(provider.calls) == 1
    history = await _history(user_client, session_id)
    assert [message["message_kind"] for message in history["messages"]] == [
        "human",
        "assistant",
    ]
    assert history["status"] == "idle"


async def test_two_hundred_always_on_skills_reach_provider_and_safe_rejection_reaches_user(
    user_client,
    pg_engine,
    install_runtime,
    monkeypatch,
):
    await _configure_provider(pg_engine)
    await _enable_compaction(pg_engine)
    files = {
        f"skills/skill-{index:03}/SKILL.md": (
            "---\n"
            f"name: skill-{index:03}\n"
            f"description: Skill {index}\n"
            "always_on: true\n"
            "---\n"
            f"complete body {index}"
        ).encode()
        for index in range(200)
    }
    workspace = _SkillPromptWorkspace(files)
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[],
                error=ProviderInvocationError(
                    "Provider request failed",
                    safe_message="Provider rejected the request (HTTP 400): context window exceeded",
                ),
            )
        ]
    )
    estimates = 0

    async def estimate_tokens(**kwargs: Any) -> int:
        nonlocal estimates
        estimates += 1
        assert kwargs["system"].count("(always-on)") == 200
        assert all(f"complete body {index}" in kwargs["system"] for index in range(200))
        return 99_000

    monkeypatch.setattr(provider, "estimate_tokens", estimate_tokens)
    monkeypatch.setattr(
        "openctopus_server.workspace.skills.count_text_tokens",
        lambda text: len(text),
    )
    runtime = install_runtime(provider, _ScriptedTool([]), workspace)
    runtime.skills_cache = SkillsCache()

    response = await _post(user_client, uuid4(), "too large because of skills")

    assert response.status_code == 200
    assert [event["status"] for event in _events(response) if event["type"] == "turn_finished"] == [
        "failed"
    ]
    assert estimates == 1
    assert len(provider.calls) == 1
    persisted = next(event for event in _events(response) if event["type"] == "message_persisted")
    text = persisted["message"]["content"][0]["text"]
    assert text.startswith("[provider_unavailable]")
    assert "context window exceeded" in text


async def test_context_admission_is_held_through_provider_and_times_out_cleanly(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    started = asyncio.Event()
    release = asyncio.Event()
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[{"type": "text", "text": "first completed"}],
                started=started,
                release=release,
            )
        ]
    )
    admission = KeyedAdmission(global_limit=1, per_key_limit=1, timeout_seconds=0.5)
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=ToolRegistry((_ScriptedTool([]),)),
        context_admission=admission,
    )
    test_app.state.chat_runtime = runtime

    first_task = asyncio.create_task(_post(user_client, uuid4(), "first"))
    await asyncio.wait_for(started.wait(), timeout=2)
    assert pg_engine.pool.checkedout() == 0
    second_task = asyncio.create_task(_post(user_client, uuid4(), "second"))
    await asyncio.sleep(0.1)
    assert pg_engine.pool.checkedout() == 0
    second = await second_task
    release.set()
    first = await asyncio.wait_for(first_task, timeout=2)

    assert [event["status"] for event in _events(first) if event["type"] == "turn_finished"] == [
        "completed"
    ]
    assert [event["status"] for event in _events(second) if event["type"] == "turn_finished"] == [
        "failed"
    ]
    persisted = next(event for event in _events(second) if event["type"] == "message_persisted")
    assert persisted["message"]["content"][0]["text"] == (
        "[provider_unavailable] The server is busy preparing other conversations. Please retry."
    )
    assert len(provider.calls) == 1
    assert admission.entry_count == 0
    await runtime.close()


@pytest.mark.parametrize("failure_phase", ["preflight", "provider"])
async def test_context_admission_is_held_while_failure_is_persisted(
    user_client,
    test_app,
    pg_engine,
    monkeypatch,
    failure_phase,
):
    await _configure_provider(pg_engine)
    second_provider_started = asyncio.Event()
    failure_persistence_started = asyncio.Event()
    release_failure_persistence = asyncio.Event()
    provider_steps = []
    if failure_phase == "provider":
        provider_steps.append(
            _ProviderStep(
                content=[],
                error=ProviderInvocationError("provider failed"),
            )
        )
    provider_steps.append(
        _ProviderStep(
            content=[{"type": "text", "text": "second completed"}],
            started=second_provider_started,
        )
    )
    provider = _ScriptedProvider(provider_steps)
    admission = KeyedAdmission(global_limit=1, per_key_limit=1, timeout_seconds=2)
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=ToolRegistry((_ScriptedTool([]),)),
        context_admission=admission,
    )
    test_app.state.chat_runtime = runtime
    if failure_phase == "preflight":
        original_prepare_turn = runtime._prepare_turn
        prepare_calls = 0

        async def fail_first_preflight(pending_turn):
            nonlocal prepare_calls
            prepare_calls += 1
            if prepare_calls == 1:
                raise RuntimeError("preflight failed")
            return await original_prepare_turn(pending_turn)

        original_fail_preflight = runtime._fail_preflight

        async def block_preflight_persistence(state, failed_turn, exc):
            failure_persistence_started.set()
            await release_failure_persistence.wait()
            await original_fail_preflight(state, failed_turn, exc)

        monkeypatch.setattr(runtime, "_prepare_turn", fail_first_preflight)
        monkeypatch.setattr(runtime, "_fail_preflight", block_preflight_persistence)
    else:
        original_fail_provider = runtime._fail_provider

        async def block_provider_failure_persistence(state, failed_turn, *, error=None):
            failure_persistence_started.set()
            await release_failure_persistence.wait()
            await original_fail_provider(state, failed_turn, error=error)

        monkeypatch.setattr(runtime, "_fail_provider", block_provider_failure_persistence)

    first_task = asyncio.create_task(_post(user_client, uuid4(), "first"))
    await asyncio.wait_for(failure_persistence_started.wait(), timeout=2)
    second_task = asyncio.create_task(_post(user_client, uuid4(), "second"))
    try:
        await asyncio.sleep(0.1)
        assert not second_provider_started.is_set()
    finally:
        release_failure_persistence.set()

    first, second = await asyncio.gather(first_task, second_task)
    assert [event["status"] for event in _events(first) if event["type"] == "turn_finished"] == [
        "failed"
    ]
    assert [event["status"] for event in _events(second) if event["type"] == "turn_finished"] == [
        "completed"
    ]
    assert admission.entry_count == 0
    await runtime.close()


async def test_context_admission_is_held_during_failure_recovery(
    user_client,
    test_app,
    pg_engine,
    monkeypatch,
):
    await _configure_provider(pg_engine)
    second_provider_started = asyncio.Event()
    recovery_started = asyncio.Event()
    release_recovery = asyncio.Event()
    provider = _ScriptedProvider(
        [
            _ProviderStep(content=[], error=ProviderInvocationError("provider failed")),
            _ProviderStep(
                content=[{"type": "text", "text": "second completed"}],
                started=second_provider_started,
            ),
        ]
    )
    admission = KeyedAdmission(global_limit=1, per_key_limit=1, timeout_seconds=2)
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=ToolRegistry((_ScriptedTool([]),)),
        context_admission=admission,
    )
    test_app.state.chat_runtime = runtime
    original_fail_provider = runtime._fail_provider
    fail_provider_calls = 0

    async def fail_first_persistence(state, failed_turn, *, error=None):
        nonlocal fail_provider_calls
        fail_provider_calls += 1
        if fail_provider_calls == 1:
            raise RuntimeError("failure persistence failed")
        await original_fail_provider(state, failed_turn, error=error)

    original_recovery = runtime._fail_unexpected_chain

    async def block_recovery(state):
        recovery_started.set()
        await release_recovery.wait()
        await original_recovery(state)

    monkeypatch.setattr(runtime, "_fail_provider", fail_first_persistence)
    monkeypatch.setattr(runtime, "_fail_unexpected_chain", block_recovery)

    first_task = asyncio.create_task(_post(user_client, uuid4(), "first"))
    await asyncio.wait_for(recovery_started.wait(), timeout=2)
    second_task = asyncio.create_task(_post(user_client, uuid4(), "second"))
    try:
        await asyncio.sleep(0.1)
        assert not second_provider_started.is_set()
    finally:
        release_recovery.set()

    first, second = await asyncio.gather(first_task, second_task)
    assert [event["status"] for event in _events(first) if event["type"] == "turn_finished"] == [
        "failed"
    ]
    assert [event["status"] for event in _events(second) if event["type"] == "turn_finished"] == [
        "completed"
    ]
    assert admission.entry_count == 0
    await runtime.close()


async def test_context_admission_is_released_before_tool_execution(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    tool_started = asyncio.Event()
    release_tool = asyncio.Event()
    provider = _ScriptedProvider(
        [
            _ProviderStep(content=[_tool_use("tool-1", "one")]),
            _ProviderStep(content=[{"type": "text", "text": "second completed"}]),
            _ProviderStep(content=[{"type": "text", "text": "first completed"}]),
        ]
    )
    tool = _ScriptedTool(
        [
            _ToolStep(
                result=ToolResult(content="result"),
                started=tool_started,
                release=release_tool,
            )
        ]
    )
    admission = KeyedAdmission(global_limit=1, per_key_limit=1, timeout_seconds=2)
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=ToolRegistry((tool,)),
        context_admission=admission,
    )
    test_app.state.chat_runtime = runtime

    first_task = asyncio.create_task(_post(user_client, uuid4(), "first"))
    await asyncio.wait_for(tool_started.wait(), timeout=2)
    second = await asyncio.wait_for(_post(user_client, uuid4(), "second"), timeout=2)
    release_tool.set()
    first = await asyncio.wait_for(first_task, timeout=2)

    assert [event["status"] for event in _events(second) if event["type"] == "turn_finished"] == [
        "completed"
    ]
    assert [event["status"] for event in _events(first) if event["type"] == "turn_finished"] == [
        "completed",
        "completed",
    ]
    assert len(provider.calls) == 3
    assert admission.entry_count == 0
    await runtime.close()


async def test_stage_two_stale_pending_recaptures_as_stage_one_boundary(
    user_client,
    pg_engine,
    install_runtime,
    monkeypatch,
):
    await _configure_provider(pg_engine)
    await _enable_compaction(pg_engine)
    stage_two_summary_started = asyncio.Event()
    release_stage_two_summary = asyncio.Event()
    provider = _ScriptedProvider(
        [
            _ProviderStep(content=[_tool_use("tool-1", "one")]),
            _ProviderStep(
                content=[{"type": "text", "text": "discarded stage two"}],
                started=stage_two_summary_started,
                release=release_stage_two_summary,
            ),
            _ProviderStep(content=[{"type": "text", "text": "stage one"}]),
            _ProviderStep(content=[{"type": "text", "text": "done"}]),
        ]
    )
    counts = deque([1, 99_000, 99_000, 1])

    async def count_tokens(**kwargs: Any) -> int:
        del kwargs
        return counts.popleft()

    monkeypatch.setattr(provider, "estimate_tokens", count_tokens)
    install_runtime(
        provider,
        _ScriptedTool([_ToolStep(result=ToolResult(content="result"))]),
    )
    session_id = uuid4()

    first_task = asyncio.create_task(_post(user_client, session_id, "first"))
    await asyncio.wait_for(stage_two_summary_started.wait(), timeout=2)
    second_task = asyncio.create_task(_post(user_client, session_id, "second"))
    pending = await _wait_for_pending(user_client, session_id, 1)
    second_id = pending["pending_messages"][0]["id"]
    release_stage_two_summary.set()

    first_response, second_response = await asyncio.wait_for(
        asyncio.gather(first_task, second_task),
        timeout=2,
    )

    assert any(event["type"] == "stream_replaced" for event in _events(first_response))
    second_started = [
        event for event in _events(second_response) if event["type"] == "turn_started"
    ]
    assert second_started[0]["message_ids"] == [second_id]
    assert len(provider.calls) == 4
    history = await _history(user_client, session_id)
    summaries = [
        message
        for message in history["messages"]
        if message["message_kind"] == "compaction_summary"
    ]
    assert len(summaries) == 1
    assert summaries[0]["content"] == [{"type": "text", "text": "stage one"}]
    assert counts == deque()


async def test_unexpected_tool_exception_repairs_ambiguous_dispatch_and_drains_pending(
    user_client,
    pg_engine,
    install_runtime,
    monkeypatch,
):
    await _configure_provider(pg_engine)
    tool_started = asyncio.Event()
    release_tool = asyncio.Event()
    provider = _ScriptedProvider(
        [
            _ProviderStep(content=[_tool_use("tool-1", "one")]),
            _ProviderStep(content=[{"type": "text", "text": "recovered"}]),
        ]
    )
    runtime = install_runtime(provider, _ScriptedTool([]))

    async def fail_after_dispatch_claim(**kwargs: Any) -> ToolResult:
        del kwargs
        tool_started.set()
        await release_tool.wait()
        raise RuntimeError("unexpected runner failure")

    monkeypatch.setattr(runtime.tool_registry, "execute", fail_after_dispatch_claim)
    session_id = uuid4()

    first_task = asyncio.create_task(_post(user_client, session_id, "first"))
    await asyncio.wait_for(tool_started.wait(), timeout=2)
    second_task = asyncio.create_task(_post(user_client, session_id, "second"))
    await _wait_for_pending(user_client, session_id, 1)
    release_tool.set()

    first_response, second_response = await asyncio.wait_for(
        asyncio.gather(first_task, second_task),
        timeout=2,
    )

    first_finished = [
        event for event in _events(first_response) if event["type"] == "turn_finished"
    ]
    second_finished = [
        event for event in _events(second_response) if event["type"] == "turn_finished"
    ]
    assert [event["status"] for event in first_finished] == ["failed"]
    assert [event["status"] for event in second_finished] == ["completed"]

    history = await _history(user_client, session_id)
    assert [message["message_kind"] for message in history["messages"]] == [
        "human",
        "assistant",
        "synthetic_tool_result",
        "synthetic_assistant_error",
        "human",
        "assistant",
    ]
    assert history["messages"][2]["content"][0]["code"] == (
        ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN.value
    )
    assert history["status"] == "idle"
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        runs = list(
            (
                await db.execute(
                    select(TurnRun)
                    .where(TurnRun.session_id == session_id)
                    .order_by(TurnRun.started_at, TurnRun.id)
                )
            )
            .scalars()
            .all()
        )
    assert [run.status for run in runs] == ["failed", "completed"]


async def test_different_tool_clears_pending_trap_warning_and_repeats_new_tool(
    user_client,
    pg_engine,
    install_runtime,
):
    await _configure_provider(pg_engine)

    def other_tool(tool_id: str) -> dict[str, Any]:
        return {
            "type": "tool_use",
            "id": tool_id,
            "name": "other_tool",
            "input": {"value": "other", DEVICE_FIELD_NAME: "server"},
        }

    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[
                    _tool_use("tool-a1", "same"),
                    _tool_use("tool-a2", "same"),
                    _tool_use("tool-a3", "same"),
                    other_tool("tool-b1"),
                ]
            ),
            _ProviderStep(content=[other_tool("tool-b2"), other_tool("tool-b3")]),
            _ProviderStep(content=[{"type": "text", "text": "done"}]),
        ]
    )
    tool = _ScriptedTool(
        [
            _ToolStep(result=ToolResult(content="a1")),
            _ToolStep(result=ToolResult(content="a2")),
            _ToolStep(result=ToolResult(content="a3")),
        ]
    )
    install_runtime(provider, tool)
    session_id = uuid4()

    response = await _post(user_client, session_id, "start")

    assert response.status_code == 200
    second_call_text = [
        block["text"]
        for message in provider.calls[1]["messages"]
        for block in message["content"]
        if block.get("type") == "text"
    ]
    assert not any("You've called" in text for text in second_call_text)
    third_call_text = [
        block["text"]
        for message in provider.calls[2]["messages"]
        for block in message["content"]
        if block.get("type") == "text"
    ]
    assert [text for text in third_call_text if "You've called" in text] == [
        "You've called `other_tool` with the same args 3 times. "
        "Reconsider or ask the user for clarification."
    ]


async def test_cancel_idle_is_noop(user_client, pg_engine):
    session_id = uuid4()
    await _insert_idle_session(pg_engine, session_id)

    response = await user_client.post(f"/api/sessions/{session_id}/cancel")

    assert response.status_code == 202
    assert response.json() == {"cancel_requested": False}
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        session = await db.get(Session, session_id)
    assert session is not None
    assert session.cancel_requested is False


async def test_cancel_commit_notification_survives_caller_cancellation(
    user_client,
    pg_engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del user_client
    session_id = uuid4()
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as setup_db:
        user = (
            await setup_db.execute(select(User).where(User.email == "user@test.com"))
        ).scalar_one()
        setup_db.add(
            Session(
                id=session_id,
                user_id=user.id,
                session_key=f"web:{session_id}",
                channel="web",
                chat_id=str(session_id),
                title="New chat",
                created_at=now,
            )
        )
        setup_db.add(
            TurnRun(
                id=uuid4(),
                session_id=session_id,
                runner_instance_id=uuid4(),
                status="running",
                started_at=now,
            )
        )
        await setup_db.commit()
        user_id = user.id

    commit_completed = asyncio.Event()
    release_commit_return = asyncio.Event()
    waiter = message_service.register_cancel_waiter(session_id)
    try:
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            original_commit = db.commit

            async def commit_then_pause() -> None:
                await original_commit()
                commit_completed.set()
                await release_commit_return.wait()

            monkeypatch.setattr(db, "commit", commit_then_pause)
            cancel_task = asyncio.create_task(
                message_service.request_cancel(
                    db,
                    user_id=user_id,
                    session_id=session_id,
                )
            )
            await asyncio.wait_for(commit_completed.wait(), timeout=1)
            cancel_task.cancel()
            release_commit_return.set()
            with pytest.raises(asyncio.CancelledError):
                await cancel_task

        assert waiter.done()
        async with AsyncSession(pg_engine, expire_on_commit=False) as verify_db:
            session = await verify_db.get(Session, session_id)
        assert session is not None and session.cancel_requested is True
    finally:
        message_service.discard_cancel_waiter(session_id, waiter)


async def test_cancel_during_final_no_tools_normal_completion_wins(
    user_client,
    pg_engine,
    install_runtime,
):
    await _configure_provider(pg_engine)
    provider_started = asyncio.Event()
    release_provider = asyncio.Event()
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[{"type": "text", "text": "complete answer"}],
                started=provider_started,
                release=release_provider,
            )
        ]
    )
    tool = _ScriptedTool([])
    install_runtime(provider, tool)
    session_id = uuid4()

    post_task = asyncio.create_task(_post(user_client, session_id, "start"))
    await asyncio.wait_for(provider_started.wait(), timeout=2)
    cancel = await user_client.post(f"/api/sessions/{session_id}/cancel")
    assert cancel.status_code == 202
    assert cancel.json() == {"cancel_requested": True}
    release_provider.set()
    response = await asyncio.wait_for(post_task, timeout=2)

    finished = [event for event in _events(response) if event["type"] == "turn_finished"]
    assert [event["status"] for event in finished] == ["completed"]
    history = await _history(user_client, session_id)
    assert [message["message_kind"] for message in history["messages"]] == [
        "human",
        "assistant",
    ]
    assert history["messages"][-1]["content"] == [{"type": "text", "text": "complete answer"}]
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        session = await db.get(Session, session_id)
        run = (
            await db.execute(select(TurnRun).where(TurnRun.session_id == session_id))
        ).scalar_one()
    assert session is not None and session.cancel_requested is False
    assert run.status == "completed"


async def test_cancel_before_tool_dispatch_synthesizes_all_results(
    user_client,
    pg_engine,
    install_runtime,
):
    await _configure_provider(pg_engine)
    provider_started = asyncio.Event()
    release_provider = asyncio.Event()
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[
                    _tool_use("tool-1", "one"),
                    _tool_use("tool-2", "two"),
                ],
                started=provider_started,
                release=release_provider,
            )
        ]
    )
    tool = _ScriptedTool([])
    install_runtime(provider, tool)
    session_id = uuid4()

    post_task = asyncio.create_task(_post(user_client, session_id, "start"))
    await asyncio.wait_for(provider_started.wait(), timeout=2)
    cancel = await user_client.post(f"/api/sessions/{session_id}/cancel")
    assert cancel.json() == {"cancel_requested": True}
    release_provider.set()
    response = await asyncio.wait_for(post_task, timeout=2)

    assert tool.calls == []
    assert len(provider.calls) == 1
    events = _events(response)
    assert [event["status"] for event in events if event["type"] == "turn_finished"] == [
        "cancelled"
    ]
    assert not any(event["type"] == "tool_progress" for event in events)
    history = await _history(user_client, session_id)
    assert [message["message_kind"] for message in history["messages"]] == [
        "human",
        "assistant",
        "synthetic_tool_result",
        "synthetic_tool_result",
        "human",
    ]
    synthetic = history["messages"][2:4]
    assert [message["content"][0]["tool_use_id"] for message in synthetic] == [
        "tool-1",
        "tool-2",
    ]
    assert all(
        message["content"][0]["code"] == ErrorCode.USER_CANCELLED.value for message in synthetic
    )
    assert history["messages"][-1]["content"] == [{"type": "text", "text": "[User pressed stop]"}]


async def test_cancel_during_first_tool_stops_wait_and_marks_outcome_unknown(
    user_client,
    pg_engine,
    install_runtime,
):
    await _configure_provider(pg_engine)
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[
                    _tool_use("tool-1", "one"),
                    _tool_use("tool-2", "two"),
                    _tool_use("tool-3", "three"),
                ]
            )
        ]
    )
    tool = _ScriptedTool(
        [
            _ToolStep(
                result=ToolResult(content="result-one"),
                started=first_started,
                release=release_first,
            )
        ]
    )
    install_runtime(provider, tool)
    session_id = uuid4()

    post_task = asyncio.create_task(_post(user_client, session_id, "start"))
    await asyncio.wait_for(first_started.wait(), timeout=2)
    cancel = await user_client.post(f"/api/sessions/{session_id}/cancel")
    assert cancel.json() == {"cancel_requested": True}
    response = await asyncio.wait_for(post_task, timeout=2)

    assert tool.calls == ["one"]
    assert tool.active == 0
    assert len(provider.calls) == 1
    progress = [event for event in _events(response) if event["type"] == "tool_progress"]
    assert [(event["kind"], event["tool_call_id"]) for event in progress] == [
        ("tool_started", "tool-1"),
    ]
    history = await _history(user_client, session_id)
    assert [message["message_kind"] for message in history["messages"]] == [
        "human",
        "assistant",
        "synthetic_tool_result",
        "synthetic_tool_result",
        "synthetic_tool_result",
        "human",
    ]
    synthetic = history["messages"][2:5]
    assert [message["content"][0]["tool_use_id"] for message in synthetic] == [
        "tool-1",
        "tool-2",
        "tool-3",
    ]
    assert [message["content"][0]["code"] for message in synthetic] == [
        ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN.value,
        ErrorCode.USER_CANCELLED.value,
        ErrorCode.USER_CANCELLED.value,
    ]
    release_first.set()


async def test_known_tool_result_wins_same_tick_cancel(
    user_client,
    pg_engine,
    install_runtime,
):
    await _configure_provider(pg_engine)
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[
                    _tool_use("tool-1", "one"),
                    _tool_use("tool-2", "two"),
                ]
            )
        ]
    )
    tool = _SelfCancellingTool(pg_engine)
    install_runtime(provider, tool)
    session_id = uuid4()

    await asyncio.wait_for(_post(user_client, session_id, "start"), timeout=2)

    assert tool.calls == ["one"]
    history = await _history(user_client, session_id)
    assert [message["message_kind"] for message in history["messages"]] == [
        "human",
        "assistant",
        "tool_result",
        "synthetic_tool_result",
        "human",
    ]
    assert history["messages"][2]["content"][0]["tool_use_id"] == "tool-1"
    assert history["messages"][3]["content"][0]["tool_use_id"] == "tool-2"
    assert history["messages"][3]["content"][0]["code"] == ErrorCode.USER_CANCELLED.value


async def test_cancel_during_device_preflight_marks_current_and_remaining_cancelled(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await db.scalar(select(User).where(User.email == "user@test.com"))
        assert owner is not None
        device = Device(
            user_id=owner.id,
            name="trusted-laptop",
            token_hash=b"p" * 32,
            token_hint="preflight-token",
            restrict_to_workspace=False,
        )
        db.add(device)
        await db.commit()
        device_id = device.id

    preflight_started = asyncio.Event()

    async def block_preflight(user_id: UUID, name: str) -> UUID | None:
        del user_id
        assert name == "trusted-laptop"
        preflight_started.set()
        await asyncio.Event().wait()
        return device_id

    def exec_use(tool_id: str, command: str) -> dict[str, Any]:
        return {
            "type": "tool_use",
            "id": tool_id,
            "name": "exec",
            "input": {
                "command": command,
                DEVICE_FIELD_NAME: "trusted-laptop",
            },
        }

    provider = _ScriptedProvider(
        [_ProviderStep(content=[exec_use("exec-1", "echo one"), exec_use("exec-2", "echo two")])]
    )
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=ToolRegistry(
            (_ExecTool([]),),
            device_resolver=block_preflight,
        ),
    )
    test_app.state.chat_runtime = runtime
    session_id = uuid4()
    try:
        post_task = asyncio.create_task(_post(user_client, session_id, "start"))
        await asyncio.wait_for(preflight_started.wait(), timeout=2)

        cancel = await user_client.post(f"/api/sessions/{session_id}/cancel")
        assert cancel.json() == {"cancel_requested": True}
        response = await asyncio.wait_for(post_task, timeout=2)

        assert [
            event["status"] for event in _events(response) if event["type"] == "turn_finished"
        ] == ["cancelled"]
        history = await _history(user_client, session_id)
        synthetic = history["messages"][2:4]
        assert [message["content"][0]["tool_use_id"] for message in synthetic] == [
            "exec-1",
            "exec-2",
        ]
        assert all(
            message["content"][0]["code"] == ErrorCode.USER_CANCELLED.value for message in synthetic
        )
    finally:
        await runtime.close()


async def test_cancel_after_device_issue_consumes_late_result_without_overwrite(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await db.scalar(select(User).where(User.email == "user@test.com"))
        assert owner is not None
        device = Device(
            user_id=owner.id,
            name="trusted-laptop",
            token_hash=b"i" * 32,
            token_hint="issued-token",
            restrict_to_workspace=False,
        )
        db.add(device)
        await db.commit()
        device_id = device.id

    transport = _DeviceTransport()
    device_registry = DeviceRegistry()
    handle = await device_registry.register(
        device_id=device_id,
        user_id=owner.id,
        device_name="trusted-laptop",
        transport=transport,
    )
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[
                    {
                        "type": "tool_use",
                        "id": "exec-1",
                        "name": "exec",
                        "input": {
                            "command": "sleep 10",
                            DEVICE_FIELD_NAME: "trusted-laptop",
                        },
                    }
                ]
            )
        ]
    )
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=ToolRegistry((_ExecTool([]),)),
        device_registry=device_registry,
    )
    test_app.state.chat_runtime = runtime
    session_id = uuid4()
    try:
        post_task = asyncio.create_task(_post(user_client, session_id, "start"))
        await asyncio.wait_for(transport.sent_event.wait(), timeout=2)
        payload = json.loads(transport.sent[-1])

        cancel = await user_client.post(f"/api/sessions/{session_id}/cancel")
        assert cancel.json() == {"cancel_requested": True}
        await asyncio.wait_for(post_task, timeout=2)
        assert len(transport.sent) == 1

        before = await _history(user_client, session_id)
        synthetic = before["messages"][2]
        assert synthetic["content"][0]["code"] == (ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN.value)
        late = ToolResultFrame(
            id=UUID(payload["id"]),
            content="late success",
            is_error=False,
        )
        assert await device_registry.resolve_tool_result(handle, late) is True
        assert await _history(user_client, session_id) == before
    finally:
        await runtime.close()
        await device_registry.unregister(handle)
