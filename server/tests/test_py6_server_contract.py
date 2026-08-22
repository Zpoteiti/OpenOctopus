from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from openctopus_server.devices.protocol import (
    DeviceCapabilities,
    DeviceConfigFrame,
    HelloFrame,
    ShellMetadata,
    ToolCallFrame,
    ToolResultFrame,
    new_uuid7,
)
from openctopus_server.devices.registry import (
    DeviceOutcomeUnknownError,
    DeviceRegistry,
    DeviceUnavailableError,
)
from openctopus_server.devices.transfer import TransferDisconnectedError
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import DeviceError, WorkspaceError
from openctopus_server.errors.http import ERROR_STATUS
from openctopus_server.services import devices
from openctopus_server.tools.base import Tool, ToolContext, ToolResult
from openctopus_server.tools.device_field import DEVICE_FIELD_NAME
from openctopus_server.tools.file_transfer import FileTransferTool
from openctopus_server.tools.registry import (
    _EXEC_SCHEMA,
    _LIST_EXEC_SESSIONS_SCHEMA,
    _WRITE_STDIN_SCHEMA,
    ToolRegistry,
    _ClientOnlyTool,
    _device_result_credit,
)


@dataclass
class _Transport:
    sent: list[str] = field(default_factory=list)
    event: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(self, payload: str) -> None:
        self.sent.append(payload)
        self.event.set()

    async def send_binary(self, payload: bytes) -> None:
        del payload

    async def close(self, code: int, reason: str) -> None:
        del code, reason


@dataclass
class _BlockingTransport(_Transport):
    started: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    fail: bool = False

    async def send_text(self, payload: str) -> None:
        self.started.set()
        await self.release.wait()
        if self.fail:
            raise OSError("socket lost")
        await super().send_text(payload)


@dataclass
class _DeviceDispatcher:
    content: str = "ok"
    code: str | None = None
    calls: list[dict[str, object]] = field(default_factory=list)

    async def dispatch_tool(self, **kwargs: object) -> ToolResultFrame:
        self.calls.append(kwargs)
        return ToolResultFrame(
            id=new_uuid7(),
            content=self.content,
            is_error=self.code is not None,
            code=self.code,
        )


class _FileTool(Tool):
    def name(self) -> str:
        return "read_file"

    def schema(self) -> dict[str, Any]:
        return {
            "name": "read_file",
            "description": "Read a file",
            "input_schema": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
                "additionalProperties": False,
            },
        }

    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        del args, ctx
        return ToolResult(content="server")


def test_v3_hello_requires_shell_inventory() -> None:
    hello = HelloFrame(
        id=new_uuid7(),
        version="3",
        client_version="0.0.1",
        os="linux",
        caps=DeviceCapabilities(),
        shells=ShellMetadata(default="bash", available=["bash", "sh"]),
    )

    assert hello.shells.default == "bash"
    with pytest.raises(ValidationError):
        ShellMetadata(default="zsh", available=["bash"])

    with pytest.raises(ValidationError):
        DeviceCapabilities(exec=False)  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        DeviceCapabilities(mcp=False)  # type: ignore[call-arg]


def test_env_allowlist_rejects_reserved_prefix_case_insensitively() -> None:
    for name in ("OPENOCTOPUS_TOKEN", "openoctopus_token", "OpenOctopus_Token"):
        with pytest.raises(ValidationError):
            DeviceConfigFrame(
                workspace_path="~/workspace",
                restrict_to_workspace=False,
                ssrf_denylist=[],
                env_allowlist=[name],
            )
        with pytest.raises(DeviceError):
            devices._validate_env_allowlist([name])


def test_exec_tool_call_requires_chat_ownership_but_shared_call_does_not() -> None:
    chat_id = uuid4()  # Session rows use PostgreSQL gen_random_uuid (UUIDv4).
    call = ToolCallFrame(
        id=new_uuid7(),
        name="exec",
        args={"command": "pwd"},
        max_result_bytes=1000,
        chat_session_id=chat_id,
    )
    assert call.chat_session_id == chat_id

    with pytest.raises(ValidationError):
        ToolCallFrame(id=new_uuid7(), name="exec", args={}, max_result_bytes=1000)

    shared = ToolCallFrame(id=new_uuid7(), name="read_file", args={}, max_result_bytes=1000)
    assert shared.chat_session_id is None


async def test_dispatch_includes_hidden_chat_session_id_and_late_result_is_consumed() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    chat_id = uuid4()
    transport = _Transport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    call = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="exec",
            args={"command": "echo ok"},
            max_result_bytes=1000,
            timeout_seconds=0.01,
            chat_session_id=chat_id,
        )
    )
    await transport.event.wait()
    payload = json.loads(transport.sent[-1])
    assert payload["chat_session_id"] == str(chat_id)
    with pytest.raises(DeviceOutcomeUnknownError):
        await call
    late = ToolResultFrame(id=UUID(payload["id"]), content="ok", is_error=False)
    assert await registry.resolve_tool_result(handle, late) is True
    assert await registry.resolve_tool_result(handle, late) is False
    assert await registry.unregister(handle) is True


async def test_late_result_tombstone_does_not_hold_pending_credit() -> None:
    registry = DeviceRegistry(pending_calls_max=1, pending_calls_max_per_user=1)
    device_id = uuid4()
    user_id = uuid4()
    transport = _Transport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    first = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="read_file",
            args={"path": "a.txt"},
            max_result_bytes=1000,
            timeout_seconds=0.01,
        )
    )
    await transport.event.wait()
    payload = json.loads(transport.sent[-1])
    with pytest.raises(DeviceOutcomeUnknownError):
        await first
    assert registry.pending_count == 0

    transport.event.clear()
    second = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="read_file",
            args={"path": "b.txt"},
            max_result_bytes=1000,
            timeout_seconds=0.01,
        )
    )
    await transport.event.wait()
    with pytest.raises(DeviceOutcomeUnknownError):
        await second

    late = ToolResultFrame(id=UUID(payload["id"]), content="ok", is_error=False)
    assert await registry.resolve_tool_result(handle, late) is True
    assert registry.pending_count == 0


async def test_disconnect_after_tool_frame_send_is_outcome_unknown() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = _Transport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    call = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="read_file",
            args={"path": "a.txt"},
            max_result_bytes=1000,
            timeout_seconds=10,
        )
    )
    await transport.event.wait()
    assert await registry.unregister(handle) is True
    with pytest.raises(DeviceOutcomeUnknownError):
        await call


async def test_issued_notification_skips_preflight_and_precedes_transport_await() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    issued: list[str] = []
    with pytest.raises(DeviceUnavailableError):
        await registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="read_file",
            args={"path": "a.txt"},
            max_result_bytes=1000,
            timeout_seconds=1,
            on_issued=lambda: issued.append("preflight"),
        )
    assert issued == []

    transport = _BlockingTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    call = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="read_file",
            args={"path": "a.txt"},
            max_result_bytes=1000,
            timeout_seconds=1,
            on_issued=lambda: issued.append("sent"),
        )
    )
    await transport.started.wait()
    assert issued == ["sent"]
    assert transport.sent == []
    transport.release.set()
    await transport.event.wait()
    payload = json.loads(transport.sent[-1])
    await registry.resolve_tool_result(
        handle,
        ToolResultFrame(id=UUID(payload["id"]), content="ok", is_error=False),
    )
    await call


async def test_issued_notification_precedes_a_transport_failure() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = _BlockingTransport(fail=True)
    await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    issued: list[bool] = []
    call = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="read_file",
            args={"path": "a.txt"},
            max_result_bytes=1000,
            timeout_seconds=1,
            on_issued=lambda: issued.append(True),
        )
    )
    await transport.started.wait()
    assert issued == [True]
    transport.release.set()
    with pytest.raises(DeviceOutcomeUnknownError):
        await call


async def test_config_fence_between_send_admission_and_issue_prevents_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = _Transport()
    await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    issued: list[bool] = []
    original_mark = registry._mark_call_issued

    async def fence_then_mark(*args: object, **kwargs: object) -> bool:
        assert await registry.begin_config_update(device_id=device_id, user_id=user_id)
        return await original_mark(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(registry, "_mark_call_issued", fence_then_mark)
    with pytest.raises(DeviceUnavailableError):
        await registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="exec",
            args={"command": "pwd"},
            max_result_bytes=1000,
            timeout_seconds=1,
            expected_device_name="laptop",
            chat_session_id=uuid4(),
            on_issued=lambda: issued.append(True),
        )

    assert issued == []
    assert transport.sent == []


async def test_ambiguous_policy_commit_retires_the_fenced_connection() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=_Transport(),
    )
    assert handle is not None
    assert await registry.begin_config_update(device_id=device_id, user_id=user_id)

    await registry.retire_config_update(device_id=device_id, user_id=user_id)

    assert await registry.is_online(device_id, user_id=user_id) is False
    assert await registry.unregister(handle) is False


def test_client_only_exec_schemas_have_all_owned_device_targets() -> None:
    registry = ToolRegistry((_ClientOnlyTool("exec", _EXEC_SCHEMA),))
    assert registry.get_tool_schemas() == []
    schema = registry.get_tool_schemas(device_names=("restricted", "unrestricted"))[0]
    assert schema["input_schema"]["properties"]["openoctopus_device"]["enum"] == [
        "restricted",
        "unrestricted",
    ]
    assert "server" not in schema["input_schema"]["properties"]["openoctopus_device"]["enum"]


def test_write_stdin_schema_explains_pipe_and_tty_control_semantics() -> None:
    description = _WRITE_STDIN_SCHEMA["description"]
    assert "pipe" in description
    assert "OS interrupt" in description
    assert "不会写入 ETX" in description
    assert "tty" in description
    assert "terminate=true" in description


async def test_client_only_dispatch_accepts_every_owned_device() -> None:
    user_id = uuid4()
    sandbox_id = uuid4()
    trusted_id = uuid4()

    async def resolve(_user_id: UUID, name: str) -> UUID | None:
        return {"sandbox": sandbox_id, "trusted": trusted_id}.get(name)

    dispatcher = _DeviceDispatcher()
    registry = ToolRegistry(
        (
            _FileTool(),
            _ClientOnlyTool("exec", _EXEC_SCHEMA),
            _ClientOnlyTool("write_stdin", _WRITE_STDIN_SCHEMA),
        ),
        device_resolver=resolve,
    )
    ctx = ToolContext(user_id=user_id, session_id=uuid4())

    restricted_exec = await registry.execute(
        name="exec",
        args={"command": "pwd", DEVICE_FIELD_NAME: "sandbox"},
        ctx=ctx,
        device_targets={"sandbox": sandbox_id, "trusted": trusted_id},
        device_registry=dispatcher,
    )
    accepted = await registry.execute(
        name="exec",
        args={"command": "pwd", DEVICE_FIELD_NAME: "trusted"},
        ctx=ctx,
        device_targets={"sandbox": sandbox_id, "trusted": trusted_id},
        device_registry=dispatcher,
    )
    sandbox_file = await registry.execute(
        name="read_file",
        args={"path": "a.txt", DEVICE_FIELD_NAME: "sandbox"},
        ctx=ctx,
        device_targets={"sandbox": sandbox_id, "trusted": trusted_id},
        device_registry=dispatcher,
    )

    assert restricted_exec.is_error is False
    assert accepted.is_error is False
    assert sandbox_file.is_error is False
    assert len(dispatcher.calls) == 3


async def test_client_only_args_credit_deadline_and_result_limit_are_server_enforced() -> None:
    device_id = uuid4()
    user_id = uuid4()

    async def resolve(_user_id: UUID, _name: str) -> UUID | None:
        return device_id

    dispatcher = _DeviceDispatcher(content="x" * 20_000)
    registry = ToolRegistry(
        (
            _ClientOnlyTool("exec", _EXEC_SCHEMA),
            _ClientOnlyTool("write_stdin", _WRITE_STDIN_SCHEMA),
            _ClientOnlyTool("list_exec_sessions", _LIST_EXEC_SESSIONS_SCHEMA),
        ),
        device_resolver=resolve,
    )
    ctx = ToolContext(user_id=user_id, session_id=uuid4())

    invalid = await registry.execute(
        name="exec",
        args={"cmd": "pwd", DEVICE_FIELD_NAME: "laptop"},
        ctx=ctx,
        device_targets={"laptop": device_id},
        device_registry=dispatcher,
    )
    result = await registry.execute(
        name="exec",
        args={
            "command": "pwd",
            "yield_time_ms": 1234,
            "max_output_chars": 50_000,
            DEVICE_FIELD_NAME: "laptop",
        },
        ctx=ctx,
        device_targets={"laptop": device_id},
        device_registry=dispatcher,
    )

    assert invalid.code is ErrorCode.TOOL_INVALID_ARGS
    assert len(dispatcher.calls) == 1
    assert dispatcher.calls[0]["max_result_bytes"] == _device_result_credit("exec", 50_000)
    assert dispatcher.calls[0]["timeout_seconds"] == pytest.approx(6.234)
    assert result.content[1]["text"] == "x" * 20_000


def test_exec_result_credit_covers_maximum_unicode_cwd_metadata() -> None:
    content = "\n".join(
        (
            f"session_id={new_uuid7()}",
            "status=running",
            "tty=false",
            "shell=bash",
            "login=false",
            "exit_code=null",
            "signal=null",
            "reason=running",
            "elapsed_ms=1",
            "cwd=/" + "界" * 8192,
            "stdout=" + "x" * 1000,
            "stderr=",
            "output=",
        )
    )
    encoded = ToolResultFrame(
        id=new_uuid7(),
        content=content,
        is_error=False,
    ).model_dump_json()

    assert len(encoded.encode("utf-8")) <= _device_result_credit("exec", 1000)


async def test_exec_normalization_preserves_bounded_output_report_metadata() -> None:
    device_id = uuid4()

    async def resolve(_user_id: UUID, _name: str) -> UUID | None:
        return device_id

    report = "\n".join(
        (
            f"session_id={new_uuid7()}",
            "status=running",
            "cwd=/" + "界" * 8192,
            "stdout=" + "x" * 1000,
            "stderr=",
            "output=",
            "response_truncated_chars=0",
            "cleanup_incomplete=false",
        )
    )
    dispatcher = _DeviceDispatcher(content=report)
    registry = ToolRegistry(
        (_ClientOnlyTool("exec", _EXEC_SCHEMA),),
        device_resolver=resolve,
    )

    result = await registry.execute(
        name="exec",
        args={
            "command": "pwd",
            "max_output_chars": 1000,
            "yield_time_ms": 0,
            DEVICE_FIELD_NAME: "laptop",
        },
        ctx=ToolContext(user_id=uuid4(), session_id=uuid4()),
        device_targets={"laptop": device_id},
        device_registry=dispatcher,
    )

    assert result.is_error is False
    assert result.content[1]["text"] == report
    assert result.content[1]["text"].endswith("cleanup_incomplete=false")


async def test_device_specific_timeout_cap_is_left_to_the_current_client_config() -> None:
    device_id = uuid4()

    async def resolve(_user_id: UUID, _name: str) -> UUID | None:
        return device_id

    dispatcher = _DeviceDispatcher()
    registry = ToolRegistry(
        (_ClientOnlyTool("exec", _EXEC_SCHEMA),),
        device_resolver=resolve,
    )
    result = await registry.execute(
        name="exec",
        args={
            "command": "sleep 1",
            "timeout": 86_400,
            "yield_time_ms": 0,
            DEVICE_FIELD_NAME: "laptop",
        },
        ctx=ToolContext(user_id=uuid4(), session_id=uuid4()),
        device_targets={"laptop": device_id},
        device_registry=dispatcher,
    )

    assert result.is_error is False
    assert dispatcher.calls[0]["args"] == {
        "command": "sleep 1",
        "timeout": 86_400,
        "yield_time_ms": 0,
    }


@pytest.mark.parametrize(
    ("name", "args", "deadline"),
    [
        ("exec", {"command": "pwd"}, 35.0),
        ("write_stdin", {"session_id": "0198e2c8-592a-7000-8000-000000000001"}, 6.0),
        (
            "write_stdin",
            {
                "session_id": "0198e2c8-592a-7000-8000-000000000001",
                "wait_for": "ready",
                "wait_timeout_ms": 2500,
            },
            7.5,
        ),
        ("list_exec_sessions", {}, 10.0),
    ],
)
async def test_client_only_transport_deadlines_follow_report_window(
    name: str,
    args: dict[str, object],
    deadline: float,
) -> None:
    device_id = uuid4()

    async def resolve(_user_id: UUID, _name: str) -> UUID | None:
        return device_id

    dispatcher = _DeviceDispatcher()
    schemas = {
        "exec": _EXEC_SCHEMA,
        "write_stdin": _WRITE_STDIN_SCHEMA,
        "list_exec_sessions": _LIST_EXEC_SESSIONS_SCHEMA,
    }
    registry = ToolRegistry(
        (_ClientOnlyTool(name, schemas[name]),),
        device_resolver=resolve,
    )
    await registry.execute(
        name=name,
        args={**args, DEVICE_FIELD_NAME: "laptop"},
        ctx=ToolContext(user_id=uuid4(), session_id=uuid4()),
        device_targets={"laptop": device_id},
        device_registry=dispatcher,
    )
    assert dispatcher.calls[0]["timeout_seconds"] == pytest.approx(deadline)


@pytest.mark.parametrize(
    "code",
    [
        "tool_exec_timeout",
        "tool_exec_failed",
        "tool_exec_session_not_found",
        "tool_exec_stdin_closed",
        "tool_exec_interrupt_failed",
        "tool_shell_unavailable",
        "tool_pty_unavailable",
        "tool_shell_login_unsupported",
        "tool_client_shutting_down",
    ],
)
async def test_client_exec_stable_errors_are_preserved(code: str) -> None:
    device_id = uuid4()

    async def resolve(_user_id: UUID, _name: str) -> UUID | None:
        return device_id

    registry = ToolRegistry(
        (_ClientOnlyTool("exec", _EXEC_SCHEMA),),
        device_resolver=resolve,
    )
    result = await registry.execute(
        name="exec",
        args={"command": "pwd", DEVICE_FIELD_NAME: "laptop"},
        ctx=ToolContext(user_id=uuid4(), session_id=uuid4()),
        device_targets={"laptop": device_id},
        device_registry=_DeviceDispatcher(code=code),
    )
    assert result.code is ErrorCode(code)


async def test_transfer_disconnect_is_outcome_unknown_at_agent_and_rest_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tool = FileTransferTool(None, None, None)

    async def disconnected(*_args: object, **_kwargs: object) -> Any:
        raise TransferDisconnectedError("lost after issue")

    monkeypatch.setattr(tool, "transfer", disconnected)
    result = await tool.execute(
        {
            "openoctopus_src_device": "server",
            "src_path": "source.txt",
            "openoctopus_dst_device": "laptop",
            "dst_path": "dest.txt",
            "mode": "copy",
        },
        ToolContext(user_id=uuid4(), session_id=uuid4()),
    )
    assert result.code is ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN

    from openctopus_server.api.workspace_files import _raise_device_transfer

    with pytest.raises(WorkspaceError) as raised:
        _raise_device_transfer(
            TransferDisconnectedError("lost after issue"),
            SimpleNamespace(rest_transfer_queue_timeout_seconds=1),  # type: ignore[arg-type]
        )
    assert raised.value.code is ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN
    assert ERROR_STATUS[raised.value.code] == 409


async def test_patch_cancellation_does_not_rollback_a_committed_policy_fence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openctopus_server.api.devices import _build_validate_and_commit
    from openctopus_server.dto.device import DevicePatchRequest
    from openctopus_server.services.devices import DeviceSnapshot

    device_id = uuid4()
    user_id = uuid4()
    committed = asyncio.Event()
    release = asyncio.Event()
    snapshot = DeviceSnapshot(
        id=device_id,
        user_id=user_id,
        name="laptop",
        token_hint="hint",
        workspace_path="~/workspace",
        restrict_to_workspace=False,
        ssrf_denylist=[],
        created_at=datetime.now(UTC),
    )

    async def get_owned_by_id(*_args: object, **_kwargs: object) -> DeviceSnapshot:
        return replace(snapshot, restrict_to_workspace=True)

    async def commit(*_args: object, **_kwargs: object) -> tuple[DeviceSnapshot, bool]:
        committed.set()
        await release.wait()
        return snapshot, True

    monkeypatch.setattr(devices, "get_owned_by_id", get_owned_by_id)
    monkeypatch.setattr(devices, "commit_config_candidate", commit)

    class _DB:
        async def rollback(self) -> None:
            return None

        async def close(self) -> None:
            return None

    class _Registry:
        def __init__(self) -> None:
            self.aborted = 0
            self.pushed = 0

        async def begin_config_update(self, **_kwargs: object) -> bool:
            return True

        async def abort_config_update(self, **_kwargs: object) -> None:
            self.aborted += 1

        async def push_config(self, **_kwargs: object) -> bool:
            self.pushed += 1
            return True

        async def is_online(self, *_args: object, **_kwargs: object) -> bool:
            return True

    registry = _Registry()
    request = asyncio.create_task(
        _build_validate_and_commit(
            db=_DB(),  # type: ignore[arg-type]
            registry=registry,  # type: ignore[arg-type]
            user_id=user_id,
            device_id=device_id,
            patch=DevicePatchRequest(
                base_config_revision=1,
                restrict_to_workspace=False,
            ),
        )
    )
    await committed.wait()
    request.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await request
    assert registry.aborted == 0
    assert registry.pushed == 1


async def test_patch_db_close_failure_still_activates_committed_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openctopus_server.api.devices import _commit_and_activate_candidate
    from openctopus_server.devices.mcp_models import SourceMcpCatalog
    from openctopus_server.dto.device import DevicePatchRequest
    from openctopus_server.services.devices import DeviceSnapshot

    device_id = uuid4()
    user_id = uuid4()
    snapshot = DeviceSnapshot(
        id=device_id,
        user_id=user_id,
        name="laptop",
        token_hint="hint",
        workspace_path="~/new-workspace",
        restrict_to_workspace=False,
        ssrf_denylist=[],
        created_at=datetime.now(UTC),
    )

    async def commit(*_args: object, **_kwargs: object) -> tuple[DeviceSnapshot, bool]:
        return snapshot, True

    monkeypatch.setattr(devices, "commit_config_candidate", commit)

    class _DB:
        async def close(self) -> None:
            raise RuntimeError("close failed after commit")

    class _Registry:
        def __init__(self) -> None:
            self.aborted = 0
            self.pushed: list[DeviceConfigFrame] = []

        async def begin_config_update(self, **_kwargs: object) -> bool:
            return True

        async def abort_config_update(self, **_kwargs: object) -> None:
            self.aborted += 1

        async def push_config(self, **kwargs: object) -> bool:
            config = kwargs["config"]
            assert isinstance(config, DeviceConfigFrame)
            self.pushed.append(config)
            return True

    registry = _Registry()
    with pytest.raises(RuntimeError, match="close failed after commit"):
        await _commit_and_activate_candidate(
            _DB(),  # type: ignore[arg-type]
            registry,  # type: ignore[arg-type]
            user_id=user_id,
            patch=DevicePatchRequest(
                base_config_revision=1,
                workspace_path="~/new-workspace",
            ),
            current=replace(snapshot, workspace_path="~/old-workspace"),
            candidate_mcp=(),
            source_catalog=SourceMcpCatalog(version=1, servers=[]),
            validation=None,
        )

    assert registry.aborted == 0
    assert [config.workspace_path for config in registry.pushed] == ["~/new-workspace"]


async def test_patch_ambiguous_commit_retires_the_fenced_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openctopus_server.api.devices import _commit_and_activate_candidate
    from openctopus_server.devices.mcp_models import SourceMcpCatalog
    from openctopus_server.dto.device import DevicePatchRequest
    from openctopus_server.services.devices import DevicePatchCommitOutcomeUnknownError

    device_id = uuid4()
    user_id = uuid4()
    snapshot = devices.DeviceSnapshot(
        id=device_id,
        user_id=user_id,
        name="laptop",
        token_hint="hint",
        workspace_path="~/old-workspace",
        restrict_to_workspace=False,
        ssrf_denylist=[],
        created_at=datetime.now(UTC),
    )
    db_closed = False

    async def commit(*_args: object, **_kwargs: object) -> tuple[object, bool]:
        cause = OSError("commit acknowledgement lost")
        raise DevicePatchCommitOutcomeUnknownError(cause, device_id=device_id)

    monkeypatch.setattr(devices, "commit_config_candidate", commit)

    class _DB:
        async def close(self) -> None:
            nonlocal db_closed
            db_closed = True

    class _Registry:
        def __init__(self) -> None:
            self.aborted = 0
            self.retired = 0

        async def begin_config_update(self, **_kwargs: object) -> bool:
            return True

        async def abort_config_update(self, **_kwargs: object) -> None:
            self.aborted += 1

        async def retire_config_update(self, **_kwargs: object) -> None:
            assert db_closed
            self.retired += 1

    registry = _Registry()
    with pytest.raises(OSError, match="commit acknowledgement lost"):
        await _commit_and_activate_candidate(
            _DB(),  # type: ignore[arg-type]
            registry,  # type: ignore[arg-type]
            user_id=user_id,
            patch=DevicePatchRequest(
                base_config_revision=1,
                workspace_path="~/new-workspace",
            ),
            current=snapshot,
            candidate_mcp=(),
            source_catalog=SourceMcpCatalog(version=1, servers=[]),
            validation=None,
        )

    assert registry.aborted == 0
    assert registry.retired == 1


async def test_candidate_transition_deadline_cleans_a_stalled_precommit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openctopus_server.api.devices import _commit_and_activate_candidate
    from openctopus_server.devices.mcp_models import SourceMcpCatalog
    from openctopus_server.devices.registry import ConfigValidation, ConnectionHandle
    from openctopus_server.dto.device import DevicePatchRequest

    device_id = uuid4()
    user_id = uuid4()
    validation_id = new_uuid7()
    snapshot = devices.DeviceSnapshot(
        id=device_id,
        user_id=user_id,
        name="laptop",
        token_hint="hint",
        workspace_path="~/old-workspace",
        restrict_to_workspace=False,
        ssrf_denylist=[],
        created_at=datetime.now(UTC),
    )
    commit_started = asyncio.Event()
    commit_cancelled = asyncio.Event()

    async def commit(*_args: object, **_kwargs: object) -> tuple[object, bool]:
        commit_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            commit_cancelled.set()
            raise

    monkeypatch.setattr(devices, "commit_config_candidate", commit)

    class _DB:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    class _Registry:
        def __init__(self) -> None:
            self.aborted = 0
            self.discarded = 0
            self.retired = 0

        async def begin_config_update(self, **_kwargs: object) -> bool:
            return True

        async def abort_config_update(self, **_kwargs: object) -> None:
            self.aborted += 1

        async def discard_validated_config(self, _validation: object) -> None:
            self.discarded += 1

        async def retire_config_update(self, **_kwargs: object) -> None:
            self.retired += 1

    db = _DB()
    registry = _Registry()
    transition = asyncio.create_task(
        _commit_and_activate_candidate(
            db,  # type: ignore[arg-type]
            registry,  # type: ignore[arg-type]
            user_id=user_id,
            patch=DevicePatchRequest(
                base_config_revision=1,
                workspace_path="~/new-workspace",
            ),
            current=snapshot,
            candidate_mcp=(),
            source_catalog=SourceMcpCatalog(version=1, servers=[]),
            validation=ConfigValidation(
                id=validation_id,
                handle=ConnectionHandle(device_id=device_id, generation=1),
                source_catalog=SourceMcpCatalog(version=1, servers=[]),
            ),
            transition_deadline=asyncio.get_running_loop().time() + 0.01,
        )
    )
    await commit_started.wait()

    with pytest.raises(DeviceError) as raised:
        await asyncio.wait_for(transition, timeout=1)

    assert raised.value.code is ErrorCode.DEVICE_CONFIG_CONFLICT
    assert commit_cancelled.is_set()
    assert db.closed
    assert registry.aborted == 1
    assert registry.discarded == 1
    assert registry.retired == 0


async def test_candidate_transition_deadline_retires_an_ambiguous_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openctopus_server.api.devices import _commit_and_activate_candidate
    from openctopus_server.devices.mcp_models import SourceMcpCatalog
    from openctopus_server.dto.device import DevicePatchRequest
    from openctopus_server.services.devices import DevicePatchCommitOutcomeUnknownError

    device_id = uuid4()
    user_id = uuid4()
    snapshot = devices.DeviceSnapshot(
        id=device_id,
        user_id=user_id,
        name="laptop",
        token_hint="hint",
        workspace_path="~/old-workspace",
        restrict_to_workspace=False,
        ssrf_denylist=[],
        created_at=datetime.now(UTC),
    )
    commit_started = asyncio.Event()

    async def commit(*_args: object, **_kwargs: object) -> tuple[object, bool]:
        commit_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError as exc:
            raise DevicePatchCommitOutcomeUnknownError(exc, device_id=device_id) from exc

    monkeypatch.setattr(devices, "commit_config_candidate", commit)

    class _DB:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    class _Registry:
        def __init__(self) -> None:
            self.aborted = 0
            self.retired = 0

        async def begin_config_update(self, **_kwargs: object) -> bool:
            return True

        async def abort_config_update(self, **_kwargs: object) -> None:
            self.aborted += 1

        async def retire_config_update(self, **_kwargs: object) -> None:
            assert db.closed
            self.retired += 1

    db = _DB()
    registry = _Registry()
    transition = asyncio.create_task(
        _commit_and_activate_candidate(
            db,  # type: ignore[arg-type]
            registry,  # type: ignore[arg-type]
            user_id=user_id,
            patch=DevicePatchRequest(
                base_config_revision=1,
                workspace_path="~/new-workspace",
            ),
            current=snapshot,
            candidate_mcp=(),
            source_catalog=SourceMcpCatalog(version=1, servers=[]),
            validation=None,
            transition_deadline=asyncio.get_running_loop().time() + 0.01,
        )
    )
    await commit_started.wait()

    with pytest.raises(DeviceError) as raised:
        await asyncio.wait_for(transition, timeout=1)

    assert raised.value.code is ErrorCode.DEVICE_CONFIG_CONFLICT
    assert db.closed
    assert registry.aborted == 0
    assert registry.retired == 1


async def test_candidate_transition_deadline_ends_only_after_push_handoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openctopus_server.api.devices import _commit_and_activate_candidate
    from openctopus_server.devices.mcp_models import SourceMcpCatalog
    from openctopus_server.dto.device import DevicePatchRequest

    device_id = uuid4()
    user_id = uuid4()
    snapshot = devices.DeviceSnapshot(
        id=device_id,
        user_id=user_id,
        name="laptop",
        token_hint="hint",
        workspace_path="~/new-workspace",
        restrict_to_workspace=False,
        ssrf_denylist=[],
        created_at=datetime.now(UTC),
    )

    async def commit(*_args: object, **_kwargs: object) -> tuple[object, bool]:
        return snapshot, True

    monkeypatch.setattr(devices, "commit_config_candidate", commit)

    class _DB:
        async def close(self) -> None:
            return None

    class _Registry:
        def __init__(self) -> None:
            self.push_started = asyncio.Event()
            self.push_release = asyncio.Event()
            self.retired = 0

        async def begin_config_update(self, **_kwargs: object) -> bool:
            return True

        async def push_config(self, **kwargs: object) -> bool:
            handoff = kwargs["handoff_future"]
            assert isinstance(handoff, asyncio.Future)
            handoff.set_result(True)
            self.push_started.set()
            await self.push_release.wait()
            return True

        async def retire_config_update(self, **_kwargs: object) -> None:
            self.retired += 1

    registry = _Registry()
    transition = asyncio.create_task(
        _commit_and_activate_candidate(
            _DB(),  # type: ignore[arg-type]
            registry,  # type: ignore[arg-type]
            user_id=user_id,
            patch=DevicePatchRequest(
                base_config_revision=1,
                workspace_path="~/new-workspace",
            ),
            current=replace(snapshot, workspace_path="~/old-workspace"),
            candidate_mcp=(),
            source_catalog=SourceMcpCatalog(version=1, servers=[]),
            validation=None,
            transition_deadline=asyncio.get_running_loop().time() + 0.01,
        )
    )
    await registry.push_started.wait()
    await asyncio.sleep(0.03)
    assert not transition.done()

    registry.push_release.set()
    assert await asyncio.wait_for(transition, timeout=1) == snapshot
    assert registry.retired == 0


async def test_candidate_transition_retires_when_push_never_takes_handoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openctopus_server.api.devices import _commit_and_activate_candidate
    from openctopus_server.devices.mcp_models import SourceMcpCatalog
    from openctopus_server.dto.device import DevicePatchRequest

    device_id = uuid4()
    user_id = uuid4()
    snapshot = devices.DeviceSnapshot(
        id=device_id,
        user_id=user_id,
        name="laptop",
        token_hint="hint",
        workspace_path="~/new-workspace",
        restrict_to_workspace=False,
        ssrf_denylist=[],
        created_at=datetime.now(UTC),
    )

    async def commit(*_args: object, **_kwargs: object) -> tuple[object, bool]:
        return snapshot, True

    monkeypatch.setattr(devices, "commit_config_candidate", commit)

    class _DB:
        async def close(self) -> None:
            return None

    class _Registry:
        def __init__(self) -> None:
            self.push_started = asyncio.Event()
            self.push_cancelled = asyncio.Event()
            self.retired = 0

        async def begin_config_update(self, **_kwargs: object) -> bool:
            return True

        async def push_config(self, **_kwargs: object) -> bool:
            self.push_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                self.push_cancelled.set()
                raise

        async def retire_config_update(self, **_kwargs: object) -> None:
            self.retired += 1

    registry = _Registry()
    transition = asyncio.create_task(
        _commit_and_activate_candidate(
            _DB(),  # type: ignore[arg-type]
            registry,  # type: ignore[arg-type]
            user_id=user_id,
            patch=DevicePatchRequest(
                base_config_revision=1,
                workspace_path="~/new-workspace",
            ),
            current=replace(snapshot, workspace_path="~/old-workspace"),
            candidate_mcp=(),
            source_catalog=SourceMcpCatalog(version=1, servers=[]),
            validation=None,
            transition_deadline=asyncio.get_running_loop().time() + 0.01,
        )
    )
    await registry.push_started.wait()

    with pytest.raises(DeviceError) as raised:
        await asyncio.wait_for(transition, timeout=1)

    assert raised.value.code is ErrorCode.DEVICE_CONFIG_CONFLICT
    assert registry.push_cancelled.is_set()
    assert registry.retired == 1


async def test_settled_handoff_does_not_retire_a_published_replacement() -> None:
    from openctopus_server.api.devices import _settle_activation_and_retire

    device_id = uuid4()
    user_id = uuid4()
    registry = DeviceRegistry()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=_Transport(),
    )
    assert old_handle is not None
    assert await registry.begin_config_update(
        device_id=device_id,
        user_id=user_id,
        expected_handle=old_handle,
    )
    await registry.abort_config_update(device_id=device_id, user_id=user_id)

    replacement = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=_Transport(),
    )
    assert replacement is not None
    handoff: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
    handoff.set_result(True)
    activation = asyncio.create_task(asyncio.sleep(0))
    activation.cancel()

    await _settle_activation_and_retire(
        activation,
        registry,
        device_id=device_id,
        user_id=user_id,
        handoff=handoff,
    )

    assert await registry.is_current(replacement)


def test_outcome_unknown_is_stable_tool_error() -> None:
    # The mapping is exercised through the public result code contract.
    assert ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN.value == "tool_execution_outcome_unknown"
    assert ToolContext(user_id=uuid4(), session_id=uuid4()).session_id is not None
