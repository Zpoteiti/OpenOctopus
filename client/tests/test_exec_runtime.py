from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from openoctopus_client.config import load_config
from openoctopus_client.connection import (
    ClientRuntime,
    CloseDisposition,
    ReconnectDisposition,
    _ToolWorker,
    reconnect_disposition_from_exception,
)
from openoctopus_client.exec_sessions import ExecPolicy, ExecStart, ExecWrite
from openoctopus_client.process import ShellInventory
from openoctopus_client.protocol import (
    ConfigUpdate,
    DeviceConfig,
    Hello,
    ShellMetadata,
    ToolCall,
    ToolResult,
)
from openoctopus_client.tools.common import ToolOutput
from openoctopus_client.writer import SerializedWriter

_HELLO_ID = UUID("0190d5a7-0000-7000-8000-000000000001")
_CALL_ID = UUID("0190d5a7-0000-7000-8000-000000000002")
_UPDATE_ID = UUID("0190d5a7-0000-7000-8000-000000000003")
_CHAT_ID = UUID("00000000-0000-4000-8000-000000000004")
_SHELLS = ShellInventory(default="bash", available=("bash", "sh"))


def _environment() -> dict[str, str]:
    return {
        "OPENOCTOPUS_SERVER_URL": "https://openoctopus.example",
        "OPENOCTOPUS_DEVICE_TOKEN": "openoctopus_dev_test-token",
    }


def _hello() -> Hello:
    return Hello.new_with_id(
        _HELLO_ID,
        "0.0.1",
        "linux",
        shells=ShellMetadata(default="bash", available=["bash", "sh"]),
    )


def _config(workspace: Path, *, timeout: int = 600) -> DeviceConfig:
    return DeviceConfig(
        workspace_path=str(workspace),
        sandbox_mode=False,
        ssrf_denylist=[],
        shell_timeout_max=timeout,
        env_allowlist=["PATH", "HOME"],
    )


class _LocalDispatcher:
    async def execute(self, name: str, args: dict[str, Any]) -> ToolOutput:
        del name, args
        return ToolOutput("local")


class _RecordingExecManager:
    def __init__(self) -> None:
        self.starts: list[tuple[UUID, ExecStart]] = []
        self.writes: list[tuple[UUID, ExecWrite]] = []
        self.lists: list[UUID] = []
        self.policies: list[ExecPolicy] = []
        self.shutdown_calls = 0

    async def start(self, owner_chat: UUID, request: ExecStart) -> ToolOutput:
        self.starts.append((owner_chat, request))
        return ToolOutput("started")

    async def write(self, owner_chat: UUID, request: ExecWrite) -> ToolOutput:
        self.writes.append((owner_chat, request))
        return ToolOutput("written")

    async def list_sessions(self, owner_chat: UUID) -> ToolOutput:
        self.lists.append(owner_chat)
        return ToolOutput("listed")

    async def apply_policy(self, policy: ExecPolicy) -> None:
        self.policies.append(policy)

    async def shutdown(self) -> bool:
        self.shutdown_calls += 1
        return True


class _Socket:
    def __init__(self, frames: list[str]) -> None:
        self.frames = frames
        self.sent: list[str] = []
        self._result_sent = asyncio.Event()

    async def send(self, payload: str | bytes) -> None:
        assert isinstance(payload, str)
        self.sent.append(payload)
        if json.loads(payload).get("type") == "tool_result":
            self._result_sent.set()

    async def recv(self) -> str | None:
        if self.frames:
            return self.frames.pop(0)
        await asyncio.wait_for(self._result_sent.wait(), 1)
        return None

    async def close(self, code: int, reason: str) -> None:
        del code, reason


def _ack(workspace: Path, *, timeout: int = 600) -> str:
    return json.dumps(
        {
            "type": "hello_ack",
            "id": str(_HELLO_ID),
            "device_name": "devbox",
            "config": _config(workspace, timeout=timeout).model_dump(mode="json"),
        }
    )


def _exec_call() -> str:
    return json.dumps(
        {
            "type": "tool_call",
            "id": str(_CALL_ID),
            "name": "exec",
            "args": {"command": "printf ok", "yield_time_ms": 1},
            "chat_session_id": str(_CHAT_ID),
            "max_result_bytes": 4096,
        }
    )


@pytest.mark.asyncio
async def test_runtime_routes_exec_with_hidden_chat_owner_and_active_policy(
    tmp_path: Path,
) -> None:
    manager = _RecordingExecManager()
    runtime = ClientRuntime(
        load_config(_environment()),
        hello_factory=_hello,
        tool_dispatcher_factory=lambda *_: _LocalDispatcher(),
        shell_inventory=_SHELLS,
        exec_session_manager=manager,
    )
    socket = _Socket([_ack(tmp_path), _exec_call()])

    disposition = await runtime.run_connection(socket)

    assert disposition is CloseDisposition.RETRY
    assert len(manager.policies) == 1
    assert manager.starts[0][0] == _CHAT_ID
    assert manager.starts[0][1].policy == manager.policies[0]
    assert manager.starts[0][1].policy.shell_timeout_max == 600
    result = next(json.loads(frame) for frame in socket.sent if "tool_result" in frame)
    assert result["content"] == "started"


@pytest.mark.asyncio
async def test_runtime_binds_file_calls_after_config_update_to_the_new_config(
    tmp_path: Path,
) -> None:
    manager = _RecordingExecManager()
    runtime = ClientRuntime(
        load_config(_environment()),
        hello_factory=_hello,
        tool_dispatcher_factory=lambda *_: _LocalDispatcher(),
        shell_inventory=_SHELLS,
        exec_session_manager=manager,
    )
    update = json.dumps(
        {
            "type": "config_update",
            "id": str(_UPDATE_ID),
            "device_name": "devbox",
            "config": _config(tmp_path, timeout=120).model_dump(mode="json"),
        }
    )
    file_call = ToolCall(
        id=_CALL_ID,
        name="read_file",
        args={"path": "notes.txt"},
        max_result_bytes=4096,
    ).model_dump_json()
    socket = _Socket([_ack(tmp_path), update, file_call])

    await runtime.run_connection(socket)

    assert [policy.shell_timeout_max for policy in manager.policies] == [600, 120]
    assert manager.policies[1].epoch > manager.policies[0].epoch
    result = next(json.loads(frame) for frame in socket.sent if "tool_result" in frame)
    assert result["content"] == "local"
    assert manager.starts == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("name", "args"),
    [
        ("exec", {"command": "printf ok", "yield_time_ms": 1}),
        ("write_stdin", {"session_id": str(_CALL_ID), "chars": "x"}),
        ("list_exec_sessions", {}),
    ],
)
async def test_exec_family_is_busy_immediately_during_config_activation(
    tmp_path: Path,
    name: str,
    args: dict[str, object],
) -> None:
    policy_started = asyncio.Event()
    release_policy = asyncio.Event()

    class _BlockingPolicyManager(_RecordingExecManager):
        async def apply_policy(self, policy: ExecPolicy) -> None:
            self.policies.append(policy)
            if len(self.policies) == 2:
                policy_started.set()
                await release_policy.wait()

    manager = _BlockingPolicyManager()
    runtime = ClientRuntime(
        load_config(_environment()),
        hello_factory=_hello,
        tool_dispatcher_factory=lambda *_: _LocalDispatcher(),
        shell_inventory=_SHELLS,
        exec_session_manager=manager,
    )
    update = ConfigUpdate(
        id=_UPDATE_ID,
        device_name="devbox",
        config=_config(tmp_path, timeout=120),
    ).model_dump_json()
    call = ToolCall(
        id=_CALL_ID,
        name=name,
        args=args,
        chat_session_id=_CHAT_ID,
        max_result_bytes=4096,
    ).model_dump_json()
    class _BlockingSocket(_Socket):
        async def recv(self) -> str | None:
            if self.frames:
                return self.frames.pop(0)
            await release_policy.wait()
            return None

    socket = _BlockingSocket([_ack(tmp_path), update, call])
    connection = asyncio.create_task(runtime.run_connection(socket))

    try:
        await asyncio.wait_for(policy_started.wait(), 1)
        for _ in range(100):
            if any(json.loads(item).get("id") == str(_CALL_ID) for item in socket.sent):
                break
            await asyncio.sleep(0)
        result = next(
            json.loads(item)
            for item in socket.sent
            if json.loads(item).get("id") == str(_CALL_ID)
        )
        assert result["code"] == "tool_device_busy"
        assert result["is_error"] is True
        assert manager.starts == []
        assert connection.done() is False
    finally:
        release_policy.set()
        assert await asyncio.wait_for(connection, timeout=1) is CloseDisposition.RETRY


@pytest.mark.asyncio
async def test_same_policy_reconnect_preserves_exec_manager_state(tmp_path: Path) -> None:
    manager = _RecordingExecManager()
    runtime = ClientRuntime(
        load_config(_environment()),
        hello_factory=_hello,
        tool_dispatcher_factory=lambda *_: _LocalDispatcher(),
        shell_inventory=_SHELLS,
        exec_session_manager=manager,
    )

    await runtime.run_connection(_Socket([_ack(tmp_path), _exec_call()]))
    await runtime.run_connection(_Socket([_ack(tmp_path), _exec_call()]))

    assert len(manager.policies) == 1
    assert len(manager.starts) == 2
    assert manager.starts[0][1].policy == manager.starts[1][1].policy
    assert manager.shutdown_calls == 0


@pytest.mark.asyncio
async def test_policy_activation_finishes_before_config_task_propagates_cancellation(
    tmp_path: Path,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    class _BlockingPolicyManager(_RecordingExecManager):
        async def apply_policy(self, policy: ExecPolicy) -> None:
            self.policies.append(policy)
            started.set()
            await release.wait()

    manager = _BlockingPolicyManager()
    runtime = ClientRuntime(
        load_config(_environment()),
        hello_factory=_hello,
        tool_dispatcher_factory=lambda *_: _LocalDispatcher(),
        shell_inventory=_SHELLS,
        exec_session_manager=manager,
    )
    install = asyncio.create_task(runtime._install_config("devbox", _config(tmp_path)))
    await asyncio.wait_for(started.wait(), 1)

    install.cancel()
    await asyncio.sleep(0)
    assert not install.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await install

    await runtime._install_config("devbox", _config(tmp_path))
    assert len(manager.policies) == 1


@pytest.mark.asyncio
async def test_policy_cleanup_failure_is_sanitized_retryable_and_can_be_retried(
    tmp_path: Path,
) -> None:
    class _FailOncePolicyManager(_RecordingExecManager):
        async def apply_policy(self, policy: ExecPolicy) -> None:
            self.policies.append(policy)
            if len(self.policies) == 1:
                raise RuntimeError("old process under /secret/workspace is still alive")

    manager = _FailOncePolicyManager()
    runtime = ClientRuntime(
        load_config(_environment()),
        hello_factory=_hello,
        tool_dispatcher_factory=lambda *_: _LocalDispatcher(),
        shell_inventory=_SHELLS,
        exec_session_manager=manager,
    )

    with pytest.raises(RuntimeError) as caught:
        await runtime._install_config("devbox", _config(tmp_path))
    assert reconnect_disposition_from_exception(caught.value) is ReconnectDisposition.RETRY
    assert "/secret/workspace" not in str(caught.value)

    await runtime._install_config("devbox", _config(tmp_path))
    assert [policy.epoch for policy in manager.policies] == [1, 2]


@pytest.mark.asyncio
async def test_permanent_runtime_exit_shuts_down_all_exec_sessions() -> None:
    manager = _RecordingExecManager()

    class _PermanentFailureRuntime(ClientRuntime):
        async def _run_connection_attempt(self) -> CloseDisposition:
            raise RuntimeError("permanent protocol failure")

    runtime = _PermanentFailureRuntime(
        load_config(_environment()),
        hello_factory=_hello,
        shell_inventory=_SHELLS,
        exec_session_manager=manager,
    )

    assert await runtime.run() == 78
    assert manager.shutdown_calls == 1


@pytest.mark.asyncio
async def test_incomplete_exec_shutdown_keeps_runtime_watchdog_armed() -> None:
    class _IncompleteManager(_RecordingExecManager):
        async def shutdown(self) -> bool:
            self.shutdown_calls += 1
            return False

    manager = _IncompleteManager()
    runtime = ClientRuntime(
        load_config(_environment()),
        hello_factory=_hello,
        shell_inventory=_SHELLS,
        exec_session_manager=manager,
        hard_exit=lambda _code: None,
    )

    runtime.request_shutdown()
    await runtime._shutdown_exec_sessions()
    assert runtime._shutdown_cleanup_incomplete is True
    assert runtime._shutdown_watchdog is not None
    runtime._cancel_shutdown_watchdog()


@pytest.mark.asyncio
async def test_exec_calls_are_immediate_or_busy_instead_of_waiting_behind_fifo() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    class _Runtime:
        async def _run_tool(self, call: ToolCall, dispatcher: object) -> ToolResult:
            del dispatcher
            started.set()
            await release.wait()
            return ToolResult(id=call.id, content="done", is_error=False)

    regular = ToolCall(
        id=_CALL_ID,
        name="read_file",
        args={"path": "notes.txt"},
        max_result_bytes=4096,
    )
    shell = ToolCall(
        id=UUID("0190d5a7-0000-7000-8000-000000000005"),
        name="exec",
        args={"command": "true"},
        chat_session_id=_CHAT_ID,
        max_result_bytes=4096,
    )
    worker = _ToolWorker(_Runtime(), SerializedWriter())  # type: ignore[arg-type]
    assert worker.enqueue(regular, _LocalDispatcher())
    await asyncio.wait_for(started.wait(), 1)

    assert worker.enqueue(shell, _LocalDispatcher()) is False

    release.set()
    await worker.stop()
