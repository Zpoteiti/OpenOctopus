from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import pytest

from openoctopus_client import cli
from openoctopus_client.config import ConfigurationError, load_config
from openoctopus_client.connection import (
    ClientRuntime,
    CloseDisposition,
    ReconnectDisposition,
    _prepare_workspace,
    _ToolWorker,
    reconnect_delay,
    reconnect_disposition,
    reconnect_disposition_from_exception,
    retry_after_from_exception,
)
from openoctopus_client.protocol import (
    ConfigUpdate,
    DeviceConfig,
    ErrorFrame,
    Hello,
    HelloAck,
    Ping,
    ProtocolError,
    ToolCall,
    ToolResult,
    TransferBegin,
    TransferEnd,
    TransferRequest,
    decode_server_frame,
    encode_binary_chunk,
    encode_frame,
)
from openoctopus_client.tools import ToolOutput
from openoctopus_client.tools import dispatcher as dispatcher_module
from openoctopus_client.tools.common import ToolFailure
from openoctopus_client.writer import SerializedWriter, WriterOverflowError


def _environment() -> dict[str, str]:
    return {
        "OPENOCTOPUS_SERVER_URL": "https://openoctopus.example:8443",
        "OPENOCTOPUS_DEVICE_TOKEN": "openoctopus_dev_secret-value",
    }


def test_load_config_consumes_secret_and_builds_device_websocket_url() -> None:
    environment = _environment()

    config = load_config(environment)

    assert config.server_url == "https://openoctopus.example:8443"
    assert config.websocket_url == "wss://openoctopus.example:8443/ws/device"
    assert config.token.reveal() == "openoctopus_dev_secret-value"
    assert "OPENOCTOPUS_DEVICE_TOKEN" not in environment
    assert "secret-value" not in repr(config.token)


@pytest.mark.parametrize(
    "field,value",
    [
        ("OPENOCTOPUS_SERVER_URL", "http://host/api"),
        ("OPENOCTOPUS_SERVER_URL", "https://user@host"),
        ("OPENOCTOPUS_SERVER_URL", "ftp://host"),
        ("OPENOCTOPUS_DEVICE_TOKEN", "wrong-prefix"),
    ],
)
def test_load_config_rejects_invalid_values(field: str, value: str) -> None:
    environment = _environment()
    environment[field] = value

    with pytest.raises(ConfigurationError):
        load_config(environment)

    assert "OPENOCTOPUS_DEVICE_TOKEN" not in environment


def test_protocol_uses_active_py5_shapes_and_uuidv7_hello() -> None:
    hello = Hello.new(client_version="0.0.1", operating_system="linux")
    payload = json.loads(encode_frame(hello))

    assert hello.id.version == 7
    assert payload == {
        "caps": {
            "exec": False,
            "file_transfer": ["send", "receive"],
            "http_relay": True,
            "mcp": False,
            "shared_tools": True,
            "web_fetch": True,
        },
        "client_version": "0.0.1",
        "id": str(hello.id),
        "os": "linux",
        "type": "hello",
        "version": "1",
    }

    acknowledgement = decode_server_frame(
        json.dumps(
            {
                "type": "hello_ack",
                "id": str(hello.id),
                "device_name": "alice-laptop",
                "config": {
                    "workspace_path": "~/openoctopus/workspace",
                    "sandbox_mode": True,
                    "ssrf_denylist": ["127.0.0.0/8"],
                },
            }
        )
    )
    assert isinstance(acknowledgement, HelloAck)
    assert acknowledgement.id == hello.id
    assert acknowledgement.config.workspace_path == "~/openoctopus/workspace"


def test_protocol_rejects_unknown_and_malformed_server_frames() -> None:
    with pytest.raises(ProtocolError):
        decode_server_frame('{"type":"config_update","unexpected":true}')
    with pytest.raises(ProtocolError):
        decode_server_frame("not-json")
    with pytest.raises(ProtocolError):
        decode_server_frame(
            json.dumps(
                {
                    "type": "tool_call",
                    "id": str(UUID("0190d5a7-0000-7000-8000-000000000002")),
                    "name": "read_file",
                    "args": {"path": "notes.txt"},
                    "max_result_bytes": 1.0,
                }
            )
        )


def test_protocol_requires_uuidv7_ids_and_result_credit() -> None:
    call_id = UUID("0190d5a7-0000-7000-8000-000000000002")
    call = ToolCall(id=call_id, name="read_file", args={"path": "notes.txt"}, max_result_bytes=128)
    assert call.max_result_bytes == 128

    with pytest.raises(ValueError, match="UUID v7"):
        ToolCall(
            id=UUID("00000000-0000-4000-8000-000000000001"),
            name="read_file",
            args={},
            max_result_bytes=128,
        )
    with pytest.raises(ValueError):
        ToolCall(id=call_id, name="read_file", args={}, max_result_bytes=0)
    update = ConfigUpdate(
        id=call_id,
        device_name="alice-laptop",
        config=DeviceConfig(workspace_path="/tmp", sandbox_mode=True, ssrf_denylist=[]),
    )
    assert update.id == call_id
    with pytest.raises(ProtocolError):
        decode_server_frame(
            '{"type":"config_update","device_name":"alice","config":'
            '{"workspace_path":"/tmp","sandbox_mode":true,"ssrf_denylist":[]}}'
        )
    with pytest.raises(ValueError):
        ToolResult(
            id=call_id,
            content=cast(Any, [{"type": "tool_use", "id": "untrusted"}]),
            is_error=False,
        )
    with pytest.raises(ValueError):
        ToolResult(id=call_id, content="error without code", is_error=True)
    with pytest.raises(ValueError):
        ToolResult(id=call_id, content="success with code", is_error=False, code="unexpected")


def test_protocol_rejects_nul_config_and_invalid_transfer_purpose_fields() -> None:
    call_id = UUID("0190d5a7-0000-7000-8000-000000000002")
    with pytest.raises(ValueError, match="NUL"):
        DeviceConfig(workspace_path="/tmp/work\x00space", sandbox_mode=True, ssrf_denylist=[])
    with pytest.raises(ValueError, match="blank"):
        DeviceConfig(workspace_path="   ", sandbox_mode=True, ssrf_denylist=[])
    with pytest.raises(ValueError, match="blank"):
        DeviceConfig(workspace_path="/tmp", sandbox_mode=True, ssrf_denylist=[" \t"])
    with pytest.raises(ValueError):
        TransferRequest(id=call_id, purpose="workspace_upload", src_path="source")
    with pytest.raises(ValueError):
        TransferRequest(id=call_id, purpose="file_transfer", src_path="source")
    with pytest.raises(ValueError):
        TransferRequest(
            id=call_id,
            purpose="http_relay",
            src_path="\x00source",
        )
    with pytest.raises(ValueError):
        TransferBegin(
            id=call_id,
            direction="client_to_server",
            purpose="http_relay",
            src_path="source",
            total_bytes=None,
        )
    with pytest.raises(ValueError):
        TransferBegin(
            id=call_id,
            direction="server_to_client",
            purpose="workspace_upload",
            dst_path="  ",
        )
    with pytest.raises(ValueError):
        TransferBegin(
            id=call_id,
            direction="server_to_client",
            purpose="file_transfer",
            src_path="source",
            dst_path="destination",
            total_bytes=None,
        )
    with pytest.raises(ValueError):
        TransferEnd(id=call_id, ack=False, ok=True)
    with pytest.raises(ValueError):
        TransferEnd(id=call_id, ack=False, ok=False, code="failed", bytes_sent=1)


def test_protocol_applies_peer_field_size_limits() -> None:
    call_id = UUID("0190d5a7-0000-7000-8000-000000000002")
    Hello.new_with_id(call_id, "v" * 64, "linux")
    with pytest.raises(ValueError):
        Hello.new_with_id(call_id, "v" * 65, "linux")

    ErrorFrame(id=call_id, code="bad", message="m" * 4096)
    with pytest.raises(ValueError):
        ErrorFrame(id=call_id, code="bad", message="m" * 4097)


def test_reconnect_policy_and_backoff_are_bounded_and_deterministic() -> None:
    assert reconnect_disposition(http_status=401) == ReconnectDisposition.PERMANENT_AUTH
    assert reconnect_disposition(http_status=404) == ReconnectDisposition.PERMANENT_CONFIG
    assert reconnect_disposition(http_status=429) == ReconnectDisposition.RETRY
    assert reconnect_disposition(http_status=500) == ReconnectDisposition.RETRY
    assert reconnect_disposition(close_code=4401) == ReconnectDisposition.PERMANENT_AUTH
    assert reconnect_disposition(close_code=4409) == ReconnectDisposition.PERMANENT_CONFIG
    assert reconnect_disposition(close_code=4408) == ReconnectDisposition.RETRY
    assert reconnect_disposition(close_code=1006) == ReconnectDisposition.RETRY
    assert reconnect_disposition(close_code=1002) == ReconnectDisposition.PERMANENT_CONFIG
    assert reconnect_disposition(close_code=1000) == ReconnectDisposition.RETRY
    assert reconnect_disposition(close_code=1001) == ReconnectDisposition.RETRY
    assert reconnect_disposition() == ReconnectDisposition.RETRY
    assert reconnect_delay(0, random_value=0.5) == 1.0
    assert reconnect_delay(4, random_value=1.0) == pytest.approx(19.2)
    assert reconnect_delay(12, random_value=0.0) == pytest.approx(24.0)
    assert reconnect_delay(12, retry_after=90.0, random_value=0.0) == 30.0


def test_reconnect_helpers_handle_websocket_close_and_retry_after() -> None:
    class Response:
        status_code = 429
        headers = {"Retry-After": "12"}

    class UpgradeError(Exception):
        response = Response()

    class ClosedError(Exception):
        code = 4409

    class AbnormalClosedError(Exception):
        code = 1006

    assert reconnect_disposition_from_exception(UpgradeError()) == ReconnectDisposition.RETRY
    assert retry_after_from_exception(UpgradeError()) == 12.0
    assert (
        reconnect_disposition_from_exception(ClosedError()) == ReconnectDisposition.PERMANENT_CONFIG
    )
    assert reconnect_disposition_from_exception(AbnormalClosedError()) == (
        ReconnectDisposition.RETRY
    )
    assert reconnect_disposition_from_exception(OSError("network down")) == (
        ReconnectDisposition.RETRY
    )
    assert reconnect_disposition_from_exception(ProtocolError("bad frame")) == (
        ReconnectDisposition.PERMANENT_CONFIG
    )
    assert reconnect_disposition_from_exception(ValueError("client bug")) == (
        ReconnectDisposition.PERMANENT_CONFIG
    )


def test_runtime_closes_and_retries_after_a_malformed_server_frame() -> None:
    async def exercise() -> tuple[CloseDisposition, list[tuple[int, str]], list[str]]:
        hello_id = UUID("0190d5a7-0000-7000-8000-000000000001")
        socket = _RecordingSocket(['{"type":"not-a-server-frame"}'])
        runtime = ClientRuntime(
            load_config(_environment()),
            hello_factory=lambda: Hello.new_with_id(hello_id, "0.0.1", "linux"),
        )

        disposition = await runtime.run_connection(socket)
        return disposition, socket.closed, socket.sent

    disposition, closed, sent = asyncio.run(exercise())
    assert disposition is CloseDisposition.RETRY
    assert closed == [(1002, "protocol_error")]
    assert json.loads(sent[-1])["type"] == "error"


class _RecordingSocket:
    def __init__(self, received: list[str | None]) -> None:
        self._received = iter(received)
        self.sent: list[str] = []
        self.closed: list[tuple[int, str]] = []

    async def send(self, payload: str | bytes) -> None:
        assert isinstance(payload, str)
        self.sent.append(payload)

    async def recv(self) -> str | None:
        await asyncio.sleep(0)
        value = next(self._received)
        if value is None:
            await asyncio.sleep(0.05)
        return value

    async def close(self, code: int, reason: str) -> None:
        self.closed.append((code, reason))


def test_runtime_shutdown_wakes_recv_and_closes_with_1001(tmp_path: Path) -> None:
    async def exercise() -> tuple[CloseDisposition, list[tuple[int, str]]]:
        class BlockingSocket:
            def __init__(self) -> None:
                self.sent: list[str] = []
                self.closed: list[tuple[int, str]] = []
                self.release = asyncio.Event()

            async def send(self, payload: str | bytes) -> None:
                assert isinstance(payload, str)
                self.sent.append(payload)

            async def recv(self) -> str | None:
                await self.release.wait()
                return None

            async def close(self, code: int, reason: str) -> None:
                self.closed.append((code, reason))

        socket = BlockingSocket()
        runtime = ClientRuntime(load_config(_environment()))
        task = asyncio.create_task(runtime.run_connection(socket))
        await asyncio.sleep(0.01)
        runtime.request_shutdown()
        result = await asyncio.wait_for(task, timeout=1)
        return result, socket.closed

    disposition, closed = asyncio.run(exercise())
    assert disposition == CloseDisposition.SHUTDOWN
    assert closed == [(1001, "shutdown")]


def test_runtime_shutdown_cancels_a_writer_stuck_sending_hello(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> list[tuple[int, str]]:
        class StuckSocket:
            def __init__(self) -> None:
                self.send_started = asyncio.Event()
                self.closed: list[tuple[int, str]] = []

            async def send(self, payload: str | bytes) -> None:
                del payload
                self.send_started.set()
                await asyncio.Event().wait()

            async def recv(self) -> str | bytes | None:
                await asyncio.Event().wait()
                return None

            async def close(self, code: int, reason: str) -> None:
                self.closed.append((code, reason))

        socket = StuckSocket()
        runtime = ClientRuntime(load_config(_environment()), hard_exit=lambda _code: None)
        task = asyncio.create_task(runtime.run_connection(socket))
        await asyncio.wait_for(socket.send_started.wait(), timeout=1)
        runtime.request_shutdown()
        assert await asyncio.wait_for(task, timeout=0.5) is CloseDisposition.SHUTDOWN
        return socket.closed

    monkeypatch.setattr("openoctopus_client.connection._SHUTDOWN_GRACE_SECONDS", 0.01)
    assert asyncio.run(exercise()) == [(1001, "shutdown")]


def test_remote_eof_cancels_a_writer_stuck_sending_control(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    async def exercise() -> CloseDisposition:
        hello_id = UUID("0190d5a7-0000-7000-8000-000000000001")
        ping_id = UUID("0190d5a7-0000-7000-8000-000000000002")

        class StuckSocket:
            def __init__(self) -> None:
                self.sent = 0
                self.control_send_started = asyncio.Event()
                self.received = 0

            async def send(self, payload: str | bytes) -> None:
                del payload
                self.sent += 1
                if self.sent == 1:
                    return
                self.control_send_started.set()
                await asyncio.Event().wait()

            async def recv(self) -> str | None:
                self.received += 1
                if self.received == 1:
                    return json.dumps(
                        {
                            "type": "hello_ack",
                            "id": str(hello_id),
                            "device_name": "device",
                            "config": {
                                "workspace_path": str(tmp_path),
                                "sandbox_mode": True,
                                "ssrf_denylist": [],
                            },
                        }
                    )
                if self.received == 2:
                    return json.dumps({"type": "ping", "id": str(ping_id)})
                await self.control_send_started.wait()
                return None

            async def close(self, code: int, reason: str) -> None:
                del code, reason

        runtime = ClientRuntime(
            load_config(_environment()),
            hello_factory=lambda: Hello.new_with_id(hello_id, "0.0.1", "linux"),
        )
        return await runtime.run_connection(StuckSocket())

    monkeypatch.setattr("openoctopus_client.connection._SHUTDOWN_GRACE_SECONDS", 0.01)
    assert asyncio.run(asyncio.wait_for(exercise(), timeout=0.5)) is CloseDisposition.RETRY


def test_dispatcher_failure_still_completes_connection_cleanup(tmp_path: Path) -> None:
    async def exercise() -> None:
        hello_id = UUID("0190d5a7-0000-7000-8000-000000000001")
        call_id = UUID("0190d5a7-0000-7000-8000-000000000002")
        blocked = asyncio.Event()

        class FailingDispatcher:
            async def execute(self, name: str, args: dict[str, object]) -> ToolOutput:
                del name, args
                raise RuntimeError("dispatcher failed")

        class Socket:
            def __init__(self) -> None:
                self.sent: list[str] = []
                self.frames: list[str] = [
                    json.dumps(
                        {
                            "type": "hello_ack",
                            "id": str(hello_id),
                            "device_name": "device",
                            "config": {
                                "workspace_path": str(tmp_path),
                                "sandbox_mode": True,
                                "ssrf_denylist": [],
                            },
                        }
                    ),
                    json.dumps(
                        {
                            "type": "tool_call",
                            "id": str(call_id),
                            "name": "read_file",
                            "args": {"path": "notes.txt"},
                            "max_result_bytes": 128,
                        }
                    ),
                ]

            async def send(self, payload: str | bytes) -> None:
                assert isinstance(payload, str)
                self.sent.append(payload)

            async def recv(self) -> str | None:
                if self.frames:
                    return self.frames.pop(0)
                await blocked.wait()
                return None

            async def close(self, code: int, reason: str) -> None:
                del code, reason

        socket = Socket()
        runtime = ClientRuntime(
            load_config(_environment()),
            hello_factory=lambda: Hello.new_with_id(hello_id, "0.0.1", "linux"),
            tool_dispatcher_factory=lambda workspace, sandbox_mode, denylist: FailingDispatcher(),
        )
        with pytest.raises(RuntimeError, match="dispatcher failed"):
            await asyncio.wait_for(runtime.run_connection(socket), timeout=1)
        assert runtime._transfer_manager is None

    asyncio.run(exercise())


def test_remote_disconnect_bounds_cleanup_when_binary_send_blocks(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr("openoctopus_client.connection._SHUTDOWN_GRACE_SECONDS", 0.01)
    (tmp_path / "source.txt").write_bytes(b"source contents")

    async def exercise() -> tuple[CloseDisposition, bool]:
        hello_id = UUID("0190d5a7-0000-7000-8000-000000000001")
        slot_id = UUID("0190d5a7-0000-7000-8000-000000000002")

        class Socket:
            def __init__(self) -> None:
                self.sent: list[str | bytes] = []
                self.begin_sent = asyncio.Event()
                self.binary_send_started = asyncio.Event()
                self.frames: list[str] = [
                    json.dumps(
                        {
                            "type": "hello_ack",
                            "id": str(hello_id),
                            "device_name": "device",
                            "config": {
                                "workspace_path": str(tmp_path),
                                "sandbox_mode": True,
                                "ssrf_denylist": [],
                            },
                        }
                    ),
                    TransferRequest(
                        id=slot_id,
                        purpose="file_transfer",
                        src_path="source.txt",
                        dst_path="remote.txt",
                    ).model_dump_json(),
                ]

            async def send(self, payload: str | bytes) -> None:
                self.sent.append(payload)
                if isinstance(payload, str) and json.loads(payload).get("type") == "transfer_begin":
                    self.begin_sent.set()
                if isinstance(payload, bytes):
                    self.binary_send_started.set()
                    await asyncio.Event().wait()

            async def recv(self) -> str | None:
                if self.frames:
                    return self.frames.pop(0)
                if not self.begin_sent.is_set():
                    await self.begin_sent.wait()
                    return json.dumps({"type": "transfer_ready", "id": str(slot_id)})
                await self.binary_send_started.wait()
                return None

            async def close(self, code: int, reason: str) -> None:
                del code, reason

        socket = Socket()
        runtime = ClientRuntime(
            load_config(_environment()),
            hello_factory=lambda: Hello.new_with_id(hello_id, "0.0.1", "linux"),
        )
        disposition = await asyncio.wait_for(runtime.run_connection(socket), timeout=0.5)
        return disposition, socket.binary_send_started.is_set()

    disposition, binary_send_started = asyncio.run(exercise())
    assert disposition is CloseDisposition.RETRY
    assert binary_send_started


async def _writer_order() -> list[str]:
    socket = _RecordingSocket([])
    writer = SerializedWriter()
    task = asyncio.create_task(writer.run(socket))
    writer.enqueue_normal('{"type":"normal"}')
    writer.enqueue_critical('{"type":"critical"}')
    await writer.drain()
    await writer.stop()
    await task
    return socket.sent


def test_writer_is_serialized_prioritized_and_bounded() -> None:
    assert asyncio.run(_writer_order()) == ['{"type":"critical"}', '{"type":"normal"}']

    writer = SerializedWriter()
    for _ in range(16):
        writer.enqueue_critical("{}")
    with pytest.raises(WriterOverflowError):
        writer.enqueue_critical("{}")


async def _writer_send_failure_does_not_leave_drain_waiting() -> None:
    class FailingSocket:
        async def send(self, payload: str | bytes) -> None:
            raise OSError("synthetic disconnect")

    writer = SerializedWriter()
    writer.enqueue_normal("{}")
    task = asyncio.create_task(writer.run(FailingSocket()))
    await asyncio.wait_for(writer.drain(), timeout=1)
    with pytest.raises(OSError, match="synthetic disconnect"):
        await task


def test_writer_send_failure_releases_waiters() -> None:
    asyncio.run(_writer_send_failure_does_not_leave_drain_waiting())


async def _run_fake_lifecycle(workspace: Path) -> tuple[_RecordingSocket, CloseDisposition]:
    hello_id = UUID("0190d5a7-0000-7000-8000-000000000001")
    workspace.mkdir(parents=True)
    (workspace / "notes.txt").write_text("hello\n")
    socket = _RecordingSocket(
        [
            json.dumps(
                {
                    "type": "hello_ack",
                    "id": str(hello_id),
                    "device_name": "alice-laptop",
                    "config": {
                        "workspace_path": str(workspace),
                        "sandbox_mode": True,
                        "ssrf_denylist": [],
                    },
                }
            ),
            json.dumps({"type": "ping", "id": str(hello_id)}),
            json.dumps(
                {
                    "type": "tool_call",
                    "id": str(UUID("0190d5a7-0000-7000-8000-000000000002")),
                    "name": "read_file",
                    "args": {"path": "notes.txt"},
                    "max_result_bytes": 16_000,
                }
            ),
            None,
        ]
    )
    config = load_config(_environment())
    runtime = ClientRuntime(
        config,
        hello_factory=lambda: Hello.new_with_id(hello_id, "0.0.1", "linux"),
    )
    disposition = await runtime.run_connection(socket)
    return socket, disposition


def test_fake_websocket_lifecycle_acks_pings_and_dispatches_tool(tmp_path: Path) -> None:
    socket, disposition = asyncio.run(_run_fake_lifecycle(tmp_path / "workspace"))

    sent = [json.loads(frame) for frame in socket.sent]
    assert sent[0]["type"] == "hello"
    assert {frame["type"] for frame in sent} == {"hello", "pong", "tool_result"}
    assert next(frame for frame in sent if frame["type"] == "pong")["id"] == sent[0]["id"]
    result = next(frame for frame in sent if frame["type"] == "tool_result")
    assert result["is_error"] is False
    assert result["content"] == "1|hello"
    assert disposition == CloseDisposition.RETRY
    assert (tmp_path / "workspace").is_dir()


def test_runtime_answers_ping_while_receiver_reservation_is_slow(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import openoctopus_client.transfer as transfer_module

    hello_id = UUID("0190d5a7-0000-7000-8000-000000000001")
    slot_id = UUID("0190d5a7-0000-7000-8000-000000000002")
    ping_id = UUID("0190d5a7-0000-7000-8000-000000000003")
    started = threading.Event()
    release = threading.Event()
    original_create_temp = transfer_module._create_temp

    def blocked_create_temp(parent: Path, name: str) -> Path:
        started.set()
        release.wait(timeout=1)
        return original_create_temp(parent, name)

    monkeypatch.setattr(transfer_module, "_create_temp", blocked_create_temp)

    class Socket:
        def __init__(self) -> None:
            self.sent: list[str | bytes] = []
            self.frames: list[str | bytes | None] = [
                json.dumps(
                    {
                        "type": "hello_ack",
                        "id": str(hello_id),
                        "device_name": "device",
                        "config": {
                            "workspace_path": str(tmp_path),
                            "sandbox_mode": True,
                            "ssrf_denylist": [],
                        },
                    }
                ),
                TransferBegin(
                    id=slot_id,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.txt",
                    dst_path="result.txt",
                    total_bytes=0,
                ).model_dump_json(),
                Ping(id=ping_id).model_dump_json(),
                None,
            ]

        async def send(self, payload: str | bytes) -> None:
            self.sent.append(payload)
            if isinstance(payload, str) and json.loads(payload).get("type") == "pong":
                release.set()

        async def recv(self) -> str | bytes | None:
            frame = self.frames.pop(0)
            if isinstance(frame, str) and json.loads(frame).get("type") == "ping":
                assert await asyncio.to_thread(started.wait, 1)
            await asyncio.sleep(0)
            return frame

        async def close(self, code: int, reason: str) -> None:
            del code, reason

    async def exercise() -> tuple[list[str | bytes], CloseDisposition]:
        socket = Socket()
        runtime = ClientRuntime(
            load_config(_environment()),
            hello_factory=lambda: Hello.new_with_id(hello_id, "0.0.1", "linux"),
        )
        try:
            disposition = await asyncio.wait_for(runtime.run_connection(socket), timeout=1)
        finally:
            release.set()
        return socket.sent, disposition

    sent, disposition = asyncio.run(exercise())
    assert any(
        isinstance(frame, str)
        and json.loads(frame)["type"] == "pong"
        and json.loads(frame)["id"] == str(ping_id)
        for frame in sent
    )
    assert disposition is CloseDisposition.RETRY


def test_runtime_acknowledges_peer_failure_during_slow_receiver_reservation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import openoctopus_client.transfer as transfer_module

    hello_id = UUID("0190d5a7-0000-7000-8000-000000000001")
    slot_id = UUID("0190d5a7-0000-7000-8000-000000000002")
    ping_id = UUID("0190d5a7-0000-7000-8000-000000000003")
    started = threading.Event()
    release = threading.Event()
    original_create_temp = transfer_module._create_temp

    def blocked_create_temp(parent: Path, name: str) -> Path:
        started.set()
        release.wait(timeout=1)
        return original_create_temp(parent, name)

    monkeypatch.setattr(transfer_module, "_create_temp", blocked_create_temp)

    class Socket:
        def __init__(self) -> None:
            self.sent: list[str | bytes] = []
            self.frames: list[str | bytes | None] = [
                json.dumps(
                    {
                        "type": "hello_ack",
                        "id": str(hello_id),
                        "device_name": "device",
                        "config": {
                            "workspace_path": str(tmp_path),
                            "sandbox_mode": True,
                            "ssrf_denylist": [],
                        },
                    }
                ),
                TransferBegin(
                    id=slot_id,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.txt",
                    dst_path="result.txt",
                    total_bytes=0,
                ).model_dump_json(),
                TransferEnd(
                    id=slot_id,
                    ack=False,
                    ok=False,
                    code="workspace_transfer_timeout",
                ).model_dump_json(),
                Ping(id=ping_id).model_dump_json(),
                None,
            ]

        async def send(self, payload: str | bytes) -> None:
            self.sent.append(payload)
            if isinstance(payload, str) and json.loads(payload).get("type") == "pong":
                release.set()

        async def recv(self) -> str | bytes | None:
            frame = self.frames.pop(0)
            if isinstance(frame, str) and json.loads(frame).get("type") == "transfer_end":
                assert await asyncio.to_thread(started.wait, 1)
            await asyncio.sleep(0)
            return frame

        async def close(self, code: int, reason: str) -> None:
            del code, reason

    async def exercise() -> tuple[list[str | bytes], CloseDisposition]:
        socket = Socket()
        runtime = ClientRuntime(
            load_config(_environment()),
            hello_factory=lambda: Hello.new_with_id(hello_id, "0.0.1", "linux"),
        )
        try:
            disposition = await asyncio.wait_for(runtime.run_connection(socket), timeout=1)
        finally:
            release.set()
        return socket.sent, disposition

    sent, disposition = asyncio.run(exercise())
    transfer_ends = [
        json.loads(frame)
        for frame in sent
        if isinstance(frame, str) and json.loads(frame)["type"] == "transfer_end"
    ]
    assert len(transfer_ends) == 1
    assert transfer_ends[0]["ack"] is True
    assert transfer_ends[0]["code"] == "workspace_transfer_timeout"
    assert any(
        isinstance(frame, str)
        and json.loads(frame)["type"] == "transfer_end"
        and json.loads(frame)["ack"] is True
        and json.loads(frame)["code"] == "workspace_transfer_timeout"
        for frame in sent
    )
    assert any(
        isinstance(frame, str)
        and json.loads(frame)["type"] == "pong"
        and json.loads(frame)["id"] == str(ping_id)
        for frame in sent
    )
    assert disposition is CloseDisposition.RETRY


def test_runtime_routes_binary_transfer_frames_and_cleans_on_disconnect(tmp_path: Path) -> None:
    async def exercise() -> tuple[list[str | bytes], CloseDisposition]:
        hello_id = UUID("0190d5a7-0000-7000-8000-000000000001")
        slot_id = UUID("0190d5a7-0000-7000-8000-000000000002")

        class Socket:
            def __init__(self) -> None:
                self.sent: list[str | bytes] = []
                self.frames: list[str | bytes | None] = [
                    json.dumps(
                        {
                            "type": "hello_ack",
                            "id": str(hello_id),
                            "device_name": "device",
                            "config": {
                                "workspace_path": str(tmp_path),
                                "sandbox_mode": True,
                                "ssrf_denylist": [],
                            },
                        }
                    ),
                    TransferBegin(
                        id=slot_id,
                        direction="server_to_client",
                        purpose="workspace_upload",
                        dst_path="result.txt",
                        total_bytes=3,
                    ).model_dump_json(),
                    encode_binary_chunk(slot_id, b"abc"),
                    TransferEnd(
                        id=slot_id,
                        ack=False,
                        ok=True,
                        bytes_sent=3,
                        sha256="ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                    ).model_dump_json(),
                    None,
                ]

            async def send(self, payload: str | bytes) -> None:
                self.sent.append(payload)

            async def recv(self) -> str | bytes | None:
                await asyncio.sleep(0)
                frame = self.frames.pop(0)
                if isinstance(frame, bytes):
                    while not any(
                        isinstance(sent, str)
                        and json.loads(sent)["type"] == "transfer_ready"
                        for sent in self.sent
                    ):
                        await asyncio.sleep(0)
                if frame is None:
                    await asyncio.sleep(0.05)
                return frame

            async def close(self, code: int, reason: str) -> None:
                return None

        socket = Socket()
        runtime = ClientRuntime(
            load_config(_environment()),
            hello_factory=lambda: Hello.new_with_id(hello_id, "0.0.1", "linux"),
        )
        disposition = await runtime.run_connection(socket)
        return socket.sent, disposition

    sent, disposition = asyncio.run(exercise())
    assert (tmp_path / "result.txt").read_bytes() == b"abc"
    assert any(
        isinstance(frame, str) and json.loads(frame)["type"] == "transfer_ready" for frame in sent
    )
    assert any(
        isinstance(frame, str)
        and json.loads(frame)["type"] == "transfer_end"
        and json.loads(frame)["ack"] is True
        for frame in sent
    )
    assert disposition == CloseDisposition.RETRY


def test_runtime_rejects_tool_output_that_exceeds_result_credit(tmp_path: Path) -> None:
    runtime = ClientRuntime(load_config(_environment()))
    asyncio.run(
        runtime._install_config(
            "device",
            DeviceConfig(workspace_path=str(tmp_path), sandbox_mode=True, ssrf_denylist=[]),
        )
    )
    (tmp_path / "large.txt").write_text("x" * 1_000)
    call = ToolCall(
        id=UUID("0190d5a7-0000-7000-8000-000000000002"),
        name="read_file",
        args={"path": "large.txt"},
        max_result_bytes=220,
    )
    result = asyncio.run(runtime._run_tool(call))
    assert result.code == "tool_result_too_large"


def test_tool_worker_keeps_control_frames_live_and_captures_config_snapshot(tmp_path: Path) -> None:
    async def exercise() -> None:
        hello_id = UUID("0190d5a7-0000-7000-8000-000000000001")
        call_id = UUID("0190d5a7-0000-7000-8000-000000000002")
        ping_id = UUID("0190d5a7-0000-7000-8000-000000000003")
        update_id = UUID("0190d5a7-0000-7000-8000-000000000004")
        started = asyncio.Event()
        release = asyncio.Event()
        closed = asyncio.Event()

        class BlockingDispatcher:
            def __init__(self, label: str) -> None:
                self._label = label

            async def execute(self, name: str, args: dict[str, object]) -> ToolOutput:
                started.set()
                await release.wait()
                return ToolOutput(content=self._label)

        class Socket:
            def __init__(self) -> None:
                self.sent: list[str] = []
                self._frames = iter(
                    [
                        json.dumps(
                            {
                                "type": "hello_ack",
                                "id": str(hello_id),
                                "device_name": "device",
                                "config": {
                                    "workspace_path": str(tmp_path / "old"),
                                    "sandbox_mode": True,
                                    "ssrf_denylist": [],
                                },
                            }
                        ),
                        json.dumps(
                            {
                                "type": "tool_call",
                                "id": str(call_id),
                                "name": "read_file",
                                "args": {"path": "notes.txt"},
                                "max_result_bytes": 4_096,
                            }
                        ),
                        json.dumps({"type": "ping", "id": str(ping_id)}),
                        json.dumps(
                            {
                                "type": "config_update",
                                "id": str(update_id),
                                "device_name": "device",
                                "config": {
                                    "workspace_path": str(tmp_path / "new"),
                                    "sandbox_mode": True,
                                    "ssrf_denylist": [],
                                },
                            }
                        ),
                    ]
                )

            async def send(self, payload: str | bytes) -> None:
                assert isinstance(payload, str)
                self.sent.append(payload)

            async def recv(self) -> str | None:
                try:
                    return next(self._frames)
                except StopIteration:
                    await closed.wait()
                    return None

            async def close(self, code: int, reason: str) -> None:
                return None

        socket = Socket()
        runtime = ClientRuntime(
            load_config(_environment()),
            hello_factory=lambda: Hello.new_with_id(hello_id, "0.0.1", "linux"),
            tool_dispatcher_factory=lambda workspace, sandbox_mode, denylist: BlockingDispatcher(
                workspace.name
            ),
        )
        task = asyncio.create_task(runtime.run_connection(socket))
        await asyncio.wait_for(started.wait(), timeout=1)
        for _ in range(100):
            sent = [json.loads(frame) for frame in socket.sent]
            if any(frame["type"] == "pong" for frame in sent) and (
                runtime._active_config is not None
                and runtime._active_config.workspace_path == str(tmp_path / "new")
            ):
                break
            await asyncio.sleep(0.01)
        assert any(json.loads(frame)["type"] == "pong" for frame in socket.sent)
        assert runtime._active_config is not None
        assert runtime._active_config.workspace_path == str(tmp_path / "new")
        release.set()
        for _ in range(20):
            if any(json.loads(frame)["type"] == "tool_result" for frame in socket.sent):
                break
            await asyncio.sleep(0)
        result = next(
            json.loads(frame) for frame in socket.sent if json.loads(frame)["type"] == "tool_result"
        )
        assert result["content"] == "old"
        closed.set()
        assert await asyncio.wait_for(task, timeout=1) == CloseDisposition.RETRY

    asyncio.run(exercise())


def test_config_update_preparation_does_not_block_ping_or_bind_later_tool_to_old_config(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        hello_id = UUID("0190d5a7-0000-7000-8000-000000000001")
        update_id = UUID("0190d5a7-0000-7000-8000-000000000002")
        ping_id = UUID("0190d5a7-0000-7000-8000-000000000003")
        call_id = UUID("0190d5a7-0000-7000-8000-000000000004")
        update_started = threading.Event()
        release_update = threading.Event()
        observed_while_preparing: list[tuple[bool, bool]] = []
        close_socket = asyncio.Event()

        class Dispatcher:
            def __init__(self, label: str) -> None:
                self.label = label

            async def execute(self, name: str, args: dict[str, object]) -> ToolOutput:
                del name, args
                return ToolOutput(self.label)

        def make_dispatcher(
            workspace: Path, sandbox_mode: bool, denylist: list[str]
        ) -> Dispatcher:
            del sandbox_mode, denylist
            if workspace.name == "new":
                update_started.set()
                release_update.wait(timeout=2)
            return Dispatcher(workspace.name)

        class Socket:
            def __init__(self) -> None:
                self.sent: list[str] = []
                self.frames = iter(
                    [
                        HelloAck(
                            id=hello_id,
                            device_name="device",
                            config=DeviceConfig(
                                workspace_path=str(tmp_path / "old"),
                                sandbox_mode=True,
                                ssrf_denylist=[],
                            ),
                        ).model_dump_json(),
                        ConfigUpdate(
                            id=update_id,
                            device_name="device",
                            config=DeviceConfig(
                                workspace_path=str(tmp_path / "new"),
                                sandbox_mode=True,
                                ssrf_denylist=[],
                            ),
                        ).model_dump_json(),
                        Ping(id=ping_id).model_dump_json(),
                        ToolCall(
                            id=call_id,
                            name="read_file",
                            args={"path": "notes.txt"},
                            max_result_bytes=4096,
                        ).model_dump_json(),
                    ]
                )

            async def send(self, payload: str | bytes) -> None:
                assert isinstance(payload, str)
                self.sent.append(payload)

            async def recv(self) -> str | None:
                try:
                    return next(self.frames)
                except StopIteration:
                    await close_socket.wait()
                    return None

            async def close(self, code: int, reason: str) -> None:
                del code, reason

        (tmp_path / "old").mkdir()
        (tmp_path / "new").mkdir()
        socket = Socket()
        runtime = ClientRuntime(
            load_config(_environment()),
            hello_factory=lambda: Hello.new_with_id(hello_id, "0.0.1", "linux"),
            tool_dispatcher_factory=make_dispatcher,
        )
        task = asyncio.create_task(runtime.run_connection(socket))

        def observe_and_release() -> None:
            assert update_started.wait(timeout=1)
            time.sleep(0.05)
            frame_types = [json.loads(item)["type"] for item in socket.sent]
            observed_while_preparing.append(
                ("pong" in frame_types, "tool_result" in frame_types)
            )
            release_update.set()

        observer = threading.Thread(target=observe_and_release)
        observer.start()
        try:
            assert await asyncio.to_thread(update_started.wait, 1)
            await asyncio.to_thread(observer.join, 1)
            assert observed_while_preparing == [(True, False)]
            for _ in range(100):
                if any(json.loads(item)["type"] == "tool_result" for item in socket.sent):
                    break
                await asyncio.sleep(0.001)
            result = next(
                json.loads(item)
                for item in socket.sent
                if json.loads(item)["type"] == "tool_result"
            )
            assert result["content"] == "new"
        finally:
            release_update.set()
            close_socket.set()
            assert await asyncio.wait_for(task, timeout=1) == CloseDisposition.RETRY

    asyncio.run(exercise())


def test_config_update_backlog_is_bounded_and_overflow_is_retryable(tmp_path: Path) -> None:
    async def exercise() -> None:
        hello_id = UUID("0190d5a7-0000-7000-8000-000000000101")
        release_preparation = threading.Event()
        all_updates_received = asyncio.Event()

        class Dispatcher:
            async def execute(self, name: str, args: dict[str, object]) -> ToolOutput:
                del name, args
                return ToolOutput("unused")

        def make_dispatcher(workspace: Path, sandbox_mode: bool, denylist: list[str]) -> Dispatcher:
            del sandbox_mode, denylist
            if workspace.name != "initial":
                release_preparation.wait(timeout=2)
            return Dispatcher()

        frames = [
            HelloAck(
                id=hello_id,
                device_name="device",
                config=DeviceConfig(
                    workspace_path=str(tmp_path / "initial"),
                    sandbox_mode=True,
                    ssrf_denylist=[],
                ),
            ).model_dump_json(),
            *(
                ConfigUpdate(
                    id=UUID(f"0190d5a7-0000-7000-8000-{index:012x}"),
                    device_name="device",
                    config=DeviceConfig(
                        workspace_path=str(tmp_path / f"update-{index}"),
                        sandbox_mode=True,
                        ssrf_denylist=[],
                    ),
                ).model_dump_json()
                for index in range(1, 18)
            ),
        ]

        class Socket:
            def __init__(self) -> None:
                self.index = 0

            async def send(self, payload: str | bytes) -> None:
                del payload

            async def recv(self) -> str:
                if self.index < len(frames):
                    frame = frames[self.index]
                    self.index += 1
                    if self.index == len(frames):
                        all_updates_received.set()
                    return frame
                await asyncio.Future()
                raise AssertionError("unreachable")

            async def close(self, code: int, reason: str) -> None:
                del code, reason

        (tmp_path / "initial").mkdir()
        for index in range(1, 18):
            (tmp_path / f"update-{index}").mkdir()
        runtime = ClientRuntime(
            load_config(_environment()),
            hello_factory=lambda: Hello.new_with_id(hello_id, "0.0.1", "linux"),
            tool_dispatcher_factory=make_dispatcher,
        )
        connection = asyncio.create_task(runtime.run_connection(Socket()))
        try:
            await asyncio.wait_for(all_updates_received.wait(), timeout=1)
            release_preparation.set()
            with pytest.raises(RuntimeError) as caught:
                await asyncio.wait_for(connection, timeout=1)
            assert reconnect_disposition_from_exception(caught.value) is ReconnectDisposition.RETRY
        finally:
            release_preparation.set()
            if not connection.done():
                connection.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await connection

    asyncio.run(exercise())


def test_peer_disconnect_waits_for_residual_tool_thread_before_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        hello_id = UUID("0190d5a7-0000-7000-8000-000000000111")
        call_id = UUID("0190d5a7-0000-7000-8000-000000000112")
        incoming: asyncio.Queue[str | None] = asyncio.Queue()
        blocking_started = threading.Event()
        release_blocking = threading.Event()
        timeout_sent = asyncio.Event()

        def blocking_read(path: Path, limit: int) -> bytes:
            del path, limit
            blocking_started.set()
            release_blocking.wait(timeout=2)
            return b"content\n"

        class Socket:
            async def send(self, payload: str | bytes) -> None:
                if isinstance(payload, str):
                    frame = json.loads(payload)
                    if frame.get("type") == "tool_result" and frame.get("id") == str(call_id):
                        timeout_sent.set()

            async def recv(self) -> str | None:
                return await incoming.get()

            async def close(self, code: int, reason: str) -> None:
                del code, reason

        monkeypatch.setattr(dispatcher_module, "_read_regular", blocking_read)
        monkeypatch.setattr(dispatcher_module, "_timeout_for", lambda _name: 0.01)
        runtime = ClientRuntime(
            load_config(_environment()),
            hello_factory=lambda: Hello.new_with_id(hello_id, "0.0.1", "linux"),
        )
        connection = asyncio.create_task(runtime.run_connection(Socket()))
        await incoming.put(
            HelloAck(
                id=hello_id,
                device_name="device",
                config=DeviceConfig(
                    workspace_path=str(tmp_path),
                    sandbox_mode=True,
                    ssrf_denylist=[],
                ),
            ).model_dump_json()
        )
        await incoming.put(
            ToolCall(
                id=call_id,
                name="read_file",
                args={"path": "notes.txt"},
                max_result_bytes=4096,
            ).model_dump_json()
        )
        try:
            assert await asyncio.to_thread(blocking_started.wait, 1)
            await asyncio.wait_for(timeout_sent.wait(), timeout=1)
            await incoming.put(None)
            await asyncio.sleep(0.05)
            assert connection.done() is False

            release_blocking.set()
            assert await asyncio.wait_for(connection, timeout=1) is CloseDisposition.RETRY
        finally:
            release_blocking.set()
            if not connection.done():
                connection.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await connection

    asyncio.run(exercise())


def test_config_update_orders_following_transfer_request_without_blocking_ping(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        hello_id = UUID("0190d5a7-0000-7000-8000-000000000011")
        update_id = UUID("0190d5a7-0000-7000-8000-000000000012")
        ping_id = UUID("0190d5a7-0000-7000-8000-000000000013")
        transfer_id = UUID("0190d5a7-0000-7000-8000-000000000014")
        update_started = threading.Event()
        release_update = threading.Event()
        incoming: asyncio.Queue[str | None] = asyncio.Queue()

        class Dispatcher:
            async def execute(self, name: str, args: dict[str, object]) -> ToolOutput:
                del name, args
                return ToolOutput("unused")

        def make_dispatcher(
            workspace: Path, sandbox_mode: bool, denylist: list[str]
        ) -> Dispatcher:
            del sandbox_mode, denylist
            if workspace.name == "new":
                update_started.set()
                release_update.wait(timeout=2)
            return Dispatcher()

        class Socket:
            def __init__(self) -> None:
                self.sent: list[str] = []

            async def send(self, payload: str | bytes) -> None:
                assert isinstance(payload, str)
                self.sent.append(payload)

            async def recv(self) -> str | None:
                return await incoming.get()

            async def close(self, code: int, reason: str) -> None:
                del code, reason

        old_workspace = tmp_path / "old"
        new_workspace = tmp_path / "new"
        old_workspace.mkdir()
        new_workspace.mkdir()
        (new_workspace / "source.txt").write_text("new source", encoding="utf-8")
        socket = Socket()
        runtime = ClientRuntime(
            load_config(_environment()),
            hello_factory=lambda: Hello.new_with_id(hello_id, "0.0.1", "linux"),
            tool_dispatcher_factory=make_dispatcher,
        )
        connection = asyncio.create_task(runtime.run_connection(socket))
        await incoming.put(
            HelloAck(
                id=hello_id,
                device_name="device",
                config=DeviceConfig(
                    workspace_path=str(old_workspace),
                    sandbox_mode=True,
                    ssrf_denylist=[],
                ),
            ).model_dump_json()
        )
        await incoming.put(
            ConfigUpdate(
                id=update_id,
                device_name="device",
                config=DeviceConfig(
                    workspace_path=str(new_workspace),
                    sandbox_mode=True,
                    ssrf_denylist=[],
                ),
            ).model_dump_json()
        )
        await incoming.put(Ping(id=ping_id).model_dump_json())
        await incoming.put(
            TransferRequest(
                id=transfer_id,
                purpose="file_transfer",
                src_path="source.txt",
                dst_path="copied.txt",
            ).model_dump_json()
        )

        try:
            assert await asyncio.to_thread(update_started.wait, 1)
            for _ in range(100):
                if any(json.loads(item)["type"] == "pong" for item in socket.sent):
                    break
                await asyncio.sleep(0.001)
            frame_types = [json.loads(item)["type"] for item in socket.sent]
            assert "pong" in frame_types
            assert "transfer_begin" not in frame_types
            assert "transfer_end" not in frame_types

            release_update.set()
            for _ in range(200):
                if any(json.loads(item)["type"] == "transfer_begin" for item in socket.sent):
                    break
                await asyncio.sleep(0.001)
            begin = next(
                json.loads(item)
                for item in socket.sent
                if json.loads(item)["type"] == "transfer_begin"
            )
            assert begin["id"] == str(transfer_id)
            assert begin["total_bytes"] == len(b"new source")
        finally:
            release_update.set()
            await incoming.put(None)
            assert await asyncio.wait_for(connection, timeout=1) == CloseDisposition.RETRY

    asyncio.run(exercise())


def test_config_update_orders_following_transfer_begin_to_new_workspace(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        hello_id = UUID("0190d5a7-0000-7000-8000-000000000021")
        update_id = UUID("0190d5a7-0000-7000-8000-000000000022")
        ping_id = UUID("0190d5a7-0000-7000-8000-000000000023")
        transfer_id = UUID("0190d5a7-0000-7000-8000-000000000024")
        update_started = threading.Event()
        release_update = threading.Event()
        incoming: asyncio.Queue[str | None] = asyncio.Queue()

        class Dispatcher:
            async def execute(self, name: str, args: dict[str, object]) -> ToolOutput:
                del name, args
                return ToolOutput("unused")

        def make_dispatcher(
            workspace: Path, sandbox_mode: bool, denylist: list[str]
        ) -> Dispatcher:
            del sandbox_mode, denylist
            if workspace.name == "new":
                update_started.set()
                release_update.wait(timeout=2)
            return Dispatcher()

        class Socket:
            def __init__(self) -> None:
                self.sent: list[str] = []

            async def send(self, payload: str | bytes) -> None:
                assert isinstance(payload, str)
                self.sent.append(payload)

            async def recv(self) -> str | None:
                return await incoming.get()

            async def close(self, code: int, reason: str) -> None:
                del code, reason

        old_workspace = tmp_path / "old"
        new_workspace = tmp_path / "new"
        old_workspace.mkdir()
        new_workspace.mkdir()
        socket = Socket()
        runtime = ClientRuntime(
            load_config(_environment()),
            hello_factory=lambda: Hello.new_with_id(hello_id, "0.0.1", "linux"),
            tool_dispatcher_factory=make_dispatcher,
        )
        connection = asyncio.create_task(runtime.run_connection(socket))
        await incoming.put(
            HelloAck(
                id=hello_id,
                device_name="device",
                config=DeviceConfig(
                    workspace_path=str(old_workspace),
                    sandbox_mode=True,
                    ssrf_denylist=[],
                ),
            ).model_dump_json()
        )
        await incoming.put(
            ConfigUpdate(
                id=update_id,
                device_name="device",
                config=DeviceConfig(
                    workspace_path=str(new_workspace),
                    sandbox_mode=True,
                    ssrf_denylist=[],
                ),
            ).model_dump_json()
        )
        await incoming.put(Ping(id=ping_id).model_dump_json())
        await incoming.put(
            TransferBegin(
                id=transfer_id,
                direction="server_to_client",
                purpose="file_transfer",
                src_device="server",
                src_path="source.txt",
                dst_device="device",
                dst_path="received.txt",
                total_bytes=0,
            ).model_dump_json()
        )

        try:
            assert await asyncio.to_thread(update_started.wait, 1)
            for _ in range(100):
                if any(json.loads(item)["type"] == "pong" for item in socket.sent):
                    break
                await asyncio.sleep(0.001)
            frame_types = [json.loads(item)["type"] for item in socket.sent]
            assert "pong" in frame_types
            assert "transfer_ready" not in frame_types

            release_update.set()
            for _ in range(200):
                if any(json.loads(item)["type"] == "transfer_ready" for item in socket.sent):
                    break
                await asyncio.sleep(0.001)
            assert any(json.loads(item)["type"] == "transfer_ready" for item in socket.sent)
            await incoming.put(
                TransferEnd(
                    id=transfer_id,
                    ack=False,
                    ok=True,
                    bytes_sent=0,
                    sha256=hashlib.sha256(b"").hexdigest(),
                ).model_dump_json()
            )
            for _ in range(200):
                if any(
                    json.loads(item)["type"] == "transfer_end"
                    and json.loads(item).get("ack") is True
                    for item in socket.sent
                ):
                    break
                await asyncio.sleep(0.001)
            assert (new_workspace / "received.txt").read_bytes() == b""
            assert not (old_workspace / "received.txt").exists()
        finally:
            release_update.set()
            await incoming.put(None)
            assert await asyncio.wait_for(connection, timeout=1) == CloseDisposition.RETRY

    asyncio.run(exercise())


def test_config_preparation_failure_is_sanitized_and_retryable(tmp_path: Path) -> None:
    async def exercise() -> BaseException:
        hello_id = UUID("0190d5a7-0000-7000-8000-000000000001")

        def reject(
            workspace: Path, sandbox_mode: bool, denylist: list[str]
        ) -> Any:
            del workspace, sandbox_mode, denylist
            raise ToolFailure(
                "workspace_permission_denied",
                f"cannot inspect {tmp_path / 'private-secret'}",
            )

        socket = _RecordingSocket(
            [
                HelloAck(
                    id=hello_id,
                    device_name="device",
                    config=DeviceConfig(
                        workspace_path=str(tmp_path),
                        sandbox_mode=True,
                        ssrf_denylist=[],
                    ),
                ).model_dump_json()
            ]
        )
        runtime = ClientRuntime(
            load_config(_environment()),
            hello_factory=lambda: Hello.new_with_id(hello_id, "0.0.1", "linux"),
            tool_dispatcher_factory=reject,
        )
        try:
            await runtime.run_connection(socket)
        except BaseException as exc:
            return exc
        raise AssertionError("configuration preparation unexpectedly succeeded")

    failure = asyncio.run(exercise())
    assert reconnect_disposition_from_exception(failure) is ReconnectDisposition.RETRY
    assert "private-secret" not in str(failure)


def test_tool_worker_waits_for_timed_out_thread_before_dequeuing_next_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def exercise() -> None:
        first_started = threading.Event()
        release_first = threading.Event()
        calls = 0
        sent = asyncio.Event()

        def blocking_read(path: Path, limit: int) -> bytes:
            nonlocal calls
            del path, limit
            calls += 1
            if calls == 1:
                first_started.set()
                release_first.wait(timeout=2)
            return b"content\n"

        class Writer:
            def __init__(self) -> None:
                self.frames: list[str] = []

            def enqueue_normal(self, payload: str) -> None:
                self.frames.append(payload)
                sent.set()

        monkeypatch.setattr(dispatcher_module, "_read_regular", blocking_read)
        monkeypatch.setattr(dispatcher_module, "_timeout_for", lambda _name: 0.01)
        dispatcher = dispatcher_module.ClientToolDispatcher(
            tmp_path, sandbox_mode=True, ssrf_denylist=[]
        )
        runtime = ClientRuntime(load_config(_environment()))
        writer = Writer()
        worker = _ToolWorker(runtime, cast(Any, writer))
        first = ToolCall(
            id=UUID("0190d5a7-0000-7000-8000-000000000002"),
            name="read_file",
            args={"path": "first.txt"},
            max_result_bytes=4096,
        )
        second = ToolCall(
            id=UUID("0190d5a7-0000-7000-8000-000000000003"),
            name="read_file",
            args={"path": "second.txt"},
            max_result_bytes=4096,
        )
        assert worker.enqueue(first, dispatcher)
        assert worker.enqueue(second, dispatcher)
        try:
            assert await asyncio.to_thread(first_started.wait, 1)
            await asyncio.wait_for(sent.wait(), timeout=1)
            assert json.loads(writer.frames[0])["code"] == "tool_exec_timeout"
            await asyncio.sleep(0.05)
            assert calls == 1
            release_first.set()
            for _ in range(100):
                if len(writer.frames) == 2:
                    break
                await asyncio.sleep(0.001)
            assert len(writer.frames) == 2
            assert calls == 2
        finally:
            release_first.set()
            await worker.stop()

    asyncio.run(exercise())


def test_tool_worker_bounds_waiting_calls_and_releases_retained_bytes() -> None:
    async def exercise() -> None:
        started = asyncio.Event()

        class Runtime:
            async def _run_tool(self, call: ToolCall, dispatcher: object) -> ToolResult:
                started.set()
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

        call = ToolCall(
            id=UUID("0190d5a7-0000-7000-8000-000000000002"),
            name="read_file",
            args={"path": "notes.txt"},
            max_result_bytes=1,
        )
        fake_dispatcher: Any = object()
        worker = _ToolWorker(cast(Any, Runtime()), SerializedWriter())
        assert worker.enqueue(call, fake_dispatcher)
        await asyncio.wait_for(started.wait(), timeout=1)
        assert all(worker.enqueue(call, fake_dispatcher) for _ in range(64))
        assert not worker.enqueue(call, fake_dispatcher)
        await worker.stop()
        assert worker._retained_bytes == 0

    asyncio.run(exercise())


def test_shutdown_watchdog_bounds_a_blocking_filesystem_mutation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr("openoctopus_client.connection._SHUTDOWN_GRACE_SECONDS", 0.05)

    async def exercise() -> None:
        started = threading.Event()
        released = threading.Event()
        forced = threading.Event()
        forced_codes: list[int] = []

        class BlockingDispatcher:
            async def execute(self, name: str, args: dict[str, Any]) -> ToolOutput:
                del name, args

                def blocked_mutation() -> None:
                    started.set()
                    released.wait()

                await dispatcher_module._run_mutation(blocked_mutation)
                return ToolOutput("finished")

        def hard_exit(code: int) -> None:
            forced_codes.append(code)
            forced.set()
            released.set()

        runtime = ClientRuntime(load_config(_environment()), hard_exit=hard_exit)
        worker = _ToolWorker(runtime, SerializedWriter())
        call = ToolCall(
            id=UUID("0190d5a7-0000-7000-8000-000000000002"),
            name="write_file",
            args={"path": "notes.txt", "content": "new"},
            max_result_bytes=128,
        )
        assert worker.enqueue(call, BlockingDispatcher())
        assert await asyncio.to_thread(started.wait, 1)

        runtime.request_shutdown()
        began = time.monotonic()
        assert await worker.stop(timeout=0.01) is False
        assert time.monotonic() - began < 0.2
        assert not forced.is_set()

        assert await asyncio.to_thread(forced.wait, 1)
        assert forced_codes == [1]
        await asyncio.wait_for(worker.stop(timeout=0.2), timeout=1)
        runtime._cancel_shutdown_watchdog()

    asyncio.run(exercise())


def test_shutdown_watchdog_bounds_a_blocking_filesystem_read(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr("openoctopus_client.connection._SHUTDOWN_GRACE_SECONDS", 0.05)

    async def exercise() -> None:
        started = threading.Event()
        released = threading.Event()
        forced = threading.Event()
        forced_codes: list[int] = []

        def blocked_read(path: Path, limit: int) -> bytes:
            del path, limit
            started.set()
            released.wait()
            return b"finished\n"

        monkeypatch.setattr(dispatcher_module, "_read_regular", blocked_read)
        dispatcher = dispatcher_module.ClientToolDispatcher(
            tmp_path, sandbox_mode=True, ssrf_denylist=[]
        )

        def hard_exit(code: int) -> None:
            forced_codes.append(code)
            forced.set()
            released.set()

        runtime = ClientRuntime(load_config(_environment()), hard_exit=hard_exit)
        worker = _ToolWorker(runtime, SerializedWriter())
        call = ToolCall(
            id=UUID("0190d5a7-0000-7000-8000-000000000002"),
            name="read_file",
            args={"path": "notes.txt"},
            max_result_bytes=128,
        )
        assert worker.enqueue(call, dispatcher)
        assert await asyncio.to_thread(started.wait, 1)

        runtime.request_shutdown()
        began = time.monotonic()
        assert await worker.stop(timeout=0.01) is False
        assert time.monotonic() - began < 0.2
        assert not forced.is_set()

        assert await asyncio.to_thread(forced.wait, 1)
        assert forced_codes == [1]
        await asyncio.wait_for(worker.stop(timeout=0.2), timeout=1)
        runtime._cancel_shutdown_watchdog()

    asyncio.run(exercise())


def test_shutdown_watchdog_bounds_blocking_workspace_config_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("openoctopus_client.connection._SHUTDOWN_GRACE_SECONDS", 0.05)

    async def exercise() -> None:
        started = threading.Event()
        released = threading.Event()
        forced = threading.Event()
        forced_codes: list[int] = []
        hello_id = UUID("0190d5a7-0000-7000-8000-000000000001")

        def blocked_workspace(path: str) -> Path:
            del path
            started.set()
            released.wait()
            return Path("/tmp/workspace")

        monkeypatch.setattr("openoctopus_client.connection._prepare_workspace", blocked_workspace)

        def hard_exit(code: int) -> None:
            forced_codes.append(code)
            forced.set()

        socket = _RecordingSocket(
            [
                json.dumps(
                    {
                        "type": "hello_ack",
                        "id": str(hello_id),
                        "device_name": "device",
                        "config": {
                            "workspace_path": "/tmp/workspace",
                            "sandbox_mode": True,
                            "ssrf_denylist": [],
                        },
                    }
                )
            ]
        )
        runtime = ClientRuntime(
            load_config(_environment()),
            hello_factory=lambda: Hello.new_with_id(hello_id, "0.0.1", "linux"),
            hard_exit=hard_exit,
        )
        connection = asyncio.create_task(runtime.run_connection(socket))
        assert await asyncio.to_thread(started.wait, 1)

        runtime.request_shutdown()
        connection.cancel()
        with pytest.raises(asyncio.CancelledError):
            await connection
        assert runtime._shutdown_cleanup_incomplete is True
        assert await asyncio.to_thread(forced.wait, 1)
        assert forced_codes == [1]
        released.set()
        assert await runtime._wait_for_config_tasks(timeout=1)
        runtime._cancel_shutdown_watchdog()

    asyncio.run(exercise())


def test_incomplete_shutdown_keeps_the_hard_exit_watchdog_armed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("openoctopus_client.connection._SHUTDOWN_GRACE_SECONDS", 1.0)

    async def exercise() -> bool:
        runtime = ClientRuntime(load_config(_environment()), hard_exit=lambda _code: None)
        runtime._shutdown_cleanup_incomplete = True
        runtime.request_shutdown()

        assert await runtime.run() == 0
        armed = runtime._shutdown_watchdog is not None
        runtime._cancel_shutdown_watchdog()
        return armed

    assert asyncio.run(exercise()) is True


def test_runtime_does_not_leave_device_token_in_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENOCTOPUS_SERVER_URL", "https://openoctopus.example")
    monkeypatch.setenv("OPENOCTOPUS_DEVICE_TOKEN", "openoctopus_dev_secret-value")

    load_config()

    assert "OPENOCTOPUS_DEVICE_TOKEN" not in os.environ


def test_cli_run_requires_environment_without_echoing_secret(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("OPENOCTOPUS_SERVER_URL", raising=False)
    monkeypatch.setenv("OPENOCTOPUS_DEVICE_TOKEN", "openoctopus_dev_secret-value")
    monkeypatch.setattr("sys.argv", ["openoctopus-client", "run"])

    assert cli.main() == 78

    captured = capsys.readouterr()
    assert "OPENOCTOPUS_SERVER_URL is required" in captured.err
    assert "secret-value" not in captured.err
    assert "OPENOCTOPUS_DEVICE_TOKEN" not in os.environ


def test_workspace_expands_only_current_user_home(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)

    workspace = _prepare_workspace("~another-user/workspace")

    assert workspace == Path("~another-user/workspace")
    assert (tmp_path / workspace).is_dir()
