from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from uuid import uuid4

import pytest

from openctopus_server.api.device_ws import WebSocketTransport
from openctopus_server.devices.protocol import (
    MAX_TEXT_FRAME_BYTES,
    DeviceConfigFrame,
    encode_binary_chunk,
    new_uuid7,
)
from openctopus_server.devices.registry import (
    DeviceOutcomeUnknownError,
    DeviceProtocolError,
    DeviceRegistry,
)


@dataclass
class _Transport:
    sent_text: list[str] = field(default_factory=list)
    closes: list[tuple[int, str]] = field(default_factory=list)
    fail_ping: bool = False

    async def send_text(self, payload: str) -> None:
        if self.fail_ping and json.loads(payload).get("type") == "ping":
            raise OSError("socket is closed")
        self.sent_text.append(payload)

    async def close(self, code: int, reason: str) -> None:
        self.closes.append((code, reason))


@pytest.mark.asyncio
async def test_heartbeat_send_failure_unregisters_generation_and_fails_pending() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = _Transport(fail_ping=True)
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    pending = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="read_file",
            args={"path": "a.txt"},
            max_result_bytes=1024,
            timeout_seconds=10,
        )
    )
    for _ in range(100):
        if transport.sent_text:
            break
        await asyncio.sleep(0)

    await _heartbeat_for_test(registry, handle, transport)

    assert await registry.is_online(device_id, user_id=user_id) is False
    with pytest.raises(DeviceOutcomeUnknownError):
        await pending


async def _heartbeat_for_test(registry: DeviceRegistry, handle, transport) -> None:
    from openctopus_server.api import device_ws

    await asyncio.wait_for(
        device_ws._heartbeat(
            registry,
            handle,
            transport,
            ping_interval_seconds=0,
            liveness_timeout_seconds=10,
        ),
        timeout=1,
    )


@pytest.mark.asyncio
async def test_oversized_tool_call_is_rejected_before_pending_admission() -> None:
    registry = DeviceRegistry()
    transport = _Transport()
    device_id = uuid4()
    user_id = uuid4()
    await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )

    with pytest.raises(DeviceProtocolError, match="12 MiB"):
        await registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="write_file",
            args={"path": "a.txt", "content": "x" * MAX_TEXT_FRAME_BYTES},
            max_result_bytes=1024,
            timeout_seconds=1,
        )

    assert registry.pending_count == 0
    assert transport.sent_text == []


def test_workspace_path_rejects_nul_in_protocol_frame() -> None:
    with pytest.raises(ValueError, match="NUL"):
        DeviceConfigFrame(
            workspace_path="/tmp/work\x00space",
            sandbox_mode=True,
            ssrf_denylist=[],
        )
    with pytest.raises(ValueError, match="NUL"):
        DeviceConfigFrame(
            workspace_path="/tmp/workspace",
            sandbox_mode=True,
            ssrf_denylist=["127.0.0.1\x00/32"],
        )


@pytest.mark.asyncio
async def test_revocation_epochs_are_bounded_and_expire() -> None:
    registry = DeviceRegistry(
        revocation_epoch_max_entries=2,
        revocation_epoch_ttl_seconds=0.01,
    )
    device_ids = [uuid4() for _ in range(3)]
    for device_id in device_ids:
        assert await registry.revoke(device_id) is False

    assert len(registry._revocation_epochs) <= 2  # noqa: SLF001
    await asyncio.sleep(0.02)
    assert await registry.registration_epoch(device_ids[-1]) == 0
    assert registry._revocation_epochs == {}  # noqa: SLF001


@pytest.mark.asyncio
async def test_writer_prioritizes_critical_control_over_queued_bulk() -> None:
    class BlockingWebSocket:
        def __init__(self) -> None:
            self.binary_started = asyncio.Event()
            self.release_binary = asyncio.Event()
            self.sent: list[tuple[str, bytes | str]] = []
            self.closed = asyncio.Event()

        async def send_text(self, payload: str) -> None:
            self.sent.append(("text", payload))

        async def send_bytes(self, payload: bytes) -> None:
            self.sent.append(("bytes", payload))
            if len(self.sent) == 1:
                self.binary_started.set()
                await self.release_binary.wait()

        async def close(self, code: int = 1000, reason: str = "") -> None:
            del code, reason
            self.closed.set()

    websocket = BlockingWebSocket()
    transport = WebSocketTransport(websocket)
    slot_id = new_uuid7()
    first = asyncio.create_task(transport.send_binary(encode_binary_chunk(slot_id, b"one")))
    await websocket.binary_started.wait()
    bulk = [
        asyncio.create_task(
            transport.send_binary(encode_binary_chunk(slot_id, f"chunk-{idx}".encode()))
        )
        for idx in range(4)
    ]
    critical = asyncio.create_task(
        transport.send_text(
            json.dumps({"type": "error", "id": str(new_uuid7()), "code": "x", "message": "x"})
        )
    )

    websocket.release_binary.set()
    await asyncio.wait_for(critical, timeout=1)
    assert websocket.sent[1][0] == "text"
    await asyncio.gather(first, *bulk)
    await transport.close(1000, "done")


@pytest.mark.asyncio
async def test_writer_times_out_a_peer_that_never_accepts_an_outbound_frame() -> None:
    class StuckWebSocket:
        def __init__(self) -> None:
            self.send_started = asyncio.Event()
            self.closed = asyncio.Event()

        async def send_text(self, payload: str) -> None:
            del payload
            self.send_started.set()
            await asyncio.Future()

        async def send_bytes(self, payload: bytes) -> None:
            del payload
            raise AssertionError("binary send was not expected")

        async def close(self, code: int = 1000, reason: str = "") -> None:
            del code, reason
            self.closed.set()

    websocket = StuckWebSocket()
    transport = WebSocketTransport(websocket, io_timeout_seconds=0.01)
    send = asyncio.create_task(transport.send_text('{"type":"tool_call"}'))
    await websocket.send_started.wait()

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(send, timeout=1)
    await asyncio.wait_for(websocket.closed.wait(), timeout=1)
    await asyncio.wait_for(transport.close(1000, "done"), timeout=1)


@pytest.mark.asyncio
async def test_config_commit_and_push_are_serialized_in_commit_order() -> None:
    class BlockingConfigTransport(_Transport):
        def __init__(self) -> None:
            super().__init__()
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def send_text(self, payload: str) -> None:
            if json.loads(payload).get("type") == "config_update" and not self.started.is_set():
                self.started.set()
                await self.release.wait()
            await super().send_text(payload)

    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = BlockingConfigTransport()
    await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )

    async def push(path: str) -> None:
        async with registry.config_update_lock(user_id=user_id, device_name="laptop"):
            await registry.push_config(
                device_id=device_id,
                user_id=user_id,
                device_name="laptop",
                config=DeviceConfigFrame(
                    workspace_path=path,
                    sandbox_mode=True,
                    ssrf_denylist=[],
                ),
            )

    first = asyncio.create_task(push("/committed/old"))
    await transport.started.wait()
    second = asyncio.create_task(push("/committed/new"))
    await asyncio.sleep(0)
    assert second.done() is False
    transport.release.set()
    await asyncio.gather(first, second)

    assert [json.loads(payload)["config"]["workspace_path"] for payload in transport.sent_text] == [
        "/committed/old",
        "/committed/new",
    ]


@pytest.mark.asyncio
async def test_config_serialization_survives_device_rename_alias() -> None:
    registry = DeviceRegistry()
    user_id = uuid4()
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    second_started = asyncio.Event()

    async def first() -> None:
        async with registry.config_update_lock(user_id=user_id, device_name="old-name"):
            first_started.set()
            await release_first.wait()

    async def second() -> None:
        async with registry.config_update_lock(user_id=user_id, device_name="new-name"):
            second_started.set()

    first_task = asyncio.create_task(first())
    await first_started.wait()
    second_task = asyncio.create_task(second())
    await asyncio.sleep(0)
    assert second_started.is_set() is False

    release_first.set()
    await asyncio.gather(first_task, second_task)
    assert second_started.is_set() is True
