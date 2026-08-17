import asyncio
import hashlib
import json
from dataclasses import dataclass, field
from typing import cast
from uuid import UUID, uuid4

import pytest

from openctopus_server.devices.protocol import (
    DeviceConfigFrame,
    ToolResultFrame,
    TransferBeginFrame,
    TransferEndFrame,
    new_uuid7,
)
from openctopus_server.devices.registry import (
    ConnectionHandle,
    DeviceBusyError,
    DeviceProtocolError,
    DeviceRegistry,
    DeviceUnavailableError,
)


@dataclass
class FakeTransport:
    sent_text: list[str] = field(default_factory=list)
    closes: list[tuple[int, str]] = field(default_factory=list)
    sent: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(self, payload: str) -> None:
        self.sent_text.append(payload)
        self.sent.set()

    async def close(self, code: int, reason: str) -> None:
        self.closes.append((code, reason))


@dataclass
class FailingTransport(FakeTransport):
    async def send_text(self, payload: str) -> None:
        del payload
        raise OSError("socket is closed")


@dataclass
class BlockingTransport(FakeTransport):
    send_started: asyncio.Event = field(default_factory=asyncio.Event)
    release_send: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(self, payload: str) -> None:
        self.send_started.set()
        await self.release_send.wait()
        self.sent_text.append(payload)
        self.sent.set()


@dataclass
class AmbiguousConfigTransport(FakeTransport):
    deliver_before_block: bool = False
    send_started: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(self, payload: str) -> None:
        if self.deliver_before_block:
            self.sent_text.append(payload)
        self.send_started.set()
        await asyncio.Future()


@dataclass
class RecordingSink:
    chunks: list[bytes] = field(default_factory=list)
    finished: bool = False
    aborted: bool = False

    async def write(self, chunk: bytes) -> None:
        self.chunks.append(chunk)

    async def finish(self) -> None:
        self.finished = True

    async def abort(self) -> None:
        self.aborted = True


async def _wait_for_sent(transport: FakeTransport) -> dict[str, object]:
    await asyncio.wait_for(transport.sent.wait(), timeout=1)
    return cast(dict[str, object], json.loads(transport.sent_text[-1]))


async def test_dispatch_correlates_result_and_enforces_owner() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )

    task = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="read_file",
            args={"path": "a.txt"},
            max_result_bytes=131_072,
            timeout_seconds=1,
        )
    )
    payload = await _wait_for_sent(transport)
    result = ToolResultFrame(
        id=UUID(str(payload["id"])),
        content="hello",
        is_error=False,
    )

    assert await registry.resolve_tool_result(handle, result) is True
    assert await task == result

    with pytest.raises(DeviceUnavailableError):
        await registry.dispatch_tool(
            device_id=device_id,
            user_id=uuid4(),
            name="read_file",
            args={},
            max_result_bytes=1024,
            timeout_seconds=1,
        )


async def test_result_must_fit_the_credit_reserved_by_its_tool_call() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
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
            max_result_bytes=256,
            timeout_seconds=10,
        )
    )
    payload = await _wait_for_sent(transport)
    oversized = ToolResultFrame(
        id=UUID(str(payload["id"])),
        content="x" * 512,
        is_error=False,
    )

    with pytest.raises(DeviceProtocolError, match="credit"):
        await registry.resolve_tool_result(
            handle,
            oversized,
            encoded_bytes=len(oversized.model_dump_json().encode("utf-8")),
        )

    assert pending.done() is False
    assert await registry.unregister(handle) is True
    with pytest.raises(DeviceUnavailableError):
        await pending


async def test_replacement_fails_old_pending_and_stale_unregister_is_harmless() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_transport = FakeTransport()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=old_transport,
    )
    pending = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="list_dir",
            args={"path": "."},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    )
    await _wait_for_sent(old_transport)

    new_transport = FakeTransport()
    new_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=new_transport,
    )

    with pytest.raises(DeviceUnavailableError):
        await pending
    assert old_transport.closes == [(4000, "connection_replaced")]
    assert new_handle.generation > old_handle.generation
    assert await registry.unregister(old_handle) is False
    assert await registry.is_online(device_id, user_id=user_id) is True


async def test_stale_generation_cannot_complete_new_call() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    new_transport = FakeTransport()
    new_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=new_transport,
    )
    pending = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=1,
        )
    )
    payload = await _wait_for_sent(new_transport)
    result = ToolResultFrame(
        id=UUID(str(payload["id"])),
        content="new",
        is_error=False,
    )

    assert await registry.resolve_tool_result(old_handle, result) is False
    assert pending.done() is False
    assert await registry.resolve_tool_result(new_handle, result) is True
    assert await pending == result


async def test_replaced_generation_cannot_send_a_late_tool_call() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_transport = FakeTransport()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=old_transport,
    )

    new_transport = FakeTransport()
    await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=new_transport,
    )

    assert await registry.send_text(old_handle, "late") is False
    assert old_transport.sent_text == []


async def test_unready_registration_is_not_online_or_routable_until_activation() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()

    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
        ready=False,
    )

    assert handle is not None
    assert await registry.is_online(device_id, user_id=user_id) is False
    assert await registry.get_handle(device_id, user_id=user_id) is None
    with pytest.raises(DeviceUnavailableError):
        await registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="list_dir",
            args={},
            max_result_bytes=1024,
            timeout_seconds=1,
        )

    assert await registry.activate(handle, "hello_ack") is True
    assert transport.sent_text == ["hello_ack"]
    assert await registry.is_online(device_id, user_id=user_id) is True


async def test_replacement_waits_for_an_admitted_tool_call_send_boundary() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_transport = BlockingTransport()
    await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=old_transport,
    )
    pending = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="write_file",
            args={"path": "a.txt", "content": "x"},
            max_result_bytes=1024,
            timeout_seconds=10,
        )
    )
    await asyncio.wait_for(old_transport.send_started.wait(), timeout=1)

    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    await asyncio.sleep(0)
    assert replacement.done() is False

    old_transport.release_send.set()
    await replacement
    with pytest.raises(DeviceUnavailableError):
        await pending


async def test_revocation_epoch_rejects_a_stale_handshake_after_token_rotation() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    epoch = await registry.registration_epoch(device_id)

    assert await registry.revoke(device_id) is False
    assert await registry.register(
        device_id=device_id,
        user_id=uuid4(),
        device_name="laptop",
        transport=FakeTransport(),
        expected_revocation_epoch=epoch,
    ) is None


async def test_config_push_updates_the_current_connection_name() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="old-name",
        transport=transport,
    )

    assert await registry.push_config(
        device_id=device_id,
        user_id=user_id,
        device_name="new-name",
        config=DeviceConfigFrame(
            workspace_path="~/workspace",
            sandbox_mode=True,
            ssrf_denylist=[],
        ),
    ) is True
    payload = json.loads(transport.sent_text[-1])
    assert payload["type"] == "config_update"
    assert payload["device_name"] == "new-name"
    with pytest.raises(DeviceUnavailableError):
        await registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            expected_device_name="old-name",
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    assert len(transport.sent_text) == 1


async def test_private_dispatch_rejects_a_changed_config_snapshot() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    assert handle is not None
    route = await registry.get_route_snapshot(
        device_id,
        user_id=user_id,
        expected_device_name="laptop",
    )
    assert route is not None

    assert await registry.push_config(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        config=DeviceConfigFrame(
            workspace_path="~/different-workspace",
            sandbox_mode=True,
            ssrf_denylist=[],
        ),
    )
    with pytest.raises(DeviceUnavailableError):
        await registry.dispatch_tool_on_snapshot(
            route=route,
            user_id=user_id,
            expected_device_name="laptop",
            name="delete_file",
            args={"path": "source.txt"},
            max_result_bytes=1024,
            timeout_seconds=1,
        )
    assert [json.loads(payload)["type"] for payload in transport.sent_text] == [
        "config_update"
    ]


async def test_config_push_send_failure_marks_the_device_offline() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FailingTransport(),
    )

    assert await registry.push_config(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        config=DeviceConfigFrame(
            workspace_path="~/workspace",
            sandbox_mode=True,
            ssrf_denylist=[],
        ),
    ) is False
    assert await registry.is_online(device_id, user_id=user_id) is False


@pytest.mark.parametrize("deliver_before_block", [False, True])
async def test_cancelled_config_push_retires_the_ambiguous_generation(
    deliver_before_block: bool,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = AmbiguousConfigTransport(deliver_before_block=deliver_before_block)
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="old-name",
        transport=transport,
    )
    update = asyncio.create_task(
        registry.push_config(
            device_id=device_id,
            user_id=user_id,
            device_name="new-name",
            config=DeviceConfigFrame(
                workspace_path="~/new-workspace",
                sandbox_mode=True,
                ssrf_denylist=[],
            ),
        )
    )
    await asyncio.wait_for(transport.send_started.wait(), timeout=1)

    update.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(update, timeout=1)

    assert await registry.is_online(device_id, user_id=user_id) is False
    assert await registry.send_text(handle, "late") is False


async def test_config_update_precedes_new_name_dispatch_and_blocks_stale_name() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = BlockingTransport()
    await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="old-name",
        transport=transport,
    )
    config_task = asyncio.create_task(
        registry.push_config(
            device_id=device_id,
            user_id=user_id,
            device_name="new-name",
            config=DeviceConfigFrame(
                workspace_path="~/workspace",
                sandbox_mode=True,
                ssrf_denylist=[],
            ),
        )
    )
    await asyncio.wait_for(transport.send_started.wait(), timeout=1)

    new_name_dispatch = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            expected_device_name="new-name",
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    )
    with pytest.raises(DeviceUnavailableError):
        await asyncio.wait_for(new_name_dispatch, timeout=0.02)
    stale = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            expected_device_name="old-name",
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    )
    transport.release_send.set()
    assert await config_task is True
    with pytest.raises(DeviceUnavailableError):
        await stale
    assert [json.loads(payload)["type"] for payload in transport.sent_text] == ["config_update"]


async def test_revoke_serializes_with_replacement_and_revokes_the_current_generation() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_transport = BlockingTransport()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=old_transport,
    )
    epoch = await registry.registration_epoch(device_id)
    in_flight = asyncio.create_task(registry.send_text(old_handle, "in-flight"))
    await asyncio.wait_for(old_transport.send_started.wait(), timeout=1)
    new_transport = FakeTransport()
    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=new_transport,
            expected_revocation_epoch=epoch,
        )
    )
    await asyncio.sleep(0)
    assert replacement.done() is False
    revocation = asyncio.create_task(registry.revoke(device_id))
    old_transport.release_send.set()

    assert await in_flight is True
    assert await replacement is not None
    assert await revocation is True
    assert await registry.is_online(device_id, user_id=user_id) is False
    assert new_transport.closes == [(4401, '{"code":"unauthorized"}')]


async def test_cancelled_replacement_retires_its_published_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_transport = FakeTransport()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=old_transport,
    )
    retirement_started = asyncio.Event()
    release_retirement = asyncio.Event()
    original_retire = registry._retire

    async def blocked_retire(connection: object, **kwargs: object) -> None:
        if getattr(connection, "handle", None) == old_handle:
            retirement_started.set()
            await release_retirement.wait()
        await original_retire(connection, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(registry, "_retire", blocked_retire)
    replacement_transport = FakeTransport()
    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=replacement_transport,
        )
    )
    await asyncio.wait_for(retirement_started.wait(), timeout=1)

    replacement.cancel()
    await asyncio.sleep(0)
    assert replacement.done() is False
    release_retirement.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(replacement, timeout=1)

    assert await registry.is_online(device_id, user_id=user_id) is False


async def test_remove_device_waits_for_every_retiring_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    retirement_started = asyncio.Event()
    release_retirement = asyncio.Event()
    original_retire = registry._retire

    async def blocked_retire(connection: object, **kwargs: object) -> None:
        if getattr(connection, "handle", None) == old_handle:
            retirement_started.set()
            await release_retirement.wait()
        await original_retire(connection, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(registry, "_retire", blocked_retire)
    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    await asyncio.wait_for(retirement_started.wait(), timeout=1)

    removal = asyncio.create_task(registry.remove_device(device_id))
    await asyncio.sleep(0)
    assert removal.done() is False

    release_retirement.set()
    assert await replacement is not None
    assert await removal is True
    assert await registry.is_online(device_id, user_id=user_id) is False


async def test_replacement_waits_for_an_in_flight_binary_transfer_frame() -> None:
    @dataclass
    class BlockingBinaryTransport(FakeTransport):
        binary_started: asyncio.Event = field(default_factory=asyncio.Event)
        release_binary: asyncio.Event = field(default_factory=asyncio.Event)
        sent_binary: list[bytes] = field(default_factory=list)

        async def send_binary(self, payload: bytes) -> None:
            self.binary_started.set()
            await self.release_binary.wait()
            self.sent_binary.append(payload)

    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_transport = BlockingBinaryTransport()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=old_transport,
    )
    payload = new_uuid7().bytes + b"chunk"
    send = asyncio.create_task(registry.send_binary(old_handle, payload))
    await asyncio.wait_for(old_transport.binary_started.wait(), timeout=1)

    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    await asyncio.sleep(0)
    assert replacement.done() is False

    old_transport.release_binary.set()
    assert await send is True
    assert await replacement is not None
    assert old_transport.sent_binary == [payload]


async def test_stale_generation_terminal_frame_cannot_commit_during_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    sink = RecordingSink()
    transfer = asyncio.create_task(
        registry.transfers.start_client_to_server(
            handle=old_handle,
            user_id=user_id,
            src_path="source.txt",
            dst_path="destination.txt",
            sink_factory=lambda _frame: _sink(sink),
        )
    )
    request = await _wait_for_sent(transport)
    slot_id = UUID(str(request["id"]))
    await registry.handle_transfer_frame(
        old_handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.txt",
            dst_path="destination.txt",
            total_bytes=0,
        ),
    )

    disconnect_started = asyncio.Event()
    release_disconnect = asyncio.Event()
    original_disconnect = registry.transfers.disconnect

    async def delayed_disconnect(handle: object) -> None:
        if handle == old_handle:
            disconnect_started.set()
            await release_disconnect.wait()
        await original_disconnect(handle)

    monkeypatch.setattr(registry.transfers, "disconnect", delayed_disconnect)
    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    await asyncio.wait_for(disconnect_started.wait(), timeout=1)
    assert await registry.get_handle(device_id, user_id=user_id) != old_handle

    digest = hashlib.sha256(b"").hexdigest()
    assert (
        await registry.handle_transfer_frame(
            old_handle,
            TransferEndFrame(
                id=slot_id,
                ack=False,
                ok=True,
                bytes_sent=0,
                sha256=digest,
            ),
        )
        is False
    )
    assert sink.finished is False

    release_disconnect.set()
    await replacement
    with pytest.raises(Exception):
        await transfer


async def test_terminal_past_registry_check_cannot_commit_after_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    assert old_handle is not None
    route = await registry.get_route_snapshot(
        device_id,
        user_id=user_id,
        expected_device_name="laptop",
    )
    assert route is not None
    sink = RecordingSink()
    transfer = asyncio.create_task(
        registry.transfers.start_client_to_server(
            handle=old_handle,
            route=route,
            user_id=user_id,
            src_path="source.txt",
            dst_path="destination.txt",
            sink_factory=lambda _frame: _sink(sink),
        )
    )
    request = await _wait_for_sent(transport)
    slot_id = UUID(str(request["id"]))
    await registry.handle_transfer_frame(
        old_handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.txt",
            dst_path="destination.txt",
            total_bytes=0,
        ),
    )
    while not any(
        json.loads(payload)["type"] == "transfer_ready" for payload in transport.sent_text
    ):
        await asyncio.sleep(0)

    frame_entered = asyncio.Event()
    release_frame = asyncio.Event()
    original_handle_frame = registry.transfers.handle_frame

    async def blocked_handle_frame(handle: object, frame: object) -> None:
        if isinstance(frame, TransferEndFrame) and frame.ok and not frame.ack:
            frame_entered.set()
            await release_frame.wait()
        await original_handle_frame(handle, frame)

    monkeypatch.setattr(registry.transfers, "handle_frame", blocked_handle_frame)
    terminal = asyncio.create_task(
        registry.handle_transfer_frame(
            old_handle,
            TransferEndFrame(
                id=slot_id,
                ack=False,
                ok=True,
                bytes_sent=0,
                sha256=hashlib.sha256(b"").hexdigest(),
            ),
        )
    )
    await frame_entered.wait()

    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    while await registry.get_handle(device_id, user_id=user_id) == old_handle:
        await asyncio.sleep(0)
    release_frame.set()

    await asyncio.gather(terminal, return_exceptions=True)
    assert await replacement is not None
    with pytest.raises(Exception):
        await transfer
    assert sink.finished is False


async def test_config_epoch_change_aborts_active_route_before_finish() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    assert handle is not None
    route = await registry.get_route_snapshot(
        device_id,
        user_id=user_id,
        expected_device_name="laptop",
    )
    assert route is not None
    sink = RecordingSink()
    transfer = asyncio.create_task(
        registry.transfers.start_client_to_server(
            handle=handle,
            route=route,
            user_id=user_id,
            src_path="source.txt",
            dst_path="destination.txt",
            sink_factory=lambda _frame: _sink(sink),
        )
    )
    request = await _wait_for_sent(transport)
    slot_id = UUID(str(request["id"]))
    await registry.handle_transfer_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.txt",
            dst_path="destination.txt",
            total_bytes=0,
        ),
    )
    while not any(
        json.loads(payload)["type"] == "transfer_ready" for payload in transport.sent_text
    ):
        await asyncio.sleep(0)

    assert await registry.push_config(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        config=DeviceConfigFrame(
            workspace_path="~/different-workspace",
            sandbox_mode=True,
            ssrf_denylist=[],
        ),
    )

    with pytest.raises(Exception):
        await transfer
    assert sink.finished is False


async def test_stale_generation_binary_frame_cannot_write_during_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    sink = RecordingSink()
    transfer = asyncio.create_task(
        registry.transfers.start_client_to_server(
            handle=old_handle,
            user_id=user_id,
            src_path="source.txt",
            dst_path="destination.txt",
            sink_factory=lambda _frame: _sink(sink),
        )
    )
    request = await _wait_for_sent(transport)
    slot_id = UUID(str(request["id"]))
    await registry.handle_transfer_frame(
        old_handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.txt",
            dst_path="destination.txt",
            total_bytes=1,
        ),
    )

    disconnect_started = asyncio.Event()
    release_disconnect = asyncio.Event()
    original_disconnect = registry.transfers.disconnect

    async def delayed_disconnect(handle: object) -> None:
        if handle == old_handle:
            disconnect_started.set()
            await release_disconnect.wait()
        await original_disconnect(handle)

    monkeypatch.setattr(registry.transfers, "disconnect", delayed_disconnect)
    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    await asyncio.wait_for(disconnect_started.wait(), timeout=1)
    assert await registry.get_handle(device_id, user_id=user_id) != old_handle

    assert await registry.handle_transfer_binary(old_handle, slot_id.bytes + b"x") is False
    assert sink.chunks == []

    release_disconnect.set()
    await replacement
    with pytest.raises(Exception):
        await transfer


async def test_blocked_sink_preparation_does_not_hold_registry_lifecycle_lock() -> None:
    registry = DeviceRegistry(transfer_idle_timeout_seconds=1.0)
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    preparation_started = asyncio.Event()
    preparation_cancelled = asyncio.Event()

    async def make_sink(_: TransferBeginFrame) -> RecordingSink:
        preparation_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            preparation_cancelled.set()
            raise
        raise AssertionError("sink preparation should remain blocked")

    transfer = asyncio.create_task(
        registry.transfers.start_client_to_server(
            handle=handle,
            user_id=user_id,
            src_path="source.txt",
            dst_path="destination.txt",
            sink_factory=make_sink,
        )
    )
    request = await _wait_for_sent(transport)
    slot_id = UUID(str(request["id"]))
    await asyncio.wait_for(
        registry.handle_transfer_frame(
            handle,
            TransferBeginFrame(
                id=slot_id,
                direction="client_to_server",
                purpose="file_transfer",
                src_path="source.txt",
                dst_path="destination.txt",
                total_bytes=0,
            ),
        ),
        timeout=0.2,
    )
    await preparation_started.wait()

    assert await asyncio.wait_for(registry.unregister(handle), timeout=0.2) is True
    await preparation_cancelled.wait()
    with pytest.raises(Exception):
        await transfer
    assert await registry.is_online(device_id, user_id=user_id) is False
    assert registry.transfers.active_slots == 0
    assert registry.transfers._admission.active_count == 0


async def _sink(sink: RecordingSink) -> RecordingSink:
    return sink


async def test_close_does_not_wait_for_inbound_transfer_backpressure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    inbound_started = asyncio.Event()
    release_inbound = asyncio.Event()

    async def blocked_handle_frame(_handle: object, _frame: object) -> None:
        inbound_started.set()
        await release_inbound.wait()

    monkeypatch.setattr(registry.transfers, "handle_frame", blocked_handle_frame)
    inbound = asyncio.create_task(registry.handle_transfer_frame(handle, object()))
    await asyncio.wait_for(inbound_started.wait(), timeout=1)

    shutdown = asyncio.create_task(registry.close())
    await asyncio.wait_for(shutdown, timeout=0.2)
    assert await registry.is_online(device_id, user_id=user_id) is False

    release_inbound.set()
    assert await inbound is False


async def test_disconnect_and_timeout_remove_pending_calls() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    timed_out = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=0.01,
        )
    )
    payload = await _wait_for_sent(transport)

    with pytest.raises(TimeoutError):
        await timed_out
    assert registry.pending_count == 0
    late = ToolResultFrame(
        id=UUID(str(payload["id"])),
        content="late",
        is_error=False,
    )
    assert await registry.resolve_tool_result(handle, late) is True
    assert await registry.resolve_tool_result(handle, late) is True
    assert (
        await registry.resolve_tool_result(
            handle,
            late.model_copy(update={"id": new_uuid7()}),
        )
        is False
    )

    transport.sent.clear()
    pending = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    )
    await _wait_for_sent(transport)
    assert await registry.unregister(handle) is True
    with pytest.raises(DeviceUnavailableError):
        await pending
    assert registry.pending_count == 0


async def test_pending_admission_rejects_without_retaining_or_sending() -> None:
    registry = DeviceRegistry(
        pending_calls_max=1,
        pending_calls_max_per_user=1,
        pending_bytes_max=1_000_000,
        pending_bytes_max_per_user=1_000_000,
    )
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
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
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    )
    first_payload = await _wait_for_sent(transport)

    with pytest.raises(DeviceBusyError):
        await registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    assert registry.pending_count == 1
    assert len(transport.sent_text) == 1

    assert await registry.resolve_tool_result(
        handle,
        ToolResultFrame(
            id=UUID(str(first_payload["id"])),
            content="done",
            is_error=False,
        ),
    )
    await first

    transport.sent.clear()
    second = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    )
    second_payload = await _wait_for_sent(transport)
    assert await registry.resolve_tool_result(
        handle,
        ToolResultFrame(
            id=UUID(str(second_payload["id"])),
            content="done",
            is_error=False,
        ),
    )
    await second


async def test_pending_byte_admission_is_per_user_and_releases_after_disconnect() -> None:
    registry = DeviceRegistry(
        pending_calls_max=4,
        pending_calls_max_per_user=2,
        pending_bytes_max=100_000,
        pending_bytes_max_per_user=20_000,
    )
    first_user = uuid4()
    second_user = uuid4()
    first_device = uuid4()
    second_device = uuid4()
    first_transport = FakeTransport()
    second_transport = FakeTransport()
    first_handle = await registry.register(
        device_id=first_device,
        user_id=first_user,
        device_name="first",
        transport=first_transport,
    )
    second_handle = await registry.register(
        device_id=second_device,
        user_id=second_user,
        device_name="second",
        transport=second_transport,
    )

    first = asyncio.create_task(
        registry.dispatch_tool(
            device_id=first_device,
            user_id=first_user,
            name="read_file",
            args={"path": "x"},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    )
    await _wait_for_sent(first_transport)

    with pytest.raises(DeviceBusyError):
        await registry.dispatch_tool(
            device_id=first_device,
            user_id=first_user,
            name="read_file",
            args={"path": "x"},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )

    second = asyncio.create_task(
        registry.dispatch_tool(
            device_id=second_device,
            user_id=second_user,
            name="read_file",
            args={"path": "x"},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    )
    await _wait_for_sent(second_transport)

    assert await registry.unregister(first_handle) is True
    with pytest.raises(DeviceUnavailableError):
        await first

    await registry.unregister(second_handle)
    with pytest.raises(DeviceUnavailableError):
        await second
    assert registry.pending_count == 0


async def test_revoke_closes_current_and_allows_later_generation() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    first = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )

    assert await registry.revoke(device_id) is True
    assert transport.closes == [(4401, '{"code":"unauthorized"}')]
    assert await registry.is_online(device_id, user_id=user_id) is False

    second = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    assert second.generation > first.generation


async def test_close_prevents_registration_after_shutdown() -> None:
    registry = DeviceRegistry()
    await registry.close()

    assert await registry.register(
        device_id=uuid4(),
        user_id=uuid4(),
        device_name="laptop",
        transport=FakeTransport(),
    ) is None


async def test_close_serializes_with_registration_already_waiting() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    await registry._register_lock.acquire()
    registration = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    await asyncio.sleep(0)
    shutdown = asyncio.create_task(registry.close())
    await asyncio.sleep(0)

    registry._register_lock.release()
    await registration
    await shutdown

    assert await registry.is_online(device_id, user_id=user_id) is False


async def test_close_waits_for_a_replaced_generation_retirement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    retirement_started = asyncio.Event()
    release_retirement = asyncio.Event()
    current_retired = asyncio.Event()
    original_retire = registry._retire

    async def blocked_retire(connection: object, **kwargs: object) -> None:
        if getattr(connection, "handle", None) == old_handle:
            retirement_started.set()
            await release_retirement.wait()
        await original_retire(connection, **kwargs)  # type: ignore[arg-type]
        if getattr(connection, "handle", None) != old_handle:
            current_retired.set()

    monkeypatch.setattr(registry, "_retire", blocked_retire)
    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    await asyncio.wait_for(retirement_started.wait(), timeout=1)

    shutdown = asyncio.create_task(registry.close())
    await asyncio.wait_for(current_retired.wait(), timeout=1)
    assert shutdown.done() is False

    release_retirement.set()
    await asyncio.wait_for(shutdown, timeout=1)
    assert await replacement is not None


async def test_pong_is_generation_scoped() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    current = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    ping_id = uuid4()
    assert await registry.send_ping(current, ping_id, "ping") is True

    assert await registry.mark_pong(old, ping_id, at=10.0) is False
    assert await registry.mark_pong(current, uuid4(), at=20.0) is False
    assert await registry.mark_pong(current, ping_id, at=20.0) is True
    assert await registry.last_pong(current) == 20.0


def test_connection_handle_is_immutable() -> None:
    handle = ConnectionHandle(device_id=uuid4(), generation=1)
    with pytest.raises((AttributeError, TypeError)):
        handle.generation = 2  # type: ignore[misc]
