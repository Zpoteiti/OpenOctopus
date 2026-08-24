import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Literal
from uuid import UUID, uuid4

import pytest

import openctopus_server.devices.transfer as transfer_module
from openctopus_server.devices.protocol import (
    MAX_BINARY_CHUNK_BYTES,
    TransferBeginFrame,
    TransferEndFrame,
    TransferProgressFrame,
    TransferReadyFrame,
    TransferRequestFrame,
    decode_binary_chunk,
    parse_server_frame,
)
from openctopus_server.devices.registry import ConnectionHandle, DeviceRouteSnapshot
from openctopus_server.devices.transfer import (
    LATE_PROGRESS_MAX,
    TRANSFER_QUEUE_CHUNKS,
    FairTransferAdmission,
    TransferBusyError,
    TransferDisconnectedError,
    TransferError,
    TransferIntegrityError,
    TransferManager,
    TransferProtocolError,
    TransferResult,
)


@dataclass(frozen=True, slots=True)
class _TextSend:
    handle: ConnectionHandle
    payload: str
    expected_device_name: str | None
    expected_config_epoch: int | None


@dataclass(slots=True)
class _BridgeTransport:
    text: list[_TextSend] = field(default_factory=list)
    binary: list[tuple[ConnectionHandle, bytes]] = field(default_factory=list)
    unavailable_handles: set[ConnectionHandle] = field(default_factory=set)
    route_checks: list[tuple[DeviceRouteSnapshot, DeviceRouteSnapshot, UUID]] = field(
        default_factory=list
    )

    async def bridge_routes_current(
        self,
        source_route: DeviceRouteSnapshot,
        destination_route: DeviceRouteSnapshot,
        *,
        user_id: UUID,
    ) -> bool:
        self.route_checks.append((source_route, destination_route, user_id))
        return True

    async def send_text(
        self,
        handle: ConnectionHandle,
        payload: str,
        *,
        expected_device_name: str | None = None,
        expected_config_epoch: int | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> bool:
        if on_issued is not None:
            on_issued()
        self.text.append(
            _TextSend(
                handle,
                payload,
                expected_device_name,
                expected_config_epoch,
            )
        )
        return handle not in self.unavailable_handles

    async def send_binary(
        self,
        handle: ConnectionHandle,
        payload: bytes,
        *,
        expected_device_name: str | None = None,
        expected_config_epoch: int | None = None,
    ) -> bool:
        del expected_device_name, expected_config_epoch
        self.binary.append((handle, payload))
        return handle not in self.unavailable_handles


@dataclass(slots=True)
class _ControlledSourceAckTransport(_BridgeTransport):
    source_handle: ConnectionHandle | None = None
    source_ack_result: bool = True
    block_source_ack: bool = False
    source_ack_started: asyncio.Event = field(default_factory=asyncio.Event)
    release_source_ack: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(
        self,
        handle: ConnectionHandle,
        payload: str,
        *,
        expected_device_name: str | None = None,
        expected_config_epoch: int | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> bool:
        result = await _BridgeTransport.send_text(
            self,
            handle,
            payload,
            expected_device_name=expected_device_name,
            expected_config_epoch=expected_config_epoch,
            on_issued=on_issued,
        )
        frame = parse_server_frame(payload)
        if (
            handle == self.source_handle
            and isinstance(frame, TransferEndFrame)
            and frame.ack
        ):
            self.source_ack_started.set()
            if self.block_source_ack:
                await self.release_source_ack.wait()
            return (
                result
                and self.source_ack_result
                and handle not in self.unavailable_handles
            )
        return result


@dataclass(slots=True)
class _BlockingFailureSendTransport(_BridgeTransport):
    block_handle: ConnectionHandle | None = None
    failure_send_started: asyncio.Event = field(default_factory=asyncio.Event)
    release_failure_send: asyncio.Event = field(default_factory=asyncio.Event)
    failure_send_blocked: bool = False

    async def send_text(
        self,
        handle: ConnectionHandle,
        payload: str,
        *,
        expected_device_name: str | None = None,
        expected_config_epoch: int | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> bool:
        result = await _BridgeTransport.send_text(
            self,
            handle,
            payload,
            expected_device_name=expected_device_name,
            expected_config_epoch=expected_config_epoch,
            on_issued=on_issued,
        )
        frame = parse_server_frame(payload)
        if (
            handle == self.block_handle
            and isinstance(frame, TransferEndFrame)
            and not frame.ack
            and not frame.ok
            and not self.failure_send_blocked
        ):
            self.failure_send_blocked = True
            self.failure_send_started.set()
            await self.release_failure_send.wait()
            return result and handle not in self.unavailable_handles
        return result


@dataclass(slots=True)
class _BlockingBinaryTransport(_BridgeTransport):
    binary_send_started: asyncio.Event = field(default_factory=asyncio.Event)
    release_binary_send: asyncio.Event = field(default_factory=asyncio.Event)
    binary_send_cancelled: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_binary(
        self,
        handle: ConnectionHandle,
        payload: bytes,
        *,
        expected_device_name: str | None = None,
        expected_config_epoch: int | None = None,
    ) -> bool:
        del expected_device_name, expected_config_epoch
        self.binary.append((handle, payload))
        self.binary_send_started.set()
        try:
            await self.release_binary_send.wait()
        except asyncio.CancelledError:
            self.binary_send_cancelled.set()
            raise
        return handle not in self.unavailable_handles


@dataclass(slots=True)
class _PacedBinaryTransport(_BridgeTransport):
    delay_seconds: float = 0.0
    first_binary_started: asyncio.Event = field(default_factory=asyncio.Event)
    allow_binary_sends: asyncio.Event = field(default_factory=asyncio.Event)
    completed_sends: int = 0

    async def send_binary(
        self,
        handle: ConnectionHandle,
        payload: bytes,
        *,
        expected_device_name: str | None = None,
        expected_config_epoch: int | None = None,
    ) -> bool:
        del expected_device_name, expected_config_epoch
        self.binary.append((handle, payload))
        self.first_binary_started.set()
        await self.allow_binary_sends.wait()
        await asyncio.sleep(self.delay_seconds)
        self.completed_sends += 1
        return handle not in self.unavailable_handles


@dataclass(slots=True)
class _BlockingSourceReadyTransport(_BridgeTransport):
    source_handle: ConnectionHandle | None = None
    ready_send_started: asyncio.Event = field(default_factory=asyncio.Event)
    release_ready_send: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(
        self,
        handle: ConnectionHandle,
        payload: str,
        *,
        expected_device_name: str | None = None,
        expected_config_epoch: int | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> bool:
        frame = parse_server_frame(payload)
        if handle == self.source_handle and isinstance(frame, TransferReadyFrame):
            self.ready_send_started.set()
            await self.release_ready_send.wait()
        return await _BridgeTransport.send_text(
            self,
            handle,
            payload,
            expected_device_name=expected_device_name,
            expected_config_epoch=expected_config_epoch,
            on_issued=on_issued,
        )


@dataclass(slots=True)
class _SplitAbortSendTransport(_BridgeTransport):
    source_handle: ConnectionHandle | None = None
    destination_handle: ConnectionHandle | None = None
    destination_abort_started: asyncio.Event = field(default_factory=asyncio.Event)
    release_destination_abort: asyncio.Event = field(default_factory=asyncio.Event)
    destination_abort_cancelled: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(
        self,
        handle: ConnectionHandle,
        payload: str,
        *,
        expected_device_name: str | None = None,
        expected_config_epoch: int | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> bool:
        frame = parse_server_frame(payload)
        is_timeout_abort = (
            isinstance(frame, TransferEndFrame)
            and not frame.ack
            and not frame.ok
            and frame.code == "workspace_transfer_timeout"
        )
        if is_timeout_abort and handle == self.source_handle:
            raise RuntimeError("source abort send failed")
        if is_timeout_abort and handle == self.destination_handle:
            self.destination_abort_started.set()
            try:
                await self.release_destination_abort.wait()
            except asyncio.CancelledError:
                self.destination_abort_cancelled.set()
                raise
        return await _BridgeTransport.send_text(
            self,
            handle,
            payload,
            expected_device_name=expected_device_name,
            expected_config_epoch=expected_config_epoch,
            on_issued=on_issued,
        )


@dataclass(slots=True)
class _BlockingAbortIssueTransport(_BridgeTransport):
    abort_started: dict[ConnectionHandle, asyncio.Event] = field(default_factory=dict)
    release_abort: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(
        self,
        handle: ConnectionHandle,
        payload: str,
        *,
        expected_device_name: str | None = None,
        expected_config_epoch: int | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> bool:
        frame = parse_server_frame(payload)
        if (
            isinstance(frame, TransferEndFrame)
            and not frame.ack
            and not frame.ok
            and frame.code == "workspace_transfer_timeout"
        ):
            self.abort_started.setdefault(handle, asyncio.Event()).set()
            await self.release_abort.wait()
        return await _BridgeTransport.send_text(
            self,
            handle,
            payload,
            expected_device_name=expected_device_name,
            expected_config_epoch=expected_config_epoch,
            on_issued=on_issued,
        )


def _manager(
    transport: _BridgeTransport,
    *,
    idle_timeout_seconds: float = 0.5,
    tombstone_ttl_seconds: float = 60.0,
) -> tuple[TransferManager, FairTransferAdmission]:
    admission = FairTransferAdmission(
        max_concurrency=2,
        max_concurrency_per_user=2,
        queue_timeout_seconds=0.1,
    )
    return (
        TransferManager(
            transport,
            admission=admission,
            idle_timeout_seconds=idle_timeout_seconds,
            tombstone_ttl_seconds=tombstone_ttl_seconds,
        ),
        admission,
    )


def _routes() -> tuple[DeviceRouteSnapshot, DeviceRouteSnapshot]:
    return (
        DeviceRouteSnapshot(
            handle=ConnectionHandle(uuid4(), 3),
            config_epoch=7,
            device_name="source-device",
        ),
        DeviceRouteSnapshot(
            handle=ConnectionHandle(uuid4(), 5),
            config_epoch=11,
            device_name="destination-device",
        ),
    )


async def _wait_for_text_frame(
    transport: _BridgeTransport,
    handle: ConnectionHandle,
    frame_type: type[Any],
    *,
    ack: bool | None = None,
) -> Any:
    async with asyncio.timeout(1):
        while True:
            for sent in transport.text:
                if sent.handle != handle:
                    continue
                frame = parse_server_frame(sent.payload)
                if not isinstance(frame, frame_type):
                    continue
                if ack is not None and getattr(frame, "ack", None) is not ack:
                    continue
                return frame
            await asyncio.sleep(0)


async def _start_bridge(
    manager: TransferManager,
    transport: _BridgeTransport,
    source_route: DeviceRouteSnapshot,
    destination_route: DeviceRouteSnapshot,
    user_id: UUID,
    *,
    mode: Literal["copy", "move"] = "copy",
    delete_source: Callable[[str], Awaitable[None]] | None = None,
    on_issued: Callable[[], None] | None = None,
) -> tuple[asyncio.Task[TransferResult], TransferRequestFrame]:
    task = asyncio.create_task(
        manager.start_client_to_client(
            source_route=source_route,
            destination_route=destination_route,
            user_id=user_id,
            src_path="source.bin",
            dst_path="destination.bin",
            mode=mode,
            delete_source=delete_source,
            on_issued=on_issued,
        )
    )
    async with asyncio.timeout(1):
        while True:
            if task.done():
                await task
            request = next(
                (
                    frame
                    for sent in transport.text
                    if sent.handle == source_route.handle
                    and isinstance(
                        frame := parse_server_frame(sent.payload),
                        TransferRequestFrame,
                    )
                ),
                None,
            )
            if request is not None:
                break
            await asyncio.sleep(0)
    return task, request


def _text_frames(
    transport: _BridgeTransport,
    handle: ConnectionHandle,
    frame_type: type[Any],
    *,
    ack: bool | None = None,
) -> list[Any]:
    frames: list[Any] = []
    for sent in transport.text:
        if sent.handle != handle:
            continue
        frame = parse_server_frame(sent.payload)
        if not isinstance(frame, frame_type):
            continue
        if ack is not None and getattr(frame, "ack", None) is not ack:
            continue
        frames.append(frame)
    return frames


async def _send_source_begin(
    manager: TransferManager,
    transport: _BridgeTransport,
    source_route: DeviceRouteSnapshot,
    destination_route: DeviceRouteSnapshot,
    request: TransferRequestFrame,
    *,
    total_bytes: int,
) -> TransferBeginFrame:
    await manager.handle_frame(
        source_route.handle,
        TransferBeginFrame(
            id=request.id,
            direction="client_to_server",
            purpose="file_transfer",
            src_device=source_route.device_name,
            src_path="source.bin",
            dst_device="server",
            dst_path="destination.bin",
            total_bytes=total_bytes,
            etag="source-v1",
        ),
    )
    destination_begin = await _wait_for_text_frame(
        transport,
        destination_route.handle,
        TransferBeginFrame,
    )
    assert destination_begin.id == request.id
    assert destination_begin.direction == "server_to_client"
    assert destination_begin.src_device == source_route.device_name
    assert destination_begin.dst_device == destination_route.device_name
    assert destination_begin.src_path == "source.bin"
    assert destination_begin.dst_path == "destination.bin"
    assert destination_begin.total_bytes == total_bytes
    assert destination_begin.etag == "source-v1"
    return destination_begin


async def _make_destination_ready(
    manager: TransferManager,
    transport: _BridgeTransport,
    source_route: DeviceRouteSnapshot,
    destination_route: DeviceRouteSnapshot,
    slot_id: UUID,
) -> None:
    assert not any(
        sent.handle == source_route.handle
        and isinstance(parse_server_frame(sent.payload), TransferReadyFrame)
        for sent in transport.text
    )
    await manager.handle_frame(
        destination_route.handle,
        TransferReadyFrame(id=slot_id),
    )
    ready = await _wait_for_text_frame(
        transport,
        source_route.handle,
        TransferReadyFrame,
    )
    assert ready.id == slot_id


async def _start_ready_bridge(
    manager: TransferManager,
    transport: _BridgeTransport,
    source_route: DeviceRouteSnapshot,
    destination_route: DeviceRouteSnapshot,
    user_id: UUID,
    *,
    total_bytes: int = 0,
    mode: Literal["copy", "move"] = "copy",
    delete_source: Callable[[str], Awaitable[None]] | None = None,
) -> tuple[asyncio.Task[TransferResult], TransferRequestFrame]:
    task, request = await _start_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        user_id,
        mode=mode,
        delete_source=delete_source,
    )
    await _send_source_begin(
        manager,
        transport,
        source_route,
        destination_route,
        request,
        total_bytes=total_bytes,
    )
    await _make_destination_ready(
        manager,
        transport,
        source_route,
        destination_route,
        request.id,
    )
    return task, request


async def _issue_source_success(
    manager: TransferManager,
    transport: _BridgeTransport,
    source_route: DeviceRouteSnapshot,
    destination_route: DeviceRouteSnapshot,
    slot_id: UUID,
    data: bytes = b"",
) -> TransferEndFrame:
    if data:
        await manager.handle_binary(source_route.handle, slot_id.bytes + data)
    digest = hashlib.sha256(data).hexdigest()
    await manager.handle_frame(
        source_route.handle,
        TransferEndFrame(
            id=slot_id,
            ack=False,
            ok=True,
            bytes_sent=len(data),
            sha256=digest,
        ),
    )
    destination_end = await _wait_for_text_frame(
        transport,
        destination_route.handle,
        TransferEndFrame,
        ack=False,
    )
    assert destination_end == TransferEndFrame(
        id=slot_id,
        ack=False,
        ok=True,
        bytes_sent=len(data),
        sha256=digest,
    )
    return destination_end


async def _cancel_bridge_task(task: asyncio.Task[TransferResult]) -> None:
    if not task.done():
        task.cancel()
    try:
        await task
    except (
        asyncio.CancelledError,
        TimeoutError,
        TransferDisconnectedError,
        TransferError,
        TransferProtocolError,
    ):
        pass


async def _complete_destination_rejection(
    manager: TransferManager,
    transport: _BridgeTransport,
    source_route: DeviceRouteSnapshot,
    destination_route: DeviceRouteSnapshot,
    *,
    total_bytes: int,
    after_ready: bool = False,
) -> tuple[TransferRequestFrame, TransferEndFrame]:
    task, request = await _start_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    await _send_source_begin(
        manager,
        transport,
        source_route,
        destination_route,
        request,
        total_bytes=total_bytes,
    )
    if after_ready:
        await _make_destination_ready(
            manager,
            transport,
            source_route,
            destination_route,
            request.id,
        )
    failure = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_storage_unavailable",
    )
    await manager.handle_frame(destination_route.handle, failure)
    forwarded = await _wait_for_text_frame(
        transport,
        source_route.handle,
        TransferEndFrame,
        ack=False,
    )
    assert forwarded == failure
    acknowledgement = failure.model_copy(update={"ack": True})
    await manager.handle_frame(source_route.handle, acknowledgement)
    returned = await _wait_for_text_frame(
        transport,
        destination_route.handle,
        TransferEndFrame,
        ack=True,
    )
    assert returned == acknowledgement
    with pytest.raises(TransferError, match="workspace_storage_unavailable"):
        await asyncio.wait_for(task, timeout=1)
    return request, failure


async def _complete_successful_empty_bridge(
    manager: TransferManager,
    transport: _BridgeTransport,
    source_route: DeviceRouteSnapshot,
    destination_route: DeviceRouteSnapshot,
) -> tuple[TransferRequestFrame, TransferEndFrame]:
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    destination_end = await _issue_source_success(
        manager,
        transport,
        source_route,
        destination_route,
        request.id,
    )
    await manager.handle_frame(
        destination_route.handle,
        destination_end.model_copy(update={"ack": True}),
    )
    assert await asyncio.wait_for(task, timeout=1) == TransferResult(
        0,
        hashlib.sha256(b"").hexdigest(),
    )
    return request, destination_end


async def _complete_unknown_destination_outcome(
    manager: TransferManager,
    transport: _BridgeTransport,
    source_route: DeviceRouteSnapshot,
    destination_route: DeviceRouteSnapshot,
    *,
    mode: Literal["copy", "move"] = "copy",
    delete_source: Callable[[str], Awaitable[None]] | None = None,
) -> tuple[TransferRequestFrame, TransferEndFrame]:
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
        mode=mode,
        delete_source=delete_source,
    )
    destination_end = await _issue_source_success(
        manager,
        transport,
        source_route,
        destination_route,
        request.id,
    )
    with pytest.raises(TransferDisconnectedError, match="outcome is unknown"):
        await asyncio.wait_for(task, timeout=1)
    return request, destination_end


def _gate_worker_failure_claim(
    manager: TransferManager,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[asyncio.Event, asyncio.Event, asyncio.Event]:
    stage_waiting = asyncio.Event()
    claim_started = asyncio.Event()
    release_claim = asyncio.Event()
    original_wait = manager._wait_bridge_stage

    async def gated_wait(bridge: Any, *futures: asyncio.Future[Any]) -> Any:
        watches_terminal_failure = bool(futures) and futures[0] is bridge.source_end_future
        if watches_terminal_failure:
            stage_waiting.set()
        result = await original_wait(bridge, *futures)
        if (
            watches_terminal_failure
            and isinstance(result, TransferEndFrame)
            and not result.ok
        ):
            claim_started.set()
            await release_claim.wait()
        return result

    monkeypatch.setattr(manager, "_wait_bridge_stage", gated_wait)
    return stage_waiting, claim_started, release_claim


async def test_bridge_uuid_collision_across_distinct_route_pairs_fails_before_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    first_source, first_destination = _routes()
    second_source, second_destination = _routes()
    user_id = uuid4()
    collision = transfer_module.new_uuid7()
    monkeypatch.setattr(transfer_module, "new_uuid7", lambda: collision)
    first_task, first_request = await _start_bridge(
        manager,
        transport,
        first_source,
        first_destination,
        user_id,
    )
    assert first_request.id == collision
    sends_before_collision = len(transport.text)

    try:
        with pytest.raises(TransferProtocolError, match="collided"):
            await manager.start_client_to_client(
                source_route=second_source,
                destination_route=second_destination,
                user_id=user_id,
                src_path="other-source.bin",
                dst_path="other-destination.bin",
                mode="copy",
                delete_source=None,
                on_issued=None,
            )
        assert len(transport.text) == sends_before_collision
        assert manager.active_slots == 1
        assert admission.active_count == 1
    finally:
        await _cancel_bridge_task(first_task)


@pytest.mark.parametrize(
    "chunks",
    [
        [],
        [b"first", b"\x00\xffsecond"],
        [b"x" * MAX_BINARY_CHUNK_BYTES, b"y"],
    ],
    ids=["empty", "multiple-chunks", "64k-plus-one"],
)
async def test_client_bridge_relays_one_logical_slot_and_commits(
    chunks: list[bytes],
) -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    user_id = uuid4()
    issued: list[None] = []
    data = b"".join(chunks)
    digest = hashlib.sha256(data).hexdigest()

    task, request = await _start_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        user_id,
        on_issued=lambda: issued.append(None),
    )

    assert issued == [None]
    assert transport.route_checks == [(source_route, destination_route, user_id)]
    assert manager.active_slots == 1
    assert admission.active_count == 1
    assert admission.active_by_user == {user_id: 1}
    source_request_send = next(
        sent
        for sent in transport.text
        if sent.handle == source_route.handle
        and isinstance(parse_server_frame(sent.payload), TransferRequestFrame)
    )
    assert source_request_send.expected_device_name == source_route.device_name
    assert source_request_send.expected_config_epoch == source_route.config_epoch

    destination_begin = await _send_source_begin(
        manager,
        transport,
        source_route,
        destination_route,
        request,
        total_bytes=len(data),
    )
    destination_begin_send = next(
        sent
        for sent in transport.text
        if sent.handle == destination_route.handle
        and isinstance(parse_server_frame(sent.payload), TransferBeginFrame)
    )
    assert destination_begin_send.expected_device_name == destination_route.device_name
    assert destination_begin_send.expected_config_epoch == destination_route.config_epoch
    assert transport.binary == []

    await _make_destination_ready(
        manager,
        transport,
        source_route,
        destination_route,
        request.id,
    )
    for chunk in chunks:
        await manager.handle_binary(source_route.handle, request.id.bytes + chunk)

    await manager.handle_frame(
        source_route.handle,
        TransferEndFrame(
            id=request.id,
            ack=False,
            ok=True,
            bytes_sent=len(data),
            sha256=digest,
        ),
    )
    destination_end = await _wait_for_text_frame(
        transport,
        destination_route.handle,
        TransferEndFrame,
        ack=False,
    )

    assert destination_end.id == request.id == destination_begin.id
    assert destination_end.ok is True
    assert destination_end.bytes_sent == len(data)
    assert destination_end.sha256 == digest
    assert [(handle, decode_binary_chunk(payload)) for handle, payload in transport.binary] == [
        (destination_route.handle, (request.id, chunk)) for chunk in chunks
    ]

    destination_ack = TransferEndFrame(
        id=request.id,
        ack=True,
        ok=True,
        bytes_sent=len(data),
        sha256=digest,
    )
    await manager.handle_frame(destination_route.handle, destination_ack)
    source_ack = await _wait_for_text_frame(
        transport,
        source_route.handle,
        TransferEndFrame,
        ack=True,
    )
    assert source_ack == destination_ack

    result = await asyncio.wait_for(task, timeout=1)
    assert result == TransferResult(len(data), digest)
    assert manager.active_slots == 0
    assert admission.active_count == 0


async def test_client_bridge_rejects_source_binary_before_destination_ready() -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    user_id = uuid4()
    task, request = await _start_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        user_id,
    )
    await _send_source_begin(
        manager,
        transport,
        source_route,
        destination_route,
        request,
        total_bytes=len(b"late"),
    )

    try:
        with pytest.raises(TransferProtocolError, match="ready"):
            await manager.handle_binary(source_route.handle, request.id.bytes + b"x")
        assert transport.binary == []
        assert manager.active_slots == 1
        assert admission.active_count == 1
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert manager.active_slots == 0
        assert admission.active_count == 0


async def test_client_bridge_forwards_destination_failure_ack_to_source() -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    user_id = uuid4()
    task, request = await _start_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        user_id,
    )
    await _send_source_begin(
        manager,
        transport,
        source_route,
        destination_route,
        request,
        total_bytes=0,
    )
    await _make_destination_ready(
        manager,
        transport,
        source_route,
        destination_route,
        request.id,
    )
    await manager.handle_frame(
        source_route.handle,
        TransferEndFrame(
            id=request.id,
            ack=False,
            ok=True,
            bytes_sent=0,
            sha256=hashlib.sha256(b"").hexdigest(),
        ),
    )
    await _wait_for_text_frame(
        transport,
        destination_route.handle,
        TransferEndFrame,
        ack=False,
    )

    destination_ack = TransferEndFrame(
        id=request.id,
        ack=True,
        ok=False,
        code="workspace_storage_unavailable",
    )
    await manager.handle_frame(destination_route.handle, destination_ack)
    source_ack = await _wait_for_text_frame(
        transport,
        source_route.handle,
        TransferEndFrame,
        ack=True,
    )
    assert source_ack == destination_ack

    with pytest.raises(TransferError, match="workspace_storage_unavailable"):
        await asyncio.wait_for(task, timeout=1)
    assert manager.active_slots == 0
    assert admission.active_count == 0


@pytest.mark.parametrize(
    ("destination_ok", "winner"),
    [
        (True, "destination"),
        (False, "destination"),
        (True, "timeout"),
        (False, "timeout"),
    ],
    ids=[
        "success-ack-first",
        "failure-ack-first",
        "timeout-before-success",
        "timeout-before-failure",
    ],
)
async def test_client_bridge_source_timeout_and_destination_ack_choose_one_source_ack(
    destination_ok: bool,
    winner: str,
) -> None:
    transport = _ControlledSourceAckTransport(
        block_source_ack=winner == "destination"
    )
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    transport.source_handle = source_route.handle
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    destination_end = await _issue_source_success(
        manager,
        transport,
        source_route,
        destination_route,
        request.id,
    )
    source_timeout = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_transfer_timeout",
    )
    destination_ack = (
        destination_end.model_copy(update={"ack": True})
        if destination_ok
        else TransferEndFrame(
            id=request.id,
            ack=True,
            ok=False,
            code="workspace_storage_unavailable",
        )
    )

    try:
        if winner == "destination":
            await manager.handle_frame(destination_route.handle, destination_ack)
            await asyncio.wait_for(transport.source_ack_started.wait(), timeout=1)
            await manager.handle_frame(source_route.handle, source_timeout)
            transport.release_source_ack.set()
        else:
            await manager.handle_frame(source_route.handle, source_timeout)
            await manager.handle_frame(destination_route.handle, destination_ack)

        if destination_ok:
            result = await asyncio.wait_for(task, timeout=1)
            assert result.warnings == (
                () if winner == "destination" else ("transfer_ack_failed",)
            )
        else:
            with pytest.raises(TransferError, match="workspace_storage_unavailable"):
                await asyncio.wait_for(task, timeout=1)

        expected_source_ack = (
            destination_ack
            if winner == "destination"
            else source_timeout.model_copy(update={"ack": True})
        )
        assert _text_frames(
            transport,
            source_route.handle,
            TransferEndFrame,
            ack=True,
        ) == [expected_source_ack]
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        transport.release_source_ack.set()
        await _cancel_bridge_task(task)


@pytest.mark.parametrize(
    "source_ack",
    ["delivered", "dropped", "timeout"],
)
async def test_client_bridge_move_deletes_only_after_chosen_ack_delivery(
    source_ack: str,
) -> None:
    transport = _ControlledSourceAckTransport(
        block_source_ack=source_ack != "timeout",
        source_ack_result=source_ack != "dropped",
    )
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    transport.source_handle = source_route.handle
    deleted_fingerprints: list[str] = []

    async def delete_source(fingerprint: str) -> None:
        deleted_fingerprints.append(fingerprint)

    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
        mode="move",
        delete_source=delete_source,
    )
    destination_end = await _issue_source_success(
        manager,
        transport,
        source_route,
        destination_route,
        request.id,
    )
    destination_ack = destination_end.model_copy(update={"ack": True})

    try:
        if source_ack == "timeout":
            await manager.handle_frame(
                source_route.handle,
                TransferEndFrame(
                    id=request.id,
                    ack=False,
                    ok=False,
                    code="workspace_transfer_timeout",
                ),
            )
            await manager.handle_frame(destination_route.handle, destination_ack)
        else:
            await manager.handle_frame(destination_route.handle, destination_ack)
            await asyncio.wait_for(transport.source_ack_started.wait(), timeout=1)
            assert deleted_fingerprints == []
            transport.release_source_ack.set()

        result = await asyncio.wait_for(task, timeout=1)
        if source_ack == "delivered":
            assert deleted_fingerprints == ["source-v1"]
            assert result.warnings == ()
        else:
            assert deleted_fingerprints == []
            assert result.warnings == (
                "transfer_ack_failed",
                "source_delete_failed",
            )
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        transport.release_source_ack.set()
        await _cancel_bridge_task(task)


@pytest.mark.parametrize("retirement", ["disconnect", "fence"])
@pytest.mark.parametrize("endpoint", ["source", "destination"])
async def test_client_bridge_endpoint_retirement_before_destination_terminal_is_unknown(
    retirement: str,
    endpoint: str,
) -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    task, _ = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    retired_route = source_route if endpoint == "source" else destination_route
    peer_route = destination_route if endpoint == "source" else source_route
    transport.unavailable_handles.add(retired_route.handle)

    if retirement == "disconnect":
        await manager.disconnect(retired_route.handle)
    else:
        manager.fence_handle(retired_route.handle)

    with pytest.raises(TransferDisconnectedError, match="outcome is unknown"):
        await asyncio.wait_for(task, timeout=1)
    peer_failures = [
        frame
        for frame in _text_frames(
            transport,
            peer_route.handle,
            TransferEndFrame,
            ack=False,
        )
        if not frame.ok
    ]
    assert len(peer_failures) == 1
    assert peer_failures[0].code == "peer_disconnected"
    assert not any(
        frame.ok
        for frame in _text_frames(
            transport,
            destination_route.handle,
            TransferEndFrame,
            ack=False,
        )
    )
    assert manager.active_slots == 0
    assert admission.active_count == 0


async def test_destination_disconnect_drops_declared_in_flight_source_binary() -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    initial_chunk = b"x"
    late_chunks = [bytes([value]) for value in range(80)]
    source_bytes = initial_chunk + b"".join(late_chunks)
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
        total_bytes=len(source_bytes),
    )
    await manager.handle_binary(source_route.handle, request.id.bytes + initial_chunk)
    transport.unavailable_handles.add(destination_route.handle)
    await asyncio.wait_for(manager.disconnect(destination_route.handle), timeout=1)
    with pytest.raises(TransferDisconnectedError, match="outcome is unknown"):
        await asyncio.wait_for(task, timeout=1)

    for chunk in late_chunks:
        await manager.handle_binary(source_route.handle, request.id.bytes + chunk)
    late_success = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=True,
        bytes_sent=len(source_bytes),
        sha256=hashlib.sha256(source_bytes).hexdigest(),
    )
    await manager.handle_frame(source_route.handle, late_success)
    await manager.handle_frame(source_route.handle, late_success)

    with pytest.raises(TransferProtocolError) as conflicting_terminal:
        await manager.handle_frame(
            source_route.handle,
            late_success.model_copy(update={"sha256": "0" * 64}),
        )
    assert conflicting_terminal.value.code == "protocol_transfer_unknown_id"
    with pytest.raises(TransferProtocolError) as beyond_declared:
        await manager.handle_binary(source_route.handle, request.id.bytes + b"extra")
    assert beyond_declared.value.code == "protocol_transfer_unknown_id"
    assert manager.active_slots == 0
    assert admission.active_count == 0


@pytest.mark.parametrize("retirement", ["disconnect", "fence"])
@pytest.mark.parametrize("endpoint", ["source", "destination"])
async def test_client_bridge_endpoint_retirement_after_destination_terminal_preserves_boundary(
    retirement: str,
    endpoint: str,
) -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport, idle_timeout_seconds=0.05)
    source_route, destination_route = _routes()
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    destination_end = await _issue_source_success(
        manager,
        transport,
        source_route,
        destination_route,
        request.id,
    )
    retired_route = source_route if endpoint == "source" else destination_route
    transport.unavailable_handles.add(retired_route.handle)
    retirement_task: asyncio.Task[None] | None = None
    if retirement == "disconnect":
        retirement_task = asyncio.create_task(manager.disconnect(retired_route.handle))
        await asyncio.sleep(0)
    else:
        manager.fence_handle(retired_route.handle)

    if endpoint == "source":
        await manager.handle_frame(
            destination_route.handle,
            destination_end.model_copy(update={"ack": True}),
        )
        result = await asyncio.wait_for(task, timeout=1)
        assert result.warnings == ("transfer_ack_failed",)
    else:
        with pytest.raises(TransferDisconnectedError, match="outcome is unknown"):
            await asyncio.wait_for(task, timeout=1)

    if retirement_task is not None:
        await asyncio.wait_for(retirement_task, timeout=1)
    destination_terminals = _text_frames(
        transport,
        destination_route.handle,
        TransferEndFrame,
        ack=False,
    )
    assert destination_terminals == [destination_end]
    assert manager.active_slots == 0
    assert admission.active_count == 0


@pytest.mark.parametrize("phase", ["pre-ready", "streaming"])
async def test_client_bridge_destination_failure_waits_for_source_matching_ack(
    phase: str,
) -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport, idle_timeout_seconds=0.1)
    source_route, destination_route = _routes()
    task, request = await _start_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    await _send_source_begin(
        manager,
        transport,
        source_route,
        destination_route,
        request,
        total_bytes=1 if phase == "streaming" else 0,
    )
    if phase == "streaming":
        await _make_destination_ready(
            manager,
            transport,
            source_route,
            destination_route,
            request.id,
        )
        await manager.handle_binary(source_route.handle, request.id.bytes + b"x")
    destination_failure = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_storage_unavailable",
    )

    try:
        await manager.handle_frame(destination_route.handle, destination_failure)
        source_failure = await _wait_for_text_frame(
            transport,
            source_route.handle,
            TransferEndFrame,
            ack=False,
        )
        assert source_failure == destination_failure
        assert not task.done()
        assert _text_frames(
            transport,
            destination_route.handle,
            TransferEndFrame,
            ack=True,
        ) == []

        source_ack = destination_failure.model_copy(update={"ack": True})
        await manager.handle_frame(source_route.handle, source_ack)
        destination_ack = await _wait_for_text_frame(
            transport,
            destination_route.handle,
            TransferEndFrame,
            ack=True,
        )
        assert destination_ack == source_ack
        with pytest.raises(TransferError, match="workspace_storage_unavailable"):
            await asyncio.wait_for(task, timeout=1)
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        await _cancel_bridge_task(task)


async def test_client_bridge_source_failure_before_destination_begin_is_acked_directly() -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport, idle_timeout_seconds=0.1)
    source_route, destination_route = _routes()
    task, request = await _start_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    source_failure = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_storage_unavailable",
    )

    try:
        await manager.handle_frame(source_route.handle, source_failure)
        source_ack = await _wait_for_text_frame(
            transport,
            source_route.handle,
            TransferEndFrame,
            ack=True,
        )
        assert source_ack == source_failure.model_copy(update={"ack": True})
        assert _text_frames(
            transport,
            destination_route.handle,
            TransferBeginFrame,
        ) == []
        with pytest.raises(TransferError, match="workspace_storage_unavailable"):
            await asyncio.wait_for(task, timeout=1)
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        await _cancel_bridge_task(task)


async def test_client_bridge_source_failure_after_destination_begin_waits_for_destination_ack() -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport, idle_timeout_seconds=0.1)
    source_route, destination_route = _routes()
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    source_failure = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_storage_unavailable",
    )

    try:
        await manager.handle_frame(source_route.handle, source_failure)
        destination_failure = await _wait_for_text_frame(
            transport,
            destination_route.handle,
            TransferEndFrame,
            ack=False,
        )
        assert destination_failure == source_failure
        assert not task.done()
        assert _text_frames(
            transport,
            source_route.handle,
            TransferEndFrame,
            ack=True,
        ) == []

        destination_ack = source_failure.model_copy(update={"ack": True})
        await manager.handle_frame(destination_route.handle, destination_ack)
        source_ack = await _wait_for_text_frame(
            transport,
            source_route.handle,
            TransferEndFrame,
            ack=True,
        )
        assert source_ack == destination_ack
        with pytest.raises(TransferError, match="workspace_storage_unavailable"):
            await asyncio.wait_for(task, timeout=1)
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        await _cancel_bridge_task(task)


async def test_bridge_provisional_tombstones_are_pinned_and_final_ttl_starts_at_cleanup() -> None:
    transport = _BlockingFailureSendTransport()
    manager, admission = _manager(
        transport,
        idle_timeout_seconds=0.2,
        tombstone_ttl_seconds=0.05,
    )
    source_route, destination_route = _routes()
    transport.block_handle = source_route.handle
    task, request = await _start_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    await _send_source_begin(
        manager,
        transport,
        source_route,
        destination_route,
        request,
        total_bytes=len(b"late"),
    )
    await _make_destination_ready(
        manager,
        transport,
        source_route,
        destination_route,
        request.id,
    )
    failure = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_storage_unavailable",
    )
    await manager.handle_frame(destination_route.handle, failure)
    await asyncio.wait_for(transport.failure_send_started.wait(), timeout=1)

    try:
        await asyncio.sleep(0.08)
        await manager.handle_frame(destination_route.handle, failure)
        await manager.handle_binary(source_route.handle, request.id.bytes + b"late")

        transport.release_failure_send.set()
        await manager.handle_frame(
            source_route.handle,
            failure.model_copy(update={"ack": True}),
        )
        await _wait_for_text_frame(
            transport,
            destination_route.handle,
            TransferEndFrame,
            ack=True,
        )
        with pytest.raises(TransferError, match="workspace_storage_unavailable"):
            await asyncio.wait_for(task, timeout=1)

        await manager.handle_frame(destination_route.handle, failure)
        await asyncio.sleep(0.08)
        with pytest.raises(TransferProtocolError) as expired:
            await manager.handle_frame(destination_route.handle, failure)
        assert expired.value.code == "protocol_transfer_unknown_id"
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        transport.release_failure_send.set()
        await _cancel_bridge_task(task)


async def test_failed_bridge_tombstone_drops_only_declared_source_binary() -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    request, _ = await _complete_destination_rejection(
        manager,
        transport,
        source_route,
        destination_route,
        total_bytes=80,
        after_ready=True,
    )

    for value in range(80):
        await manager.handle_binary(
            source_route.handle,
            request.id.bytes + bytes([value]),
        )
    with pytest.raises(TransferProtocolError) as exhausted:
        await manager.handle_binary(source_route.handle, request.id.bytes + b"late")
    assert exhausted.value.code == "protocol_transfer_unknown_id"

    with pytest.raises(TransferProtocolError) as wrong_role:
        await manager.handle_binary(destination_route.handle, request.id.bytes + b"late")
    assert wrong_role.value.code == "protocol_transfer_unknown_id"
    with pytest.raises(TransferProtocolError) as empty:
        await manager.handle_binary(source_route.handle, request.id.bytes)
    assert empty.value.code == "protocol_transfer_unknown_id"
    assert manager.active_slots == 0
    assert admission.active_count == 0


async def test_pre_ready_rejection_does_not_open_late_source_data_gate() -> None:
    transport = _BridgeTransport()
    manager, _ = _manager(transport)
    source_route, destination_route = _routes()
    request, _ = await _complete_destination_rejection(
        manager,
        transport,
        source_route,
        destination_route,
        total_bytes=1,
    )

    with pytest.raises(TransferProtocolError):
        await manager.handle_binary(source_route.handle, request.id.bytes + b"x")
    with pytest.raises(TransferProtocolError):
        await manager.handle_frame(
            source_route.handle,
            TransferProgressFrame(id=request.id, bytes_sent=1),
        )


async def test_failed_bridge_tombstone_drops_at_most_64_late_progress_frames() -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    request, _ = await _complete_destination_rejection(
        manager,
        transport,
        source_route,
        destination_route,
        total_bytes=LATE_PROGRESS_MAX + 1,
        after_ready=True,
    )

    assert LATE_PROGRESS_MAX == 64
    for bytes_sent in range(1, LATE_PROGRESS_MAX + 1):
        await manager.handle_frame(
            source_route.handle,
            TransferProgressFrame(id=request.id, bytes_sent=bytes_sent),
        )
    with pytest.raises(TransferProtocolError) as exhausted:
        await manager.handle_frame(
            source_route.handle,
            TransferProgressFrame(id=request.id, bytes_sent=LATE_PROGRESS_MAX + 1),
        )
    assert exhausted.value.code == "protocol_transfer_unknown_id"
    assert manager.active_slots == 0
    assert admission.active_count == 0


@pytest.mark.parametrize(
    "progress_values",
    [(2, 1), (9,)],
    ids=["backwards", "over-declared"],
)
async def test_failed_bridge_tombstone_rejects_invalid_late_progress(
    progress_values: tuple[int, ...],
) -> None:
    transport = _BridgeTransport()
    manager, _ = _manager(transport)
    source_route, destination_route = _routes()
    request, _ = await _complete_destination_rejection(
        manager,
        transport,
        source_route,
        destination_route,
        total_bytes=8,
        after_ready=True,
    )

    if len(progress_values) == 2:
        await manager.handle_frame(
            source_route.handle,
            TransferProgressFrame(id=request.id, bytes_sent=progress_values[0]),
        )
    with pytest.raises(TransferProtocolError) as invalid:
        await manager.handle_frame(
            source_route.handle,
            TransferProgressFrame(id=request.id, bytes_sent=progress_values[-1]),
        )
    assert invalid.value.code == "protocol_transfer_unknown_id"


async def test_destination_ack_winner_tombstone_consumes_late_timeout_without_second_ack() -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    request, _ = await _complete_successful_empty_bridge(
        manager,
        transport,
        source_route,
        destination_route,
    )
    timeout = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_transfer_timeout",
    )
    source_acks_before = _text_frames(
        transport,
        source_route.handle,
        TransferEndFrame,
        ack=True,
    )
    assert len(source_acks_before) == 1

    await manager.handle_frame(source_route.handle, timeout)
    await manager.handle_frame(source_route.handle, timeout)

    assert _text_frames(
        transport,
        source_route.handle,
        TransferEndFrame,
        ack=True,
    ) == source_acks_before
    assert manager.active_slots == 0
    assert admission.active_count == 0


async def test_timeout_winner_tombstone_sends_at_most_one_matching_ack_after_cleanup() -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport, idle_timeout_seconds=0.05)
    source_route, destination_route = _routes()
    request, _ = await _complete_unknown_destination_outcome(
        manager,
        transport,
        source_route,
        destination_route,
    )
    timeout = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_transfer_timeout",
    )

    await manager.handle_frame(source_route.handle, timeout)
    await manager.handle_frame(source_route.handle, timeout)

    assert _text_frames(
        transport,
        source_route.handle,
        TransferEndFrame,
        ack=True,
    ) == [timeout.model_copy(update={"ack": True})]
    assert manager.active_slots == 0
    assert admission.active_count == 0


@pytest.mark.parametrize("destination_ok", [True, False], ids=["success", "failure"])
async def test_late_destination_ack_after_unknown_outcome_is_consumed_without_side_effects(
    destination_ok: bool,
) -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport, idle_timeout_seconds=0.05)
    source_route, destination_route = _routes()
    deleted_fingerprints: list[str] = []

    async def delete_source(fingerprint: str) -> None:
        deleted_fingerprints.append(fingerprint)

    request, destination_end = await _complete_unknown_destination_outcome(
        manager,
        transport,
        source_route,
        destination_route,
        mode="move",
        delete_source=delete_source,
    )
    late_ack = (
        destination_end.model_copy(update={"ack": True})
        if destination_ok
        else TransferEndFrame(
            id=request.id,
            ack=True,
            ok=False,
            code="workspace_storage_unavailable",
        )
    )
    source_acks_before = _text_frames(
        transport,
        source_route.handle,
        TransferEndFrame,
        ack=True,
    )

    await manager.handle_frame(destination_route.handle, late_ack)
    await manager.handle_frame(destination_route.handle, late_ack)

    conflicting_ack = (
        TransferEndFrame(
            id=request.id,
            ack=True,
            ok=False,
            code="workspace_storage_unavailable",
        )
        if destination_ok
        else destination_end.model_copy(update={"ack": True})
    )
    with pytest.raises(TransferProtocolError) as conflicting:
        await manager.handle_frame(destination_route.handle, conflicting_ack)
    assert conflicting.value.code == "protocol_transfer_unknown_id"

    assert deleted_fingerprints == []
    assert _text_frames(
        transport,
        source_route.handle,
        TransferEndFrame,
        ack=True,
    ) == source_acks_before
    assert manager.active_slots == 0
    assert admission.active_count == 0


async def test_bridge_tombstones_reject_wrong_endpoint_role_and_conflicting_terminal() -> None:
    transport = _BridgeTransport()
    manager, _ = _manager(transport)
    source_route, destination_route = _routes()
    request, source_terminal = await _complete_successful_empty_bridge(
        manager,
        transport,
        source_route,
        destination_route,
    )
    destination_ack = source_terminal.model_copy(update={"ack": True})
    timeout = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_transfer_timeout",
    )
    conflicting = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_storage_unavailable",
    )
    conflicting_destination_ack = conflicting.model_copy(update={"ack": True})
    wrong_generation = ConnectionHandle(
        source_route.handle.device_id,
        source_route.handle.generation + 1,
    )

    for handle, frame in (
        (source_route.handle, destination_ack),
        (destination_route.handle, source_terminal),
        (destination_route.handle, timeout),
        (destination_route.handle, conflicting_destination_ack),
        (source_route.handle, conflicting),
        (wrong_generation, timeout),
    ):
        with pytest.raises(TransferProtocolError) as rejected:
            await manager.handle_frame(handle, frame)
        assert rejected.value.code == "protocol_transfer_unknown_id"


async def test_bridge_reserves_two_endpoint_tombstones_but_counts_one_logical_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(transfer_module, "TOMBSTONE_MAX_ENTRIES", 2)
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    task, _ = await _start_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    other_source, other_destination = _routes()

    try:
        assert manager.active_slots == 1
        assert admission.active_count == 1
        with pytest.raises(TransferBusyError, match="tombstone capacity"):
            await manager.start_client_to_client(
                source_route=other_source,
                destination_route=other_destination,
                user_id=uuid4(),
                src_path="source.bin",
                dst_path="destination.bin",
                mode="copy",
                delete_source=None,
                on_issued=None,
            )
        assert manager.active_slots == 1
        assert admission.active_count == 1
        assert len(
            [
                frame
                for sent in transport.text
                if isinstance(
                    frame := parse_server_frame(sent.payload),
                    TransferRequestFrame,
                )
            ]
        ) == 1
    finally:
        await _cancel_bridge_task(task)
    assert manager.active_slots == 0
    assert admission.active_count == 0


async def test_tombstone_capacity_evicts_final_entries_without_evicting_pinned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(transfer_module, "TOMBSTONE_MAX_ENTRIES", 4)
    transport = _BlockingFailureSendTransport()
    manager, admission = _manager(transport, idle_timeout_seconds=0.2)
    pinned_source, pinned_destination = _routes()
    transport.block_handle = pinned_source.handle
    pinned_task, pinned_request = await _start_bridge(
        manager,
        transport,
        pinned_source,
        pinned_destination,
        uuid4(),
    )
    await _send_source_begin(
        manager,
        transport,
        pinned_source,
        pinned_destination,
        pinned_request,
        total_bytes=0,
    )
    pinned_failure = TransferEndFrame(
        id=pinned_request.id,
        ack=False,
        ok=False,
        code="workspace_storage_unavailable",
    )
    await manager.handle_frame(pinned_destination.handle, pinned_failure)
    await asyncio.wait_for(transport.failure_send_started.wait(), timeout=1)

    final_source, final_destination = _routes()
    final_request, _ = await _complete_successful_empty_bridge(
        manager,
        transport,
        final_source,
        final_destination,
    )
    next_source, next_destination = _routes()
    next_task, _ = await _start_bridge(
        manager,
        transport,
        next_source,
        next_destination,
        uuid4(),
    )

    try:
        await manager.handle_frame(pinned_destination.handle, pinned_failure)
        evicted_final_timeout = TransferEndFrame(
            id=final_request.id,
            ack=False,
            ok=False,
            code="workspace_transfer_timeout",
        )
        with pytest.raises(TransferProtocolError) as evicted:
            await manager.handle_frame(final_source.handle, evicted_final_timeout)
        assert evicted.value.code == "protocol_transfer_unknown_id"

        transport.release_failure_send.set()
        pinned_ack = pinned_failure.model_copy(update={"ack": True})
        await manager.handle_frame(pinned_source.handle, pinned_ack)
        await _wait_for_text_frame(
            transport,
            pinned_destination.handle,
            TransferEndFrame,
            ack=True,
        )
        with pytest.raises(TransferError, match="workspace_storage_unavailable"):
            await asyncio.wait_for(pinned_task, timeout=1)
        assert manager.active_slots == 1
        assert admission.active_count == 1
    finally:
        transport.release_failure_send.set()
        await _cancel_bridge_task(pinned_task)
        await _cancel_bridge_task(next_task)
    assert manager.active_slots == 0
    assert admission.active_count == 0


async def test_bridge_source_activity_uses_a_sliding_idle_deadline() -> None:
    transport = _BridgeTransport()
    idle_timeout = 0.06
    manager, admission = _manager(
        transport,
        idle_timeout_seconds=idle_timeout,
    )
    source_route, destination_route = _routes()
    chunks = [b"a", b"b", b"c", b"d"]
    data = b"".join(chunks)
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
        total_bytes=len(data),
    )
    started_at = asyncio.get_running_loop().time()

    for chunk in chunks:
        await asyncio.sleep(0.035)
        await manager.handle_binary(source_route.handle, request.id.bytes + chunk)
    elapsed = asyncio.get_running_loop().time() - started_at
    assert elapsed > idle_timeout

    digest = hashlib.sha256(data).hexdigest()
    await manager.handle_frame(
        source_route.handle,
        TransferEndFrame(
            id=request.id,
            ack=False,
            ok=True,
            bytes_sent=len(data),
            sha256=digest,
        ),
    )
    destination_end = await _wait_for_text_frame(
        transport,
        destination_route.handle,
        TransferEndFrame,
        ack=False,
    )
    await manager.handle_frame(
        destination_route.handle,
        destination_end.model_copy(update={"ack": True}),
    )

    assert await asyncio.wait_for(task, timeout=1) == TransferResult(len(data), digest)
    assert manager.active_slots == 0
    assert admission.active_count == 0


async def test_bridge_destination_writer_stall_times_out_and_releases_all_state() -> None:
    transport = _BlockingBinaryTransport()
    manager, admission = _manager(transport, idle_timeout_seconds=0.05)
    source_route, destination_route = _routes()
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
        total_bytes=1,
    )
    await manager.handle_binary(source_route.handle, request.id.bytes + b"x")
    await asyncio.wait_for(transport.binary_send_started.wait(), timeout=1)
    await manager.handle_frame(
        source_route.handle,
        TransferEndFrame(
            id=request.id,
            ack=False,
            ok=True,
            bytes_sent=1,
            sha256=hashlib.sha256(b"x").hexdigest(),
        ),
    )

    try:
        with pytest.raises(TransferDisconnectedError, match="outcome is unknown"):
            await asyncio.wait_for(task, timeout=1)
        assert transport.binary_send_cancelled.is_set()
        assert not any(
            frame.ok
            for frame in _text_frames(
                transport,
                destination_route.handle,
                TransferEndFrame,
                ack=False,
            )
        )
        assert manager.active_slots == 0
        assert manager.slot_ids == ()
        assert admission.active_count == 0
        assert admission.active_by_user == {}
    finally:
        transport.release_binary_send.set()
        await _cancel_bridge_task(task)


async def test_source_failure_remains_authoritative_when_fenced_before_worker_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    stage_waiting, claim_started, release_claim = _gate_worker_failure_claim(
        manager,
        monkeypatch,
    )
    source_route, destination_route = _routes()
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    await asyncio.wait_for(stage_waiting.wait(), timeout=1)
    source_failure = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_storage_unavailable",
    )

    try:
        await manager.handle_frame(source_route.handle, source_failure)
        assert not claim_started.is_set()
        manager.fence_handle(source_route.handle)

        with pytest.raises(TransferError) as failed:
            await asyncio.wait_for(task, timeout=1)
        assert failed.value.code == source_failure.code
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        release_claim.set()
        await _cancel_bridge_task(task)


async def test_destination_failure_remains_authoritative_when_fenced_before_worker_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    stage_waiting, claim_started, release_claim = _gate_worker_failure_claim(
        manager,
        monkeypatch,
    )
    source_route, destination_route = _routes()
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    await asyncio.wait_for(stage_waiting.wait(), timeout=1)
    destination_failure = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_storage_unavailable",
    )

    try:
        await manager.handle_frame(destination_route.handle, destination_failure)
        assert not claim_started.is_set()
        manager.fence_handle(destination_route.handle)

        with pytest.raises(TransferError) as failed:
            await asyncio.wait_for(task, timeout=1)
        assert failed.value.code == destination_failure.code
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        release_claim.set()
        await _cancel_bridge_task(task)


@pytest.mark.parametrize("failure_origin", ["source", "destination"])
async def test_peer_rejection_and_disconnect_send_failure_once(
    monkeypatch: pytest.MonkeyPatch,
    failure_origin: str,
) -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport, idle_timeout_seconds=5)
    source_route, destination_route = _routes()
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    failure = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_storage_unavailable",
    )
    first_send_started = asyncio.Event()
    release_first_send = asyncio.Event()
    duplicate_send_started = asyncio.Event()
    failure_sends = 0
    original_send_text = manager._send_text  # noqa: SLF001
    failure_route = source_route if failure_origin == "source" else destination_route
    peer_route = destination_route if failure_origin == "source" else source_route

    async def gated_send_text(
        handle: object,
        payload: str,
        *,
        route: Any,
        on_issued: Callable[[], None] | None = None,
    ) -> bool:
        nonlocal failure_sends
        frame = parse_server_frame(payload)
        if handle == peer_route.handle and frame == failure:
            failure_sends += 1
            if failure_sends == 1:
                first_send_started.set()
                await release_first_send.wait()
            else:
                duplicate_send_started.set()
        return await original_send_text(
            handle,
            payload,
            route=route,
            on_issued=on_issued,
        )

    monkeypatch.setattr(manager, "_send_text", gated_send_text)
    await manager.handle_frame(failure_route.handle, failure)
    await asyncio.wait_for(first_send_started.wait(), timeout=1)
    bridge = manager._bridges[request.id]  # noqa: SLF001
    assert bridge.worker is not None
    disconnect = asyncio.create_task(manager.disconnect(failure_route.handle))

    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(
                asyncio.shield(duplicate_send_started.wait()),
                timeout=0.02,
            )
        assert failure_sends == 1
    finally:
        release_first_send.set()

    await asyncio.wait_for(disconnect, timeout=1)
    with pytest.raises(TransferError, match="workspace_storage_unavailable"):
        await asyncio.wait_for(task, timeout=1)
    async with asyncio.timeout(0.1):
        while not bridge.worker.done():
            await asyncio.sleep(0)
    assert manager.active_slots == 0
    assert admission.active_count == 0


async def test_blocked_timeout_ack_cleanup_deduplicates_tombstone_timeout() -> None:
    transport = _ControlledSourceAckTransport(block_source_ack=True)
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    transport.source_handle = source_route.handle
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    destination_end = await _issue_source_success(
        manager,
        transport,
        source_route,
        destination_route,
        request.id,
    )
    timeout = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_transfer_timeout",
    )
    timeout_task = asyncio.create_task(
        manager.handle_frame(source_route.handle, timeout)
    )

    try:
        await asyncio.wait_for(transport.source_ack_started.wait(), timeout=1)
        await manager.handle_frame(
            destination_route.handle,
            destination_end.model_copy(update={"ack": True}),
        )
        for _ in range(10):
            await asyncio.sleep(0)
        assert not task.done()
        assert manager.active_slots == 1
        assert admission.active_count == 1

        await asyncio.wait_for(
            manager.handle_frame(source_route.handle, timeout),
            timeout=0.1,
        )
        assert _text_frames(
            transport,
            source_route.handle,
            TransferEndFrame,
            ack=True,
        ) == [timeout.model_copy(update={"ack": True})]

        transport.release_source_ack.set()
        await asyncio.wait_for(timeout_task, timeout=1)
        result = await asyncio.wait_for(task, timeout=1)
        assert result.warnings == ("transfer_ack_failed",)
        assert manager.active_slots == 0
        await manager.handle_frame(source_route.handle, timeout)
        assert len(
            _text_frames(
                transport,
                source_route.handle,
                TransferEndFrame,
                ack=True,
            )
        ) == 1
        assert admission.active_count == 0
    finally:
        transport.release_source_ack.set()
        if not timeout_task.done():
            timeout_task.cancel()
        await asyncio.gather(timeout_task, return_exceptions=True)
        await _cancel_bridge_task(task)


async def test_timeout_ack_attempt_is_bounded_before_final_tombstone_ttl() -> None:
    transport = _ControlledSourceAckTransport(block_source_ack=True)
    manager, admission = _manager(
        transport,
        idle_timeout_seconds=0.05,
        tombstone_ttl_seconds=0.05,
    )
    source_route, destination_route = _routes()
    transport.source_handle = source_route.handle
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    destination_end = await _issue_source_success(
        manager,
        transport,
        source_route,
        destination_route,
        request.id,
    )
    timeout = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_transfer_timeout",
    )
    timeout_task = asyncio.create_task(
        manager.handle_frame(source_route.handle, timeout)
    )

    try:
        await asyncio.wait_for(transport.source_ack_started.wait(), timeout=1)
        await manager.handle_frame(
            destination_route.handle,
            destination_end.model_copy(update={"ack": True}),
        )
        await asyncio.sleep(0.02)
        assert manager.active_slots == 1
        assert admission.active_count == 1

        result = await asyncio.wait_for(task, timeout=1)
        await asyncio.wait_for(timeout_task, timeout=1)
        assert result.warnings == ("transfer_ack_failed",)
        assert manager.active_slots == 0
        assert admission.active_count == 0

        await manager.handle_frame(source_route.handle, timeout)
        assert _text_frames(
            transport,
            source_route.handle,
            TransferEndFrame,
            ack=True,
        ) == [timeout.model_copy(update={"ack": True})]
    finally:
        transport.release_source_ack.set()
        if not timeout_task.done():
            timeout_task.cancel()
        await asyncio.gather(timeout_task, return_exceptions=True)
        await _cancel_bridge_task(task)


async def test_bridge_queue_drain_uses_sliding_destination_write_deadline() -> None:
    idle_timeout = 0.06
    transport = _PacedBinaryTransport(delay_seconds=0.035)
    manager, admission = _manager(
        transport,
        idle_timeout_seconds=idle_timeout,
    )
    source_route, destination_route = _routes()
    chunks = [b"a", b"b", b"c", b"d"]
    data = b"".join(chunks)
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
        total_bytes=len(data),
    )

    await manager.handle_binary(source_route.handle, request.id.bytes + chunks[0])
    await asyncio.wait_for(transport.first_binary_started.wait(), timeout=1)
    for chunk in chunks[1:]:
        await manager.handle_binary(source_route.handle, request.id.bytes + chunk)
    digest = hashlib.sha256(data).hexdigest()
    await manager.handle_frame(
        source_route.handle,
        TransferEndFrame(
            id=request.id,
            ack=False,
            ok=True,
            bytes_sent=len(data),
            sha256=digest,
        ),
    )
    drain_started = asyncio.get_running_loop().time()
    transport.allow_binary_sends.set()

    destination_end = await _wait_for_text_frame(
        transport,
        destination_route.handle,
        TransferEndFrame,
        ack=False,
    )
    drain_elapsed = asyncio.get_running_loop().time() - drain_started
    assert drain_elapsed > idle_timeout
    assert transport.completed_sends == len(chunks)
    await manager.handle_frame(
        destination_route.handle,
        destination_end.model_copy(update={"ack": True}),
    )

    assert await asyncio.wait_for(task, timeout=1) == TransferResult(len(data), digest)
    assert manager.active_slots == 0
    assert admission.active_count == 0


async def test_source_failure_after_begin_but_before_destination_issue_stays_source_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    source_begin_claimed = asyncio.Event()
    release_source_begin = asyncio.Event()
    original_wait = manager._wait_bridge_stage

    async def gate_source_begin(
        bridge: Any,
        *futures: asyncio.Future[Any],
    ) -> Any:
        result = await original_wait(bridge, *futures)
        if (
            futures
            and futures[0] is bridge.source_begin_future
            and isinstance(result, TransferBeginFrame)
        ):
            source_begin_claimed.set()
            await release_source_begin.wait()
        return result

    monkeypatch.setattr(manager, "_wait_bridge_stage", gate_source_begin)
    task, request = await _start_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    source_begin = TransferBeginFrame(
        id=request.id,
        direction="client_to_server",
        purpose="file_transfer",
        src_device=source_route.device_name,
        src_path="source.bin",
        dst_device="server",
        dst_path="destination.bin",
        total_bytes=0,
        etag="source-v1",
    )
    source_failure = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_storage_unavailable",
    )

    try:
        await manager.handle_frame(source_route.handle, source_begin)
        await asyncio.wait_for(source_begin_claimed.wait(), timeout=1)
        await manager.handle_frame(source_route.handle, source_failure)
        release_source_begin.set()

        source_ack = await _wait_for_text_frame(
            transport,
            source_route.handle,
            TransferEndFrame,
            ack=True,
        )
        assert source_ack == source_failure.model_copy(update={"ack": True})
        assert _text_frames(
            transport,
            destination_route.handle,
            TransferBeginFrame,
        ) == []
        assert _text_frames(
            transport,
            destination_route.handle,
            TransferEndFrame,
        ) == []
        with pytest.raises(TransferError, match="workspace_storage_unavailable"):
            await asyncio.wait_for(task, timeout=1)
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        release_source_begin.set()
        await _cancel_bridge_task(task)


async def test_destination_failure_after_ready_before_worker_claim_is_valid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    destination_ready_claimed = asyncio.Event()
    release_destination_ready = asyncio.Event()
    original_wait = manager._wait_bridge_stage

    async def gate_destination_ready(
        bridge: Any,
        *futures: asyncio.Future[Any],
    ) -> Any:
        result = await original_wait(bridge, *futures)
        if (
            futures
            and futures[0] is bridge.destination_ready_future
            and isinstance(result, TransferReadyFrame)
        ):
            destination_ready_claimed.set()
            await release_destination_ready.wait()
        return result

    monkeypatch.setattr(manager, "_wait_bridge_stage", gate_destination_ready)
    task, request = await _start_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    await _send_source_begin(
        manager,
        transport,
        source_route,
        destination_route,
        request,
        total_bytes=0,
    )
    await manager.handle_frame(
        destination_route.handle,
        TransferReadyFrame(id=request.id),
    )
    await asyncio.wait_for(destination_ready_claimed.wait(), timeout=1)
    destination_failure = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_storage_unavailable",
    )

    try:
        await manager.handle_frame(destination_route.handle, destination_failure)
        release_destination_ready.set()
        source_failure = await _wait_for_text_frame(
            transport,
            source_route.handle,
            TransferEndFrame,
            ack=False,
        )
        assert source_failure == destination_failure

        source_ack = destination_failure.model_copy(update={"ack": True})
        await manager.handle_frame(source_route.handle, source_ack)
        destination_ack = await _wait_for_text_frame(
            transport,
            destination_route.handle,
            TransferEndFrame,
            ack=True,
        )
        assert destination_ack == source_ack
        with pytest.raises(TransferError, match="workspace_storage_unavailable"):
            await asyncio.wait_for(task, timeout=1)
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        release_destination_ready.set()
        await _cancel_bridge_task(task)


async def test_destination_failure_stops_queued_binary_relay_immediately() -> None:
    transport = _BlockingBinaryTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    chunks = [bytes([index]) for index in range(5)]
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
        total_bytes=len(chunks),
    )

    try:
        await manager.handle_binary(source_route.handle, request.id.bytes + chunks[0])
        await asyncio.wait_for(transport.binary_send_started.wait(), timeout=1)
        for chunk in chunks[1:]:
            await manager.handle_binary(source_route.handle, request.id.bytes + chunk)

        destination_failure = TransferEndFrame(
            id=request.id,
            ack=False,
            ok=False,
            code="workspace_storage_unavailable",
        )
        await manager.handle_frame(destination_route.handle, destination_failure)
        source_failure = await _wait_for_text_frame(
            transport,
            source_route.handle,
            TransferEndFrame,
            ack=False,
        )
        assert source_failure == destination_failure

        transport.release_binary_send.set()
        await asyncio.sleep(0.01)
        assert len(transport.binary) == 1

        source_ack = destination_failure.model_copy(update={"ack": True})
        await manager.handle_frame(source_route.handle, source_ack)
        await _wait_for_text_frame(
            transport,
            destination_route.handle,
            TransferEndFrame,
            ack=True,
        )
        with pytest.raises(TransferError, match="workspace_storage_unavailable"):
            await asyncio.wait_for(task, timeout=1)
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        transport.release_binary_send.set()
        await _cancel_bridge_task(task)


async def test_source_failure_stops_queued_binary_relay_immediately() -> None:
    transport = _BlockingBinaryTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    chunks = [bytes([index]) for index in range(5)]
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
        total_bytes=len(chunks),
    )

    try:
        await manager.handle_binary(source_route.handle, request.id.bytes + chunks[0])
        await asyncio.wait_for(transport.binary_send_started.wait(), timeout=1)
        for chunk in chunks[1:]:
            await manager.handle_binary(source_route.handle, request.id.bytes + chunk)

        source_failure = TransferEndFrame(
            id=request.id,
            ack=False,
            ok=False,
            code="workspace_storage_unavailable",
        )
        await manager.handle_frame(source_route.handle, source_failure)
        destination_failure = await _wait_for_text_frame(
            transport,
            destination_route.handle,
            TransferEndFrame,
            ack=False,
        )
        assert destination_failure == source_failure

        transport.release_binary_send.set()
        await asyncio.sleep(0.01)
        assert len(transport.binary) == 1

        destination_ack = source_failure.model_copy(update={"ack": True})
        await manager.handle_frame(destination_route.handle, destination_ack)
        await _wait_for_text_frame(
            transport,
            source_route.handle,
            TransferEndFrame,
            ack=True,
        )
        with pytest.raises(TransferError, match="workspace_storage_unavailable"):
            await asyncio.wait_for(task, timeout=1)
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        transport.release_binary_send.set()
        await _cancel_bridge_task(task)


@pytest.mark.parametrize("fenced_endpoint", ["source", "destination"])
async def test_endpoint_fence_rejects_same_tick_source_binary_without_mutation(
    fenced_endpoint: str,
) -> None:
    transport = _BlockingBinaryTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
        total_bytes=1,
    )
    bridge = manager._bridges[request.id]  # noqa: SLF001
    fenced_route = source_route if fenced_endpoint == "source" else destination_route

    manager.fence_handle(fenced_route.handle)
    await manager.handle_binary(source_route.handle, request.id.bytes + b"x")

    assert bridge.bytes_received == 0
    assert bridge.digest.hexdigest() == hashlib.sha256(b"").hexdigest()
    assert bridge.queue.empty()
    with pytest.raises(TransferDisconnectedError, match="outcome is unknown"):
        await asyncio.wait_for(task, timeout=1)
    assert manager.active_slots == 0
    assert admission.active_count == 0


async def test_bridge_move_delete_uses_its_independent_private_call_timeout() -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport, idle_timeout_seconds=0.05)
    source_route, destination_route = _routes()
    deleted = asyncio.Event()

    async def delete_source(fingerprint: str) -> None:
        assert fingerprint == "source-v1"
        await asyncio.sleep(0.08)
        deleted.set()

    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
        mode="move",
        delete_source=delete_source,
    )
    destination_end = await _issue_source_success(
        manager,
        transport,
        source_route,
        destination_route,
        request.id,
    )
    await manager.handle_frame(
        destination_route.handle,
        destination_end.model_copy(update={"ack": True}),
    )

    result = await asyncio.wait_for(task, timeout=1)
    assert deleted.is_set()
    assert result.warnings == ()
    assert manager.active_slots == 0
    assert admission.active_count == 0


async def test_destination_failure_during_source_end_drain_remains_authoritative() -> None:
    transport = _BlockingBinaryTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
        total_bytes=1,
    )
    await manager.handle_binary(source_route.handle, request.id.bytes + b"x")
    await asyncio.wait_for(transport.binary_send_started.wait(), timeout=1)
    await manager.handle_frame(
        source_route.handle,
        TransferEndFrame(
            id=request.id,
            ack=False,
            ok=True,
            bytes_sent=1,
            sha256=hashlib.sha256(b"x").hexdigest(),
        ),
    )
    failure = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_storage_unavailable",
    )

    try:
        await manager.handle_frame(destination_route.handle, failure)
        forwarded = await _wait_for_text_frame(
            transport,
            source_route.handle,
            TransferEndFrame,
            ack=False,
        )
        assert forwarded == failure
        await manager.handle_frame(
            source_route.handle,
            failure.model_copy(update={"ack": True}),
        )
        returned = await _wait_for_text_frame(
            transport,
            destination_route.handle,
            TransferEndFrame,
            ack=True,
        )
        assert returned == failure.model_copy(update={"ack": True})
        with pytest.raises(TransferError, match="workspace_storage_unavailable"):
            await asyncio.wait_for(task, timeout=1)
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        transport.release_binary_send.set()
        await _cancel_bridge_task(task)


async def test_source_timeout_during_source_end_drain_runs_abort_handshake() -> None:
    transport = _BlockingBinaryTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
        total_bytes=1,
    )
    await manager.handle_binary(source_route.handle, request.id.bytes + b"x")
    await asyncio.wait_for(transport.binary_send_started.wait(), timeout=1)
    await manager.handle_frame(
        source_route.handle,
        TransferEndFrame(
            id=request.id,
            ack=False,
            ok=True,
            bytes_sent=1,
            sha256=hashlib.sha256(b"x").hexdigest(),
        ),
    )
    timeout = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_transfer_timeout",
    )

    try:
        await manager.handle_frame(source_route.handle, timeout)
        forwarded = await _wait_for_text_frame(
            transport,
            destination_route.handle,
            TransferEndFrame,
            ack=False,
        )
        assert forwarded == timeout
        await manager.handle_frame(
            destination_route.handle,
            timeout.model_copy(update={"ack": True}),
        )
        returned = await _wait_for_text_frame(
            transport,
            source_route.handle,
            TransferEndFrame,
            ack=True,
        )
        assert returned == timeout.model_copy(update={"ack": True})
        with pytest.raises(TransferError, match="workspace_transfer_timeout"):
            await asyncio.wait_for(task, timeout=1)
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        transport.release_binary_send.set()
        await _cancel_bridge_task(task)


async def test_destination_failure_wakes_source_chunk_blocked_on_full_queue() -> None:
    transport = _BlockingBinaryTransport()
    manager, admission = _manager(transport, idle_timeout_seconds=0.1)
    source_route, destination_route = _routes()
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
        total_bytes=TRANSFER_QUEUE_CHUNKS + 2,
    )
    await manager.handle_binary(source_route.handle, request.id.bytes + b"0")
    await asyncio.wait_for(transport.binary_send_started.wait(), timeout=1)
    for value in range(1, TRANSFER_QUEUE_CHUNKS + 1):
        await manager.handle_binary(source_route.handle, request.id.bytes + bytes([value]))
    blocked = asyncio.create_task(
        manager.handle_binary(
            source_route.handle,
            request.id.bytes + bytes([TRANSFER_QUEUE_CHUNKS + 1]),
        )
    )
    await asyncio.sleep(0)
    assert not blocked.done()
    failure = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_storage_unavailable",
    )

    try:
        await manager.handle_frame(destination_route.handle, failure)
        await asyncio.wait_for(blocked, timeout=0.05)
        await manager.handle_frame(
            source_route.handle,
            failure.model_copy(update={"ack": True}),
        )
        await _wait_for_text_frame(
            transport,
            destination_route.handle,
            TransferEndFrame,
            ack=True,
        )
        with pytest.raises(TransferError, match="workspace_storage_unavailable"):
            await asyncio.wait_for(task, timeout=1)
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        transport.release_binary_send.set()
        if not blocked.done():
            blocked.cancel()
        await asyncio.gather(blocked, return_exceptions=True)
        await _cancel_bridge_task(task)


async def test_bridge_lookup_rechecks_tombstone_after_endpoint_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    destination_end = await _issue_source_success(
        manager,
        transport,
        source_route,
        destination_route,
        request.id,
    )
    chosen_ack = destination_end.model_copy(update={"ack": True})
    late_ack = destination_end.model_copy(update={"ack": True})
    first_lookup_done = asyncio.Event()
    release_late_lookup = asyncio.Event()
    original = manager._handle_bridge_tombstone_frame  # noqa: SLF001

    async def gate_first_lookup(handle: object, frame: object) -> bool:
        if frame is late_ack:
            handled = await original(handle, frame)
            first_lookup_done.set()
            await release_late_lookup.wait()
            return handled
        return await original(handle, frame)

    monkeypatch.setattr(manager, "_handle_bridge_tombstone_frame", gate_first_lookup)
    late = asyncio.create_task(manager.handle_frame(destination_route.handle, late_ack))
    await asyncio.wait_for(first_lookup_done.wait(), timeout=1)
    await manager.handle_frame(destination_route.handle, chosen_ack)
    assert await asyncio.wait_for(task, timeout=1) == TransferResult(
        0,
        hashlib.sha256(b"").hexdigest(),
    )
    release_late_lookup.set()
    await asyncio.wait_for(late, timeout=1)
    assert manager.active_slots == 0
    assert admission.active_count == 0


@pytest.mark.parametrize("violation", ["digest", "extra_binary"])
async def test_source_integrity_violation_finishes_with_stable_integrity_error(
    violation: str,
) -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
        total_bytes=1,
    )
    await manager.handle_binary(source_route.handle, request.id.bytes + b"x")

    with pytest.raises(TransferProtocolError):
        if violation == "digest":
            await manager.handle_frame(
                source_route.handle,
                TransferEndFrame(
                    id=request.id,
                    ack=False,
                    ok=True,
                    bytes_sent=1,
                    sha256="0" * 64,
                ),
            )
        else:
            await manager.handle_binary(source_route.handle, request.id.bytes + b"y")

    with pytest.raises(TransferIntegrityError):
        await asyncio.wait_for(task, timeout=1)
    destination_failure = await _wait_for_text_frame(
        transport,
        destination_route.handle,
        TransferEndFrame,
        ack=False,
    )
    assert destination_failure.code == "workspace_transfer_integrity_failed"
    assert manager.active_slots == 0
    assert admission.active_count == 0


async def test_source_binary_is_rejected_until_ready_send_is_issued() -> None:
    transport = _BlockingSourceReadyTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    transport.source_handle = source_route.handle
    task, request = await _start_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    await _send_source_begin(
        manager,
        transport,
        source_route,
        destination_route,
        request,
        total_bytes=1,
    )
    ready = asyncio.create_task(
        manager.handle_frame(
            destination_route.handle,
            TransferReadyFrame(id=request.id),
        )
    )
    await asyncio.wait_for(transport.ready_send_started.wait(), timeout=1)

    try:
        with pytest.raises(TransferProtocolError):
            await manager.handle_binary(source_route.handle, request.id.bytes + b"x")
        assert manager._bridges[request.id].bytes_received == 0  # noqa: SLF001
    finally:
        transport.release_ready_send.set()
        await asyncio.wait_for(ready, timeout=1)
        await _cancel_bridge_task(task)
    assert manager.active_slots == 0
    assert admission.active_count == 0


async def test_destination_initial_send_failure_after_source_issue_is_unknown() -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    task, request = await _start_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    transport.unavailable_handles.add(destination_route.handle)
    await manager.handle_frame(
        source_route.handle,
        TransferBeginFrame(
            id=request.id,
            direction="client_to_server",
            purpose="file_transfer",
            src_device=source_route.device_name,
            src_path="source.bin",
            dst_device="server",
            dst_path="destination.bin",
            total_bytes=0,
            etag="source-v1",
        ),
    )

    with pytest.raises(TransferDisconnectedError, match="outcome is unknown"):
        await asyncio.wait_for(task, timeout=1)
    assert manager.active_slots == 0
    assert admission.active_count == 0


async def test_preterminal_timeout_is_stable_only_after_both_matching_acks() -> None:
    transport = _BlockingBinaryTransport()
    manager, admission = _manager(transport, idle_timeout_seconds=0.05)
    source_route, destination_route = _routes()
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
        total_bytes=1,
    )
    await manager.handle_binary(source_route.handle, request.id.bytes + b"x")
    await asyncio.wait_for(transport.binary_send_started.wait(), timeout=1)
    source_failure = await _wait_for_text_frame(
        transport,
        source_route.handle,
        TransferEndFrame,
        ack=False,
    )
    destination_failure = await _wait_for_text_frame(
        transport,
        destination_route.handle,
        TransferEndFrame,
        ack=False,
    )
    assert source_failure.code == destination_failure.code == "workspace_transfer_timeout"
    assert not task.done()
    await manager.handle_frame(
        source_route.handle,
        source_failure.model_copy(update={"ack": True}),
    )
    await manager.handle_frame(
        destination_route.handle,
        destination_failure.model_copy(update={"ack": True}),
    )

    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(task, timeout=1)
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        transport.release_binary_send.set()
        await _cancel_bridge_task(task)


async def test_timeout_boundary_does_not_adopt_ack_validated_after_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport, idle_timeout_seconds=0.05)
    source_route, destination_route = _routes()
    ack_entered = asyncio.Event()
    release_ack = asyncio.Event()
    timeout_selected = asyncio.Event()
    release_publication = asyncio.Event()
    original_destination_end = manager._handle_bridge_destination_end  # noqa: SLF001
    original_publish = manager._publish_bridge_tombstones  # noqa: SLF001

    async def gated_destination_end(bridge: Any, frame: TransferEndFrame) -> None:
        if frame.ack:
            ack_entered.set()
            await release_ack.wait()
        await original_destination_end(bridge, frame)

    async def gated_publish(bridge: Any) -> None:
        if (
            bridge.destination_terminal_issued
            and bridge.source_resolution.value == "timeout_ack"
            and not timeout_selected.is_set()
        ):
            timeout_selected.set()
            await release_publication.wait()
        await original_publish(bridge)

    monkeypatch.setattr(manager, "_handle_bridge_destination_end", gated_destination_end)
    monkeypatch.setattr(manager, "_publish_bridge_tombstones", gated_publish)
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    destination_end = await _issue_source_success(
        manager,
        transport,
        source_route,
        destination_route,
        request.id,
    )
    late_ack = asyncio.create_task(
        manager.handle_frame(
            destination_route.handle,
            destination_end.model_copy(update={"ack": True}),
        )
    )

    try:
        await asyncio.wait_for(ack_entered.wait(), timeout=1)
        await asyncio.wait_for(timeout_selected.wait(), timeout=1)
        release_ack.set()
        await asyncio.wait_for(late_ack, timeout=1)
        release_publication.set()
        with pytest.raises(TransferDisconnectedError, match="outcome is unknown"):
            await asyncio.wait_for(task, timeout=1)
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        release_ack.set()
        release_publication.set()
        if not late_ack.done():
            late_ack.cancel()
        await asyncio.gather(late_ack, return_exceptions=True)
        await _cancel_bridge_task(task)


async def test_finalization_updates_an_in_flight_provisional_timeout_ack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _ControlledSourceAckTransport(block_source_ack=True)
    manager, admission = _manager(
        transport,
        idle_timeout_seconds=0.2,
        tombstone_ttl_seconds=0.02,
    )
    source_route, destination_route = _routes()
    transport.source_handle = source_route.handle
    tombstones_published = asyncio.Event()
    release_cleanup = asyncio.Event()
    original_publish = manager._publish_bridge_tombstones  # noqa: SLF001

    async def gated_publish(bridge: Any) -> None:
        await original_publish(bridge)
        if bridge.destination_terminal_issued and not tombstones_published.is_set():
            tombstones_published.set()
            await release_cleanup.wait()

    monkeypatch.setattr(manager, "_publish_bridge_tombstones", gated_publish)
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    await _issue_source_success(
        manager,
        transport,
        source_route,
        destination_route,
        request.id,
    )
    timeout = TransferEndFrame(
        id=request.id,
        ack=False,
        ok=False,
        code="workspace_transfer_timeout",
    )
    timeout_task: asyncio.Task[None] | None = None

    try:
        await asyncio.wait_for(tombstones_published.wait(), timeout=1)
        timeout_task = asyncio.create_task(
            manager.handle_frame(source_route.handle, timeout)
        )
        await asyncio.wait_for(transport.source_ack_started.wait(), timeout=1)
        release_cleanup.set()
        with pytest.raises(TransferDisconnectedError, match="outcome is unknown"):
            await asyncio.wait_for(task, timeout=1)
        transport.release_source_ack.set()
        await asyncio.wait_for(timeout_task, timeout=1)

        await asyncio.sleep(0.03)
        with pytest.raises(TransferProtocolError) as expired:
            await manager.handle_frame(source_route.handle, timeout)
        assert expired.value.code == "protocol_transfer_unknown_id"
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        release_cleanup.set()
        transport.release_source_ack.set()
        if timeout_task is not None and not timeout_task.done():
            timeout_task.cancel()
        if timeout_task is not None:
            await asyncio.gather(timeout_task, return_exceptions=True)
        await _cancel_bridge_task(task)


async def test_abort_send_failure_cancels_the_other_endpoint_send() -> None:
    transport = _SplitAbortSendTransport()
    manager, admission = _manager(transport, idle_timeout_seconds=0.05)
    source_route, destination_route = _routes()
    transport.source_handle = source_route.handle
    transport.destination_handle = destination_route.handle
    task, _ = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )

    try:
        await asyncio.wait_for(transport.destination_abort_started.wait(), timeout=1)
        with pytest.raises(TransferDisconnectedError, match="outcome is unknown"):
            await asyncio.wait_for(task, timeout=1)
        await asyncio.sleep(0)
        assert transport.destination_abort_cancelled.is_set()
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        transport.release_destination_abort.set()
        await _cancel_bridge_task(task)


@pytest.mark.parametrize("endpoint", ["source", "destination"])
async def test_simultaneous_endpoint_timeout_completes_abort_handshake(
    endpoint: str,
) -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport, idle_timeout_seconds=0.05)
    source_route, destination_route = _routes()
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    source_failure = await _wait_for_text_frame(
        transport,
        source_route.handle,
        TransferEndFrame,
        ack=False,
    )
    destination_failure = await _wait_for_text_frame(
        transport,
        destination_route.handle,
        TransferEndFrame,
        ack=False,
    )
    assert source_failure == destination_failure
    simultaneous_route = source_route if endpoint == "source" else destination_route
    peer_route = destination_route if endpoint == "source" else source_route

    await manager.handle_frame(simultaneous_route.handle, source_failure)
    matching_ack = await _wait_for_text_frame(
        transport,
        simultaneous_route.handle,
        TransferEndFrame,
        ack=True,
    )
    assert matching_ack == source_failure.model_copy(update={"ack": True})
    await manager.handle_frame(
        peer_route.handle,
        source_failure.model_copy(update={"ack": True}),
    )

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(task, timeout=1)
    assert manager.active_slots == 0
    assert admission.active_count == 0


async def test_destination_fence_drops_same_tick_source_progress() -> None:
    transport = _BridgeTransport()
    manager, admission = _manager(transport)
    source_route, destination_route = _routes()
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
        total_bytes=1,
    )
    forwarded_before = len(
        _text_frames(
            transport,
            destination_route.handle,
            TransferProgressFrame,
        )
    )
    transport.unavailable_handles.add(destination_route.handle)
    manager.fence_handle(destination_route.handle)

    await manager.handle_frame(
        source_route.handle,
        TransferProgressFrame(id=request.id, bytes_sent=1),
    )

    assert len(
        _text_frames(
            transport,
            destination_route.handle,
            TransferProgressFrame,
        )
    ) == forwarded_before
    with pytest.raises(TransferDisconnectedError, match="outcome is unknown"):
        await asyncio.wait_for(task, timeout=1)
    assert manager.active_slots == 0
    assert admission.active_count == 0


@pytest.mark.parametrize("endpoint", ["source", "destination"])
async def test_abort_ack_is_rejected_before_its_endpoint_issue_boundary(
    endpoint: str,
) -> None:
    transport = _BlockingAbortIssueTransport()
    manager, admission = _manager(transport, idle_timeout_seconds=0.2)
    source_route, destination_route = _routes()
    task, request = await _start_ready_bridge(
        manager,
        transport,
        source_route,
        destination_route,
        uuid4(),
    )
    endpoint_route = source_route if endpoint == "source" else destination_route
    bridge = manager._bridges[request.id]  # noqa: SLF001

    try:
        async with asyncio.timeout(1):
            while len(transport.abort_started) != 2:
                await asyncio.sleep(0)
        early_ack = TransferEndFrame(
            id=request.id,
            ack=True,
            ok=False,
            code="workspace_transfer_timeout",
        )
        with pytest.raises(TransferProtocolError):
            await manager.handle_frame(endpoint_route.handle, early_ack)
        endpoint_future = (
            bridge.source_ack_future
            if endpoint == "source"
            else bridge.destination_ack_future
        )
        assert endpoint_future is not None
        assert not endpoint_future.done()

        transport.release_abort.set()
        source_failure = await _wait_for_text_frame(
            transport,
            source_route.handle,
            TransferEndFrame,
            ack=False,
        )
        destination_failure = await _wait_for_text_frame(
            transport,
            destination_route.handle,
            TransferEndFrame,
            ack=False,
        )
        await manager.handle_frame(
            source_route.handle,
            source_failure.model_copy(update={"ack": True}),
        )
        await manager.handle_frame(
            destination_route.handle,
            destination_failure.model_copy(update={"ack": True}),
        )
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(task, timeout=1)
        assert manager.active_slots == 0
        assert admission.active_count == 0
    finally:
        transport.release_abort.set()
        await _cancel_bridge_task(task)
