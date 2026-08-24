import asyncio
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from openctopus_server.devices.protocol import (
    TransferBeginFrame,
    TransferEndFrame,
    TransferReadyFrame,
    decode_binary_chunk,
    new_uuid7,
    parse_server_frame,
)
from openctopus_server.devices.transfer import (
    TOMBSTONE_MAX_ENTRIES,
    FairTransferAdmission,
    TransferBusyError,
    TransferCommitResult,
    TransferCommittedAfterCancellation,
    TransferDisconnectedError,
    TransferError,
    TransferManager,
    TransferProtocolError,
    TransferResult,
    TransferRoute,
    TransferSink,
    TransferUnavailableError,
)


@dataclass(frozen=True)
class Handle:
    device_id: UUID
    generation: int


@dataclass
class Transport:
    text: list[tuple[object, str]] = field(default_factory=list)
    binary: list[tuple[object, bytes]] = field(default_factory=list)
    text_event: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(self, handle: object, payload: str) -> bool:
        self.text.append((handle, payload))
        self.text_event.set()
        return True

    async def send_binary(self, handle: object, payload: bytes) -> bool:
        self.binary.append((handle, payload))
        return True


@dataclass
class AckObservingTransport(Transport):
    ack_sent: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(self, handle: object, payload: str) -> bool:
        self.text.append((handle, payload))
        frame = parse_server_frame(payload)
        if isinstance(frame, TransferEndFrame) and frame.ack and frame.ok:
            self.ack_sent.set()
        self.text_event.set()
        return True


@dataclass
class TerminalDropTransport(Transport):
    async def send_text(self, handle: object, payload: str) -> bool:
        self.text.append((handle, payload))
        self.text_event.set()
        frame = parse_server_frame(payload)
        return not (
            isinstance(frame, TransferEndFrame)
            and not frame.ack
            and frame.ok
        )


@dataclass
class FencedRouteTransport(Transport):
    async def send_text(
        self,
        handle: object,
        payload: str,
        *,
        expected_device_name: str | None = None,
        expected_config_epoch: int | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> bool:
        del handle, payload, expected_device_name, expected_config_epoch, on_issued
        return False


@dataclass
class IssuingTransport(Transport):
    async def send_text(
        self,
        handle: object,
        payload: str,
        *,
        expected_device_name: str | None = None,
        expected_config_epoch: int | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> bool:
        del expected_device_name, expected_config_epoch
        if on_issued is not None:
            on_issued()
        return await super().send_text(handle, payload)


@dataclass
class SuccessAckDropTransport(Transport):
    async def send_text(self, handle: object, payload: str) -> bool:
        self.text.append((handle, payload))
        self.text_event.set()
        frame = parse_server_frame(payload)
        return not (
            isinstance(frame, TransferEndFrame)
            and frame.ack
            and frame.ok
        )


@dataclass
class BlockingSuccessAckTransport(Transport):
    ack_started: asyncio.Event = field(default_factory=asyncio.Event)
    release_ack: asyncio.Event = field(default_factory=asyncio.Event)
    ack_completed: asyncio.Event = field(default_factory=asyncio.Event)
    ack_cancelled: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(self, handle: object, payload: str) -> bool:
        self.text.append((handle, payload))
        self.text_event.set()
        frame = parse_server_frame(payload)
        if isinstance(frame, TransferEndFrame) and frame.ack and frame.ok:
            self.ack_started.set()
            try:
                await self.release_ack.wait()
            except asyncio.CancelledError:
                self.ack_cancelled.set()
                raise
            self.ack_completed.set()
        return True


@dataclass
class BlockingFailureTransport(Transport):
    failure_started: asyncio.Event = field(default_factory=asyncio.Event)
    release_failure: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(self, handle: object, payload: str) -> bool:
        self.text.append((handle, payload))
        self.text_event.set()
        frame = parse_server_frame(payload)
        if isinstance(frame, TransferEndFrame) and not frame.ack and not frame.ok:
            self.failure_started.set()
            await self.release_failure.wait()
        return True


@dataclass
class Source:
    chunks: list[bytes]
    etag: str | None = None
    closed: bool = False

    async def read(self) -> bytes:
        await asyncio.sleep(0)
        return self.chunks.pop(0) if self.chunks else b""

    async def aclose(self) -> None:
        self.closed = True


@dataclass
class Sink:
    chunks: list[bytes] = field(default_factory=list)
    finished: bool = False
    aborted: bool = False

    async def write(self, chunk: bytes) -> None:
        self.chunks.append(chunk)

    async def finish(self) -> None:
        self.finished = True

    async def abort(self) -> None:
        self.aborted = True


@dataclass
class StalledSink:
    started: asyncio.Event = field(default_factory=asyncio.Event)
    aborted: bool = False

    async def write(self, _chunk: bytes) -> None:
        self.started.set()
        await asyncio.Future()

    async def finish(self) -> None:
        raise AssertionError("a stalled sink must not finish")

    async def abort(self) -> None:
        self.aborted = True


@dataclass
class FinishStalledSink:
    chunks: list[bytes] = field(default_factory=list)
    finish_started: asyncio.Event = field(default_factory=asyncio.Event)
    aborted: bool = False

    async def write(self, chunk: bytes) -> None:
        self.chunks.append(chunk)

    async def finish(self) -> None:
        self.finish_started.set()
        await asyncio.Future()

    async def abort(self) -> None:
        self.aborted = True


def _manager(
    transport: Transport,
    *,
    max_concurrency: int = 4,
    idle_timeout_seconds: float = 0.5,
) -> TransferManager:
    return TransferManager(
        transport,
        admission=FairTransferAdmission(
            max_concurrency=max_concurrency,
            max_concurrency_per_user=1,
            queue_timeout_seconds=0.05,
        ),
        idle_timeout_seconds=idle_timeout_seconds,
    )


async def _wait_for_ready(transport: Transport, slot_id: UUID) -> None:
    while True:
        for _, payload in transport.text:
            frame = parse_server_frame(payload)
            if isinstance(frame, TransferReadyFrame) and frame.id == slot_id:
                return
        await asyncio.sleep(0)


async def _send_client_relay(
    manager: TransferManager,
    transport: Transport,
    handle: Handle,
    sink: TransferSink,
    *,
    route: TransferRoute | None = None,
) -> tuple[asyncio.Task[TransferResult], UUID]:
    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return sink

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            route=route,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path=None,
            sink_factory=make_sink,
            purpose="http_relay",
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    digest = hashlib.sha256(b"payload").hexdigest()
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="http_relay",
            src_path="source.bin",
            total_bytes=7,
        ),
    )
    await _wait_for_ready(transport, slot_id)
    await manager.handle_binary(handle, slot_id.bytes + b"payload")
    await manager.handle_frame(
        handle,
        TransferEndFrame(id=slot_id, ack=False, ok=True, bytes_sent=7, sha256=digest),
    )
    return task, slot_id


@pytest.mark.parametrize("direction", ["server_to_client", "client_to_server"])
async def test_initial_route_fence_is_definitely_unavailable(direction: str) -> None:
    transport = FencedRouteTransport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)
    route = SimpleNamespace(handle=handle, config_epoch=7, device_name="laptop")
    issued: list[None] = []

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return Sink()

    if direction == "server_to_client":
        transfer = manager.start_server_to_client(
            handle=handle,
            route=route,
            user_id=uuid4(),
            src_path="source.txt",
            dst_path="destination.txt",
            source=Source([]),
            total_bytes=0,
            on_issued=lambda: issued.append(None),
        )
    else:
        transfer = manager.start_client_to_server(
            handle=handle,
            route=route,
            user_id=uuid4(),
            src_path="source.txt",
            dst_path="destination.txt",
            sink_factory=make_sink,
            on_issued=lambda: issued.append(None),
        )

    with pytest.raises(TransferUnavailableError):
        await transfer
    assert manager.active_slots == 0
    assert manager._admission.active_count == 0
    assert issued == []


async def test_disconnect_after_initial_frame_is_outcome_unknown() -> None:
    transport = IssuingTransport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return Sink()

    issued: list[None] = []

    transfer = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.txt",
            dst_path="destination.txt",
            sink_factory=make_sink,
            on_issued=lambda: issued.append(None),
        )
    )
    await transport.text_event.wait()
    assert issued == [None]
    await manager.disconnect(handle)

    with pytest.raises(TransferDisconnectedError):
        await transfer
    assert manager.active_slots == 0
    assert manager._admission.active_count == 0


async def test_cancel_while_waiting_for_transfer_admission_is_not_issued() -> None:
    admission = FairTransferAdmission(
        max_concurrency=1,
        max_concurrency_per_user=1,
        queue_timeout_seconds=1,
    )
    manager = TransferManager(Transport(), admission=admission)
    user_id = uuid4()
    lease = await admission.acquire(user_id)
    issued: list[None] = []

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return Sink()

    transfer = asyncio.create_task(
        manager.start_client_to_server(
            handle=Handle(uuid4(), 1),
            user_id=user_id,
            src_path="source.txt",
            dst_path="destination.txt",
            sink_factory=make_sink,
            on_issued=lambda: issued.append(None),
        )
    )
    await asyncio.sleep(0)
    transfer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await transfer
    await lease.aclose()

    assert issued == []
    assert manager.active_slots == 0
    assert admission.active_count == 0


async def test_admission_round_robins_users_and_releases_on_cancel() -> None:
    admission = FairTransferAdmission(
        max_concurrency=1,
        max_concurrency_per_user=1,
        queue_timeout_seconds=0.02,
    )
    first_user, second_user = uuid4(), uuid4()
    first = await admission.acquire(first_user)
    second_task = asyncio.create_task(admission.acquire(second_user))
    await asyncio.sleep(0)
    await first.aclose()
    second = await second_task
    assert admission.active_by_user == {second_user: 1}
    await second.aclose()
    assert admission.active_count == 0
    with pytest.raises(TransferBusyError):
        holder = await admission.acquire(first_user)
        try:
            await asyncio.wait_for(admission.acquire(second_user), timeout=0.05)
        finally:
            await holder.aclose()


async def test_admission_timeout_after_grant_closes_the_raced_lease() -> None:
    admission = FairTransferAdmission(
        max_concurrency=1,
        max_concurrency_per_user=1,
        queue_timeout_seconds=0.01,
    )
    first_user, second_user = uuid4(), uuid4()
    holder = await admission.acquire(first_user)
    waiter_task = asyncio.create_task(admission.acquire(second_user))
    await asyncio.sleep(0)
    assert admission.waiting_count == 1

    # Hold the admission lock while the timeout callback cancels the waiter.
    # Granting it before releasing that lock deterministically exercises the
    # waiter.queued=False/future-done race in the timeout cleanup path.
    await admission._lock.acquire()
    await asyncio.sleep(0.02)
    admission._active = 0
    admission._active_by_user.clear()
    admission._drain_locked()
    admission._lock.release()
    holder._closed = True

    with pytest.raises(TransferBusyError):
        await waiter_task
    assert admission.active_count == 0
    assert admission.waiting_count == 0


async def test_admission_bounds_waiters_globally_and_per_user() -> None:
    admission = FairTransferAdmission(
        max_concurrency=2,
        max_concurrency_per_user=1,
        queue_timeout_seconds=0.05,
    )
    noisy_user, other_user, third_user = uuid4(), uuid4(), uuid4()
    first = await admission.acquire(noisy_user)
    second = await admission.acquire(other_user)
    noisy_waiter = asyncio.create_task(admission.acquire(noisy_user))
    other_waiter = asyncio.create_task(admission.acquire(other_user))
    await asyncio.sleep(0)

    with pytest.raises(TransferBusyError):
        await admission.acquire(noisy_user)
    with pytest.raises(TransferBusyError):
        await admission.acquire(third_user)
    assert admission.waiting_count == 2

    await first.aclose()
    await second.aclose()
    noisy_lease = await noisy_waiter
    other_lease = await other_waiter
    await noisy_lease.aclose()
    await other_lease.aclose()
    assert admission.active_count == 0


async def test_server_to_client_source_factory_waits_for_admission_and_cleans_up() -> None:
    transport = Transport()
    admission = FairTransferAdmission(
        max_concurrency=1,
        max_concurrency_per_user=1,
        queue_timeout_seconds=1.0,
    )
    manager = TransferManager(transport, admission=admission, idle_timeout_seconds=0.5)
    held = await admission.acquire(uuid4())
    opened = asyncio.Event()
    source = Source([])

    async def source_factory() -> Source:
        opened.set()
        return source

    task = asyncio.create_task(
        manager.start_server_to_client(
            handle=Handle(uuid4(), 1),
            user_id=uuid4(),
            src_path="from.txt",
            dst_path="to.txt",
            source_factory=source_factory,
            total_bytes=0,
        )
    )
    await asyncio.sleep(0)
    assert opened.is_set() is False

    await held.aclose()
    await asyncio.wait_for(opened.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert source.closed is True
    assert admission.active_count == 0
    assert manager.active_slots == 0


async def test_server_to_client_closes_source_factory_result_after_disconnect() -> None:
    transport = Transport()
    manager = _manager(transport, idle_timeout_seconds=0.05)
    handle = Handle(uuid4(), 1)
    factory_started = asyncio.Event()
    release_factory = asyncio.Event()
    source = Source([])

    async def source_factory() -> Source:
        factory_started.set()
        try:
            await release_factory.wait()
        except asyncio.CancelledError:
            await release_factory.wait()
        return source

    task = asyncio.create_task(
        manager.start_server_to_client(
            handle=handle,
            user_id=uuid4(),
            src_path="from.txt",
            dst_path="to.txt",
            source_factory=source_factory,
            total_bytes=0,
        )
    )
    await factory_started.wait()

    await asyncio.wait_for(manager.disconnect(handle), timeout=0.2)
    with pytest.raises(TransferDisconnectedError):
        await asyncio.wait_for(task, timeout=0.2)
    assert source.closed is False

    release_factory.set()
    async with asyncio.timeout(0.2):
        while not source.closed:
            await asyncio.sleep(0)
    assert transport.text == []
    assert manager.active_slots == 0
    assert manager._admission.active_count == 0


async def test_server_to_client_source_factory_is_idle_timed_out() -> None:
    manager = _manager(Transport(), idle_timeout_seconds=0.02)
    source = Source([])
    release_factory = asyncio.Event()

    async def source_factory() -> Source:
        try:
            await release_factory.wait()
        except asyncio.CancelledError:
            await release_factory.wait()
        return source

    task = asyncio.create_task(
        manager.start_server_to_client(
            handle=Handle(uuid4(), 1),
            user_id=uuid4(),
            src_path="from.txt",
            dst_path="to.txt",
            source_factory=source_factory,
            total_bytes=0,
        )
    )

    await asyncio.sleep(0.05)
    assert task.done()
    with pytest.raises(TimeoutError):
        await task
    release_factory.set()
    async with asyncio.timeout(0.2):
        while not source.closed:
            await asyncio.sleep(0)
    assert manager.active_slots == 0
    assert manager._admission.active_count == 0


async def test_server_to_client_streams_chunks_then_waits_for_ack_and_moves_source() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)
    user_id = uuid4()
    source = Source([b"a" * 10, b"b" * 10], etag="source-etag")
    deleted = False

    async def delete_source() -> None:
        nonlocal deleted
        deleted = True

    task = asyncio.create_task(
        manager.start_server_to_client(
            handle=handle,
            user_id=user_id,
            src_path="from.txt",
            dst_path="to.txt",
            source=source,
            total_bytes=20,
            mode="move",
            delete_source=delete_source,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    begin = parse_server_frame(transport.text[-1][1])
    assert isinstance(begin, TransferBeginFrame)
    await manager.handle_frame(handle, TransferReadyFrame(id=begin.id))
    result_end: TransferEndFrame | None = None
    while result_end is None:
        await asyncio.sleep(0)
        for _, payload in transport.text:
            frame = parse_server_frame(payload)
            if isinstance(frame, TransferEndFrame) and not frame.ack:
                result_end = frame
                break
    assert [decode_binary_chunk(payload)[1] for _, payload in transport.binary] == [b"a" * 10, b"b" * 10]
    assert result_end.bytes_sent == 20
    assert result_end.sha256 == hashlib.sha256(b"a" * 10 + b"b" * 10).hexdigest()
    await manager.handle_frame(
        handle,
        TransferEndFrame(
            id=begin.id,
            ack=True,
            ok=True,
            bytes_sent=20,
            sha256=result_end.sha256,
        ),
    )
    result = await task
    assert result.bytes_transferred == result_end.bytes_sent
    assert result.sha256 == result_end.sha256
    assert result.warnings == ()
    assert deleted is True
    assert source.closed is True
    assert manager.active_slots == 0


async def test_server_to_client_fence_during_move_cleanup_keeps_committed_success() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)
    delete_started = asyncio.Event()
    release_delete = asyncio.Event()

    async def delete_source() -> None:
        delete_started.set()
        await release_delete.wait()

    task = asyncio.create_task(
        manager.start_server_to_client(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            source=Source([b"payload"], etag="source-v1"),
            total_bytes=7,
            mode="move",
            delete_source=delete_source,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    begin = parse_server_frame(transport.text[0][1])
    assert isinstance(begin, TransferBeginFrame)
    await manager.handle_frame(handle, TransferReadyFrame(id=begin.id))
    while not any(
        isinstance(frame := parse_server_frame(payload), TransferEndFrame) and not frame.ack
        for _, payload in transport.text
    ):
        await asyncio.sleep(0)
    end = next(
        frame
        for _, payload in transport.text
        if isinstance(frame := parse_server_frame(payload), TransferEndFrame) and not frame.ack
    )
    await manager.handle_frame(
        handle,
        TransferEndFrame(
            id=begin.id,
            ack=True,
            ok=True,
            bytes_sent=end.bytes_sent,
            sha256=end.sha256,
        ),
    )
    await delete_started.wait()

    manager.fence_handle(handle)
    release_delete.set()

    result = await asyncio.wait_for(task, timeout=1)
    assert result.sha256 == hashlib.sha256(b"payload").hexdigest()
    assert result.warnings == ("source_delete_failed",)
    assert manager.active_slots == 0


async def test_workspace_upload_result_propagates_ack_metadata() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 2)
    task = asyncio.create_task(
        manager.start_server_to_client(
            handle=handle,
            user_id=uuid4(),
            src_path=None,
            dst_path="uploaded.txt",
            source=Source([b"payload"]),
            total_bytes=7,
            purpose="workspace_upload",
            if_none_match=True,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    begin = parse_server_frame(transport.text[-1][1])
    assert isinstance(begin, TransferBeginFrame)
    assert begin.if_none_match is True
    await manager.handle_frame(handle, TransferReadyFrame(id=begin.id))
    end: TransferEndFrame | None = None
    while end is None:
        await asyncio.sleep(0)
        for _, payload in transport.text:
            frame = parse_server_frame(payload)
            if isinstance(frame, TransferEndFrame) and not frame.ack:
                end = frame
                break
    etag = "e" * 64
    await manager.handle_frame(
        handle,
        TransferEndFrame(
            id=begin.id,
            ack=True,
            ok=True,
            bytes_sent=7,
            sha256=end.sha256,
            etag=etag,
            created=True,
        ),
    )
    result = await task
    assert result.etag == etag
    assert result.created is True


async def test_workspace_upload_success_without_ack_metadata_is_rejected() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 2)
    task = asyncio.create_task(
        manager.start_server_to_client(
            handle=handle,
            user_id=uuid4(),
            src_path=None,
            dst_path="uploaded.txt",
            source=Source([b"payload"]),
            total_bytes=7,
            purpose="workspace_upload",
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    begin = parse_server_frame(transport.text[-1][1])
    assert isinstance(begin, TransferBeginFrame)
    await manager.handle_frame(handle, TransferReadyFrame(id=begin.id))
    end: TransferEndFrame | None = None
    while end is None:
        await asyncio.sleep(0)
        for _, payload in transport.text:
            frame = parse_server_frame(payload)
            if isinstance(frame, TransferEndFrame) and not frame.ack:
                end = frame
                break
    assert end is not None
    await manager.handle_frame(
        handle,
        TransferEndFrame(
            id=begin.id,
            ack=True,
            ok=True,
            bytes_sent=7,
            sha256=end.sha256,
        ),
    )
    with pytest.raises(TransferProtocolError, match="destination metadata"):
        await task
    assert manager.active_slots == 0


async def test_http_relay_result_propagates_source_etag_without_created_metadata() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 3)
    sink = Sink()

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return sink

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.txt",
            dst_path=None,
            sink_factory=make_sink,
            purpose="http_relay",
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    etag = "f" * 64
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="http_relay",
            src_path="source.txt",
            dst_path=None,
            total_bytes=3,
            etag=etag,
        ),
    )
    await _wait_for_ready(transport, slot_id)
    await manager.handle_binary(handle, slot_id.bytes + b"abc")
    digest = hashlib.sha256(b"abc").hexdigest()
    await manager.handle_frame(
        handle,
        TransferEndFrame(id=slot_id, ack=False, ok=True, bytes_sent=3, sha256=digest),
    )
    result = await task
    assert result.etag == etag
    assert result.created is None


async def test_server_tombstones_are_bounded_and_evict_oldest() -> None:
    manager = _manager(Transport())
    first = (uuid4(), 1, uuid4())
    for index in range(TOMBSTONE_MAX_ENTRIES + 1):
        key = first if index == 0 else (uuid4(), 1, uuid4())
        manager._remember_tombstone_locked(key, (0.0, None, False))
    assert len(manager._tombstones) == TOMBSTONE_MAX_ENTRIES
    assert first not in manager._tombstones
    await manager.close()


async def test_server_sender_accepts_destination_rejection_before_ready() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)
    source = Source([b"payload"])
    task = asyncio.create_task(
        manager.start_server_to_client(
            handle=handle,
            user_id=uuid4(),
            src_path="from.txt",
            dst_path="to.txt",
            source=source,
            total_bytes=7,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    begin = parse_server_frame(transport.text[-1][1])
    assert isinstance(begin, TransferBeginFrame)

    await manager.handle_frame(
        handle,
        TransferEndFrame(
            id=begin.id,
            ack=False,
            ok=False,
            code="workspace_file_changed",
        ),
    )

    with pytest.raises(TransferError, match="workspace_file_changed"):
        await task
    frames = [parse_server_frame(payload) for _, payload in transport.text]
    assert any(
        isinstance(frame, TransferEndFrame)
        and frame.ack
        and not frame.ok
        and frame.code == "workspace_file_changed"
        for frame in frames
    )
    assert transport.binary == []
    assert source.closed is True
    assert manager.active_slots == 0


async def test_server_receiver_acknowledges_sender_rejection_before_begin() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return Sink()

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="missing.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])

    await manager.handle_frame(
        handle,
        TransferEndFrame(
            id=slot_id,
            ack=False,
            ok=False,
            code="workspace_not_found",
        ),
    )

    with pytest.raises(TransferError, match="workspace_not_found"):
        await task
    frames = [parse_server_frame(payload) for _, payload in transport.text]
    assert any(
        isinstance(frame, TransferEndFrame)
        and frame.ack
        and not frame.ok
        and frame.code == "workspace_not_found"
        for frame in frames
    )
    assert manager.active_slots == 0


async def test_client_receiver_defers_sink_preparation_and_rejects_early_frames() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)
    sink = Sink()
    preparation_started = asyncio.Event()
    release_preparation = asyncio.Event()

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        preparation_started.set()
        await release_preparation.wait()
        return sink

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    begin = TransferBeginFrame(
        id=slot_id,
        direction="client_to_server",
        purpose="file_transfer",
        src_path="source.bin",
        dst_path="destination.bin",
        total_bytes=1,
    )

    await asyncio.wait_for(manager.handle_frame(handle, begin), timeout=0.2)
    await preparation_started.wait()
    with pytest.raises(TransferProtocolError, match="before transfer_ready"):
        await manager.handle_binary(handle, slot_id.bytes + b"x")
    with pytest.raises(TransferProtocolError, match="invalid state"):
        await manager.handle_frame(handle, begin)

    release_preparation.set()
    await _wait_for_ready(transport, slot_id)
    await manager.handle_binary(handle, slot_id.bytes + b"x")
    digest = hashlib.sha256(b"x").hexdigest()
    await manager.handle_frame(
        handle,
        TransferEndFrame(id=slot_id, ack=False, ok=True, bytes_sent=1, sha256=digest),
    )
    result = await task
    assert result.bytes_transferred == 1
    assert sink.finished is True
    assert manager.active_slots == 0
    assert manager._admission.active_count == 0


async def test_client_receiver_failure_during_sink_preparation_cleans_admission() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)
    preparation_started = asyncio.Event()
    preparation_cancelled = asyncio.Event()

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        preparation_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            preparation_cancelled.set()
            raise
        raise AssertionError("sink preparation should remain blocked")

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    begin = TransferBeginFrame(
        id=slot_id,
        direction="client_to_server",
        purpose="file_transfer",
        src_path="source.bin",
        dst_path="destination.bin",
        total_bytes=1,
    )
    await manager.handle_frame(handle, begin)
    await preparation_started.wait()

    failure = TransferEndFrame(
        id=slot_id,
        ack=False,
        ok=False,
        code="workspace_storage_unavailable",
    )
    await asyncio.wait_for(manager.handle_frame(handle, failure), timeout=0.2)
    await preparation_cancelled.wait()
    with pytest.raises(TransferError, match="workspace_storage_unavailable"):
        await task

    frames = [parse_server_frame(payload) for _, payload in transport.text]
    assert failure.model_copy(update={"ack": True}) in frames
    assert manager.active_slots == 0
    assert manager._admission.active_count == 0


async def test_client_receiver_sink_preparation_failure_emits_tombstone() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)
    preparation_started = asyncio.Event()
    release_preparation = asyncio.Event()

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        preparation_started.set()
        await release_preparation.wait()
        raise TransferError("workspace_storage_unavailable")

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="destination.bin",
            total_bytes=1,
        ),
    )
    await preparation_started.wait()
    release_preparation.set()

    with pytest.raises(TransferError, match="workspace_storage_unavailable"):
        await task
    failure = next(
        frame
        for _, payload in transport.text
        if isinstance(frame := parse_server_frame(payload), TransferEndFrame)
        and not frame.ack
        and not frame.ok
    )
    assert failure.code == "workspace_storage_unavailable"
    matching_ack = failure.model_copy(update={"ack": True})
    await manager.handle_frame(handle, matching_ack)
    await manager.handle_frame(handle, matching_ack)
    assert manager.active_slots == 0
    assert manager._admission.active_count == 0


async def test_server_sender_acknowledges_receiver_failure_after_sender_end() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)
    task = asyncio.create_task(
        manager.start_server_to_client(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            source=Source([b"payload"]),
            total_bytes=7,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    begin = parse_server_frame(transport.text[0][1])
    assert isinstance(begin, TransferBeginFrame)
    await manager.handle_frame(handle, TransferReadyFrame(id=begin.id))
    while not any(
        isinstance(frame := parse_server_frame(payload), TransferEndFrame)
        and not frame.ack
        and frame.ok
        for _, payload in transport.text
    ):
        await asyncio.sleep(0)

    await manager.handle_frame(
        handle,
        TransferEndFrame(
            id=begin.id,
            ack=False,
            ok=False,
            code="workspace_storage_unavailable",
        ),
    )

    with pytest.raises(TransferError, match="workspace_storage_unavailable"):
        await task
    frames = [parse_server_frame(payload) for _, payload in transport.text]
    assert any(
        isinstance(frame, TransferEndFrame)
        and frame.ack
        and not frame.ok
        and frame.code == "workspace_storage_unavailable"
        for frame in frames
    )
    assert manager.active_slots == 0


async def test_client_to_server_streams_into_bounded_sink_and_verifies_digest() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 7)
    user_id = uuid4()
    sink = Sink()

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return sink

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=user_id,
            src_path="source.bin",
            dst_path="dest.bin",
            sink_factory=make_sink,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    request = json.loads(transport.text[0][1])
    slot_id = UUID(request["id"])
    digest = hashlib.sha256(b"hello world").hexdigest()
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="dest.bin",
            total_bytes=11,
        ),
    )
    await _wait_for_ready(transport, slot_id)
    await manager.handle_binary(handle, slot_id.bytes + b"hello ")
    await manager.handle_binary(handle, slot_id.bytes + b"world")
    await manager.handle_frame(
        handle,
        TransferEndFrame(id=slot_id, ack=False, ok=True, bytes_sent=11, sha256=digest),
    )
    result = await task
    assert result.bytes_transferred == 11
    assert result.sha256 == digest
    assert result.warnings == ()
    assert b"".join(sink.chunks) == b"hello world"
    assert sink.finished is True
    assert sink.aborted is False
    assert manager.active_slots == 0


async def test_client_to_server_ack_loss_after_commit_returns_success_and_keeps_move_source() -> None:
    transport = SuccessAckDropTransport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 7)
    sink = Sink()
    committed = False
    source_deleted = False

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return sink

    async def commit_sink(
        _sink: TransferSink,
        _begin: TransferBeginFrame,
        _size: int,
        _digest: str,
    ) -> None:
        nonlocal committed
        committed = True

    async def delete_source() -> None:
        nonlocal source_deleted
        source_deleted = True

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
            commit_sink=commit_sink,
            delete_source=delete_source,
            mode="move",
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    digest = hashlib.sha256(b"payload").hexdigest()
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="destination.bin",
            total_bytes=7,
            etag="source-v1",
        ),
    )
    await _wait_for_ready(transport, slot_id)
    await manager.handle_binary(handle, slot_id.bytes + b"payload")
    await manager.handle_frame(
        handle,
        TransferEndFrame(id=slot_id, ack=False, ok=True, bytes_sent=7, sha256=digest),
    )

    result = await asyncio.wait_for(task, timeout=0.2)

    assert committed is True
    assert source_deleted is False
    assert result.warnings == ("transfer_ack_failed", "source_delete_failed")
    assert sink.aborted is False
    assert manager.active_slots == 0


async def test_client_to_server_committed_ack_survives_caller_cancellation() -> None:
    transport = BlockingSuccessAckTransport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 7)
    sink = Sink()
    task, _ = await _send_client_relay(manager, transport, handle, sink)
    await transport.ack_started.wait()

    task.cancel()
    await asyncio.sleep(0)

    assert transport.ack_cancelled.is_set() is False
    assert task.done() is False

    transport.release_ack.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert transport.ack_completed.is_set() is True
    assert sink.aborted is False
    assert not any(
        isinstance(frame := parse_server_frame(payload), TransferEndFrame)
        and not frame.ack
        and not frame.ok
        for _, payload in transport.text
    )
    assert manager.active_slots == 0
    assert manager._admission.active_count == 0


async def test_committed_client_sender_accepts_only_its_late_timeout_until_tombstoned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 7)
    route = SimpleNamespace(handle=handle, config_epoch=3, device_name="laptop")
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    sent_routes: list[object | None] = []
    original_cleanup = manager._cleanup

    async def blocked_cleanup(slot: object, *, skip_worker: bool = False) -> None:
        cleanup_started.set()
        await release_cleanup.wait()
        await original_cleanup(slot, skip_worker=skip_worker)  # type: ignore[arg-type]

    async def capture_send(
        sent_handle: object,
        payload: str,
        *,
        route: TransferRoute | None,
        on_issued: Callable[[], None] | None = None,
    ) -> bool:
        if on_issued is not None:
            on_issued()
        sent_routes.append(route)
        return await transport.send_text(sent_handle, payload)

    monkeypatch.setattr(manager, "_cleanup", blocked_cleanup)
    monkeypatch.setattr(manager, "_send_text", capture_send)
    task, slot_id = await _send_client_relay(
        manager,
        transport,
        handle,
        Sink(),
        route=route,
    )
    await cleanup_started.wait()
    late_timeout = TransferEndFrame(
        id=slot_id,
        ack=False,
        ok=False,
        code="workspace_transfer_timeout",
    )

    try:
        await manager.handle_frame(handle, late_timeout)

        assert parse_server_frame(transport.text[-1][1]) == late_timeout.model_copy(
            update={"ack": True}
        )
        assert sent_routes[-1] is None
        with pytest.raises(TransferProtocolError):
            await manager.handle_frame(
                handle,
                late_timeout.model_copy(update={"code": "workspace_storage_unavailable"}),
            )
    finally:
        release_cleanup.set()
        await task

    await manager.handle_frame(handle, late_timeout)
    assert parse_server_frame(transport.text[-1][1]) == late_timeout.model_copy(
        update={"ack": True}
    )
    assert sent_routes[-1] is None

    with pytest.raises(TransferProtocolError):
        await manager.handle_frame(
            Handle(handle.device_id, handle.generation + 1),
            late_timeout,
        )
    with pytest.raises(TransferProtocolError):
        await manager.handle_frame(
            handle,
            late_timeout.model_copy(update={"id": new_uuid7()}),
        )


async def test_client_to_server_cancellation_during_publish_commits_before_propagating() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 7)
    sink = Sink()
    publish_started = asyncio.Event()
    release_publish = asyncio.Event()
    published = False

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return sink

    async def commit_sink(
        _sink: TransferSink,
        _begin: TransferBeginFrame,
        _size: int,
        _digest: str,
    ) -> bool:
        nonlocal published
        publish_started.set()
        cancelled = False
        try:
            await release_publish.wait()
        except asyncio.CancelledError:
            cancelled = True
            await release_publish.wait()
        published = True
        return cancelled

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
            commit_sink=commit_sink,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    digest = hashlib.sha256(b"payload").hexdigest()
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="destination.bin",
            total_bytes=7,
        ),
    )
    await _wait_for_ready(transport, slot_id)
    await manager.handle_binary(handle, slot_id.bytes + b"payload")
    await manager.handle_frame(
        handle,
        TransferEndFrame(id=slot_id, ack=False, ok=True, bytes_sent=7, sha256=digest),
    )
    await publish_started.wait()

    task.cancel()
    await asyncio.sleep(0)
    assert task.done() is False
    release_publish.set()

    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)
    assert published is True
    assert sink.aborted is False
    assert not any(
        isinstance(frame := parse_server_frame(payload), TransferEndFrame)
        and not frame.ack
        and not frame.ok
        for _, payload in transport.text
    )
    assert manager.active_slots == 0


async def test_committed_finish_cleanup_survives_worker_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 7)
    sink = Sink()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    original_cleanup = manager._cleanup

    async def blocked_cleanup(
        slot: object,
        *,
        skip_worker: bool = False,
    ) -> None:
        cleanup_started.set()
        await release_cleanup.wait()
        await original_cleanup(slot, skip_worker=skip_worker)  # type: ignore[arg-type]

    monkeypatch.setattr(manager, "_cleanup", blocked_cleanup)

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return sink

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    digest = hashlib.sha256(b"").hexdigest()
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="destination.bin",
            total_bytes=0,
        ),
    )
    await _wait_for_ready(transport, slot_id)
    await manager.handle_frame(
        handle,
        TransferEndFrame(id=slot_id, ack=False, ok=True, bytes_sent=0, sha256=digest),
    )
    await cleanup_started.wait()
    slot = next(iter(manager._slots.values()))
    assert slot.worker is not None
    slot.worker.cancel()
    release_cleanup.set()

    result = await asyncio.wait_for(task, timeout=1)
    assert result.sha256 == digest
    assert manager.active_slots == 0
    assert manager._admission.active_count == 0


async def test_client_to_server_failure_tombstone_exists_before_failure_send_completes() -> None:
    transport = BlockingFailureTransport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 7)

    async def fail_sink(_: TransferBeginFrame) -> TransferSink:
        raise TransferError("workspace_storage_unavailable")

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=fail_sink,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="destination.bin",
            total_bytes=7,
        ),
    )
    await transport.failure_started.wait()
    matching_ack = TransferEndFrame(
        id=slot_id,
        ack=True,
        ok=False,
        code="workspace_storage_unavailable",
    )

    await manager.handle_binary(handle, slot_id.bytes + b"already-queued")
    await manager.handle_frame(handle, matching_ack)
    transport.release_failure.set()

    with pytest.raises(TransferError, match="workspace_storage_unavailable"):
        await task
    with pytest.raises(TransferProtocolError):
        await manager.handle_binary(handle, slot_id.bytes + b"after-ack")


async def test_server_failure_tombstone_accepts_matching_ack_and_rejects_conflict() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 7)

    async def fail_sink(_: TransferBeginFrame) -> TransferSink:
        raise TransferError("workspace_storage_unavailable")

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=fail_sink,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="destination.bin",
            total_bytes=7,
        ),
    )
    with pytest.raises(TransferError, match="workspace_storage_unavailable"):
        await task

    # The sender may already have put a bounded number of binary chunks on the
    # wire before it observes our terminal failure.  Those in-flight chunks
    # belong to this known failed slot and must not tear down the device socket.
    await manager.handle_binary(handle, slot_id.bytes + b"queued-before-failure")

    matching_ack = TransferEndFrame(
        id=slot_id,
        ack=True,
        ok=False,
        code="workspace_storage_unavailable",
    )
    await manager.handle_frame(handle, matching_ack)
    await manager.handle_frame(handle, matching_ack)
    with pytest.raises(TransferProtocolError):
        await manager.handle_binary(handle, slot_id.bytes + b"after-ack")
    with pytest.raises(TransferProtocolError):
        await manager.handle_frame(
            handle,
            matching_ack.model_copy(update={"code": "workspace_file_changed"}),
        )


async def test_server_sender_fails_immediately_when_generation_drops_at_terminal_end() -> None:
    transport = TerminalDropTransport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)
    task = asyncio.create_task(
        manager.start_server_to_client(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            source=Source([b"payload"]),
            total_bytes=7,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    begin = parse_server_frame(transport.text[0][1])
    assert isinstance(begin, TransferBeginFrame)
    await manager.handle_frame(handle, TransferReadyFrame(id=begin.id))

    with pytest.raises(TransferDisconnectedError):
        await task

    assert manager.active_slots == 0


async def test_client_move_ack_precedes_remote_source_delete_to_avoid_path_lock_deadlock() -> None:
    transport = AckObservingTransport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 8)
    sink = Sink()
    source_delete_started = asyncio.Event()

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return sink

    async def delete_source() -> None:
        source_delete_started.set()
        await transport.ack_sent.wait()

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="dest.bin",
            sink_factory=make_sink,
            delete_source=delete_source,
            mode="move",
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    digest = hashlib.sha256(b"hello").hexdigest()
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="dest.bin",
            total_bytes=5,
            etag="source-etag",
        ),
    )
    await _wait_for_ready(transport, slot_id)
    await manager.handle_binary(handle, slot_id.bytes + b"hello")
    await manager.handle_frame(
        handle,
        TransferEndFrame(id=slot_id, ack=False, ok=True, bytes_sent=5, sha256=digest),
    )

    result = await asyncio.wait_for(task, timeout=0.5)

    assert source_delete_started.is_set()
    assert result.warnings == ()


async def test_client_to_server_move_acknowledges_before_remote_source_delete() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 7)
    sink = Sink()
    deleted_after_ack = False

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return sink

    async def delete_source() -> None:
        nonlocal deleted_after_ack
        terminal = parse_server_frame(transport.text[-1][1])
        deleted_after_ack = isinstance(terminal, TransferEndFrame) and terminal.ack

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
            delete_source=delete_source,
            mode="move",
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    digest = hashlib.sha256(b"x").hexdigest()
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="destination.bin",
            total_bytes=1,
            etag="source-etag",
        ),
    )
    await _wait_for_ready(transport, slot_id)
    await manager.handle_binary(handle, slot_id.bytes + b"x")
    await manager.handle_frame(
        handle,
        TransferEndFrame(id=slot_id, ack=False, ok=True, bytes_sent=1, sha256=digest),
    )

    result = await task
    assert deleted_after_ack is True
    assert result.warnings == ()


async def test_server_to_client_move_keeps_source_when_version_changed_before_delete() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 9)
    source = Source([b"payload"], etag="source-v1")
    source_changed = False
    delete_attempted = False

    async def delete_source() -> None:
        nonlocal delete_attempted
        delete_attempted = True
        if source_changed:
            raise RuntimeError("source etag no longer matches")

    task = asyncio.create_task(
        manager.start_server_to_client(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            source=source,
            total_bytes=7,
            mode="move",
            delete_source=delete_source,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    begin = parse_server_frame(transport.text[0][1])
    assert isinstance(begin, TransferBeginFrame)
    assert begin.etag == "source-v1"
    await manager.handle_frame(handle, TransferReadyFrame(id=begin.id))
    while not any(
        isinstance(frame := parse_server_frame(payload), TransferEndFrame) and not frame.ack
        for _, payload in transport.text
    ):
        await asyncio.sleep(0)
    source_changed = True
    end = next(
        frame
        for _, payload in transport.text
        if isinstance(frame := parse_server_frame(payload), TransferEndFrame) and not frame.ack
    )
    await manager.handle_frame(
        handle,
        TransferEndFrame(
            id=begin.id,
            ack=True,
            ok=True,
            bytes_sent=end.bytes_sent,
            sha256=end.sha256,
        ),
    )

    result = await task
    assert delete_attempted is True
    assert result.warnings == ("source_delete_failed",)


async def test_client_to_server_move_keeps_source_when_version_changed_before_delete() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 10)
    sink = Sink()
    source_changed = False
    delete_attempted = False

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return sink

    async def delete_source() -> None:
        nonlocal delete_attempted
        delete_attempted = True
        if source_changed:
            raise RuntimeError("source etag no longer matches")

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
            delete_source=delete_source,
            mode="move",
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    digest = hashlib.sha256(b"payload").hexdigest()
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="destination.bin",
            total_bytes=7,
            etag="source-v1",
        ),
    )
    await _wait_for_ready(transport, slot_id)
    await manager.handle_binary(handle, slot_id.bytes + b"payload")
    source_changed = True
    await manager.handle_frame(
        handle,
        TransferEndFrame(id=slot_id, ack=False, ok=True, bytes_sent=7, sha256=digest),
    )

    result = await task
    assert delete_attempted is True
    assert result.warnings == ("source_delete_failed",)


async def test_disconnect_cleans_slot_and_ignores_no_old_generation() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)
    task = asyncio.create_task(
        manager.start_server_to_client(
            handle=handle,
            user_id=uuid4(),
            src_path="a",
            dst_path="b",
            source=Source([b"x"]),
            total_bytes=1,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    await manager.disconnect(handle)
    with pytest.raises(Exception):
        await task
    assert manager.active_slots == 0


async def test_cancelling_start_server_consumes_completion_and_releases_lease() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)
    task = asyncio.create_task(
        manager.start_server_to_client(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            source=Source([b"payload"]),
            total_bytes=7,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)
    assert manager.active_slots == 0
    assert manager._admission.active_count == 0


async def test_cancelling_start_client_consumes_completion_and_releases_lease() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return Sink()

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)
    assert manager.active_slots == 0
    assert manager._admission.active_count == 0


@pytest.mark.parametrize("direction", ["server_to_client", "client_to_server"])
async def test_cancelling_while_new_slot_waits_releases_admission_lease(direction: str) -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return Sink()

    await manager._lock.acquire()
    try:
        if direction == "server_to_client":
            task = asyncio.create_task(
                manager.start_server_to_client(
                    handle=handle,
                    user_id=uuid4(),
                    src_path="source.bin",
                    dst_path="destination.bin",
                    source=Source([b"payload"]),
                    total_bytes=7,
                )
            )
        else:
            task = asyncio.create_task(
                manager.start_client_to_server(
                    handle=handle,
                    user_id=uuid4(),
                    src_path="source.bin",
                    dst_path="destination.bin",
                    sink_factory=make_sink,
                )
            )
        while manager._admission.active_count == 0:
            await asyncio.sleep(0)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert manager._admission.active_count == 0
        assert manager.active_slots == 0
    finally:
        manager._lock.release()


async def test_client_to_server_end_wakes_an_idle_zero_byte_receiver() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)
    sink = Sink()

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return sink

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="empty.bin",
            dst_path="empty.bin",
            sink_factory=make_sink,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="empty.bin",
            dst_path="empty.bin",
            total_bytes=0,
        ),
    )
    await _wait_for_ready(transport, slot_id)
    digest = hashlib.sha256(b"").hexdigest()

    await manager.handle_frame(
        handle,
        TransferEndFrame(id=slot_id, ack=False, ok=True, bytes_sent=0, sha256=digest),
    )

    result = await asyncio.wait_for(task, timeout=0.2)
    assert result.bytes_transferred == 0
    assert sink.finished is True


async def test_client_to_server_rejects_bytes_beyond_declared_size_before_sink_write() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)
    sink = Sink()

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return sink

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="destination.bin",
            total_bytes=3,
        ),
    )
    await _wait_for_ready(transport, slot_id)

    with pytest.raises(TransferProtocolError):
        await manager.handle_binary(handle, slot_id.bytes + b"four")
    with pytest.raises(Exception):
        await task

    assert sink.chunks == []
    assert sink.aborted is True
    assert manager.active_slots == 0


async def test_client_to_server_sink_write_is_idle_timed_out() -> None:
    transport = Transport()
    manager = _manager(transport, idle_timeout_seconds=0.05)
    handle = Handle(uuid4(), 1)
    sink = StalledSink()

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return sink

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="destination.bin",
            total_bytes=1,
        ),
    )
    await _wait_for_ready(transport, slot_id)
    await manager.handle_binary(handle, slot_id.bytes + b"x")
    await sink.started.wait()
    await manager.handle_frame(
        handle,
        TransferEndFrame(
            id=slot_id,
            ack=False,
            ok=True,
            bytes_sent=1,
            sha256=hashlib.sha256(b"x").hexdigest(),
        ),
    )

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(task, timeout=1)
    assert sink.aborted is True
    assert manager.active_slots == 0


async def test_client_to_server_sink_finish_is_idle_timed_out() -> None:
    transport = Transport()
    manager = _manager(transport, idle_timeout_seconds=0.02)
    handle = Handle(uuid4(), 1)
    sink = FinishStalledSink()

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return sink

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    digest = hashlib.sha256(b"x").hexdigest()
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="destination.bin",
            total_bytes=1,
        ),
    )
    await _wait_for_ready(transport, slot_id)
    await manager.handle_binary(handle, slot_id.bytes + b"x")
    await manager.handle_frame(
        handle,
        TransferEndFrame(id=slot_id, ack=False, ok=True, bytes_sent=1, sha256=digest),
    )
    await sink.finish_started.wait()

    await asyncio.sleep(0.05)
    assert task.done()
    with pytest.raises(TimeoutError):
        await task
    assert sink.aborted is True
    assert manager.active_slots == 0


async def test_client_to_server_commit_is_idle_timed_out_before_publish() -> None:
    transport = Transport()
    manager = _manager(transport, idle_timeout_seconds=0.02)
    handle = Handle(uuid4(), 1)
    sink = Sink()
    commit_started = asyncio.Event()

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return sink

    async def commit_sink(
        _sink: TransferSink,
        _begin: TransferBeginFrame,
        _size: int,
        _digest: str,
    ) -> None:
        commit_started.set()
        await asyncio.Future()

    task = asyncio.create_task(
        manager.start_client_to_server(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
            commit_sink=commit_sink,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    slot_id = UUID(json.loads(transport.text[0][1])["id"])
    digest = hashlib.sha256(b"x").hexdigest()
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="destination.bin",
            total_bytes=1,
        ),
    )
    await _wait_for_ready(transport, slot_id)
    await manager.handle_binary(handle, slot_id.bytes + b"x")
    await manager.handle_frame(
        handle,
        TransferEndFrame(id=slot_id, ack=False, ok=True, bytes_sent=1, sha256=digest),
    )
    await commit_started.wait()

    await asyncio.sleep(0.05)
    assert task.done()
    with pytest.raises(TimeoutError):
        await task
    assert sink.aborted is True
    assert manager.active_slots == 0


async def test_tombstone_ignores_only_the_identical_terminal_ack() -> None:
    transport = Transport()
    manager = _manager(transport)
    handle = Handle(uuid4(), 1)
    task = asyncio.create_task(
        manager.start_server_to_client(
            handle=handle,
            user_id=uuid4(),
            src_path="source.bin",
            dst_path="destination.bin",
            source=Source([b"x"]),
            total_bytes=1,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    begin = parse_server_frame(transport.text[0][1])
    assert isinstance(begin, TransferBeginFrame)
    await manager.handle_frame(handle, TransferReadyFrame(id=begin.id))
    while not any(
        isinstance(frame := parse_server_frame(payload), TransferEndFrame) and not frame.ack
        for _, payload in transport.text
    ):
        await asyncio.sleep(0)
    digest = hashlib.sha256(b"x").hexdigest()
    ack = TransferEndFrame(
        id=begin.id,
        ack=True,
        ok=True,
        bytes_sent=1,
        sha256=digest,
    )
    await manager.handle_frame(handle, ack)
    await task

    await manager.handle_frame(handle, ack)
    with pytest.raises(TransferProtocolError):
        await manager.handle_frame(
            handle,
            ack.model_copy(update={"bytes_sent": 2}),
        )


async def test_already_admitted_child_reuses_outer_lease_and_exact_slot_id() -> None:
    transport = Transport()
    admission = FairTransferAdmission(
        max_concurrency=1,
        max_concurrency_per_user=1,
        queue_timeout_seconds=0.05,
    )
    manager = TransferManager(transport, admission=admission)
    handle = Handle(uuid4(), 1)
    user_id = uuid4()
    slot_id = new_uuid7()
    lease = await manager.acquire_operation(user_id)

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return Sink()

    async def commit_sink(
        _sink: TransferSink,
        _begin: TransferBeginFrame,
        _size: int,
        _digest: str,
    ) -> TransferCommitResult:
        return TransferCommitResult(etag="destination-v1")

    task = asyncio.create_task(
        manager.start_client_to_server_admitted(
            handle=handle,
            user_id=user_id,
            slot_id=slot_id,
            operation_lease=lease,
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
            commit_sink=commit_sink,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    request = parse_server_frame(transport.text[0][1])
    assert request.id == slot_id
    assert admission.active_count == 1

    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="destination.bin",
            total_bytes=0,
        ),
    )
    await _wait_for_ready(transport, slot_id)
    digest = hashlib.sha256(b"").hexdigest()
    await manager.handle_frame(
        handle,
        TransferEndFrame(
            id=slot_id,
            ack=False,
            ok=True,
            bytes_sent=0,
            sha256=digest,
        ),
    )
    assert (await task).sha256 == digest
    assert admission.active_count == 1

    await lease.aclose()
    assert admission.active_count == 0


async def test_regular_admitted_client_to_server_preserves_move_and_outer_lease() -> None:
    transport = Transport()
    manager = _manager(transport, max_concurrency=1)
    admission = manager._admission
    handle = Handle(uuid4(), 1)
    user_id = uuid4()
    slot_id = new_uuid7()
    lease = await manager.acquire_operation(user_id)
    deleted: list[None] = []

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return Sink()

    async def delete_source() -> None:
        deleted.append(None)

    task = asyncio.create_task(
        manager.start_client_to_server_regular_admitted(
            handle=handle,
            operation_lease=lease,
            slot_id=slot_id,
            user_id=user_id,
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
            mode="move",
            delete_source=delete_source,
        )
    )
    await transport.text_event.wait()
    request = parse_server_frame(transport.text[0][1])
    assert request.id == slot_id
    assert admission.active_count == 1
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="destination.bin",
            total_bytes=0,
            etag="source-v1",
        ),
    )
    await _wait_for_ready(transport, slot_id)
    digest = hashlib.sha256(b"").hexdigest()
    await manager.handle_frame(
        handle,
        TransferEndFrame(
            id=slot_id,
            ack=False,
            ok=True,
            bytes_sent=0,
            sha256=digest,
        ),
    )

    result = await task
    assert result.sha256 == digest
    assert result.etag is None
    assert result.created is None
    assert deleted == [None]
    assert admission.active_count == 1
    await lease.aclose()
    assert admission.active_count == 0


async def test_regular_admitted_server_to_client_preserves_move_callback() -> None:
    transport = Transport()
    manager = _manager(transport, max_concurrency=1)
    handle = Handle(uuid4(), 1)
    user_id = uuid4()
    slot_id = new_uuid7()
    lease = await manager.acquire_operation(user_id)
    deleted: list[None] = []

    async def delete_source() -> None:
        deleted.append(None)

    task = asyncio.create_task(
        manager.start_server_to_client_regular_admitted(
            handle=handle,
            operation_lease=lease,
            slot_id=slot_id,
            user_id=user_id,
            src_path="source.bin",
            dst_path="destination.bin",
            source=Source([], etag="source-v1"),
            total_bytes=0,
            mode="move",
            delete_source=delete_source,
        )
    )
    await transport.text_event.wait()
    await manager.handle_frame(handle, TransferReadyFrame(id=slot_id))
    end: TransferEndFrame | None = None
    while end is None:
        await asyncio.sleep(0)
        end = next(
            (
                frame
                for _, payload in transport.text
                if isinstance(frame := parse_server_frame(payload), TransferEndFrame)
                and not frame.ack
            ),
            None,
        )
    await manager.handle_frame(handle, end.model_copy(update={"ack": True}))

    assert (await task).warnings == ()
    assert deleted == [None]
    assert manager._admission.active_count == 1
    await lease.aclose()


async def test_regular_admitted_wrapper_validates_uuid_and_active_lease() -> None:
    transport = Transport()
    manager = _manager(transport, max_concurrency=1)
    handle = Handle(uuid4(), 1)
    user_id = uuid4()
    lease = await manager.acquire_operation(user_id)

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return Sink()

    with pytest.raises(ValueError, match="UUIDv7"):
        await manager.start_client_to_server_regular_admitted(
            handle=handle,
            operation_lease=lease,
            slot_id=uuid4(),
            user_id=user_id,
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
            mode="copy",
            delete_source=None,
        )
    await lease.aclose()
    with pytest.raises(ValueError, match="not active"):
        await manager.start_client_to_server_regular_admitted(
            handle=handle,
            operation_lease=lease,
            slot_id=new_uuid7(),
            user_id=user_id,
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
            mode="copy",
            delete_source=None,
        )


def test_transfer_manager_exposes_directory_control_idle_timeout() -> None:
    manager = _manager(Transport(), idle_timeout_seconds=12.5)

    assert manager.idle_timeout_seconds == 12.5


async def test_cancelled_already_admitted_child_does_not_release_outer_lease() -> None:
    transport = Transport()
    admission = FairTransferAdmission(
        max_concurrency=1,
        max_concurrency_per_user=1,
        queue_timeout_seconds=0.05,
    )
    manager = TransferManager(transport, admission=admission)
    handle = Handle(uuid4(), 1)
    user_id = uuid4()
    lease = await manager.acquire_operation(user_id)

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return Sink()

    task = asyncio.create_task(
        manager.start_client_to_server_admitted(
            handle=handle,
            user_id=user_id,
            slot_id=new_uuid7(),
            operation_lease=lease,
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
        )
    )
    await transport.text_event.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert manager.active_slots == 0
    assert admission.active_count == 1
    await lease.aclose()
    assert admission.active_count == 0


async def test_already_admitted_destination_ack_returns_commit_metadata() -> None:
    transport = Transport()
    admission = FairTransferAdmission(
        max_concurrency=1,
        max_concurrency_per_user=1,
        queue_timeout_seconds=0.05,
    )
    manager = TransferManager(transport, admission=admission)
    handle = Handle(uuid4(), 1)
    user_id = uuid4()
    lease = await manager.acquire_operation(user_id)
    slot_id = new_uuid7()
    task = asyncio.create_task(
        manager.start_server_to_client_admitted(
            handle=handle,
            operation_lease=lease,
            slot_id=slot_id,
            user_id=user_id,
            src_path="source.bin",
            dst_path="destination.bin",
            source=Source([b"payload"]),
            total_bytes=7,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    await manager.handle_frame(handle, TransferReadyFrame(id=slot_id))
    end: TransferEndFrame | None = None
    while end is None:
        await asyncio.sleep(0)
        end = next(
            (
                frame
                for _, payload in transport.text
                if isinstance(frame := parse_server_frame(payload), TransferEndFrame)
                and not frame.ack
            ),
            None,
        )
    await manager.handle_frame(
        handle,
        end.model_copy(
            update={"ack": True, "etag": "destination-v1", "created": True}
        ),
    )

    result = await task
    assert result.etag == "destination-v1"
    assert result.created is True
    assert admission.active_count == 1
    await lease.aclose()


async def test_already_admitted_server_destination_returns_commit_metadata() -> None:
    transport = Transport()
    admission = FairTransferAdmission(
        max_concurrency=1,
        max_concurrency_per_user=1,
        queue_timeout_seconds=0.05,
    )
    manager = TransferManager(transport, admission=admission)
    handle = Handle(uuid4(), 1)
    user_id = uuid4()
    lease = await manager.acquire_operation(user_id)
    slot_id = new_uuid7()

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return Sink()

    async def commit_sink(
        _sink: TransferSink,
        _begin: TransferBeginFrame,
        _size: int,
        _digest: str,
    ) -> TransferCommitResult:
        return TransferCommitResult(etag="destination-v1", created=True)

    task = asyncio.create_task(
        manager.start_client_to_server_admitted(
            handle=handle,
            operation_lease=lease,
            slot_id=slot_id,
            user_id=user_id,
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
            commit_sink=commit_sink,
        )
    )
    while not transport.text:
        await asyncio.sleep(0)
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="destination.bin",
            total_bytes=0,
        ),
    )
    await _wait_for_ready(transport, slot_id)
    digest = hashlib.sha256(b"").hexdigest()
    await manager.handle_frame(
        handle,
        TransferEndFrame(
            id=slot_id,
            ack=False,
            ok=True,
            bytes_sent=0,
            sha256=digest,
        ),
    )

    result = await task
    assert result.etag == "destination-v1"
    assert result.created is True
    ack = next(
        frame
        for _, payload in transport.text
        if isinstance(frame := parse_server_frame(payload), TransferEndFrame)
        and frame.ack
        and frame.ok
    )
    assert ack.etag is None
    assert ack.created is None
    assert admission.active_count == 1
    await lease.aclose()


async def test_admitted_client_to_server_reports_commit_after_caller_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = Transport()
    admission = FairTransferAdmission(
        max_concurrency=1,
        max_concurrency_per_user=1,
        queue_timeout_seconds=0.05,
    )
    manager = TransferManager(transport, admission=admission)
    handle = Handle(uuid4(), 1)
    user_id = uuid4()
    lease = await manager.acquire_operation(user_id)
    slot_id = new_uuid7()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    original_cleanup = manager._cleanup

    async def blocked_cleanup(slot: object, *, skip_worker: bool = False) -> None:
        if getattr(slot, "committed_result", None) is not None:
            cleanup_started.set()
            await release_cleanup.wait()
        await original_cleanup(slot, skip_worker=skip_worker)  # type: ignore[arg-type]

    monkeypatch.setattr(manager, "_cleanup", blocked_cleanup)

    async def make_sink(_: TransferBeginFrame) -> TransferSink:
        return Sink()

    async def commit_sink(
        _sink: TransferSink,
        _begin: TransferBeginFrame,
        _size: int,
        _digest: str,
    ) -> TransferCommitResult:
        return TransferCommitResult(etag="destination-v1")

    task = asyncio.create_task(
        manager.start_client_to_server_admitted(
            handle=handle,
            operation_lease=lease,
            slot_id=slot_id,
            user_id=user_id,
            src_path="source.bin",
            dst_path="destination.bin",
            sink_factory=make_sink,
            commit_sink=commit_sink,
        )
    )
    await transport.text_event.wait()
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.bin",
            dst_path="destination.bin",
            total_bytes=0,
        ),
    )
    await _wait_for_ready(transport, slot_id)
    digest = hashlib.sha256(b"").hexdigest()
    await manager.handle_frame(
        handle,
        TransferEndFrame(
            id=slot_id,
            ack=False,
            ok=True,
            bytes_sent=0,
            sha256=digest,
        ),
    )
    await cleanup_started.wait()

    task.cancel()
    release_cleanup.set()
    with pytest.raises(TransferCommittedAfterCancellation) as caught:
        await task

    assert caught.value.result == TransferResult(
        bytes_transferred=0,
        sha256=digest,
        etag="destination-v1",
        created=True,
    )
    assert admission.active_count == 1
    await lease.aclose()


async def test_admitted_server_to_client_reports_commit_after_caller_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = Transport()
    admission = FairTransferAdmission(
        max_concurrency=1,
        max_concurrency_per_user=1,
        queue_timeout_seconds=0.05,
    )
    manager = TransferManager(transport, admission=admission)
    handle = Handle(uuid4(), 1)
    user_id = uuid4()
    lease = await manager.acquire_operation(user_id)
    slot_id = new_uuid7()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    original_cleanup = manager._cleanup

    async def blocked_cleanup(slot: object, *, skip_worker: bool = False) -> None:
        if getattr(slot, "committed_result", None) is not None:
            cleanup_started.set()
            await release_cleanup.wait()
        await original_cleanup(slot, skip_worker=skip_worker)  # type: ignore[arg-type]

    monkeypatch.setattr(manager, "_cleanup", blocked_cleanup)
    task = asyncio.create_task(
        manager.start_server_to_client_admitted(
            handle=handle,
            operation_lease=lease,
            slot_id=slot_id,
            user_id=user_id,
            src_path="source.bin",
            dst_path="destination.bin",
            source=Source([b"payload"]),
            total_bytes=7,
        )
    )
    await transport.text_event.wait()
    await manager.handle_frame(handle, TransferReadyFrame(id=slot_id))
    end: TransferEndFrame | None = None
    while end is None:
        await asyncio.sleep(0)
        end = next(
            (
                frame
                for _, payload in transport.text
                if isinstance(frame := parse_server_frame(payload), TransferEndFrame)
                and not frame.ack
            ),
            None,
        )
    await manager.handle_frame(
        handle,
        end.model_copy(
            update={"ack": True, "etag": "destination-v1", "created": True}
        ),
    )
    await cleanup_started.wait()

    task.cancel()
    release_cleanup.set()
    with pytest.raises(TransferCommittedAfterCancellation) as caught:
        await task

    assert caught.value.result.etag == "destination-v1"
    assert caught.value.result.created is True
    assert caught.value.result.warnings == ()
    assert admission.active_count == 1
    await lease.aclose()


async def test_server_sender_cancellation_after_success_end_reconciles_original_ack() -> None:
    transport = Transport()
    admission = FairTransferAdmission(
        max_concurrency=1,
        max_concurrency_per_user=1,
        queue_timeout_seconds=0.05,
    )
    manager = TransferManager(transport, admission=admission)
    handle = Handle(uuid4(), 1)
    user_id = uuid4()
    lease = await manager.acquire_operation(user_id)
    slot_id = new_uuid7()
    task = asyncio.create_task(
        manager.start_server_to_client_admitted(
            handle=handle,
            operation_lease=lease,
            slot_id=slot_id,
            user_id=user_id,
            src_path="source.bin",
            dst_path="destination.bin",
            source=Source([b"payload"]),
            total_bytes=7,
        )
    )
    await transport.text_event.wait()
    await manager.handle_frame(handle, TransferReadyFrame(id=slot_id))
    success_end: TransferEndFrame | None = None
    while success_end is None:
        await asyncio.sleep(0)
        success_end = next(
            (
                frame
                for _, payload in transport.text
                if isinstance(frame := parse_server_frame(payload), TransferEndFrame)
                and not frame.ack
                and frame.ok
            ),
            None,
        )

    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    assert not any(
        isinstance(frame := parse_server_frame(payload), TransferEndFrame)
        and not frame.ack
        and not frame.ok
        for _, payload in transport.text
    )

    acknowledgement = success_end.model_copy(
        update={"ack": True, "etag": "destination-v1", "created": True}
    )
    await manager.handle_frame(handle, acknowledgement)
    with pytest.raises(TransferCommittedAfterCancellation) as caught:
        await task

    assert caught.value.result.etag == "destination-v1"
    assert caught.value.result.created is True
    await manager.handle_frame(handle, acknowledgement)
    assert admission.active_count == 1
    await lease.aclose()
