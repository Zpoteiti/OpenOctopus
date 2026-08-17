"""Bounded, generation-scoped device file transfers.

The WebSocket route owns frame decoding and calls :class:`TransferManager` for
transfer frames.  Keeping the slot table here makes transfer admission and
cleanup independent from the device tool-call registry.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID

from openctopus_server.devices.protocol import (
    MAX_BINARY_CHUNK_BYTES,
    TransferBeginFrame,
    TransferDirection,
    TransferEndFrame,
    TransferProgressFrame,
    TransferPurpose,
    TransferReadyFrame,
    TransferRequestFrame,
    decode_binary_chunk,
    new_uuid7,
)

TRANSFER_QUEUE_CHUNKS = 4
DEFAULT_TOMBSTONE_TTL_SECONDS = 60.0
TOMBSTONE_MAX_ENTRIES = 4096


class TransferProtocolError(RuntimeError):
    """A peer sent a transfer frame that cannot be accepted."""

    def __init__(self, message: str, *, code: str = "protocol_transfer_invalid_state") -> None:
        super().__init__(message)
        self.code = code


class TransferIntegrityError(RuntimeError):
    """The declared byte count or SHA-256 does not match the received stream."""

    code = "workspace_transfer_integrity_failed"


class TransferDisconnectedError(RuntimeError):
    code = "peer_disconnected"


class TransferBusyError(TimeoutError):
    code = "workspace_transfer_busy"


TRANSFER_TIMEOUT_CODE = "workspace_transfer_timeout"


class TransferState(StrEnum):
    REQUESTED = "requested"
    BEGUN = "begun"
    READY = "ready"
    STREAMING = "streaming"
    SENDER_ENDED = "sender_ended"
    COMMITTED = "committed"
    ABORTED = "aborted"


class TransferTransport(Protocol):
    async def send_text(
        self,
        handle: Any,
        payload: str,
        *,
        expected_device_name: str | None = None,
        expected_config_epoch: int | None = None,
    ) -> bool: ...

    async def send_binary(
        self,
        handle: Any,
        payload: bytes,
        *,
        expected_device_name: str | None = None,
        expected_config_epoch: int | None = None,
    ) -> bool: ...


class TransferRoute(Protocol):
    @property
    def handle(self) -> object: ...

    @property
    def config_epoch(self) -> int: ...

    @property
    def device_name(self) -> str: ...


class TransferSource(Protocol):
    async def read(self) -> bytes: ...

    async def aclose(self) -> None: ...


class TransferSink(Protocol):
    async def write(self, chunk: bytes) -> None: ...

    async def finish(self) -> Any: ...

    async def abort(self) -> None: ...


DeleteSource = Callable[[], Awaitable[None]]
SinkFactory = Callable[[TransferBeginFrame], Awaitable[TransferSink]]
CommitSink = Callable[[TransferSink, TransferBeginFrame, int, str], Awaitable[bool | None]]
SourceFactory = Callable[[], Awaitable[TransferSource]]


@dataclass(frozen=True, slots=True)
class TransferResult:
    bytes_transferred: int
    sha256: str
    warnings: tuple[str, ...] = ()
    etag: str | None = None
    created: bool | None = None

    def __post_init__(self) -> None:
        if self.bytes_transferred < 0:
            raise ValueError("transfer byte count must be non-negative")
        if self.created is not None and self.etag is None:
            raise ValueError("created metadata requires an etag")
        if self.etag is not None and (
            not 1 <= len(self.etag) <= 512
            or any(
                character in {'"', "\x00"}
                or not 0x21 <= ord(character) <= 0x7E
                for character in self.etag
            )
        ):
            raise ValueError("transfer etag is invalid")


@dataclass(slots=True)
class TransferLease:
    """Idempotent ownership of one global and one per-user transfer slot."""

    user_id: UUID
    _release: Callable[[], Awaitable[None]]
    _closed: bool = False

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        release_task: asyncio.Future[None] = asyncio.ensure_future(self._release())
        try:
            await asyncio.shield(release_task)
        except asyncio.CancelledError:
            await asyncio.shield(release_task)
            raise

    async def __aenter__(self) -> TransferLease:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()


@dataclass(slots=True)
class _Waiter:
    user_id: UUID
    future: asyncio.Future[TransferLease]
    queued: bool = True


class FairTransferAdmission:
    """Global/per-user transfer admission with round-robin user fairness.

    Waiters are kept in one FIFO per user.  Once a user has an admitted slot,
    that user is rotated behind other non-empty users, so a slow user cannot
    monopolize the global service.  The queue has no semaphore waiter ordering
    dependency and every timeout/cancellation removes its waiter.
    """

    def __init__(
        self,
        *,
        max_concurrency: int,
        max_concurrency_per_user: int,
        queue_timeout_seconds: float,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("global transfer concurrency must be positive")
        if not 1 <= max_concurrency_per_user <= max_concurrency:
            raise ValueError("per-user transfer concurrency is invalid")
        if queue_timeout_seconds <= 0:
            raise ValueError("transfer queue timeout must be positive")
        self.max_concurrency = max_concurrency
        self.max_concurrency_per_user = max_concurrency_per_user
        self.queue_timeout_seconds = queue_timeout_seconds
        self._lock = asyncio.Lock()
        self._active = 0
        self._active_by_user: dict[UUID, int] = {}
        self._waiters: dict[UUID, deque[_Waiter]] = {}
        self._round_robin: deque[UUID] = deque()

    @property
    def active_count(self) -> int:
        return self._active

    @property
    def waiting_count(self) -> int:
        return sum(len(waiters) for waiters in self._waiters.values())

    @property
    def active_by_user(self) -> dict[UUID, int]:
        return dict(self._active_by_user)

    async def acquire(self, user_id: UUID) -> TransferLease:
        loop = asyncio.get_running_loop()
        waiter = _Waiter(user_id=user_id, future=loop.create_future())
        async with self._lock:
            if self._can_grant_locked(user_id) and not self._round_robin:
                lease = self._grant_locked(user_id)
                return lease
            queue = self._waiters.get(user_id)
            if (
                (queue is not None and len(queue) >= self.max_concurrency_per_user)
                or self.waiting_count >= self.max_concurrency
            ):
                raise TransferBusyError
            queue = self._waiters.setdefault(user_id, deque())
            queue.append(waiter)
            if user_id not in self._round_robin:
                self._round_robin.append(user_id)
            self._drain_locked()
        try:
            async with asyncio.timeout(self.queue_timeout_seconds):
                return await asyncio.shield(waiter.future)
        except TimeoutError as exc:
            await self._cleanup_waiter(waiter)
            raise TransferBusyError from exc
        except asyncio.CancelledError:
            await self._cleanup_waiter(waiter)
            raise

    async def _cleanup_waiter(self, waiter: _Waiter) -> None:
        """Remove a waiter and close a lease granted at timeout/cancel boundary."""

        lease: TransferLease | None = None
        async with self._lock:
            if waiter.queued:
                waiter.queued = False
                wait_queue = self._waiters.get(waiter.user_id)
                if wait_queue is not None:
                    try:
                        wait_queue.remove(waiter)
                    except ValueError:
                        pass
                    if not wait_queue:
                        self._waiters.pop(waiter.user_id, None)
                        try:
                            self._round_robin.remove(waiter.user_id)
                        except ValueError:
                            pass
                self._drain_locked()
            if waiter.future.done() and not waiter.future.cancelled():
                lease = waiter.future.result()
            elif not waiter.future.done():
                waiter.future.cancel()
        if lease is not None:
            close_task = asyncio.create_task(lease.aclose())
            try:
                await asyncio.shield(close_task)
            except asyncio.CancelledError:
                await asyncio.shield(close_task)
                raise

    def _can_grant_locked(self, user_id: UUID) -> bool:
        return self._active < self.max_concurrency and self._active_by_user.get(user_id, 0) < (
            self.max_concurrency_per_user
        )

    def _grant_locked(self, user_id: UUID) -> TransferLease:
        self._active += 1
        self._active_by_user[user_id] = self._active_by_user.get(user_id, 0) + 1
        return TransferLease(user_id, lambda: self._release(user_id))

    def _drain_locked(self) -> None:
        if not self._round_robin:
            return
        # At most one full rotation can be blocked by per-user limits.
        blocked = 0
        while self._active < self.max_concurrency and self._round_robin and blocked < len(
            self._round_robin
        ):
            user_id = self._round_robin.popleft()
            queue = self._waiters.get(user_id)
            if not queue:
                self._waiters.pop(user_id, None)
                blocked = 0
                continue
            if not self._can_grant_locked(user_id):
                self._round_robin.append(user_id)
                blocked += 1
                continue
            waiter = queue.popleft()
            waiter.queued = False
            lease = self._grant_locked(user_id)
            if queue:
                self._round_robin.append(user_id)
            else:
                self._waiters.pop(user_id, None)
            blocked = 0
            if not waiter.future.done():
                waiter.future.set_result(lease)

    async def _release(self, user_id: UUID) -> None:
        async with self._lock:
            self._active = max(0, self._active - 1)
            current = self._active_by_user.get(user_id, 0)
            if current <= 1:
                self._active_by_user.pop(user_id, None)
            else:
                self._active_by_user[user_id] = current - 1
            self._drain_locked()


@dataclass(slots=True)
class _TransferSlot:
    handle: object
    route: TransferRoute | None
    device_id: UUID
    generation: int
    user_id: UUID
    slot_id: UUID
    direction: TransferDirection
    purpose: TransferPurpose
    state: TransferState
    lease: TransferLease
    queue: asyncio.Queue[bytes | None] = field(
        default_factory=lambda: asyncio.Queue(maxsize=TRANSFER_QUEUE_CHUNKS)
    )
    source: TransferSource | None = None
    sink: TransferSink | None = None
    begin: TransferBeginFrame | None = None
    end: TransferEndFrame | None = None
    ready_future: asyncio.Future[TransferReadyFrame] | None = None
    ack_future: asyncio.Future[TransferEndFrame] | None = None
    completion: asyncio.Future[TransferResult] | None = None
    worker: asyncio.Task[None] | None = None
    source_factory_task: asyncio.Task[TransferSource] | None = None
    abort_event: asyncio.Event = field(default_factory=asyncio.Event)
    delete_source: DeleteSource | None = None
    source_etag: str | None = None
    commit_sink: CommitSink | None = None
    sink_factory: SinkFactory | None = None
    mode: str = "copy"
    bytes_seen: int = 0
    bytes_received: int = 0
    digest: Any = field(default_factory=hashlib.sha256)
    last_progress: int = 0
    terminal_ack: TransferEndFrame | None = None
    committed_result: TransferResult | None = None
    commit_resolution: asyncio.Future[bool] | None = None
    success_ack_delivered: bool = False
    fenced: bool = False
    finish_task: asyncio.Task[None] | None = None


class TransferManager:
    """Own transfer slots for all generations of one process-local registry."""

    def __init__(
        self,
        transport: TransferTransport,
        *,
        admission: FairTransferAdmission,
        idle_timeout_seconds: float = 30.0,
        tombstone_ttl_seconds: float = DEFAULT_TOMBSTONE_TTL_SECONDS,
    ) -> None:
        if idle_timeout_seconds <= 0:
            raise ValueError("transfer idle timeout must be positive")
        self._transport = transport
        self._admission = admission
        self._idle_timeout_seconds = idle_timeout_seconds
        self._tombstone_ttl_seconds = tombstone_ttl_seconds
        self._slots: dict[tuple[UUID, int, UUID], _TransferSlot] = {}
        self._tombstones: dict[
            tuple[UUID, int, UUID], tuple[float, TransferEndFrame | None, bool]
        ] = {}
        self._acknowledged_failure_tombstones: set[tuple[UUID, int, UUID]] = set()
        self._source_cleanup_tasks: set[asyncio.Task[None]] = set()
        self._lock = asyncio.Lock()

    @property
    def active_slots(self) -> int:
        return len(self._slots)

    @property
    def slot_ids(self) -> tuple[UUID, ...]:
        return tuple(slot.slot_id for slot in self._slots.values())

    def fence_handle(self, handle: object) -> None:
        """Synchronously prevent a retired generation from making more progress."""

        for slot in tuple(self._slots.values()):
            if _same_handle(slot.handle, handle):
                self._fence_slot(slot)

    def fence_route(self, route: TransferRoute) -> None:
        """Synchronously prevent an old configuration snapshot from progressing."""

        for slot in tuple(self._slots.values()):
            if slot.route == route:
                self._fence_slot(slot)

    @staticmethod
    def _fence_slot(slot: _TransferSlot) -> None:
        if slot.fenced:
            return
        slot.fenced = True
        slot.abort_event.set()
        if slot.worker is not None and not slot.worker.done():
            slot.worker.cancel()

    async def start_server_to_client(
        self,
        *,
        handle: object,
        route: TransferRoute | None = None,
        user_id: UUID,
        src_path: str | None,
        dst_path: str,
        source: TransferSource | None = None,
        source_factory: SourceFactory | None = None,
        total_bytes: int | None = None,
        sha256: str | None = None,
        mime: str | None = None,
        purpose: TransferPurpose = "file_transfer",
        if_match: str | None = None,
        if_none_match: bool | None = None,
        mode: str = "copy",
        delete_source: DeleteSource | None = None,
        src_device: str = "server",
        dst_device: str | None = None,
    ) -> TransferResult:
        if mode not in {"copy", "move"}:
            raise ValueError("transfer mode must be copy or move")
        if (source is None) == (source_factory is None):
            raise ValueError("exactly one transfer source or source factory is required")
        if purpose != "workspace_upload" and (if_match is not None or if_none_match is not None):
            raise ValueError("transfer preconditions are only valid for workspace_upload")
        if if_none_match is False:
            if_none_match = None
        lease = await self._admission.acquire(user_id)
        slot = await self._new_slot(
            handle=handle,
            route=route,
            user_id=user_id,
            lease=lease,
            direction="server_to_client",
            purpose=purpose,
            state=TransferState.BEGUN,
            source=source,
            source_etag=_source_etag(source) if source is not None else None,
            delete_source=delete_source,
            mode=mode,
        )
        try:
            if source_factory is not None:
                created_source = await self._prepare_source(slot, source_factory)
                slot.source = created_source
                slot.source_etag = _source_etag(created_source)
                if total_bytes is None:
                    source_size = getattr(created_source, "size", None)
                    if not isinstance(source_size, int) or source_size < 0:
                        raise TransferProtocolError(
                            "transfer source did not declare its size"
                        )
                    total_bytes = source_size
            if slot.source is None:
                raise TransferProtocolError("transfer source is not configured")
            begin = TransferBeginFrame(
                id=slot.slot_id,
                direction="server_to_client",
                purpose=purpose,
                src_device=src_device,
                src_path=src_path,
                dst_device=dst_device,
                dst_path=dst_path,
                total_bytes=total_bytes,
                sha256=sha256,
                mime=mime,
                etag=(
                    slot.source_etag
                    if purpose in {"file_transfer", "http_relay"}
                    else None
                ),
                if_match=if_match,
                if_none_match=if_none_match,
            )
        except BaseException:
            await self._cleanup(slot)
            raise
        slot.begin = begin
        slot.ready_future = asyncio.get_running_loop().create_future()
        slot.ack_future = asyncio.get_running_loop().create_future()
        slot.completion = asyncio.get_running_loop().create_future()
        slot.worker = asyncio.create_task(self._send_server_source(slot))
        try:
            return await asyncio.shield(slot.completion)
        except asyncio.CancelledError:
            await self._abort(slot, "cancelled", send_frame=True)
            raise

    async def start_client_to_server(
        self,
        *,
        handle: object,
        route: TransferRoute | None = None,
        user_id: UUID,
        src_path: str,
        dst_path: str | None,
        sink_factory: SinkFactory,
        commit_sink: CommitSink | None = None,
        delete_source: DeleteSource | None = None,
        purpose: TransferPurpose = "file_transfer",
        mode: str = "copy",
    ) -> TransferResult:
        if mode not in {"copy", "move"}:
            raise ValueError("transfer mode must be copy or move")
        lease = await self._admission.acquire(user_id)
        slot = await self._new_slot(
            handle=handle,
            route=route,
            user_id=user_id,
            lease=lease,
            direction="client_to_server",
            purpose=purpose,
            state=TransferState.REQUESTED,
            commit_sink=commit_sink,
            sink_factory=sink_factory,
            delete_source=delete_source,
            mode=mode,
        )
        slot.completion = asyncio.get_running_loop().create_future()
        try:
            request = TransferRequestFrame(
                id=slot.slot_id,
                purpose=purpose,
                src_path=src_path,
                dst_path=dst_path,
            )
            if not await self._send_text(handle, request.model_dump_json(), route=slot.route):
                raise TransferDisconnectedError("device connection was replaced")
            return await asyncio.shield(slot.completion)
        except asyncio.CancelledError:
            await self._abort(slot, "cancelled", send_frame=True)
            raise
        except BaseException as exc:
            await self._abort(slot, _error_code(exc), send_frame=True)
            raise

    async def handle_frame(self, handle: object, frame: object) -> None:
        """Route one already-validated client transfer control frame."""
        if isinstance(frame, TransferReadyFrame):
            await self._handle_ready(handle, frame)
        elif isinstance(frame, TransferBeginFrame):
            await self._handle_begin(handle, frame)
        elif isinstance(frame, TransferEndFrame):
            await self._handle_end(handle, frame)
        elif isinstance(frame, TransferProgressFrame):
            await self._handle_progress(handle, frame)
        else:
            raise TransferProtocolError("not a transfer frame")

    async def handle_binary(self, handle: object, payload: bytes) -> None:
        slot_id, chunk = self._decode_binary(payload)
        if await self._is_failed_tombstone(handle, slot_id):
            # A bounded number of chunks may already be in the peer's writer
            # when it observes our terminal failure.  Drain only that known
            # failed slot; unknown or normally completed slots remain errors.
            return
        slot = await self._get_slot(handle, slot_id)
        if slot.direction != "client_to_server" or slot.state not in {
            TransferState.READY,
            TransferState.STREAMING,
        }:
            raise TransferProtocolError("binary chunk arrived before transfer_ready")
        if slot.end is not None:
            raise TransferProtocolError("binary chunk arrived after transfer_end")
        if not chunk:
            return
        declared = slot.begin.total_bytes if slot.begin is not None else None
        if declared is not None and slot.bytes_received + len(chunk) > declared:
            await self._abort(
                slot,
                "workspace_transfer_integrity_failed",
                send_frame=True,
            )
            raise TransferProtocolError(
                "binary bytes exceed the declared transfer size",
                code="protocol_transfer_length_mismatch",
            )
        if slot.state is TransferState.READY:
            slot.state = TransferState.STREAMING
        # Queue capacity is intentionally four 64 KiB chunks.  This await is
        # the backpressure point: no whole-file buffer is created server-side.
        slot.bytes_received += len(chunk)
        try:
            async with asyncio.timeout(self._idle_timeout_seconds):
                await slot.queue.put(chunk)
        except TimeoutError as exc:
            await self._abort(slot, TRANSFER_TIMEOUT_CODE, send_frame=True)
            raise TransferProtocolError(
                "transfer receive queue is stalled",
                code=TRANSFER_TIMEOUT_CODE,
            ) from exc

    async def disconnect(self, handle: object) -> None:
        """Abort every slot owned by a stale/replaced socket generation."""
        slots = [slot for slot in self._slots.values() if _same_handle(slot.handle, handle)]
        await asyncio.gather(
            *(self._abort(slot, "peer_disconnected", send_frame=False) for slot in slots),
            return_exceptions=True,
        )

    async def close(self) -> None:
        slots = list(self._slots.values())
        await asyncio.gather(
            *(self._abort(slot, "server_shutdown", send_frame=False) for slot in slots),
            return_exceptions=True,
        )

    async def _send_server_source(self, slot: _TransferSlot) -> None:
        assert slot.source is not None
        assert slot.begin is not None
        assert slot.ready_future is not None
        assert slot.ack_future is not None
        try:
            if not await self._send_text(
                slot.handle,
                slot.begin.model_dump_json(),
                route=slot.route,
            ):
                raise TransferDisconnectedError("device connection was replaced")
            async with asyncio.timeout(self._idle_timeout_seconds):
                await asyncio.shield(slot.ready_future)
            if slot.state is not TransferState.READY:
                raise TransferProtocolError("transfer_ready arrived in an invalid state")
            slot.state = TransferState.STREAMING
            while True:
                async with asyncio.timeout(self._idle_timeout_seconds):
                    chunk = await slot.source.read()
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise TransferProtocolError("transfer source returned a non-byte chunk")
                for start in range(0, len(chunk), MAX_BINARY_CHUNK_BYTES):
                    piece = chunk[start : start + MAX_BINARY_CHUNK_BYTES]
                    slot.bytes_seen += len(piece)
                    slot.digest.update(piece)
                    if not await self._send_binary(slot.handle, slot.slot_id, piece):
                        raise TransferDisconnectedError("device connection was replaced")
            digest = slot.digest.hexdigest()
            if slot.begin.total_bytes is not None and slot.begin.total_bytes != slot.bytes_seen:
                raise TransferIntegrityError("source byte count did not match transfer metadata")
            if slot.begin.sha256 is not None and slot.begin.sha256 != digest:
                raise TransferIntegrityError("source digest did not match transfer metadata")
            end = TransferEndFrame(
                id=slot.slot_id,
                ack=False,
                ok=True,
                bytes_sent=slot.bytes_seen,
                sha256=digest,
            )
            slot.state = TransferState.SENDER_ENDED
            if not await self._send_text(slot.handle, end.model_dump_json(), route=slot.route):
                raise TransferDisconnectedError("device connection was replaced")
            async with asyncio.timeout(self._idle_timeout_seconds):
                ack = await asyncio.shield(slot.ack_future)
            slot.terminal_ack = ack
            if not ack.ok:
                raise TransferError(ack.code or "transfer_rejected")
            if ack.bytes_sent is not None and ack.bytes_sent != slot.bytes_seen:
                raise TransferIntegrityError("receiver byte count did not match")
            if ack.sha256 is not None and ack.sha256 != digest:
                raise TransferIntegrityError("receiver digest did not match")
            if slot.purpose != "workspace_upload" and (
                ack.etag is not None or ack.created is not None
            ):
                raise TransferProtocolError(
                    "transfer metadata is only valid for workspace_upload"
                )
            slot.committed_result = self._result_for_slot(
                slot,
                digest=digest,
                warnings=(),
                etag=ack.etag if slot.purpose == "workspace_upload" else None,
                created=ack.created if slot.purpose == "workspace_upload" else None,
            )
            slot.success_ack_delivered = True
            slot.state = TransferState.COMMITTED
            warnings: list[str] = []
            if slot.mode == "move":
                if slot.delete_source is None or slot.source_etag is None:
                    warnings.append("source_delete_failed")
                else:
                    try:
                        await slot.delete_source()
                    except Exception:
                        warnings.append("source_delete_failed")
            result = self._result_for_slot(
                slot,
                digest=digest,
                warnings=tuple(warnings),
                etag=ack.etag if slot.purpose == "workspace_upload" else None,
                created=ack.created if slot.purpose == "workspace_upload" else None,
            )
            slot.committed_result = result
            await self._finish(slot, result)
        except asyncio.CancelledError:
            await self._abort(slot, "cancelled", send_frame=False)
        except BaseException as exc:
            # A peer terminal failure has already been acknowledged in
            # ``_handle_end``; do not emit a second non-ACK terminal frame.
            peer_failed = slot.ack_future is not None and slot.ack_future.done()
            await self._abort(slot, _error_code(exc), send_frame=not peer_failed, error=exc)

    async def _handle_ready(self, handle: object, frame: TransferReadyFrame) -> None:
        slot = await self._get_slot(handle, frame.id)
        if slot.direction != "server_to_client" or slot.state is not TransferState.BEGUN:
            raise TransferProtocolError("transfer_ready arrived in an invalid state")
        assert slot.ready_future is not None
        slot.state = TransferState.READY
        if not slot.ready_future.done():
            slot.ready_future.set_result(frame)

    async def _handle_begin(self, handle: object, frame: TransferBeginFrame) -> None:
        slot = await self._get_slot(handle, frame.id)
        if slot.direction != "client_to_server" or slot.state is not TransferState.REQUESTED:
            raise TransferProtocolError("transfer_begin arrived in an invalid state")
        if frame.direction != "client_to_server" or frame.purpose != slot.purpose:
            raise TransferProtocolError("transfer_begin direction or purpose mismatched")
        if frame.total_bytes is None:
            raise TransferProtocolError("client file transfer requires total_bytes")
        if frame.src_path is None:
            raise TransferProtocolError("client transfer metadata is missing a source path")
        if frame.purpose == "file_transfer" and frame.dst_path is None:
            raise TransferProtocolError("file transfer metadata is missing a destination path")
        if frame.purpose == "http_relay" and frame.dst_path is not None:
            raise TransferProtocolError("http relay metadata must not include a destination path")
        slot.begin = frame
        slot.source_etag = frame.etag
        slot.state = TransferState.BEGUN
        slot.worker = asyncio.create_task(self._consume_client_source(slot))

    async def _consume_client_source(self, slot: _TransferSlot) -> None:
        assert slot.begin is not None
        try:
            # Sink preparation may reserve a non-visible RustFS object and can
            # therefore be slow. Keep it in the slot worker so inbound control
            # frames and connection lifecycle operations are not serialized
            # behind that await.
            sink_factory = getattr(slot, "sink_factory", None)
            if sink_factory is None:
                raise TransferProtocolError("transfer sink is not configured")
            async with asyncio.timeout(self._idle_timeout_seconds):
                sink = await sink_factory(slot.begin)
            if slot.state is not TransferState.BEGUN:
                # The slot may have been terminated while a factory
                # suppressed cancellation. It never became slot-owned, so
                # abort it here instead of emitting transfer_ready.
                try:
                    await sink.abort()
                except BaseException:
                    pass
                return
            assert slot.sink is None
            slot.sink = sink
            slot.state = TransferState.READY
            ready = TransferReadyFrame(id=slot.slot_id)
            if not await self._send_text(slot.handle, ready.model_dump_json(), route=slot.route):
                await self._abort(slot, "peer_disconnected", send_frame=False)
                return
            assert slot.sink is not None
            while True:
                async with asyncio.timeout(self._idle_timeout_seconds):
                    chunk = await slot.queue.get()
                if chunk is None:
                    break
                slot.bytes_seen += len(chunk)
                slot.digest.update(chunk)
                async with asyncio.timeout(self._idle_timeout_seconds):
                    await slot.sink.write(chunk)
            end = slot.end
            if end is None or end.ack:
                raise TransferProtocolError("sender did not provide a terminal transfer frame")
            if not end.ok:
                raise TransferError(end.code or "transfer_rejected")
            digest = slot.digest.hexdigest()
            if end.bytes_sent is None or end.bytes_sent != slot.bytes_seen:
                raise TransferIntegrityError("sender byte count did not match")
            if end.sha256 is None or end.sha256 != digest:
                raise TransferIntegrityError("sender digest did not match")
            if slot.begin.total_bytes != slot.bytes_seen:
                raise TransferIntegrityError("declared file size did not match")
            async with asyncio.timeout(self._idle_timeout_seconds):
                await slot.sink.finish()
            cancel_after_commit = False
            if slot.commit_sink is not None:
                resolution = asyncio.get_running_loop().create_future()
                slot.commit_resolution = resolution
                try:
                    async with asyncio.timeout(self._idle_timeout_seconds):
                        cancel_after_commit = bool(
                            await slot.commit_sink(
                                slot.sink,
                                slot.begin,
                                slot.bytes_seen,
                                digest,
                            )
                        )
                except BaseException:
                    resolution.set_result(False)
                    raise
            slot.committed_result = self._result_for_slot(
                slot,
                digest=digest,
                warnings=(),
                etag=slot.begin.etag if slot.purpose == "http_relay" else None,
            )
            if slot.commit_resolution is not None:
                slot.commit_resolution.set_result(True)
            ack = TransferEndFrame(
                id=slot.slot_id,
                ack=True,
                ok=True,
                bytes_sent=slot.bytes_seen,
                sha256=digest,
            )
            try:
                async with asyncio.timeout(self._idle_timeout_seconds):
                    ack_delivered = await self._send_text(
                        slot.handle,
                        ack.model_dump_json(),
                        route=slot.route,
                    )
            except Exception:
                ack_delivered = False
            slot.success_ack_delivered = ack_delivered
            # The destination commit is the irreversible success point.  ACK
            # loss cannot roll it back or turn it into a reported failure.  A
            # move deletes its source only after confirmed ACK delivery because
            # the client retains the source path lock until it observes that ACK.
            warnings: list[str] = []
            if not ack_delivered:
                warnings.append("transfer_ack_failed")
                if slot.mode == "move":
                    warnings.append("source_delete_failed")
            elif slot.mode == "move":
                if slot.delete_source is None or slot.source_etag is None:
                    warnings.append("source_delete_failed")
                else:
                    try:
                        await slot.delete_source()
                    except Exception:
                        warnings.append("source_delete_failed")
            result = self._result_for_slot(
                slot,
                digest=digest,
                warnings=tuple(warnings),
                etag=slot.begin.etag if slot.purpose == "http_relay" else None,
            )
            slot.state = TransferState.COMMITTED
            await self._finish(slot, result)
            if cancel_after_commit:
                current = asyncio.current_task()
                if current is not None:
                    asyncio.get_running_loop().call_soon(current.cancel)
        except asyncio.CancelledError:
            await self._abort(slot, "cancelled", send_frame=False)
        except BaseException as exc:
            code = _error_code(exc)
            if slot.end is not None and not slot.end.ack:
                try:
                    await self._send_text(
                        slot.handle,
                        TransferEndFrame(
                            id=slot.slot_id,
                            ack=True,
                            ok=False,
                            code=code,
                        ).model_dump_json(),
                        route=slot.route,
                    )
                except Exception:
                    pass
                await self._abort(slot, code, send_frame=False, error=exc)
            else:
                await self._abort(slot, code, send_frame=True, error=exc)

    async def _handle_end(self, handle: object, frame: TransferEndFrame) -> None:
        if frame.ack and await self._matches_tombstone(handle, frame):
            return
        matches_timeout, timeout_route = await self._matches_committed_sender_timeout(
            handle,
            frame,
        )
        if matches_timeout:
            try:
                await self._send_text(
                    handle,
                    frame.model_copy(update={"ack": True}).model_dump_json(),
                    route=timeout_route,
                )
            except Exception:
                pass
            return
        slot = await self._get_slot(handle, frame.id, terminal=True)
        if frame.ack:
            if slot.direction != "server_to_client" or slot.state is not TransferState.SENDER_ENDED:
                raise TransferProtocolError("transfer acknowledgement arrived in an invalid state")
            assert slot.ack_future is not None
            if not slot.ack_future.done():
                slot.ack_future.set_result(frame)
            return
        if slot.direction == "client_to_server":
            if not frame.ok:
                if slot.state not in {
                    TransferState.REQUESTED,
                    TransferState.BEGUN,
                    TransferState.READY,
                    TransferState.STREAMING,
                    TransferState.SENDER_ENDED,
                }:
                    raise TransferProtocolError("sender terminal frame arrived in an invalid state")
                slot.end = frame
                slot.state = TransferState.SENDER_ENDED
                code = frame.code or "transfer_rejected"
                try:
                    await self._send_text(
                        slot.handle,
                        frame.model_copy(update={"ack": True}).model_dump_json(),
                        route=slot.route,
                    )
                except Exception:
                    pass
                await self._abort(
                    slot,
                    code,
                    send_frame=False,
                    error=TransferError(code),
                )
                return
            if slot.state not in {TransferState.READY, TransferState.STREAMING}:
                raise TransferProtocolError("sender terminal frame arrived in an invalid state")
            slot.end = frame
            slot.state = TransferState.SENDER_ENDED
            try:
                async with asyncio.timeout(self._idle_timeout_seconds):
                    await slot.queue.put(None)
            except TimeoutError as exc:
                await self._abort(slot, TRANSFER_TIMEOUT_CODE, send_frame=True, error=exc)
            return
        if slot.direction == "server_to_client":
            if frame.ok or slot.state not in {
                TransferState.BEGUN,
                TransferState.READY,
                TransferState.STREAMING,
                TransferState.SENDER_ENDED,
            }:
                raise TransferProtocolError("peer terminal frame arrived in an invalid state")
            code = frame.code or "transfer_rejected"
            ack = frame.model_copy(update={"ack": True})
            await self._send_text(slot.handle, ack.model_dump_json(), route=slot.route)
            await self._abort(
                slot,
                code,
                send_frame=False,
                error=TransferError(code),
            )

    async def _handle_progress(self, handle: object, frame: TransferProgressFrame) -> None:
        slot = await self._get_slot(handle, frame.id)
        if frame.bytes_sent < slot.last_progress:
            raise TransferProtocolError("transfer progress moved backwards")
        slot.last_progress = frame.bytes_sent

    async def _new_slot(
        self,
        *,
        handle: object,
        route: TransferRoute | None,
        user_id: UUID,
        lease: TransferLease,
        direction: TransferDirection,
        purpose: TransferPurpose,
        state: TransferState,
        source: TransferSource | None = None,
        delete_source: DeleteSource | None = None,
        commit_sink: CommitSink | None = None,
        sink_factory: SinkFactory | None = None,
        source_etag: str | None = None,
        mode: str = "copy",
    ) -> _TransferSlot:
        try:
            device_id, generation = _handle_identity(handle)
            slot = _TransferSlot(
                handle=handle,
                route=route,
                device_id=device_id,
                generation=generation,
                user_id=user_id,
                slot_id=new_uuid7(),
                direction=direction,
                purpose=purpose,
                state=state,
                lease=lease,
                source=source,
                source_etag=source_etag,
                delete_source=delete_source,
                commit_sink=commit_sink,
                sink_factory=sink_factory,
                mode=mode,
            )
            async with self._lock:
                self._expire_tombstones_locked()
                key = (device_id, generation, slot.slot_id)
                self._slots[key] = slot
            return slot
        except BaseException:
            await lease.aclose()
            raise

    async def _get_slot(
        self,
        handle: object,
        slot_id: UUID,
        *,
        terminal: bool = False,
    ) -> _TransferSlot:
        device_id, generation = _handle_identity(handle)
        key = (device_id, generation, slot_id)
        async with self._lock:
            self._expire_tombstones_locked()
            slot = self._slots.get(key)
            if slot is not None:
                if slot.fenced:
                    raise TransferDisconnectedError("device route was replaced")
                return slot
            if key in self._tombstones and terminal:
                raise TransferProtocolError(
                    "late transfer terminal frame conflicts with a closed slot",
                    code="protocol_transfer_unknown_id",
                )
        raise TransferProtocolError("unknown transfer slot", code="protocol_transfer_unknown_id")

    async def _finish(self, slot: _TransferSlot, result: TransferResult) -> None:
        if slot.finish_task is None:
            async def finish() -> None:
                await self._cleanup(slot, skip_worker=True)
                if slot.completion is not None and not slot.completion.done():
                    slot.completion.set_result(result)

            slot.finish_task = asyncio.create_task(finish())
        cancelled = False
        while True:
            try:
                await asyncio.shield(slot.finish_task)
                break
            except asyncio.CancelledError:
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError

    @staticmethod
    def _result_for_slot(
        slot: _TransferSlot,
        *,
        digest: str,
        warnings: tuple[str, ...],
        etag: str | None = None,
        created: bool | None = None,
    ) -> TransferResult:
        """Keep relay/upload metadata scoped to its one protocol purpose."""

        if slot.purpose == "workspace_upload":
            if slot.direction != "server_to_client":
                raise TransferProtocolError(
                    "workspace_upload metadata has an invalid transfer direction"
                )
            if etag is None or created is None:
                raise TransferProtocolError(
                    "workspace_upload result is missing destination metadata"
                )
        elif slot.purpose == "http_relay":
            if slot.direction != "client_to_server" or created is not None:
                raise TransferProtocolError("http_relay metadata is invalid")
        elif etag is not None or created is not None:
            raise TransferProtocolError("file_transfer must not carry metadata")
        return TransferResult(
            slot.bytes_seen,
            digest,
            warnings,
            etag=etag,
            created=created,
        )

    async def _abort(
        self,
        slot: _TransferSlot,
        code: str,
        *,
        send_frame: bool,
        error: BaseException | None = None,
    ) -> None:
        resolution = slot.commit_resolution
        current = asyncio.current_task()
        if (
            resolution is not None
            and not resolution.done()
            and slot.worker is not None
            and slot.worker is not current
        ):
            slot.worker.cancel()
            while not resolution.done():
                try:
                    await asyncio.shield(resolution)
                except asyncio.CancelledError:
                    continue
            await self._abort(slot, code, send_frame=send_frame, error=error)
            return
        terminal = (
            TransferEndFrame(id=slot.slot_id, ack=False, ok=False, code=code)
            if send_frame
            else None
        )
        key = (slot.device_id, slot.generation, slot.slot_id)
        committed_result: TransferResult | None = None
        committed_worker: asyncio.Task[None] | None = None
        async with self._lock:
            if slot.state is TransferState.ABORTED:
                return
            if slot.committed_result is not None:
                warnings = list(slot.committed_result.warnings)
                if not slot.success_ack_delivered:
                    warnings.append("transfer_ack_failed")
                if slot.mode == "move":
                    warnings.append("source_delete_failed")
                committed_result = TransferResult(
                    bytes_transferred=slot.committed_result.bytes_transferred,
                    sha256=slot.committed_result.sha256,
                    warnings=tuple(warnings),
                    etag=slot.committed_result.etag,
                    created=slot.committed_result.created,
                )
                slot.state = TransferState.COMMITTED
                slot.abort_event.set()
                if slot.worker is not current and slot.worker is not None:
                    committed_worker = slot.worker
            else:
                slot.state = TransferState.ABORTED
                slot.abort_event.set()
                if terminal is not None:
                    slot.terminal_ack = terminal.model_copy(update={"ack": True})
                self._remember_tombstone_locked(
                    key,
                    (
                        time.monotonic() + self._tombstone_ttl_seconds,
                        slot.terminal_ack,
                        False,
                    ),
                )
        if committed_result is not None:
            cancelled = False
            if committed_worker is not None:
                while not committed_worker.done():
                    try:
                        await asyncio.shield(committed_worker)
                    except asyncio.CancelledError:
                        cancelled = True
            await self._finish(slot, committed_result)
            if cancelled:
                raise asyncio.CancelledError
            return
        if terminal is not None:
            try:
                await self._send_text(
                    slot.handle,
                    terminal.model_dump_json(),
                    route=slot.route,
                )
            except Exception:
                pass
        await self._cleanup(slot)
        if slot.completion is not None and not slot.completion.done():
            if error is None:
                error = TransferError(code)
            slot.completion.set_exception(error)
            # The start_* caller may have been cancelled while shielding this
            # future.  Mark the exception retrieved here so asyncio debug mode
            # does not report an unhandled completion future.
            slot.completion.exception()

    async def _cleanup(
        self,
        slot: _TransferSlot,
        *,
        skip_worker: bool = False,
    ) -> None:
        current = asyncio.current_task()
        if (
            not skip_worker
            and slot.worker is not None
            and slot.worker is not current
            and not slot.worker.done()
        ):
            slot.worker.cancel()
            await asyncio.gather(slot.worker, return_exceptions=True)
        if slot.source is not None:
            try:
                await slot.source.aclose()
            except Exception:
                pass
            slot.source = None
        if slot.sink is not None and slot.state is not TransferState.COMMITTED:
            try:
                await slot.sink.abort()
            except Exception:
                pass
            slot.sink = None
        await slot.lease.aclose()
        key = (slot.device_id, slot.generation, slot.slot_id)
        async with self._lock:
            self._slots.pop(key, None)
            if key not in self._tombstones:
                self._remember_tombstone_locked(
                    key,
                    (
                        time.monotonic() + self._tombstone_ttl_seconds,
                        slot.terminal_ack,
                        slot.direction == "client_to_server"
                        and slot.committed_result is not None,
                    ),
                )

    async def _prepare_source(
        self,
        slot: _TransferSlot,
        source_factory: SourceFactory,
    ) -> TransferSource:
        task = asyncio.ensure_future(source_factory())
        slot.source_factory_task = task
        abort_waiter = asyncio.create_task(slot.abort_event.wait())
        try:
            done, _ = await asyncio.wait(
                (task, abort_waiter),
                timeout=self._idle_timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            slot.source_factory_task = None
            self._retire_source_factory(task)
            raise
        finally:
            abort_waiter.cancel()
        if slot.abort_event.is_set():
            slot.source_factory_task = None
            self._retire_source_factory(task)
            raise TransferDisconnectedError("device connection was replaced")
        if not done:
            slot.source_factory_task = None
            self._retire_source_factory(task)
            raise TimeoutError
        slot.source_factory_task = None
        try:
            source = task.result()
        except asyncio.CancelledError:
            if slot.state is TransferState.ABORTED:
                raise TransferDisconnectedError("device connection was replaced") from None
            raise
        if slot.state is not TransferState.BEGUN:
            try:
                await source.aclose()
            except Exception:
                pass
            raise TransferDisconnectedError("device connection was replaced")
        return source

    def _retire_source_factory(self, task: asyncio.Task[TransferSource]) -> None:
        task.cancel()

        def close_result(done: asyncio.Task[TransferSource]) -> None:
            try:
                source = done.result()
            except BaseException:
                return
            cleanup = asyncio.create_task(self._close_source(source))
            self._source_cleanup_tasks.add(cleanup)
            cleanup.add_done_callback(self._source_cleanup_tasks.discard)

        task.add_done_callback(close_result)

    @staticmethod
    async def _close_source(source: TransferSource) -> None:
        try:
            await source.aclose()
        except Exception:
            pass

    def _remember_tombstone_locked(
        self,
        key: tuple[UUID, int, UUID],
        value: tuple[float, TransferEndFrame | None, bool],
    ) -> None:
        self._tombstones.pop(key, None)
        self._acknowledged_failure_tombstones.discard(key)
        self._tombstones[key] = value
        while len(self._tombstones) > TOMBSTONE_MAX_ENTRIES:
            evicted = next(iter(self._tombstones))
            self._tombstones.pop(evicted)
            self._acknowledged_failure_tombstones.discard(evicted)

    async def _send_text(
        self,
        handle: object,
        payload: str,
        *,
        route: TransferRoute | None,
    ) -> bool:
        if route is None:
            result = await self._transport.send_text(handle, payload)
        else:
            result = await self._transport.send_text(
                handle,
                payload,
                expected_device_name=route.device_name,
                expected_config_epoch=route.config_epoch,
            )
        return result is not False

    async def _send_binary(self, handle: object, slot_id: UUID, payload: bytes) -> bool:
        if len(payload) > MAX_BINARY_CHUNK_BYTES:
            raise TransferProtocolError("transfer chunk exceeds 64 KiB")
        slot = await self._get_slot(handle, slot_id)
        if slot.route is None:
            result = await self._transport.send_binary(handle, slot_id.bytes + payload)
        else:
            result = await self._transport.send_binary(
                handle,
                slot_id.bytes + payload,
                expected_device_name=slot.route.device_name,
                expected_config_epoch=slot.route.config_epoch,
            )
        return result is not False

    def _decode_binary(self, payload: bytes) -> tuple[UUID, bytes]:
        try:
            return decode_binary_chunk(payload)
        except ValueError as exc:
            raise TransferProtocolError(str(exc), code="protocol_malformed_frame") from exc

    def _expire_tombstones_locked(self) -> None:
        now = time.monotonic()
        for key, (expires_at, _, _) in tuple(self._tombstones.items()):
            if expires_at <= now:
                self._tombstones.pop(key, None)
                self._acknowledged_failure_tombstones.discard(key)

    async def _matches_tombstone(self, handle: object, frame: TransferEndFrame) -> bool:
        device_id, generation = _handle_identity(handle)
        key = (device_id, generation, frame.id)
        async with self._lock:
            self._expire_tombstones_locked()
            tombstone = self._tombstones.get(key)
            if tombstone is None:
                return False
            _, expected, _ = tombstone
            if expected == frame:
                self._acknowledged_failure_tombstones.add(key)
                return True
        raise TransferProtocolError(
            "late transfer terminal frame conflicts with a closed slot",
            code="protocol_transfer_unknown_id",
        )

    async def _matches_committed_sender_timeout(
        self,
        handle: object,
        frame: TransferEndFrame,
    ) -> tuple[bool, TransferRoute | None]:
        expected = TransferEndFrame(
            id=frame.id,
            ack=False,
            ok=False,
            code=TRANSFER_TIMEOUT_CODE,
        )
        if frame != expected:
            return False, None
        device_id, generation = _handle_identity(handle)
        key = (device_id, generation, frame.id)
        async with self._lock:
            self._expire_tombstones_locked()
            slot = self._slots.get(key)
            if (
                slot is not None
                and slot.direction == "client_to_server"
                and slot.committed_result is not None
            ):
                return True, slot.route
            tombstone = self._tombstones.get(key)
            return tombstone is not None and tombstone[2], None

    async def _is_failed_tombstone(self, handle: object, slot_id: UUID) -> bool:
        device_id, generation = _handle_identity(handle)
        key = (device_id, generation, slot_id)
        async with self._lock:
            self._expire_tombstones_locked()
            tombstone = self._tombstones.get(key)
            if tombstone is None:
                return False
            _, expected_ack, _ = tombstone
            return (
                key not in self._acknowledged_failure_tombstones
                and expected_ack is not None
                and expected_ack.ack
                and not expected_ack.ok
            )


class TransferError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _handle_identity(handle: object) -> tuple[UUID, int]:
    device_id = getattr(handle, "device_id", None)
    generation = getattr(handle, "generation", None)
    if not isinstance(device_id, UUID) or not isinstance(generation, int):
        raise TypeError("transfer handle must expose device_id and generation")
    return device_id, generation


def _same_handle(left: object, right: object) -> bool:
    try:
        return _handle_identity(left) == _handle_identity(right)
    except TypeError:
        return left == right


def _error_code(exc: BaseException) -> str:
    code = getattr(exc, "code", None)
    if isinstance(code, str) and code:
        return code
    if isinstance(exc, TimeoutError):
        return TRANSFER_TIMEOUT_CODE
    if isinstance(exc, asyncio.CancelledError):
        return "cancelled"
    return "transfer_failed"


def _source_etag(source: TransferSource) -> str | None:
    value = getattr(source, "etag", None)
    return value if isinstance(value, str) and value else None
