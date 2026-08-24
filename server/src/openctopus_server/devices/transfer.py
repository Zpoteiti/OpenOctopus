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
from typing import Any, Literal, Protocol
from uuid import UUID

from openctopus_server.async_utils import await_future_cancellation_safe
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
BRIDGE_SOURCE_DELETE_TIMEOUT_SECONDS = 30.0
DEFAULT_TOMBSTONE_TTL_SECONDS = 60.0
TOMBSTONE_MAX_ENTRIES = 4096
LATE_PROGRESS_MAX = 64


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


class TransferUnavailableError(RuntimeError):
    """The initial route fence rejected a transfer before transport issue."""

    code = "tool_device_unreachable"


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


class BridgeState(StrEnum):
    ADMITTED = "admitted"
    SOURCE_REQUESTED = "source_requested"
    SOURCE_BEGUN = "source_begun"
    DESTINATION_BEGUN = "destination_begun"
    READY = "ready"
    STREAMING = "streaming"
    SOURCE_ENDED = "source_ended"
    DESTINATION_COMMITTED = "destination_committed"
    DESTINATION_FAILED = "destination_failed"
    COMPLETED = "completed"
    ABORTING = "aborting"
    ABORTED = "aborted"
    OUTCOME_UNKNOWN = "outcome_unknown"


class BridgeRole(StrEnum):
    SOURCE = "source"
    DESTINATION = "destination"


class SourceResolution(StrEnum):
    OPEN = "open"
    DESTINATION_ACK = "destination_ack"
    TIMEOUT_ACK = "timeout_ack"


class TransferTransport(Protocol):
    async def send_text(
        self,
        handle: Any,
        payload: str,
        *,
        expected_device_name: str | None = None,
        expected_config_epoch: int | None = None,
        on_issued: Callable[[], None] | None = None,
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
DeleteBridgeSource = Callable[[str], Awaitable[None]]
SinkFactory = Callable[[TransferBeginFrame], Awaitable[TransferSink]]


@dataclass(frozen=True, slots=True)
class TransferCommitResult:
    """Coordinator-only metadata returned by a directory child commit."""

    etag: str
    created: Literal[True] = True
    cancel_after_commit: bool = False

    def __post_init__(self) -> None:
        if (
            not 1 <= len(self.etag) <= 512
            or any(
                character in {'"', "\x00"}
                or not 0x21 <= ord(character) <= 0x7E
                for character in self.etag
            )
        ):
            raise ValueError("transfer commit etag is invalid")


CommitSink = Callable[
    [TransferSink, TransferBeginFrame, int, str],
    Awaitable[bool | TransferCommitResult | None],
]
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
    _owner: object
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
        return TransferLease(user_id, lambda: self._release(user_id), self)

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
    lease: TransferLease | None
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
    on_issued: Callable[[], None] | None = None
    directory_child: bool = False


@dataclass(slots=True)
class _BridgeSlot:
    source_route: TransferRoute
    destination_route: TransferRoute
    user_id: UUID
    slot_id: UUID
    src_path: str
    dst_path: str
    mode: Literal["copy", "move"]
    lease: TransferLease | None
    delete_source: DeleteBridgeSource | None
    on_issued: Callable[[], None] | None
    directory_child: bool = False
    state: BridgeState = BridgeState.ADMITTED
    queue: asyncio.Queue[bytes | None] = field(
        default_factory=lambda: asyncio.Queue(maxsize=TRANSFER_QUEUE_CHUNKS)
    )
    completion: asyncio.Future[TransferResult] | None = None
    source_begin_future: asyncio.Future[TransferBeginFrame | TransferEndFrame] | None = None
    destination_ready_future: asyncio.Future[TransferReadyFrame | TransferEndFrame] | None = None
    source_end_future: asyncio.Future[TransferEndFrame] | None = None
    source_drain_failure_future: asyncio.Future[TransferEndFrame] | None = None
    destination_ack_future: asyncio.Future[TransferEndFrame] | None = None
    destination_failure_future: asyncio.Future[TransferEndFrame] | None = None
    source_ack_future: asyncio.Future[TransferEndFrame] | None = None
    worker: asyncio.Task[None] | None = None
    relay_task: asyncio.Task[None] | None = None
    finish_task: asyncio.Task[None] | None = None
    abort_event: asyncio.Event = field(default_factory=asyncio.Event)
    activity_event: asyncio.Event = field(default_factory=asyncio.Event)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    source_begin: TransferBeginFrame | None = None
    source_end: TransferEndFrame | None = None
    source_drain_failure: TransferEndFrame | None = None
    destination_ack: TransferEndFrame | None = None
    destination_failure: TransferEndFrame | None = None
    authoritative_failure: TransferEndFrame | None = None
    source_fingerprint: str | None = None
    bytes_received: int = 0
    bytes_forwarded: int = 0
    digest: Any = field(default_factory=hashlib.sha256)
    last_progress: int = 0
    source_issued: bool = False
    destination_issued: bool = False
    source_ready_issued: bool = False
    destination_terminal_issued: bool = False
    destination_committed: bool = False
    source_ack_delivered: bool = False
    source_ack_impossible: bool = False
    source_resolution: SourceResolution = SourceResolution.OPEN
    source_timeout_ack_attempted: bool = False
    source_timeout_ack_sent: bool = False
    source_timeout_ack_in_flight: bool = False
    source_timeout_ack_task: asyncio.Task[None] | None = None
    source_fenced: bool = False
    destination_fenced: bool = False
    cleanup_started: bool = False
    tombstone_credits: int = 0
    tombstones_published: bool = False
    source_failure_terminal: TransferEndFrame | None = None
    destination_failure_terminal: TransferEndFrame | None = None
    source_failure_issued: bool = False
    destination_failure_issued: bool = False
    source_failure_send_task: asyncio.Task[bool] | None = None
    destination_failure_send_task: asyncio.Task[bool] | None = None
    late_binary_bytes: int = 0
    late_binary_digest: Any | None = None
    late_source_success_terminal: TransferEndFrame | None = None
    late_progress_remaining: int = LATE_PROGRESS_MAX


@dataclass(slots=True)
class _BridgeTombstone:
    role: BridgeRole
    pinned: bool = True
    expires_at: float | None = None
    expected_terminals: tuple[TransferEndFrame, ...] = ()
    sender_success_terminal: TransferEndFrame | None = None
    source_resolution: SourceResolution | None = None
    source_timeout_ack_attempted: bool = False
    source_timeout_ack_sent: bool = False
    source_timeout_ack_in_flight: bool = False
    failed: bool = False
    failure_terminal: TransferEndFrame | None = None
    failure_issued: bool = False
    source_ready_issued: bool = False
    simultaneous_failure_ack_in_flight: bool = False
    simultaneous_failure_ack_sent: bool = False
    binary_bytes_seen: int = 0
    binary_digest: Any = field(default_factory=hashlib.sha256)
    progress_remaining: int = 0
    last_progress: int = 0
    declared_bytes: int | None = None
    bridge: _BridgeSlot | None = None
    accept_late_destination_ack: bool = False
    late_destination_ack: TransferEndFrame | None = None
    late_source_success_terminal: TransferEndFrame | None = None


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
        self._bridges: dict[UUID, _BridgeSlot] = {}
        self._bridge_endpoints: dict[
            tuple[UUID, int, UUID], tuple[_BridgeSlot, BridgeRole]
        ] = {}
        self._bridge_tombstones: dict[tuple[UUID, int, UUID], _BridgeTombstone] = {}
        self._reserved_tombstone_credits = 0
        self._tombstones: dict[
            tuple[UUID, int, UUID], tuple[float, TransferEndFrame | None, bool]
        ] = {}
        self._acknowledged_failure_tombstones: set[tuple[UUID, int, UUID]] = set()
        self._source_cleanup_tasks: set[asyncio.Task[None]] = set()
        self._bridge_worker_cleanup_tasks: set[asyncio.Task[None]] = set()
        self._lock = asyncio.Lock()

    @property
    def active_slots(self) -> int:
        return len(self._slots) + len(self._bridges)

    @property
    def slot_ids(self) -> tuple[UUID, ...]:
        return tuple(slot.slot_id for slot in self._slots.values()) + tuple(self._bridges)

    async def acquire_operation(self, user_id: UUID) -> TransferLease:
        """Acquire one transfer credit owned by a multi-file coordinator."""

        return await self._admission.acquire(user_id)

    def _validate_operation_lease(
        self,
        lease: TransferLease,
        *,
        user_id: UUID,
        slot_id: UUID,
    ) -> None:
        if lease._owner is not self._admission or lease.user_id != user_id or lease._closed:
            raise ValueError("directory operation lease is not active for this user")
        if slot_id.version != 7:
            raise ValueError("directory child slot id must be UUIDv7")

    def fence_handle(self, handle: object) -> None:
        """Synchronously prevent a retired generation from making more progress."""

        for slot in tuple(self._slots.values()):
            if _same_handle(slot.handle, handle):
                self._fence_slot(slot)
        for bridge in tuple(self._bridges.values()):
            if _same_handle(bridge.source_route.handle, handle):
                self._fence_bridge(bridge, BridgeRole.SOURCE)
            if _same_handle(bridge.destination_route.handle, handle):
                self._fence_bridge(bridge, BridgeRole.DESTINATION)

    def fence_route(self, route: TransferRoute) -> None:
        """Fence only slots that have not crossed their initial send boundary."""

        for slot in tuple(self._slots.values()):
            if slot.route == route:
                self._fence_slot(slot)
        for bridge in tuple(self._bridges.values()):
            if bridge.source_route == route and not bridge.source_issued:
                self._fence_bridge(bridge, BridgeRole.SOURCE)
            if bridge.destination_route == route and not bridge.destination_issued:
                self._fence_bridge(bridge, BridgeRole.DESTINATION)

    @staticmethod
    def _fence_slot(slot: _TransferSlot) -> None:
        if slot.fenced:
            return
        slot.fenced = True
        slot.abort_event.set()
        if slot.worker is not None and not slot.worker.done():
            slot.worker.cancel()

    @staticmethod
    def _fence_bridge(bridge: _BridgeSlot, role: BridgeRole) -> None:
        if role is BridgeRole.SOURCE:
            bridge.source_fenced = True
        else:
            bridge.destination_fenced = True
        bridge.abort_event.set()
        if (
            not bridge.destination_terminal_issued
            and bridge.worker is not None
            and not bridge.worker.done()
        ):
            bridge.worker.cancel()

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
        on_issued: Callable[[], None] | None = None,
        _slot_id: UUID | None = None,
        _operation_lease: TransferLease | None = None,
        _directory_child: bool = False,
    ) -> TransferResult:
        if mode not in {"copy", "move"}:
            raise ValueError("transfer mode must be copy or move")
        if (source is None) == (source_factory is None):
            raise ValueError("exactly one transfer source or source factory is required")
        if purpose != "workspace_upload" and (if_match is not None or if_none_match is not None):
            raise ValueError("transfer preconditions are only valid for workspace_upload")
        if if_none_match is False:
            if_none_match = None
        if _operation_lease is None:
            lease = await self._admission.acquire(user_id)
        else:
            if _slot_id is None:
                raise ValueError("already-admitted transfer requires a slot id")
            self._validate_operation_lease(
                _operation_lease,
                user_id=user_id,
                slot_id=_slot_id,
            )
            lease = None
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
            on_issued=on_issued,
            slot_id=_slot_id,
            directory_child=_directory_child,
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

    async def start_server_to_client_admitted(
        self,
        *,
        handle: object,
        operation_lease: TransferLease,
        slot_id: UUID,
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
        src_device: str = "server",
        dst_device: str | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> TransferResult:
        """Run one copy child while the caller retains the operation lease."""

        return await self.start_server_to_client(
            handle=handle,
            route=route,
            user_id=user_id,
            src_path=src_path,
            dst_path=dst_path,
            source=source,
            source_factory=source_factory,
            total_bytes=total_bytes,
            sha256=sha256,
            mime=mime,
            purpose=purpose,
            if_match=if_match,
            if_none_match=if_none_match,
            mode="copy",
            src_device=src_device,
            dst_device=dst_device,
            on_issued=on_issued,
            _slot_id=slot_id,
            _operation_lease=operation_lease,
            _directory_child=True,
        )

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
        on_issued: Callable[[], None] | None = None,
        _slot_id: UUID | None = None,
        _operation_lease: TransferLease | None = None,
        _directory_child: bool = False,
    ) -> TransferResult:
        if mode not in {"copy", "move"}:
            raise ValueError("transfer mode must be copy or move")
        if _operation_lease is None:
            lease = await self._admission.acquire(user_id)
        else:
            if _slot_id is None:
                raise ValueError("already-admitted transfer requires a slot id")
            self._validate_operation_lease(
                _operation_lease,
                user_id=user_id,
                slot_id=_slot_id,
            )
            lease = None
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
            on_issued=on_issued,
            slot_id=_slot_id,
            directory_child=_directory_child,
        )
        slot.completion = asyncio.get_running_loop().create_future()
        try:
            request = TransferRequestFrame(
                id=slot.slot_id,
                purpose=purpose,
                src_path=src_path,
                dst_path=dst_path,
            )
            if not await self._send_text(
                handle,
                request.model_dump_json(),
                route=slot.route,
                on_issued=self._initial_issue_callback(slot),
            ):
                raise TransferUnavailableError("device route was unavailable before send")
            slot.route = None
            return await asyncio.shield(slot.completion)
        except asyncio.CancelledError:
            await self._abort(slot, "cancelled", send_frame=True)
            raise
        except BaseException as exc:
            await self._abort(slot, _error_code(exc), send_frame=True)
            raise

    async def start_client_to_server_admitted(
        self,
        *,
        handle: object,
        operation_lease: TransferLease,
        slot_id: UUID,
        route: TransferRoute | None = None,
        user_id: UUID,
        src_path: str,
        dst_path: str | None,
        sink_factory: SinkFactory,
        commit_sink: CommitSink | None = None,
        purpose: TransferPurpose = "file_transfer",
        on_issued: Callable[[], None] | None = None,
    ) -> TransferResult:
        """Run one copy child while the caller retains the operation lease."""

        return await self.start_client_to_server(
            handle=handle,
            route=route,
            user_id=user_id,
            src_path=src_path,
            dst_path=dst_path,
            sink_factory=sink_factory,
            commit_sink=commit_sink,
            purpose=purpose,
            mode="copy",
            on_issued=on_issued,
            _slot_id=slot_id,
            _operation_lease=operation_lease,
            _directory_child=True,
        )

    async def start_client_to_client(
        self,
        *,
        source_route: TransferRoute,
        destination_route: TransferRoute,
        user_id: UUID,
        src_path: str,
        dst_path: str,
        mode: Literal["copy", "move"],
        delete_source: DeleteBridgeSource | None,
        on_issued: Callable[[], None] | None,
        _slot_id: UUID | None = None,
        _operation_lease: TransferLease | None = None,
        _directory_child: bool = False,
    ) -> TransferResult:
        """Relay one file directly between two current device generations."""

        if mode not in {"copy", "move"}:
            raise ValueError("transfer mode must be copy or move")
        source_identity = _handle_identity(source_route.handle)
        destination_identity = _handle_identity(destination_route.handle)
        if source_identity[0] == destination_identity[0]:
            raise ValueError("client bridge requires two distinct devices")

        if _operation_lease is None:
            lease = await self._admission.acquire(user_id)
        else:
            if _slot_id is None:
                raise ValueError("already-admitted transfer requires a slot id")
            self._validate_operation_lease(
                _operation_lease,
                user_id=user_id,
                slot_id=_slot_id,
            )
            lease = None
        bridge: _BridgeSlot | None = None
        try:
            if not await self._bridge_routes_current(
                source_route,
                destination_route,
                user_id=user_id,
            ):
                raise TransferUnavailableError("device route was unavailable before send")
            bridge = await self._new_bridge_slot(
                source_route=source_route,
                destination_route=destination_route,
                user_id=user_id,
                src_path=src_path,
                dst_path=dst_path,
                mode=mode,
                lease=lease,
                delete_source=delete_source,
                on_issued=on_issued,
                slot_id=_slot_id,
                directory_child=_directory_child,
            )
        except BaseException:
            if bridge is None and lease is not None:
                await lease.aclose()
            raise

        loop = asyncio.get_running_loop()
        bridge.completion = loop.create_future()
        bridge.source_begin_future = loop.create_future()
        bridge.destination_ready_future = loop.create_future()
        bridge.source_end_future = loop.create_future()
        bridge.source_drain_failure_future = loop.create_future()
        bridge.destination_ack_future = loop.create_future()
        bridge.destination_failure_future = loop.create_future()
        bridge.source_ack_future = loop.create_future()
        bridge.worker = asyncio.create_task(self._run_bridge(bridge))
        try:
            return await asyncio.shield(bridge.completion)
        except asyncio.CancelledError:
            if bridge.destination_terminal_issued and bridge.worker is not None:
                await await_future_cancellation_safe(bridge.worker)
                raise
            await self._abort_bridge(
                bridge,
                "cancelled",
                error=TransferError("cancelled"),
            )
            raise

    async def start_client_to_client_admitted(
        self,
        *,
        source_route: TransferRoute,
        destination_route: TransferRoute,
        operation_lease: TransferLease,
        slot_id: UUID,
        user_id: UUID,
        src_path: str,
        dst_path: str,
        on_issued: Callable[[], None] | None,
    ) -> TransferResult:
        """Relay one copy child while the caller retains the operation lease."""

        return await self.start_client_to_client(
            source_route=source_route,
            destination_route=destination_route,
            user_id=user_id,
            src_path=src_path,
            dst_path=dst_path,
            mode="copy",
            delete_source=None,
            on_issued=on_issued,
            _slot_id=slot_id,
            _operation_lease=operation_lease,
            _directory_child=True,
        )

    async def handle_frame(self, handle: object, frame: object) -> None:
        """Route one already-validated client transfer control frame."""
        frame_id = getattr(frame, "id", None)
        if isinstance(frame_id, UUID):
            if await self._handle_bridge_tombstone_frame(handle, frame):
                return
            endpoint = await self._find_bridge_endpoint(handle, frame_id)
            if await self._handle_bridge_tombstone_frame(handle, frame):
                return
            if endpoint is not None:
                bridge, role = endpoint
                await self._handle_bridge_frame(bridge, role, frame)
                return
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
        if await self._handle_bridge_tombstone_binary(handle, slot_id, chunk):
            return
        endpoint = await self._find_bridge_endpoint(handle, slot_id)
        if await self._handle_bridge_tombstone_binary(handle, slot_id, chunk):
            return
        if endpoint is not None:
            bridge, role = endpoint
            await self._handle_bridge_binary(bridge, role, chunk)
            return
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
        bridges = [
            bridge
            for bridge in self._bridges.values()
            if _same_handle(bridge.source_route.handle, handle)
            or _same_handle(bridge.destination_route.handle, handle)
        ]
        await asyncio.gather(
            *(
                self._abort(
                    slot,
                    "peer_disconnected",
                    send_frame=False,
                    error=TransferDisconnectedError("device transfer outcome is unknown"),
                )
                for slot in slots
            ),
            *(
                self._disconnect_bridge(
                    bridge,
                    "peer_disconnected",
                )
                for bridge in bridges
            ),
            return_exceptions=True,
        )
        if self._bridge_worker_cleanup_tasks:
            await asyncio.gather(
                *tuple(self._bridge_worker_cleanup_tasks),
                return_exceptions=True,
            )

    async def close(self) -> None:
        slots = list(self._slots.values())
        bridges = list(self._bridges.values())
        await asyncio.gather(
            *(
                self._abort(
                    slot,
                    "server_shutdown",
                    send_frame=False,
                    error=TransferDisconnectedError("device transfer outcome is unknown"),
                )
                for slot in slots
            ),
            *(
                self._disconnect_bridge(
                    bridge,
                    "server_shutdown",
                )
                for bridge in bridges
            ),
            return_exceptions=True,
        )
        if self._bridge_worker_cleanup_tasks:
            await asyncio.gather(
                *tuple(self._bridge_worker_cleanup_tasks),
                return_exceptions=True,
            )

    async def _disconnect_bridge(self, bridge: _BridgeSlot, code: str) -> None:
        if bridge.destination_terminal_issued and bridge.worker is not None:
            await await_future_cancellation_safe(bridge.worker)
            return
        error: TransferUnavailableError | TransferDisconnectedError
        if bridge.source_issued:
            error = TransferDisconnectedError("device transfer outcome is unknown")
        else:
            error = TransferUnavailableError("device route was unavailable before send")
        await self._abort_bridge(
            bridge,
            code if bridge.source_issued else error.code,
            error=error,
        )

    async def _new_bridge_slot(
        self,
        *,
        source_route: TransferRoute,
        destination_route: TransferRoute,
        user_id: UUID,
        src_path: str,
        dst_path: str,
        mode: Literal["copy", "move"],
        lease: TransferLease | None,
        delete_source: DeleteBridgeSource | None,
        on_issued: Callable[[], None] | None,
        slot_id: UUID | None = None,
        directory_child: bool = False,
    ) -> _BridgeSlot:
        slot_id = slot_id or new_uuid7()
        bridge = _BridgeSlot(
            source_route=source_route,
            destination_route=destination_route,
            user_id=user_id,
            slot_id=slot_id,
            src_path=src_path,
            dst_path=dst_path,
            mode=mode,
            lease=lease,
            delete_source=delete_source,
            on_issued=on_issued,
            directory_child=directory_child,
        )
        source_device, source_generation = _handle_identity(source_route.handle)
        destination_device, destination_generation = _handle_identity(destination_route.handle)
        source_key = (source_device, source_generation, slot_id)
        destination_key = (destination_device, destination_generation, slot_id)
        async with self._lock:
            self._expire_tombstones_locked()
            if (
                slot_id in self._bridges
                or self._key_in_use_locked(source_key)
                or self._key_in_use_locked(destination_key)
            ):
                raise TransferProtocolError("transfer slot id collided with an active slot")
            self._reserve_tombstone_credits_locked(2)
            bridge.tombstone_credits = 2
            self._bridges[slot_id] = bridge
            self._bridge_endpoints[source_key] = (bridge, BridgeRole.SOURCE)
            self._bridge_endpoints[destination_key] = (bridge, BridgeRole.DESTINATION)
        return bridge

    def _key_in_use_locked(self, key: tuple[UUID, int, UUID]) -> bool:
        return (
            key in self._slots
            or key in self._bridge_endpoints
            or key in self._tombstones
            or key in self._bridge_tombstones
        )

    def _reserve_tombstone_credits_locked(self, count: int) -> None:
        while self._tombstone_occupancy_locked() + count > TOMBSTONE_MAX_ENTRIES:
            if not self._evict_one_final_tombstone_locked():
                raise TransferBusyError("transfer tombstone capacity is exhausted")
        self._reserved_tombstone_credits += count

    def _tombstone_occupancy_locked(self) -> int:
        return (
            len(self._tombstones)
            + len(self._bridge_tombstones)
            + self._reserved_tombstone_credits
        )

    def _evict_one_final_tombstone_locked(self) -> bool:
        if self._tombstones:
            key = next(iter(self._tombstones))
            self._tombstones.pop(key, None)
            self._acknowledged_failure_tombstones.discard(key)
            return True
        for key, tombstone in tuple(self._bridge_tombstones.items()):
            if not tombstone.pinned and not tombstone.source_timeout_ack_in_flight:
                self._bridge_tombstones.pop(key, None)
                return True
        return False

    async def _bridge_routes_current(
        self,
        source_route: TransferRoute,
        destination_route: TransferRoute,
        *,
        user_id: UUID,
    ) -> bool:
        validator = getattr(self._transport, "bridge_routes_current", None)
        if validator is None:
            raise TypeError("transfer transport does not support bridge route validation")
        return bool(await validator(source_route, destination_route, user_id=user_id))

    async def _find_bridge_endpoint(
        self,
        handle: object,
        slot_id: UUID,
    ) -> tuple[_BridgeSlot, BridgeRole] | None:
        device_id, generation = _handle_identity(handle)
        async with self._lock:
            return self._bridge_endpoints.get((device_id, generation, slot_id))

    async def _handle_bridge_tombstone_frame(self, handle: object, frame: object) -> bool:
        frame_id = getattr(frame, "id", None)
        if not isinstance(frame_id, UUID):
            return False
        device_id, generation = _handle_identity(handle)
        key = (device_id, generation, frame_id)
        timeout_ack: TransferEndFrame | None = None
        simultaneous_failure_ack: TransferEndFrame | None = None
        async with self._lock:
            self._expire_tombstones_locked()
            tombstone = self._bridge_tombstones.get(key)
            if tombstone is None:
                return False
            if isinstance(frame, TransferProgressFrame):
                if (
                    tombstone.role is BridgeRole.SOURCE
                    and tombstone.failed
                    and (
                        tombstone.source_ready_issued
                        or (
                            tombstone.bridge is not None
                            and tombstone.bridge.source_ready_issued
                        )
                    )
                    and tombstone.progress_remaining > 0
                    and tombstone.declared_bytes is not None
                    and frame.bytes_sent >= tombstone.last_progress
                    and frame.bytes_sent <= tombstone.declared_bytes
                ):
                    tombstone.last_progress = frame.bytes_sent
                    tombstone.progress_remaining -= 1
                    return True
                raise TransferProtocolError(
                    "late transfer progress conflicts with a closed bridge",
                    code="protocol_transfer_unknown_id",
                )
            if not isinstance(frame, TransferEndFrame):
                raise TransferProtocolError(
                    "late frame conflicts with a closed bridge",
                    code="protocol_transfer_unknown_id",
                )
            if frame in tombstone.expected_terminals:
                active_bridge = tombstone.bridge
                if frame.ack and tombstone.failure_terminal is not None:
                    expected_failure_ack = tombstone.failure_terminal.model_copy(
                        update={"ack": True}
                    )
                    if frame == expected_failure_ack:
                        failure_issued = tombstone.failure_issued
                        if active_bridge is not None:
                            failure_issued = failure_issued or (
                                active_bridge.source_failure_issued
                                if tombstone.role is BridgeRole.SOURCE
                                else active_bridge.destination_failure_issued
                            )
                        if not failure_issued:
                            raise TransferProtocolError(
                                "failure acknowledgement arrived before terminal issue",
                                code="protocol_transfer_unknown_id",
                            )
                if active_bridge is not None and frame.ack:
                    if tombstone.role is BridgeRole.SOURCE:
                        future = active_bridge.source_ack_future
                    else:
                        future = active_bridge.destination_ack_future
                    if future is not None and not future.done():
                        future.set_result(frame)
                        active_bridge.activity_event.set()
                return True
            if (
                tombstone.role is BridgeRole.SOURCE
                and _is_sender_timeout(frame)
                and tombstone.source_resolution is SourceResolution.DESTINATION_ACK
            ):
                return True
            if (
                tombstone.role is BridgeRole.SOURCE
                and _is_sender_timeout(frame)
                and tombstone.source_resolution is SourceResolution.TIMEOUT_ACK
            ):
                if (
                    tombstone.source_timeout_ack_attempted
                    or tombstone.source_timeout_ack_sent
                    or tombstone.source_timeout_ack_in_flight
                ):
                    return True
                tombstone.source_timeout_ack_attempted = True
                tombstone.source_timeout_ack_in_flight = True
                timeout_ack = frame.model_copy(update={"ack": True})
            elif (
                tombstone.failed
                and tombstone.failure_terminal is not None
                and frame == tombstone.failure_terminal
            ):
                if (
                    tombstone.simultaneous_failure_ack_in_flight
                    or tombstone.simultaneous_failure_ack_sent
                ):
                    return True
                tombstone.simultaneous_failure_ack_in_flight = True
                simultaneous_failure_ack = frame.model_copy(update={"ack": True})
            elif (
                tombstone.role is BridgeRole.SOURCE
                and tombstone.failed
                and tombstone.source_ready_issued
                and not frame.ack
                and frame.ok
                and tombstone.declared_bytes is not None
                and tombstone.binary_bytes_seen == tombstone.declared_bytes
                and frame.bytes_sent == tombstone.declared_bytes
                and frame.sha256 == tombstone.binary_digest.hexdigest()
            ):
                if (
                    tombstone.late_source_success_terminal is not None
                    and tombstone.late_source_success_terminal != frame
                ):
                    raise TransferProtocolError(
                        "late transfer terminal conflicts with a closed bridge",
                        code="protocol_transfer_unknown_id",
                    )
                tombstone.late_source_success_terminal = frame
                return True
            elif (
                tombstone.role is BridgeRole.DESTINATION
                and tombstone.accept_late_destination_ack
                and _ack_resolves_success_terminal(tombstone.sender_success_terminal, frame)
            ):
                if (
                    tombstone.late_destination_ack is not None
                    and tombstone.late_destination_ack != frame
                ):
                    raise TransferProtocolError(
                        "late transfer terminal conflicts with a closed bridge",
                        code="protocol_transfer_unknown_id",
                    )
                tombstone.late_destination_ack = frame
                return True
            else:
                raise TransferProtocolError(
                    "late transfer terminal conflicts with a closed bridge",
                    code="protocol_transfer_unknown_id",
                )
        acknowledgement = timeout_ack or simultaneous_failure_ack
        assert acknowledgement is not None
        cancelled = False
        try:
            async with asyncio.timeout(self._idle_timeout_seconds):
                delivered = await self._send_text(
                    handle,
                    acknowledgement.model_dump_json(),
                    route=None,
                )
        except asyncio.CancelledError:
            cancelled = True
            delivered = False
        except Exception:
            delivered = False
        if timeout_ack is not None:
            cleanup = asyncio.create_task(
                self._finish_bridge_tombstone_timeout_ack(key, delivered=delivered)
            )
        else:
            cleanup = asyncio.create_task(
                self._finish_bridge_simultaneous_failure_ack(
                    key,
                    acknowledgement,
                    delivered=delivered,
                )
            )
        try:
            await await_future_cancellation_safe(cleanup)
        except asyncio.CancelledError:
            cancelled = True
        if cancelled:
            raise asyncio.CancelledError
        return True

    async def _finish_bridge_tombstone_timeout_ack(
        self,
        key: tuple[UUID, int, UUID],
        *,
        delivered: bool,
    ) -> None:
        async with self._lock:
            current = self._bridge_tombstones.get(key)
            if current is not None:
                if delivered:
                    current.source_timeout_ack_sent = True
                current.source_timeout_ack_in_flight = False

    async def _finish_bridge_simultaneous_failure_ack(
        self,
        key: tuple[UUID, int, UUID],
        acknowledgement: TransferEndFrame,
        *,
        delivered: bool,
    ) -> None:
        async with self._lock:
            current = self._bridge_tombstones.get(key)
            if current is None:
                return
            current.simultaneous_failure_ack_in_flight = False
            if not delivered:
                return
            current.simultaneous_failure_ack_sent = True
            bridge = current.bridge
            if bridge is None:
                return
            future = (
                bridge.source_ack_future
                if current.role is BridgeRole.SOURCE
                else bridge.destination_ack_future
            )
            if future is not None and not future.done():
                future.set_result(acknowledgement)
                bridge.activity_event.set()

    async def _handle_bridge_tombstone_binary(
        self,
        handle: object,
        slot_id: UUID,
        chunk: bytes,
    ) -> bool:
        device_id, generation = _handle_identity(handle)
        key = (device_id, generation, slot_id)
        async with self._lock:
            self._expire_tombstones_locked()
            tombstone = self._bridge_tombstones.get(key)
            if tombstone is None:
                return False
            if (
                tombstone.role is BridgeRole.SOURCE
                and tombstone.failed
                and (
                    tombstone.source_ready_issued
                    or (
                        tombstone.bridge is not None
                        and tombstone.bridge.source_ready_issued
                    )
                )
                and chunk
                and tombstone.declared_bytes is not None
                and tombstone.binary_bytes_seen + len(chunk) <= tombstone.declared_bytes
            ):
                tombstone.binary_bytes_seen += len(chunk)
                tombstone.binary_digest.update(chunk)
                return True
        raise TransferProtocolError(
            "late binary frame conflicts with a closed bridge",
            code="protocol_transfer_unknown_id",
        )

    async def _run_bridge(self, bridge: _BridgeSlot) -> None:
        assert bridge.source_begin_future is not None
        assert bridge.destination_ready_future is not None
        assert bridge.source_end_future is not None
        assert bridge.source_drain_failure_future is not None
        assert bridge.destination_ack_future is not None
        assert bridge.destination_failure_future is not None
        try:
            request = TransferRequestFrame(
                id=bridge.slot_id,
                purpose="file_transfer",
                src_path=bridge.src_path,
                dst_path=bridge.dst_path,
            )
            if not await self._send_text(
                bridge.source_route.handle,
                request.model_dump_json(),
                route=bridge.source_route,
                on_issued=self._bridge_issue_callback(bridge, BridgeRole.SOURCE),
            ):
                raise TransferUnavailableError("device route was unavailable before send")
            source_begin = await self._wait_bridge_stage(
                bridge,
                bridge.source_begin_future,
            )
            if isinstance(source_begin, TransferEndFrame):
                await self._resolve_bridge_source_failure(bridge, source_begin)
                return

            destination_begin = TransferBeginFrame(
                id=bridge.slot_id,
                direction="server_to_client",
                purpose="file_transfer",
                src_device=bridge.source_route.device_name,
                src_path=bridge.src_path,
                dst_device=bridge.destination_route.device_name,
                dst_path=bridge.dst_path,
                total_bytes=source_begin.total_bytes,
                sha256=source_begin.sha256,
                mime=source_begin.mime,
                etag=source_begin.etag,
            )
            if not await self._send_text(
                bridge.destination_route.handle,
                destination_begin.model_dump_json(),
                route=bridge.destination_route,
                on_issued=self._bridge_issue_callback(bridge, BridgeRole.DESTINATION),
            ):
                raise TransferDisconnectedError("device transfer outcome is unknown")
            destination_ready = await self._wait_bridge_stage(
                bridge,
                bridge.destination_ready_future,
                bridge.source_end_future,
                bridge.destination_failure_future,
            )
            if (
                isinstance(destination_ready, TransferEndFrame)
                and destination_ready is bridge.source_end
            ):
                await self._resolve_bridge_source_failure(bridge, destination_ready)
                return
            if isinstance(destination_ready, TransferEndFrame):
                await self._resolve_bridge_destination_rejection(bridge, destination_ready)
                return
            if bridge.destination_failure_future.done():
                destination_failure = bridge.destination_failure_future.result()
                await self._resolve_bridge_destination_rejection(
                    bridge,
                    destination_failure,
                )
                return
            bridge.relay_task = asyncio.create_task(self._relay_bridge(bridge))
            if not await self._send_text(
                bridge.source_route.handle,
                TransferReadyFrame(id=bridge.slot_id).model_dump_json(),
                route=None,
                on_issued=self._bridge_source_ready_callback(bridge),
            ):
                raise TransferDisconnectedError("source device connection was replaced")

            source_end = await self._wait_bridge_stage(
                bridge,
                bridge.source_end_future,
                bridge.destination_failure_future,
                bridge.relay_task,
            )
            if source_end is bridge.destination_failure:
                assert isinstance(source_end, TransferEndFrame)
                await self._resolve_bridge_destination_rejection(bridge, source_end)
                return
            assert isinstance(source_end, TransferEndFrame)
            if not source_end.ok:
                await self._resolve_bridge_source_failure(bridge, source_end)
                return
            assert bridge.relay_task is not None
            drain_result = await self._wait_bridge_stage(
                bridge,
                bridge.source_drain_failure_future,
                bridge.destination_failure_future,
                bridge.relay_task,
            )
            if isinstance(drain_result, TransferEndFrame):
                if drain_result is bridge.destination_failure:
                    await self._resolve_bridge_destination_rejection(
                        bridge,
                        drain_result,
                    )
                else:
                    await self._resolve_bridge_source_failure(bridge, drain_result)
                return
            if bridge.source_drain_failure_future.done():
                await self._resolve_bridge_source_failure(
                    bridge,
                    bridge.source_drain_failure_future.result(),
                )
                return
            if bridge.destination_failure_future.done():
                await self._resolve_bridge_destination_rejection(
                    bridge,
                    bridge.destination_failure_future.result(),
                )
                return
            bridge.state = BridgeState.SOURCE_ENDED
            if not await self._send_text(
                bridge.destination_route.handle,
                source_end.model_dump_json(),
                route=None,
                on_issued=self._bridge_destination_terminal_callback(bridge),
            ):
                raise TransferDisconnectedError("destination transfer outcome is unknown")

            async with asyncio.timeout(self._idle_timeout_seconds):
                destination_ack = await asyncio.shield(bridge.destination_ack_future)
            async with bridge.lock:
                if bridge.source_resolution is SourceResolution.OPEN:
                    bridge.source_resolution = SourceResolution.DESTINATION_ACK
                chosen = bridge.source_resolution is SourceResolution.DESTINATION_ACK
            if not chosen:
                await self._finish_bridge_after_timeout_resolution(bridge, destination_ack)
                return

            source_ack = destination_ack
            if bridge.directory_child and destination_ack.ok:
                source_ack = destination_ack.model_copy(
                    update={"etag": None, "created": None}
                )
            bridge.source_ack_delivered = await self._send_text(
                bridge.source_route.handle,
                source_ack.model_dump_json(),
                route=None,
            )
            if not destination_ack.ok:
                bridge.state = BridgeState.DESTINATION_FAILED
                await self._complete_bridge_error(
                    bridge,
                    TransferError(destination_ack.code or "transfer_rejected"),
                )
                return

            bridge.destination_committed = True
            bridge.state = BridgeState.DESTINATION_COMMITTED
            warnings: list[str] = []
            if not bridge.source_ack_delivered:
                warnings.append("transfer_ack_failed")
                if bridge.mode == "move":
                    warnings.append("source_delete_failed")
            elif bridge.mode == "move":
                if bridge.delete_source is None or bridge.source_fingerprint is None:
                    warnings.append("source_delete_failed")
                else:
                    try:
                        async with asyncio.timeout(BRIDGE_SOURCE_DELETE_TIMEOUT_SECONDS):
                            await bridge.delete_source(bridge.source_fingerprint)
                    except Exception:
                        warnings.append("source_delete_failed")
            result = TransferResult(
                bridge.bytes_received,
                bridge.digest.hexdigest(),
                tuple(warnings),
                etag=destination_ack.etag if bridge.directory_child else None,
                created=destination_ack.created if bridge.directory_child else None,
            )
            bridge.state = BridgeState.COMPLETED
            await self._complete_bridge_result(bridge, result)
        except asyncio.CancelledError:
            if not bridge.cleanup_started:
                error: TransferUnavailableError | TransferDisconnectedError
                if bridge.source_issued:
                    error = TransferDisconnectedError("device transfer outcome is unknown")
                else:
                    error = TransferUnavailableError(
                        "device route was unavailable before send"
                    )
                await self._abort_bridge(
                    bridge,
                    (
                        error.code
                        if bridge.abort_event.is_set()
                        else "cancelled"
                    ),
                    error=error if bridge.abort_event.is_set() else TransferError("cancelled"),
                    skip_worker=True,
                )
        except BaseException as exc:
            await self._abort_bridge(
                bridge,
                _error_code(exc),
                error=exc,
                skip_worker=True,
            )

    @staticmethod
    def _bridge_issue_callback(
        bridge: _BridgeSlot,
        role: BridgeRole,
    ) -> Callable[[], None]:
        def issued() -> None:
            if role is BridgeRole.SOURCE:
                if bridge.source_issued:
                    return
                bridge.source_issued = True
                bridge.state = BridgeState.SOURCE_REQUESTED
                if bridge.on_issued is not None:
                    bridge.on_issued()
            else:
                bridge.destination_issued = True
                bridge.state = BridgeState.DESTINATION_BEGUN

        return issued

    @staticmethod
    def _bridge_destination_terminal_callback(bridge: _BridgeSlot) -> Callable[[], None]:
        def issued() -> None:
            bridge.destination_terminal_issued = True

        return issued

    @staticmethod
    def _bridge_source_ready_callback(bridge: _BridgeSlot) -> Callable[[], None]:
        def issued() -> None:
            bridge.source_ready_issued = True
            if bridge.abort_event.is_set():
                return
            bridge.state = BridgeState.READY

        return issued

    @staticmethod
    def _bridge_failure_issue_callback(
        bridge: _BridgeSlot,
        role: BridgeRole,
    ) -> Callable[[], None]:
        def issued() -> None:
            if role is BridgeRole.SOURCE:
                bridge.source_failure_issued = True
            else:
                bridge.destination_failure_issued = True

        return issued

    async def _send_bridge_failure_once(
        self,
        bridge: _BridgeSlot,
        role: BridgeRole,
    ) -> bool:
        async with bridge.lock:
            if role is BridgeRole.SOURCE:
                terminal = bridge.source_failure_terminal
                issued = bridge.source_failure_issued
                task = bridge.source_failure_send_task
                handle = bridge.source_route.handle
            else:
                terminal = bridge.destination_failure_terminal
                issued = bridge.destination_failure_issued
                task = bridge.destination_failure_send_task
                handle = bridge.destination_route.handle
            if task is None:
                if issued:
                    return True
                assert terminal is not None
                task = asyncio.create_task(
                    self._send_text(
                        handle,
                        terminal.model_dump_json(),
                        route=None,
                        on_issued=self._bridge_failure_issue_callback(bridge, role),
                    )
                )
                if role is BridgeRole.SOURCE:
                    bridge.source_failure_send_task = task
                else:
                    bridge.destination_failure_send_task = task
        return await await_future_cancellation_safe(task)

    async def _relay_bridge(self, bridge: _BridgeSlot) -> None:
        while True:
            async with asyncio.timeout(self._idle_timeout_seconds):
                chunk = await bridge.queue.get()
            if chunk is None:
                return
            async with asyncio.timeout(self._idle_timeout_seconds):
                if not await self._send_bridge_binary(bridge, chunk):
                    raise TransferDisconnectedError("destination device connection was replaced")
            bridge.bytes_forwarded += len(chunk)
            bridge.activity_event.set()

    async def _wait_bridge_stage(
        self,
        bridge: _BridgeSlot,
        *futures: asyncio.Future[Any],
    ) -> Any:
        while True:
            for future in futures:
                if future.done():
                    return future.result()
            bridge.activity_event.clear()
            activity = asyncio.create_task(bridge.activity_event.wait())
            try:
                done, _ = await asyncio.wait(
                    (*futures, activity),
                    timeout=self._idle_timeout_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                if not activity.done():
                    activity.cancel()
                    await asyncio.gather(activity, return_exceptions=True)
            for future in futures:
                if future in done:
                    return future.result()
            if not done:
                raise TimeoutError

    async def _put_bridge_queue(
        self,
        bridge: _BridgeSlot,
        item: bytes | None,
    ) -> bool:
        if bridge.abort_event.is_set():
            return False
        put = asyncio.create_task(bridge.queue.put(item))
        aborted = asyncio.create_task(bridge.abort_event.wait())
        try:
            done, _ = await asyncio.wait(
                (put, aborted),
                timeout=self._idle_timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if aborted in done:
                if not put.done():
                    put.cancel()
                await asyncio.gather(put, return_exceptions=True)
                return False
            if put in done:
                await put
                return True
            raise TimeoutError
        finally:
            for task in (put, aborted):
                if not task.done():
                    task.cancel()
            await asyncio.gather(put, aborted, return_exceptions=True)

    async def _send_bridge_binary(self, bridge: _BridgeSlot, chunk: bytes) -> bool:
        if len(chunk) > MAX_BINARY_CHUNK_BYTES:
            raise TransferProtocolError("transfer chunk exceeds 64 KiB")
        try:
            result = await self._transport.send_binary(
                bridge.destination_route.handle,
                bridge.slot_id.bytes + chunk,
            )
        except TransferDisconnectedError:
            raise
        except Exception as exc:
            raise TransferDisconnectedError("destination transfer outcome is unknown") from exc
        return result is not False

    async def _handle_bridge_frame(
        self,
        bridge: _BridgeSlot,
        role: BridgeRole,
        frame: object,
    ) -> None:
        if role is BridgeRole.SOURCE and bridge.source_fenced:
            raise TransferDisconnectedError("source device route was replaced")
        if role is BridgeRole.DESTINATION and bridge.destination_fenced:
            raise TransferDisconnectedError("destination device route was replaced")
        if isinstance(frame, TransferBeginFrame):
            await self._handle_bridge_source_begin(bridge, role, frame)
        elif isinstance(frame, TransferReadyFrame):
            await self._handle_bridge_destination_ready(bridge, role, frame)
        elif isinstance(frame, TransferProgressFrame):
            await self._handle_bridge_progress(bridge, role, frame)
        elif isinstance(frame, TransferEndFrame):
            await self._handle_bridge_end(bridge, role, frame)
        else:
            raise TransferProtocolError("not a transfer frame")

    async def _handle_bridge_source_begin(
        self,
        bridge: _BridgeSlot,
        role: BridgeRole,
        frame: TransferBeginFrame,
    ) -> None:
        if role is not BridgeRole.SOURCE or bridge.state is not BridgeState.SOURCE_REQUESTED:
            raise TransferProtocolError("transfer_begin arrived in an invalid bridge state")
        if (
            frame.direction != "client_to_server"
            or frame.purpose != "file_transfer"
            or frame.src_path != bridge.src_path
            or frame.dst_path != bridge.dst_path
            or frame.total_bytes is None
            or (frame.src_device is not None and frame.src_device != bridge.source_route.device_name)
            or frame.dst_device not in {None, "server"}
        ):
            raise TransferProtocolError("source transfer_begin metadata mismatched bridge request")
        bridge.source_begin = frame
        bridge.source_fingerprint = frame.etag
        bridge.state = BridgeState.SOURCE_BEGUN
        bridge.activity_event.set()
        assert bridge.source_begin_future is not None
        if bridge.source_begin_future.done():
            raise TransferProtocolError("duplicate source transfer_begin")
        bridge.source_begin_future.set_result(frame)

    async def _handle_bridge_destination_ready(
        self,
        bridge: _BridgeSlot,
        role: BridgeRole,
        frame: TransferReadyFrame,
    ) -> None:
        if role is not BridgeRole.DESTINATION or bridge.state is not BridgeState.DESTINATION_BEGUN:
            raise TransferProtocolError("transfer_ready arrived in an invalid bridge state")
        assert bridge.destination_ready_future is not None
        if bridge.destination_ready_future.done():
            raise TransferProtocolError("duplicate destination transfer_ready")
        bridge.destination_ready_future.set_result(frame)
        bridge.activity_event.set()

    async def _handle_bridge_progress(
        self,
        bridge: _BridgeSlot,
        role: BridgeRole,
        frame: TransferProgressFrame,
    ) -> None:
        declared = bridge.source_begin.total_bytes if bridge.source_begin is not None else None
        if bridge.abort_event.is_set() or bridge.source_fenced or bridge.destination_fenced:
            if (
                role is BridgeRole.SOURCE
                and bridge.source_ready_issued
                and not bridge.destination_terminal_issued
                and bridge.late_progress_remaining > 0
                and declared is not None
                and frame.bytes_sent >= bridge.last_progress
                and frame.bytes_sent <= declared
            ):
                bridge.last_progress = frame.bytes_sent
                bridge.late_progress_remaining -= 1
                return
            raise TransferProtocolError(
                "late transfer progress conflicts with an aborting bridge",
                code="protocol_transfer_unknown_id",
            )
        if role is not BridgeRole.SOURCE or bridge.state not in {
            BridgeState.READY,
            BridgeState.STREAMING,
        }:
            raise TransferProtocolError("transfer_progress arrived in an invalid bridge state")
        if frame.bytes_sent < bridge.last_progress or (
            declared is not None and frame.bytes_sent > declared
        ):
            raise TransferProtocolError("transfer progress is invalid")
        bridge.last_progress = frame.bytes_sent
        if bridge.destination_issued:
            if not await self._send_text(
                bridge.destination_route.handle,
                frame.model_dump_json(),
                route=None,
            ):
                raise TransferDisconnectedError("destination device connection was replaced")

    async def _handle_bridge_end(
        self,
        bridge: _BridgeSlot,
        role: BridgeRole,
        frame: TransferEndFrame,
    ) -> None:
        if role is BridgeRole.SOURCE:
            await self._handle_bridge_source_end(bridge, frame)
            return
        await self._handle_bridge_destination_end(bridge, frame)

    async def _handle_bridge_source_end(
        self,
        bridge: _BridgeSlot,
        frame: TransferEndFrame,
    ) -> None:
        if frame.ack:
            expected = bridge.source_failure_terminal
            if expected is None or frame != expected.model_copy(update={"ack": True}):
                raise TransferProtocolError(
                    "source acknowledgement arrived in an invalid bridge state"
                )
            assert bridge.source_ack_future is not None
            if bridge.source_ack_future.done():
                raise TransferProtocolError("duplicate source acknowledgement")
            bridge.source_ack_future.set_result(frame)
            bridge.activity_event.set()
            return
        if (
            bridge.abort_event.is_set()
            and bridge.source_end is None
            and bridge.source_ready_issued
            and not bridge.destination_terminal_issued
            and frame.ok
        ):
            declared = (
                bridge.source_begin.total_bytes
                if bridge.source_begin is not None
                else None
            )
            bytes_seen = bridge.bytes_received + bridge.late_binary_bytes
            digest = bridge.late_binary_digest or bridge.digest
            if (
                declared is None
                or bytes_seen != declared
                or frame.bytes_sent != declared
                or frame.sha256 != digest.hexdigest()
                or (
                    bridge.late_source_success_terminal is not None
                    and bridge.late_source_success_terminal != frame
                )
            ):
                raise TransferProtocolError(
                    "late transfer terminal conflicts with an aborting bridge",
                    code="protocol_transfer_unknown_id",
                )
            bridge.late_source_success_terminal = frame
            return
        if bridge.destination_terminal_issued and _is_sender_timeout(frame):
            await self._resolve_bridge_source_timeout(bridge, frame)
            return
        if (
            _is_sender_timeout(frame)
            and bridge.source_end is not None
            and bridge.source_end.ok
            and bridge.state is BridgeState.SOURCE_ENDED
        ):
            assert bridge.source_drain_failure_future is not None
            if bridge.source_drain_failure_future.done():
                raise TransferProtocolError("duplicate source sender timeout")
            bridge.source_drain_failure = frame
            bridge.authoritative_failure = frame
            if bridge.destination_issued:
                bridge.destination_failure_terminal = frame
            bridge.abort_event.set()
            bridge.source_drain_failure_future.set_result(frame)
            bridge.activity_event.set()
            if bridge.relay_task is not None and not bridge.relay_task.done():
                bridge.relay_task.cancel()
            await self._publish_bridge_tombstones(bridge)
            return
        if frame.ok:
            if bridge.state not in {BridgeState.READY, BridgeState.STREAMING}:
                raise TransferProtocolError("source terminal arrived in an invalid bridge state")
            digest = bridge.digest.hexdigest()
            declared = bridge.source_begin.total_bytes if bridge.source_begin is not None else None
            if (
                frame.bytes_sent != bridge.bytes_received
                or frame.sha256 != digest
                or declared != bridge.bytes_received
            ):
                await self._abort_bridge(
                    bridge,
                    TransferIntegrityError.code,
                    error=TransferIntegrityError(
                        "source terminal did not match relayed bytes"
                    ),
                )
                raise TransferProtocolError(
                    "source terminal did not match relayed bytes",
                    code="protocol_transfer_length_mismatch",
                )
        elif bridge.state not in {
            BridgeState.SOURCE_REQUESTED,
            BridgeState.SOURCE_BEGUN,
            BridgeState.DESTINATION_BEGUN,
            BridgeState.READY,
            BridgeState.STREAMING,
        }:
            raise TransferProtocolError("source terminal arrived in an invalid bridge state")
        if bridge.source_end is not None:
            raise TransferProtocolError("duplicate source terminal")
        if not frame.ok:
            bridge.authoritative_failure = frame
            if bridge.destination_issued:
                bridge.destination_failure_terminal = frame
            bridge.abort_event.set()
        bridge.source_end = frame
        bridge.state = BridgeState.SOURCE_ENDED
        bridge.activity_event.set()
        assert bridge.source_end_future is not None
        bridge.source_end_future.set_result(frame)
        if not frame.ok:
            assert bridge.source_begin_future is not None
            if not bridge.source_begin_future.done():
                bridge.source_begin_future.set_result(frame)
            if bridge.relay_task is not None and not bridge.relay_task.done():
                bridge.relay_task.cancel()
            if (
                not bridge.destination_issued
                and bridge.worker is not None
                and bridge.worker is not asyncio.current_task()
                and not bridge.worker.done()
            ):
                bridge.worker.cancel()
            await self._publish_bridge_tombstones(bridge)
            return
        if frame.ok:
            try:
                await self._put_bridge_queue(bridge, None)
            except TimeoutError as exc:
                raise TransferProtocolError(
                    "bridge relay queue is stalled",
                    code=TRANSFER_TIMEOUT_CODE,
                ) from exc

    async def _handle_bridge_destination_end(
        self,
        bridge: _BridgeSlot,
        frame: TransferEndFrame,
    ) -> None:
        if not frame.ack:
            if frame.ok or bridge.state not in {
                BridgeState.DESTINATION_BEGUN,
                BridgeState.READY,
                BridgeState.STREAMING,
                BridgeState.SOURCE_ENDED,
            }:
                raise TransferProtocolError("destination terminal arrived in an invalid bridge state")
            bridge.destination_failure = frame
            bridge.authoritative_failure = frame
            bridge.source_failure_terminal = frame
            bridge.abort_event.set()
            assert bridge.destination_failure_future is not None
            if bridge.destination_failure_future.done():
                raise TransferProtocolError("duplicate destination terminal")
            bridge.destination_failure_future.set_result(frame)
            bridge.activity_event.set()
            if bridge.state is BridgeState.DESTINATION_BEGUN:
                assert bridge.destination_ready_future is not None
                if not bridge.destination_ready_future.done():
                    bridge.destination_ready_future.set_result(frame)
            if bridge.relay_task is not None and not bridge.relay_task.done():
                bridge.relay_task.cancel()
            await self._publish_bridge_tombstones(bridge)
            return
        if (
            bridge.destination_failure_terminal is not None
            and not bridge.destination_terminal_issued
        ):
            expected = bridge.destination_failure_terminal.model_copy(update={"ack": True})
            if frame != expected:
                raise TransferProtocolError("destination failure acknowledgement mismatched")
            assert bridge.destination_ack_future is not None
            if bridge.destination_ack_future.done():
                raise TransferProtocolError("duplicate destination acknowledgement")
            bridge.destination_ack = frame
            bridge.destination_ack_future.set_result(frame)
            bridge.activity_event.set()
            return
        if not bridge.destination_terminal_issued:
            raise TransferProtocolError("destination acknowledgement arrived before terminal issue")
        if frame.ok:
            digest = bridge.digest.hexdigest()
            metadata_mismatched = (
                frame.etag is None or frame.created is not True
                if bridge.directory_child
                else frame.etag is not None or frame.created is not None
            )
            if (
                frame.bytes_sent != bridge.bytes_received
                or frame.sha256 != digest
                or metadata_mismatched
            ):
                raise TransferProtocolError("destination acknowledgement mismatched bridge bytes")
        async with bridge.lock:
            if bridge.destination_ack is not None:
                raise TransferProtocolError("duplicate destination acknowledgement")
            bridge.destination_ack = frame
            if frame.ok:
                bridge.destination_committed = True
            if bridge.source_resolution is SourceResolution.OPEN:
                bridge.source_resolution = SourceResolution.DESTINATION_ACK
        assert bridge.destination_ack_future is not None
        if not bridge.destination_ack_future.done():
            bridge.destination_ack_future.set_result(frame)

    async def _handle_bridge_binary(
        self,
        bridge: _BridgeSlot,
        role: BridgeRole,
        chunk: bytes,
    ) -> None:
        if bridge.abort_event.is_set() or bridge.source_fenced or bridge.destination_fenced:
            declared = (
                bridge.source_begin.total_bytes
                if bridge.source_begin is not None
                else None
            )
            if (
                role is BridgeRole.SOURCE
                and bridge.source_ready_issued
                and chunk
                and declared is not None
                and bridge.bytes_received + bridge.late_binary_bytes + len(chunk)
                <= declared
            ):
                bridge.late_binary_bytes += len(chunk)
                if bridge.late_binary_digest is None:
                    bridge.late_binary_digest = bridge.digest.copy()
                bridge.late_binary_digest.update(chunk)
                return
            raise TransferProtocolError(
                "late binary frame conflicts with an aborting bridge",
                code="protocol_transfer_unknown_id",
            )
        if role is not BridgeRole.SOURCE or bridge.state not in {
            BridgeState.READY,
            BridgeState.STREAMING,
        }:
            raise TransferProtocolError("binary chunk arrived before destination ready")
        if bridge.source_end is not None:
            raise TransferProtocolError("binary chunk arrived after source terminal")
        if not chunk:
            return
        declared = bridge.source_begin.total_bytes if bridge.source_begin is not None else None
        if declared is None or bridge.bytes_received + len(chunk) > declared:
            await self._abort_bridge(
                bridge,
                TransferIntegrityError.code,
                error=TransferIntegrityError(
                    "source bytes exceeded the declared transfer size"
                ),
            )
            raise TransferProtocolError(
                "binary bytes exceed the declared transfer size",
                code="protocol_transfer_length_mismatch",
            )
        if bridge.state is BridgeState.READY:
            bridge.state = BridgeState.STREAMING
        bridge.bytes_received += len(chunk)
        bridge.digest.update(chunk)
        try:
            queued = await self._put_bridge_queue(bridge, chunk)
        except TimeoutError as exc:
            raise TransferProtocolError(
                "bridge receive queue is stalled",
                code=TRANSFER_TIMEOUT_CODE,
            ) from exc
        if not queued:
            return
        bridge.activity_event.set()

    async def _resolve_bridge_source_timeout(
        self,
        bridge: _BridgeSlot,
        frame: TransferEndFrame,
    ) -> None:
        send_ack = False
        ack_task: asyncio.Task[None] | None = None
        async with bridge.lock:
            if bridge.source_resolution is SourceResolution.OPEN:
                bridge.source_resolution = SourceResolution.TIMEOUT_ACK
                bridge.source_ack_impossible = True
                send_ack = not (
                    bridge.source_timeout_ack_attempted
                    or bridge.source_timeout_ack_sent
                    or bridge.source_timeout_ack_in_flight
                )
            elif bridge.source_resolution is SourceResolution.TIMEOUT_ACK:
                send_ack = not (
                    bridge.source_timeout_ack_attempted
                    or bridge.source_timeout_ack_sent
                    or bridge.source_timeout_ack_in_flight
                )
            else:
                return
            if send_ack:
                bridge.source_timeout_ack_attempted = True
                bridge.source_timeout_ack_in_flight = True
                ack_task = asyncio.create_task(
                    self._send_bridge_source_timeout_ack(bridge, frame)
                )
                bridge.source_timeout_ack_task = ack_task
        if ack_task is not None:
            await await_future_cancellation_safe(ack_task)

    async def _send_bridge_source_timeout_ack(
        self,
        bridge: _BridgeSlot,
        frame: TransferEndFrame,
    ) -> None:
        delivered = False
        try:
            async with asyncio.timeout(self._idle_timeout_seconds):
                delivered = await self._send_text(
                    bridge.source_route.handle,
                    frame.model_copy(update={"ack": True}).model_dump_json(),
                    route=None,
                )
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        async with bridge.lock:
            bridge.source_timeout_ack_in_flight = False
            if delivered:
                bridge.source_timeout_ack_sent = True
            else:
                bridge.source_ack_impossible = True
        source_device, source_generation = _handle_identity(bridge.source_route.handle)
        async with self._lock:
            tombstone = self._bridge_tombstones.get(
                (source_device, source_generation, bridge.slot_id)
            )
            if tombstone is not None:
                tombstone.source_timeout_ack_in_flight = False
                if delivered:
                    tombstone.source_timeout_ack_sent = True

    async def _resolve_bridge_destination_rejection(
        self,
        bridge: _BridgeSlot,
        failure: TransferEndFrame,
    ) -> None:
        bridge.source_failure_terminal = failure
        await self._publish_bridge_tombstones(bridge)
        try:
            if not await self._send_bridge_failure_once(bridge, BridgeRole.SOURCE):
                raise TransferDisconnectedError("source device connection was replaced")
            assert bridge.source_ack_future is not None
            source_ack = await self._wait_bridge_stage(bridge, bridge.source_ack_future)
            if not await self._send_text(
                bridge.destination_route.handle,
                source_ack.model_dump_json(),
                route=None,
            ):
                raise TransferDisconnectedError("destination device connection was replaced")
        finally:
            await self._complete_bridge_error(
                bridge,
                TransferError(failure.code or "transfer_rejected"),
            )

    async def _resolve_bridge_source_failure(
        self,
        bridge: _BridgeSlot,
        failure: TransferEndFrame,
    ) -> None:
        acknowledgement = failure.model_copy(update={"ack": True})
        if bridge.destination_issued:
            bridge.destination_failure_terminal = failure
        await self._publish_bridge_tombstones(bridge)
        try:
            if bridge.destination_issued:
                if not await self._send_bridge_failure_once(
                    bridge,
                    BridgeRole.DESTINATION,
                ):
                    raise TransferDisconnectedError("destination device connection was replaced")
                assert bridge.destination_ack_future is not None
                acknowledgement = await self._wait_bridge_stage(
                    bridge,
                    bridge.destination_ack_future,
                )
            if not await self._send_text(
                bridge.source_route.handle,
                acknowledgement.model_dump_json(),
                route=None,
            ):
                raise TransferDisconnectedError("source device connection was replaced")
        finally:
            await self._complete_bridge_error(
                bridge,
                TransferError(failure.code or "transfer_rejected"),
            )

    async def _finish_bridge_after_timeout_resolution(
        self,
        bridge: _BridgeSlot,
        destination_ack: TransferEndFrame,
    ) -> None:
        if not destination_ack.ok:
            await self._complete_bridge_error(
                bridge,
                TransferError(destination_ack.code or "transfer_rejected"),
            )
            return
        warnings = ["transfer_ack_failed"]
        if bridge.mode == "move":
            warnings.append("source_delete_failed")
        bridge.state = BridgeState.COMPLETED
        await self._complete_bridge_result(
            bridge,
            TransferResult(
                bridge.bytes_received,
                bridge.digest.hexdigest(),
                tuple(warnings),
                etag=destination_ack.etag if bridge.directory_child else None,
                created=destination_ack.created if bridge.directory_child else None,
            ),
        )

    async def _complete_bridge_result(
        self,
        bridge: _BridgeSlot,
        result: TransferResult,
    ) -> None:
        async with bridge.lock:
            if bridge.finish_task is None:
                bridge.finish_task = asyncio.create_task(
                    self._finish_bridge_once(bridge, result=result, error=None)
                )
            task = bridge.finish_task
        await await_future_cancellation_safe(task)

    async def _complete_bridge_error(
        self,
        bridge: _BridgeSlot,
        error: BaseException,
    ) -> None:
        async with bridge.lock:
            if bridge.finish_task is None:
                bridge.finish_task = asyncio.create_task(
                    self._finish_bridge_once(bridge, result=None, error=error)
                )
            task = bridge.finish_task
        await await_future_cancellation_safe(task)

    async def _abort_bridge(
        self,
        bridge: _BridgeSlot,
        code: str,
        *,
        error: BaseException,
        skip_worker: bool = False,
    ) -> None:
        async with bridge.lock:
            if bridge.finish_task is None:
                bridge.finish_task = asyncio.create_task(
                    self._abort_bridge_once(
                        bridge,
                        code,
                        error=error,
                        skip_worker=skip_worker,
                    )
                )
            task = bridge.finish_task
        await await_future_cancellation_safe(task)

    async def _abort_bridge_once(
        self,
        bridge: _BridgeSlot,
        code: str,
        *,
        error: BaseException,
        skip_worker: bool,
    ) -> None:
        bridge.cleanup_started = True
        bridge.abort_event.set()
        if bridge.destination_terminal_issued:
            destination_ack: TransferEndFrame | None
            async with bridge.lock:
                if bridge.source_resolution is SourceResolution.OPEN:
                    bridge.source_resolution = SourceResolution.TIMEOUT_ACK
                    bridge.source_ack_impossible = True
                destination_ack = bridge.destination_ack
            await self._publish_bridge_tombstones(bridge)
            bridge.state = BridgeState.OUTCOME_UNKNOWN
            if destination_ack is not None:
                if destination_ack.ok:
                    warnings = ["transfer_ack_failed"]
                    if bridge.mode == "move":
                        warnings.append("source_delete_failed")
                    await self._finish_bridge_once(
                        bridge,
                        result=TransferResult(
                            bridge.bytes_received,
                            bridge.digest.hexdigest(),
                            tuple(warnings),
                            etag=(
                                destination_ack.etag
                                if bridge.directory_child
                                else None
                            ),
                            created=(
                                destination_ack.created
                                if bridge.directory_child
                                else None
                            ),
                        ),
                        error=None,
                    )
                else:
                    await self._finish_bridge_once(
                        bridge,
                        result=None,
                        error=TransferError(destination_ack.code or "transfer_rejected"),
                    )
            else:
                await self._finish_bridge_once(
                    bridge,
                    result=None,
                    error=TransferDisconnectedError("device transfer outcome is unknown"),
                )
            return
        authoritative = bridge.authoritative_failure
        if authoritative is not None:
            await self._publish_bridge_tombstones(bridge)
            bridge.state = BridgeState.ABORTING
            if bridge.destination_failure is authoritative:
                if bridge.source_issued:
                    try:
                        await self._send_bridge_failure_once(
                            bridge,
                            BridgeRole.SOURCE,
                        )
                    except Exception:
                        pass
                try:
                    await self._send_text(
                        bridge.destination_route.handle,
                        authoritative.model_copy(update={"ack": True}).model_dump_json(),
                        route=None,
                    )
                except Exception:
                    pass
            else:
                if bridge.destination_issued:
                    try:
                        await self._send_bridge_failure_once(
                            bridge,
                            BridgeRole.DESTINATION,
                        )
                    except Exception:
                        pass
                try:
                    await self._send_text(
                        bridge.source_route.handle,
                        authoritative.model_copy(update={"ack": True}).model_dump_json(),
                        route=None,
                    )
                except Exception:
                    pass
            bridge.state = BridgeState.ABORTED
            await self._finish_bridge_once(
                bridge,
                result=None,
                error=TransferError(authoritative.code or "transfer_rejected"),
            )
            return
        bridge.state = BridgeState.ABORTING
        terminal = TransferEndFrame(id=bridge.slot_id, ack=False, ok=False, code=code)
        if bridge.source_issued:
            bridge.source_failure_terminal = terminal
        if bridge.destination_issued:
            bridge.destination_failure_terminal = terminal
        await self._publish_bridge_tombstones(bridge)
        current = asyncio.current_task()
        if (
            not skip_worker
            and bridge.worker is not None
            and bridge.worker is not current
            and not bridge.worker.done()
        ):
            bridge.worker.cancel()
            await asyncio.gather(bridge.worker, return_exceptions=True)
        if bridge.relay_task is not None and bridge.relay_task is not current:
            bridge.relay_task.cancel()
            await asyncio.gather(bridge.relay_task, return_exceptions=True)
        timeout_confirmed = False
        if code == TRANSFER_TIMEOUT_CODE:
            timeout_confirmed = await self._send_bridge_abort_and_wait_for_acks(
                bridge,
                terminal,
            )
        else:
            if bridge.source_issued:
                try:
                    await self._send_text(
                        bridge.source_route.handle,
                        terminal.model_dump_json(),
                        route=None,
                        on_issued=self._bridge_failure_issue_callback(
                            bridge,
                            BridgeRole.SOURCE,
                        ),
                    )
                except Exception:
                    pass
            if bridge.destination_issued and not bridge.destination_terminal_issued:
                try:
                    await self._send_text(
                        bridge.destination_route.handle,
                        terminal.model_dump_json(),
                        route=None,
                        on_issued=self._bridge_failure_issue_callback(
                            bridge,
                            BridgeRole.DESTINATION,
                        ),
                    )
                except Exception:
                    pass
        bridge.state = BridgeState.ABORTED
        final_error = error
        if code == TRANSFER_TIMEOUT_CODE and bridge.source_issued and not timeout_confirmed:
            final_error = TransferDisconnectedError("device transfer outcome is unknown")
        await self._finish_bridge_once(bridge, result=None, error=final_error)

    async def _send_bridge_abort_and_wait_for_acks(
        self,
        bridge: _BridgeSlot,
        terminal: TransferEndFrame,
    ) -> bool:
        sends: list[asyncio.Task[bool]] = []
        acknowledgements: list[asyncio.Future[TransferEndFrame]] = []
        if bridge.source_issued:
            assert bridge.source_ack_future is not None
            sends.append(
                asyncio.create_task(
                    self._send_text(
                        bridge.source_route.handle,
                        terminal.model_dump_json(),
                        route=None,
                        on_issued=self._bridge_failure_issue_callback(
                            bridge,
                            BridgeRole.SOURCE,
                        ),
                    )
                )
            )
            acknowledgements.append(bridge.source_ack_future)
        if bridge.destination_issued and not bridge.destination_terminal_issued:
            assert bridge.destination_ack_future is not None
            sends.append(
                asyncio.create_task(
                    self._send_text(
                        bridge.destination_route.handle,
                        terminal.model_dump_json(),
                        route=None,
                        on_issued=self._bridge_failure_issue_callback(
                            bridge,
                            BridgeRole.DESTINATION,
                        ),
                    )
                )
            )
            acknowledgements.append(bridge.destination_ack_future)
        try:
            async with asyncio.timeout(self._idle_timeout_seconds):
                delivered = await asyncio.gather(*sends)
                if not all(delivered):
                    return False
                await asyncio.gather(
                    *(asyncio.shield(future) for future in acknowledgements)
                )
        except Exception:
            return False
        finally:
            for send in sends:
                if not send.done():
                    send.cancel()
            await asyncio.gather(*sends, return_exceptions=True)
        return True

    async def _finish_bridge_once(
        self,
        bridge: _BridgeSlot,
        *,
        result: TransferResult | None,
        error: BaseException | None,
    ) -> None:
        bridge.cleanup_started = True
        await self._publish_bridge_tombstones(bridge)
        current = asyncio.current_task()
        if bridge.relay_task is not None and bridge.relay_task is not current:
            if not bridge.relay_task.done():
                bridge.relay_task.cancel()
            await asyncio.gather(bridge.relay_task, return_exceptions=True)
        timeout_ack_task = bridge.source_timeout_ack_task
        if timeout_ack_task is not None and timeout_ack_task is not current:
            await await_future_cancellation_safe(timeout_ack_task)
        source_device, source_generation = _handle_identity(bridge.source_route.handle)
        destination_device, destination_generation = _handle_identity(
            bridge.destination_route.handle
        )
        async with self._lock:
            self._bridges.pop(bridge.slot_id, None)
            self._bridge_endpoints.pop(
                (source_device, source_generation, bridge.slot_id),
                None,
            )
            self._bridge_endpoints.pop(
                (destination_device, destination_generation, bridge.slot_id),
                None,
            )
            self._finalize_bridge_tombstones_locked(bridge, error=error)
        while not bridge.queue.empty():
            try:
                bridge.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        if bridge.lease is not None:
            await bridge.lease.aclose()
        if bridge.completion is not None and not bridge.completion.done():
            if error is None:
                assert result is not None
                bridge.completion.set_result(result)
            else:
                bridge.completion.set_exception(error)
                bridge.completion.exception()
        worker = bridge.worker
        if worker is not None and worker is not current and not worker.done():
            worker.cancel()
            cleanup = asyncio.create_task(self._drain_bridge_worker(worker))
            self._bridge_worker_cleanup_tasks.add(cleanup)
            cleanup.add_done_callback(self._bridge_worker_cleanup_tasks.discard)

    @staticmethod
    async def _drain_bridge_worker(worker: asyncio.Task[None]) -> None:
        await asyncio.gather(worker, return_exceptions=True)

    async def _publish_bridge_tombstones(self, bridge: _BridgeSlot) -> None:
        async with self._lock:
            if bridge.tombstones_published:
                return
            source_device, source_generation = _handle_identity(bridge.source_route.handle)
            destination_device, destination_generation = _handle_identity(
                bridge.destination_route.handle
            )
            issued = (
                (
                    bridge.source_issued,
                    (source_device, source_generation, bridge.slot_id),
                    BridgeRole.SOURCE,
                ),
                (
                    bridge.destination_issued,
                    (destination_device, destination_generation, bridge.slot_id),
                    BridgeRole.DESTINATION,
                ),
            )
            for was_issued, key, role in issued:
                if not was_issued:
                    continue
                if bridge.tombstone_credits <= 0 or self._reserved_tombstone_credits <= 0:
                    raise RuntimeError("bridge tombstone reservation was lost")
                self._reserved_tombstone_credits -= 1
                bridge.tombstone_credits -= 1
                self._bridge_tombstones[key] = self._bridge_tombstone_for(
                    bridge,
                    role,
                    pinned=True,
                    error=None,
                )
            if bridge.tombstone_credits:
                self._reserved_tombstone_credits -= bridge.tombstone_credits
                bridge.tombstone_credits = 0
            bridge.tombstones_published = True

    def _finalize_bridge_tombstones_locked(
        self,
        bridge: _BridgeSlot,
        *,
        error: BaseException | None,
    ) -> None:
        source_device, source_generation = _handle_identity(bridge.source_route.handle)
        destination_device, destination_generation = _handle_identity(
            bridge.destination_route.handle
        )
        for key, role in (
            ((source_device, source_generation, bridge.slot_id), BridgeRole.SOURCE),
            (
                (destination_device, destination_generation, bridge.slot_id),
                BridgeRole.DESTINATION,
            ),
        ):
            current = self._bridge_tombstones.get(key)
            if current is None:
                continue
            finalized = self._bridge_tombstone_for(
                bridge,
                role,
                pinned=False,
                error=error,
            )
            if current.failed and finalized.failed:
                if current.binary_bytes_seen >= finalized.binary_bytes_seen:
                    finalized.binary_digest = current.binary_digest.copy()
                finalized.binary_bytes_seen = max(
                    current.binary_bytes_seen,
                    finalized.binary_bytes_seen,
                )
                finalized.progress_remaining = min(
                    current.progress_remaining,
                    finalized.progress_remaining,
                )
                finalized.last_progress = max(current.last_progress, finalized.last_progress)
            finalized.source_timeout_ack_sent = (
                current.source_timeout_ack_sent or finalized.source_timeout_ack_sent
            )
            finalized.source_timeout_ack_attempted = (
                current.source_timeout_ack_attempted
                or finalized.source_timeout_ack_attempted
            )
            finalized.source_timeout_ack_in_flight = (
                current.source_timeout_ack_in_flight
                or finalized.source_timeout_ack_in_flight
            )
            finalized.failure_issued = current.failure_issued or finalized.failure_issued
            finalized.simultaneous_failure_ack_in_flight = (
                current.simultaneous_failure_ack_in_flight
                or finalized.simultaneous_failure_ack_in_flight
            )
            finalized.simultaneous_failure_ack_sent = (
                current.simultaneous_failure_ack_sent
                or finalized.simultaneous_failure_ack_sent
            )
            finalized.late_destination_ack = current.late_destination_ack
            finalized.late_source_success_terminal = (
                current.late_source_success_terminal
                or finalized.late_source_success_terminal
            )
            self._bridge_tombstones[key] = finalized

    def _bridge_tombstone_for(
        self,
        bridge: _BridgeSlot,
        role: BridgeRole,
        *,
        pinned: bool,
        error: BaseException | None,
    ) -> _BridgeTombstone:
        expected: list[TransferEndFrame] = []
        inbound_frames = (
            (bridge.source_end, bridge.source_drain_failure)
            if role is BridgeRole.SOURCE
            else (bridge.destination_failure, bridge.destination_ack)
        )
        for inbound in inbound_frames:
            if inbound is not None and inbound not in expected:
                expected.append(inbound)
        failure = (
            bridge.source_failure_terminal
            if role is BridgeRole.SOURCE
            else bridge.destination_failure_terminal
        )
        failure_issued = (
            bridge.source_failure_issued
            if role is BridgeRole.SOURCE
            else bridge.destination_failure_issued
        )
        if failure is not None:
            acknowledgement = failure.model_copy(update={"ack": True})
            if acknowledgement not in expected:
                expected.append(acknowledgement)
        failed = (
            not bridge.destination_terminal_issued
            and not bridge.destination_committed
            and (
                error is not None
                or bridge.source_failure_terminal is not None
                or bridge.destination_failure_terminal is not None
            )
        )
        declared = bridge.source_begin.total_bytes if bridge.source_begin is not None else None
        return _BridgeTombstone(
            role=role,
            pinned=pinned,
            expires_at=(
                None if pinned else time.monotonic() + self._tombstone_ttl_seconds
            ),
            expected_terminals=tuple(expected),
            sender_success_terminal=(
                bridge.source_end
                if role is BridgeRole.DESTINATION
                and bridge.source_end is not None
                and bridge.source_end.ok
                else None
            ),
            source_resolution=(
                bridge.source_resolution if role is BridgeRole.SOURCE else None
            ),
            source_timeout_ack_attempted=bridge.source_timeout_ack_attempted,
            source_timeout_ack_sent=bridge.source_timeout_ack_sent,
            source_timeout_ack_in_flight=bridge.source_timeout_ack_in_flight,
            failed=failed,
            failure_terminal=failure,
            failure_issued=failure_issued,
            source_ready_issued=bridge.source_ready_issued,
            binary_bytes_seen=bridge.bytes_received + bridge.late_binary_bytes,
            binary_digest=(bridge.late_binary_digest or bridge.digest).copy(),
            progress_remaining=(
                bridge.late_progress_remaining
                if failed and role is BridgeRole.SOURCE
                else 0
            ),
            last_progress=bridge.last_progress,
            declared_bytes=declared,
            bridge=bridge if pinned else None,
            accept_late_destination_ack=(
                role is BridgeRole.DESTINATION
                and bridge.destination_terminal_issued
                and bridge.destination_ack is None
                and bridge.source_resolution is SourceResolution.TIMEOUT_ACK
            ),
            late_source_success_terminal=bridge.late_source_success_terminal,
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
                on_issued=self._initial_issue_callback(slot),
            ):
                raise TransferUnavailableError("device route was unavailable before send")
            slot.route = None
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
            if slot.purpose != "workspace_upload" and not slot.directory_child and (
                ack.etag is not None or ack.created is not None
            ):
                raise TransferProtocolError(
                    "transfer metadata is only valid for workspace_upload"
                )
            if slot.directory_child and (ack.etag is None or ack.created is not True):
                raise TransferProtocolError(
                    "directory child result is missing destination metadata"
                )
            include_destination_metadata = (
                slot.purpose == "workspace_upload" or slot.directory_child
            )
            slot.committed_result = self._result_for_slot(
                slot,
                digest=digest,
                warnings=(),
                etag=ack.etag if include_destination_metadata else None,
                created=ack.created if include_destination_metadata else None,
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
                etag=ack.etag if include_destination_metadata else None,
                created=ack.created if include_destination_metadata else None,
            )
            slot.committed_result = result
            await self._finish(slot, result)
        except asyncio.CancelledError:
            if slot.fenced:
                error = _fenced_transfer_error(slot)
                await self._abort(
                    slot,
                    error.code,
                    send_frame=False,
                    error=error,
                )
            else:
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
                await self._abort(
                    slot,
                    "peer_disconnected",
                    send_frame=False,
                    error=TransferDisconnectedError("device transfer outcome is unknown"),
                )
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
            destination_etag: str | None = None
            destination_created: bool | None = None
            if slot.commit_sink is not None:
                resolution = asyncio.get_running_loop().create_future()
                slot.commit_resolution = resolution
                try:
                    async with asyncio.timeout(self._idle_timeout_seconds):
                        commit_result = await slot.commit_sink(
                            slot.sink,
                            slot.begin,
                            slot.bytes_seen,
                            digest,
                        )
                    if isinstance(commit_result, TransferCommitResult):
                        destination_etag = commit_result.etag
                        destination_created = commit_result.created
                        cancel_after_commit = commit_result.cancel_after_commit
                    else:
                        cancel_after_commit = bool(commit_result)
                except BaseException:
                    resolution.set_result(False)
                    raise
            slot.committed_result = self._result_for_slot(
                slot,
                digest=digest,
                warnings=(),
                etag=(
                    destination_etag
                    if slot.directory_child
                    else slot.begin.etag if slot.purpose == "http_relay" else None
                ),
                created=destination_created if slot.directory_child else None,
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
                etag=(
                    destination_etag
                    if slot.directory_child
                    else slot.begin.etag if slot.purpose == "http_relay" else None
                ),
                created=destination_created if slot.directory_child else None,
            )
            slot.state = TransferState.COMMITTED
            await self._finish(slot, result)
            if cancel_after_commit:
                current = asyncio.current_task()
                if current is not None:
                    asyncio.get_running_loop().call_soon(current.cancel)
        except asyncio.CancelledError:
            if slot.fenced:
                error = _fenced_transfer_error(slot)
                await self._abort(
                    slot,
                    error.code,
                    send_frame=False,
                    error=error,
                )
            else:
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
        lease: TransferLease | None,
        direction: TransferDirection,
        purpose: TransferPurpose,
        state: TransferState,
        source: TransferSource | None = None,
        delete_source: DeleteSource | None = None,
        commit_sink: CommitSink | None = None,
        sink_factory: SinkFactory | None = None,
        source_etag: str | None = None,
        mode: str = "copy",
        on_issued: Callable[[], None] | None = None,
        slot_id: UUID | None = None,
        directory_child: bool = False,
    ) -> _TransferSlot:
        try:
            device_id, generation = _handle_identity(handle)
            slot = _TransferSlot(
                handle=handle,
                route=route,
                device_id=device_id,
                generation=generation,
                user_id=user_id,
                slot_id=slot_id or new_uuid7(),
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
                on_issued=on_issued,
                directory_child=directory_child,
            )
            async with self._lock:
                self._expire_tombstones_locked()
                key = (device_id, generation, slot.slot_id)
                if self._key_in_use_locked(key):
                    raise TransferProtocolError("transfer slot id collided with an active slot")
                self._slots[key] = slot
            return slot
        except BaseException:
            if lease is not None:
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
        elif slot.directory_child:
            if slot.purpose != "file_transfer" or etag is None or created is not True:
                raise TransferProtocolError(
                    "directory child result is missing destination metadata"
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
        if slot.lease is not None:
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
            if slot.fenced and slot.route is not None:
                raise TransferUnavailableError("device route was unavailable before send")
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
        while self._tombstone_occupancy_locked() > TOMBSTONE_MAX_ENTRIES:
            if not self._evict_one_final_tombstone_locked():
                raise RuntimeError("transfer tombstone capacity invariant was violated")

    async def _send_text(
        self,
        handle: object,
        payload: str,
        *,
        route: TransferRoute | None,
        on_issued: Callable[[], None] | None = None,
    ) -> bool:
        try:
            if route is None:
                if on_issued is None:
                    result = await self._transport.send_text(handle, payload)
                else:
                    result = await self._transport.send_text(
                        handle,
                        payload,
                        on_issued=on_issued,
                    )
            else:
                if on_issued is None:
                    result = await self._transport.send_text(
                        handle,
                        payload,
                        expected_device_name=route.device_name,
                        expected_config_epoch=route.config_epoch,
                    )
                else:
                    result = await self._transport.send_text(
                        handle,
                        payload,
                        expected_device_name=route.device_name,
                        expected_config_epoch=route.config_epoch,
                        on_issued=on_issued,
                    )
        except TransferDisconnectedError:
            raise
        except Exception as exc:
            raise TransferDisconnectedError("device transfer outcome is unknown") from exc
        return result is not False

    @staticmethod
    def _initial_issue_callback(slot: _TransferSlot) -> Callable[[], None] | None:
        if slot.route is None and slot.on_issued is None:
            return None

        def issued() -> None:
            slot.route = None
            if slot.on_issued is not None:
                slot.on_issued()

        return issued

    async def _send_binary(self, handle: object, slot_id: UUID, payload: bytes) -> bool:
        if len(payload) > MAX_BINARY_CHUNK_BYTES:
            raise TransferProtocolError("transfer chunk exceeds 64 KiB")
        slot = await self._get_slot(handle, slot_id)
        try:
            if slot.route is None:
                result = await self._transport.send_binary(handle, slot_id.bytes + payload)
            else:
                result = await self._transport.send_binary(
                    handle,
                    slot_id.bytes + payload,
                    expected_device_name=slot.route.device_name,
                    expected_config_epoch=slot.route.config_epoch,
                )
        except TransferDisconnectedError:
            raise
        except Exception as exc:
            raise TransferDisconnectedError("device transfer outcome is unknown") from exc
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
        for key, tombstone in tuple(self._bridge_tombstones.items()):
            if (
                not tombstone.pinned
                and not tombstone.source_timeout_ack_in_flight
                and not tombstone.simultaneous_failure_ack_in_flight
                and tombstone.expires_at is not None
                and tombstone.expires_at <= now
            ):
                self._bridge_tombstones.pop(key, None)

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


def _is_sender_timeout(frame: TransferEndFrame) -> bool:
    return frame == TransferEndFrame(
        id=frame.id,
        ack=False,
        ok=False,
        code=TRANSFER_TIMEOUT_CODE,
    )


def _ack_resolves_success_terminal(
    terminal: TransferEndFrame | None,
    acknowledgement: TransferEndFrame,
) -> bool:
    if (
        terminal is None
        or terminal.ack
        or not terminal.ok
        or not acknowledgement.ack
    ):
        return False
    if not acknowledgement.ok:
        return True
    return (
        acknowledgement.bytes_sent == terminal.bytes_sent
        and acknowledgement.sha256 == terminal.sha256
        and acknowledgement.etag is None
        and acknowledgement.created is None
    )


def _fenced_transfer_error(
    slot: _TransferSlot,
) -> TransferUnavailableError | TransferDisconnectedError:
    if slot.route is not None:
        return TransferUnavailableError("device route was unavailable before send")
    return TransferDisconnectedError("device transfer outcome is unknown")


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
