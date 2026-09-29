"""Device transfer states, frame contracts, and terminal result helpers."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Literal, Protocol
from uuid import UUID

from openctopus_server.devices.protocol import (
    TransferBeginFrame,
    TransferDirection,
    TransferEndFrame,
    TransferPurpose,
    TransferReadyFrame,
)

from .transfer_admission import TransferLease

TRANSFER_QUEUE_CHUNKS = 4


BRIDGE_SOURCE_DELETE_TIMEOUT_SECONDS = 30.0


DEFAULT_TOMBSTONE_TTL_SECONDS = 60.0


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
        if not 1 <= len(self.etag) <= 512 or any(
            character in {'"', "\x00"} or not 0x21 <= ord(character) <= 0x7E
            for character in self.etag
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
                character in {'"', "\x00"} or not 0x21 <= ord(character) <= 0x7E
                for character in self.etag
            )
        ):
            raise ValueError("transfer etag is invalid")


class TransferCommittedAfterCancellation(asyncio.CancelledError):
    """An irreversible transfer commit completed before cancellation."""

    def __init__(self, result: TransferResult) -> None:
        super().__init__()
        self.result = result


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
    expected_source_size: int | None
    expected_source_fingerprint: str | None
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


class TransferError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _completed_transfer_result(
    completion: asyncio.Future[TransferResult] | None,
) -> TransferResult | None:
    if completion is None or not completion.done() or completion.cancelled():
        return None
    try:
        return completion.result()
    except BaseException:
        return None


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
    if terminal is None or terminal.ack or not terminal.ok or not acknowledgement.ack:
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
