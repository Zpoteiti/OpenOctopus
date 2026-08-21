from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import secrets
import stat
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from openoctopus_client.protocol import (
    ProtocolError,
    TransferBegin,
    TransferEnd,
    TransferFrame,
    TransferProgress,
    TransferReady,
    TransferRequest,
    decode_binary_chunk,
    encode_frame,
)
from openoctopus_client.tools.common import ToolFailure
from openoctopus_client.tools.fingerprints import opaque_stat_fingerprint
from openoctopus_client.tools.locks import PathLocks
from openoctopus_client.tools.paths import WorkspacePaths
from openoctopus_client.writer import SerializedWriter, WriterOverflowError

MAX_ACTIVE_TRANSFER_SLOTS = 2
TRANSFER_CHUNKS_PER_SLOT = 4
TRANSFER_CHUNK_BYTES = 64 * 1024
MAX_BINARY_CHUNK_BYTES = TRANSFER_CHUNK_BYTES
TRANSFER_IDLE_TIMEOUT_SECONDS = 30.0
TOMBSTONE_TTL_SECONDS = 60.0
TOMBSTONE_MAX_ENTRIES = 4096
_TO_THREAD_CANCEL_GRACE_SECONDS = 0.1
_TEMPORARY_UNLINK_ATTEMPTS = 10
_TEMPORARY_UNLINK_RETRY_SECONDS = 0.05


class TransferState(StrEnum):
    REQUESTED = "REQUESTED"
    BEGUN = "BEGUN"
    READY = "READY"
    STREAMING = "STREAMING"
    SENDER_ENDED = "SENDER_ENDED"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


class TransferProtocolError(ProtocolError):
    """A transfer frame violated slot or ordering rules."""


class TransferOperationError(RuntimeError):
    """A local transfer operation failed with a stable client-facing code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class TransferConfigSnapshot:
    """The immutable local policy captured when a transfer slot starts."""

    workspace_path: Path
    sandbox_mode: bool
    ssrf_denylist: tuple[str, ...] = ()
    device_name: str = ""

    @classmethod
    def from_values(
        cls,
        workspace_path: Path,
        *,
        sandbox_mode: bool,
        ssrf_denylist: list[str] | tuple[str, ...] = (),
        device_name: str = "",
    ) -> TransferConfigSnapshot:
        return cls(
            workspace_path=workspace_path,
            sandbox_mode=sandbox_mode,
            ssrf_denylist=tuple(ssrf_denylist),
            device_name=device_name,
        )


@dataclass(frozen=True)
class _Tombstone:
    ack: bool
    ok: bool
    code: str | None
    bytes_sent: int | None
    sha256: str | None
    etag: str | None
    created: bool | None
    expires_at: float


@dataclass(frozen=True)
class _DestinationReservation:
    initial_fingerprint: str | None
    created: bool
    temporary: Path


@dataclass
class _Slot:
    slot_id: UUID
    role: Literal["sender", "receiver"]
    purpose: str
    snapshot: TransferConfigSnapshot
    state: str
    begin: TransferBegin | None = None
    request: TransferRequest | None = None
    destination: Path | None = None
    destination_paths: WorkspacePaths | None = None
    temporary: Path | None = None
    lock_stack: contextlib.AsyncExitStack | None = None
    inbound: asyncio.Queue[bytes | None] | None = None
    inbound_bytes: int = 0
    last_progress: int = 0
    received_bytes: int = 0
    received_digest: Any = field(default_factory=hashlib.sha256)
    sender_ready: asyncio.Event = field(default_factory=asyncio.Event)
    sender_ack: asyncio.Event = field(default_factory=asyncio.Event)
    remote_end: TransferEnd | None = None
    task: asyncio.Task[None] | None = None
    source_fd: int | None = None
    source_path: Path | None = None
    source_identity: tuple[int, int, int, int, int] | None = None
    destination_initial_fingerprint: str | None = None
    destination_created: bool | None = None
    temporary_identity: tuple[int, int, int, int, int] | None = None
    final_end: TransferEnd | None = None


class TransferManager:
    """Bounded client-side implementation of the Py6 single-file wire flow.

    The reader calls :meth:`handle_control` and :meth:`handle_binary`; all
    filesystem work runs in per-slot tasks.  Four queued chunks per receiver
    and four queued chunks per writer lane are the only retained bulk bytes.
    """

    def __init__(
        self,
        workspace: Path | TransferConfigSnapshot,
        writer: SerializedWriter,
        *,
        sandbox_mode: bool = True,
        ssrf_denylist: list[str] | tuple[str, ...] = (),
        device_name: str = "",
        path_locks: PathLocks | None = None,
        idle_timeout_seconds: float = TRANSFER_IDLE_TIMEOUT_SECONDS,
    ) -> None:
        if isinstance(workspace, TransferConfigSnapshot):
            snapshot = workspace
        else:
            snapshot = TransferConfigSnapshot.from_values(
                workspace,
                sandbox_mode=sandbox_mode,
                ssrf_denylist=ssrf_denylist,
                device_name=device_name,
            )
        self._snapshot = snapshot
        self._writer = writer
        self._locks = path_locks or PathLocks()
        self._idle_timeout = max(0.1, idle_timeout_seconds)
        self._slots: dict[UUID, _Slot] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._blocking_tasks: set[asyncio.Task[Any]] = set()
        self._tombstones: dict[UUID, _Tombstone] = {}
        self._closed = False
        self._failed: asyncio.Future[None] | None = None

    @property
    def failed(self) -> asyncio.Future[None]:
        """Complete exceptionally when a transfer cannot emit a critical frame."""

        if self._failed is None:
            self._failed = asyncio.get_running_loop().create_future()
            self._failed.add_done_callback(_consume_future_exception)
        return self._failed

    @property
    def active_count(self) -> int:
        return len(self._slots)

    @property
    def active_slot_ids(self) -> frozenset[UUID]:
        return frozenset(self._slots)

    def slot_state(self, slot_id: UUID) -> TransferState | None:
        slot = self._slots.get(slot_id)
        if slot is None:
            return None
        return TransferState(slot.state)

    @property
    def tombstone_count(self) -> int:
        self._purge_tombstones()
        return len(self._tombstones)

    @property
    def path_locks(self) -> PathLocks:
        return self._locks

    def update_config(self, snapshot: TransferConfigSnapshot) -> None:
        """Install policy for future slots; active slots keep their snapshot."""

        if self._closed:
            return
        self._snapshot = snapshot

    async def handle_control(
        self,
        frame: TransferFrame,
        *,
        start_snapshot: TransferConfigSnapshot | None = None,
    ) -> None:
        self._purge_tombstones()
        if self._closed:
            raise TransferProtocolError("Transfer manager is closed")
        if isinstance(frame, TransferRequest):
            await self._accept_request(frame, start_snapshot or self._snapshot)
        elif isinstance(frame, TransferBegin):
            await self._accept_begin(frame, start_snapshot or self._snapshot)
        elif isinstance(frame, TransferReady):
            self._accept_ready(frame)
        elif isinstance(frame, TransferEnd):
            await self._accept_end(frame)
        elif isinstance(frame, TransferProgress):
            self._accept_progress(frame)
        else:  # pragma: no cover - kept for future frame additions
            raise TransferProtocolError("Unknown transfer frame")

    async def receive_control(self, frame: TransferFrame) -> None:
        """Compatibility alias used by small transport adapters and tests."""

        await self.handle_control(frame)

    def reject_busy_start(self, frame: TransferRequest | TransferBegin) -> None:
        """Reject a bounded config-ordered start queue overflow."""

        self._purge_tombstones()
        if self._closed:
            raise TransferProtocolError("Transfer manager is closed")
        self._ensure_new_slot(frame.id)
        self._send_terminal_rejection(frame.id, "tool_device_busy")

    async def handle_binary(self, payload: bytes) -> None:
        slot_id, chunk = decode_binary_chunk(payload)
        self._purge_tombstones()
        slot = self._slots.get(slot_id)
        if slot is None:
            tombstone = self._tombstones.get(slot_id)
            if tombstone is not None and not tombstone.ack and not tombstone.ok:
                # The peer may already have queued bounded binary frames before
                # it observes our local failure terminal.
                return
            if tombstone is not None:
                raise TransferProtocolError("Binary data arrived for a closed transfer")
            raise TransferProtocolError("Binary data arrived for an unknown transfer")
        if (
            slot.role != "receiver"
            or slot.state not in {"READY", "STREAMING"}
            or slot.inbound is None
        ):
            if (
                slot.role == "receiver"
                and slot.state == "ABORTED"
                and slot.final_end is not None
                and not slot.final_end.ack
                and not slot.final_end.ok
            ):
                # The receiver already sent its terminal timeout/failure.  A
                # peer may still have queued binary data until it observes
                # that frame; do not turn that expected drain into a socket
                # protocol failure.
                return
            raise TransferProtocolError("Binary data arrived before transfer_ready")
        declared = slot.begin.total_bytes if slot.begin is not None else None
        if declared is not None and slot.inbound_bytes + len(chunk) > declared:
            raise TransferProtocolError("Transfer sent more bytes than declared")
        slot.inbound_bytes += len(chunk)
        try:
            async with asyncio.timeout(self._idle_timeout):
                await slot.inbound.put(chunk)
        except TimeoutError as exc:
            await self._abort_receiver(slot, "workspace_transfer_timeout")
            del exc
            return

    async def receive_binary(self, payload: bytes) -> None:
        await self.handle_binary(payload)

    async def _abort_receiver(self, slot: _Slot, code: str) -> None:
        """Stop a stalled receiver without making the transport a protocol error."""

        if slot.state in {"COMMITTED", "ABORTED"}:
            return
        slot.state = "ABORTED"
        overflow: WriterOverflowError | None = None
        if slot.final_end is None:
            try:
                self._enqueue_end(slot, ok=False, code=code, ack=False)
            except WriterOverflowError as exc:
                overflow = exc
        task = slot.task
        if task is not None and task is not asyncio.current_task() and not task.done():
            # The receiver's cancellation path owns filesystem and lock cleanup.
            self._cancel_slot_worker(slot)
        elif task is None or task.done():
            await self._cleanup_slot(slot)
        if overflow is not None:
            raise overflow

    async def shutdown(self, *, code: str = "peer_disconnected") -> None:
        if self._closed:
            return
        self._closed = True
        slots = list(self._slots.values())
        overflow: WriterOverflowError | None = None
        for slot in slots:
            if slot.final_end is None:
                try:
                    self._enqueue_end(slot, ok=False, code=code, ack=False)
                except WriterOverflowError as exc:
                    overflow = overflow or exc
            if slot.inbound is not None:
                with contextlib.suppress(asyncio.QueueFull):
                    slot.inbound.put_nowait(None)
            if slot.task is not None:
                slot.task.cancel()
        if self._tasks:
            await asyncio.gather(*tuple(self._tasks), return_exceptions=True)
        if self._blocking_tasks:
            await asyncio.wait(
                tuple(self._blocking_tasks), timeout=_TO_THREAD_CANCEL_GRACE_SECONDS
            )
        for slot in list(self._slots.values()):
            await self._cleanup_slot(slot)
        if overflow is not None:
            raise overflow

    async def close(self) -> None:
        await self.shutdown()

    async def _accept_request(
        self,
        request: TransferRequest,
        snapshot: TransferConfigSnapshot,
    ) -> None:
        self._ensure_new_slot(request.id)
        if len(self._slots) >= MAX_ACTIVE_TRANSFER_SLOTS:
            self._send_terminal_rejection(request.id, "tool_device_busy")
            return
        slot = _Slot(
            slot_id=request.id,
            role="sender",
            purpose=request.purpose,
            snapshot=snapshot,
            state="REQUESTED",
            request=request,
        )
        self._slots[request.id] = slot
        slot.task = self._spawn(self._send_requested(slot))

    async def _accept_begin(
        self,
        begin: TransferBegin,
        snapshot: TransferConfigSnapshot,
    ) -> None:
        self._ensure_new_slot(begin.id)
        if len(self._slots) >= MAX_ACTIVE_TRANSFER_SLOTS:
            self._send_terminal_rejection(begin.id, "tool_device_busy")
            return
        if begin.direction != "server_to_client":
            raise TransferProtocolError("Client cannot receive a client_to_server begin")
        slot = _Slot(
            slot_id=begin.id,
            role="receiver",
            purpose=begin.purpose,
            snapshot=snapshot,
            state="BEGUN",
            begin=begin,
            inbound=asyncio.Queue(maxsize=TRANSFER_CHUNKS_PER_SLOT),
        )
        self._slots[begin.id] = slot
        slot.task = self._spawn(self._prepare_receiver(slot))

    async def _prepare_receiver(self, slot: _Slot) -> None:
        """Reserve a receiver destination without holding up the connection reader."""

        try:
            await self._reserve_destination(slot)
        except asyncio.CancelledError:
            await self._cleanup_slot(slot)
            raise
        except TransferOperationError as exc:
            if slot.state == "ABORTED":
                return
            slot.state = "ABORTED"
            try:
                self._enqueue_end(slot, ok=False, code=exc.code, ack=False)
            except WriterOverflowError:
                await self._cleanup_slot(slot)
                raise
            slot.task = self._spawn(self._await_rejection_ack(slot))
            return
        try:
            self._enqueue_critical(encode_frame(TransferReady(id=slot.slot_id)))
        except WriterOverflowError:
            await self._cleanup_slot(slot)
            raise
        slot.state = "READY"
        slot.task = self._spawn(self._receive_destination(slot))

    def _accept_ready(self, ready: TransferReady) -> None:
        slot = self._require_slot(ready.id)
        if slot.role != "sender" or slot.state != "BEGUN":
            raise TransferProtocolError("transfer_ready is invalid in the current state")
        slot.state = "STREAMING"
        slot.sender_ready.set()

    async def _accept_end(self, end: TransferEnd) -> None:
        slot = self._slots.get(end.id)
        if slot is None:
            tombstone = self._tombstones.get(end.id)
            if tombstone is not None and self._same_terminal(tombstone, end):
                if end.ack and not tombstone.ack:
                    self._remember_tombstone(end.id, end)
                return
            raise TransferProtocolError("transfer_end arrived for an unknown transfer")
        if end.ack:
            if slot.role == "receiver" and slot.state == "ABORTED":
                if not self._ack_matches_terminal(slot, end):
                    raise TransferProtocolError("transfer acknowledgement conflicts with terminal")
                slot.final_end = end
                slot.sender_ack.set()
                if slot.task is None or slot.task.done():
                    await self._cleanup_slot(slot)
                return
            if slot.role != "sender" or slot.state not in {"SENDER_ENDED", "ABORTED"}:
                raise TransferProtocolError("transfer acknowledgement is out of order")
            if not self._ack_matches_terminal(slot, end):
                raise TransferProtocolError("transfer acknowledgement conflicts with terminal")
            slot.remote_end = end
            slot.sender_ack.set()
            return
        if (
            slot.role == "sender"
            and slot.state in {"REQUESTED", "BEGUN", "STREAMING", "SENDER_ENDED"}
            and not end.ok
        ):
            cancel_sender = slot.state != "SENDER_ENDED"
            slot.remote_end = end
            slot.state = "ABORTED"
            receiver_overflow: WriterOverflowError | None = None
            try:
                self._enqueue_end(slot, ack=True, ok=False, code=end.code)
            except WriterOverflowError as exc:
                receiver_overflow = exc
            await self._writer.discard_binary_lane(slot.slot_id)
            slot.sender_ready.set()
            slot.sender_ack.set()
            if cancel_sender and slot.task is not None:
                self._cancel_slot_worker(slot)
            if receiver_overflow is not None:
                raise receiver_overflow
            return
        if (
            slot.role == "receiver"
            and slot.state in {"BEGUN", "READY", "STREAMING"}
            and not end.ok
        ):
            slot.remote_end = end
            slot.state = "ABORTED"
            overflow: WriterOverflowError | None = None
            try:
                self._enqueue_end(
                    slot,
                    ack=True,
                    ok=False,
                    code=end.code,
                    bytes_sent=end.bytes_sent,
                    sha256=end.sha256,
                    etag=end.etag,
                    created=end.created,
                )
            except WriterOverflowError as exc:
                overflow = exc
            task = slot.task
            if task is not None and task is not asyncio.current_task() and not task.done():
                self._cancel_slot_worker(slot)
            elif task is None or task.done():
                await self._cleanup_slot(slot)
            if overflow is not None:
                raise overflow
            return
        if slot.role != "receiver" or slot.state not in {"READY", "STREAMING"}:
            raise TransferProtocolError("transfer_end is out of order")
        slot.remote_end = end
        slot.state = "SENDER_ENDED"
        if slot.inbound is not None:
            try:
                async with asyncio.timeout(self._idle_timeout):
                    await slot.inbound.put(None)
            except TimeoutError:
                await self._abort_receiver(slot, "workspace_transfer_timeout")

    def _accept_progress(self, progress: TransferProgress) -> None:
        slot = self._require_slot(progress.id)
        if slot.role != "receiver" or slot.state not in {"READY", "STREAMING"}:
            raise TransferProtocolError("transfer_progress is out of order")
        if progress.bytes_sent < slot.last_progress:
            raise TransferProtocolError("transfer_progress moved backwards")
        slot.last_progress = progress.bytes_sent

    def _ensure_new_slot(self, slot_id: UUID) -> None:
        if slot_id in self._slots:
            raise TransferProtocolError("Transfer slot ID is already active")
        if slot_id in self._tombstones:
            raise TransferProtocolError("Transfer slot ID was already closed")

    def _require_slot(self, slot_id: UUID) -> _Slot:
        slot = self._slots.get(slot_id)
        if slot is None:
            raise TransferProtocolError("Unknown transfer slot ID")
        return slot

    def _spawn(self, coroutine: Any) -> asyncio.Task[None]:
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)
        return task

    def _task_done(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        error: BaseException | None = None
        try:
            error = task.exception()
        except BaseException:
            return
        if isinstance(error, WriterOverflowError):
            self._record_failure(error)

    def _enqueue_critical(self, payload: str) -> None:
        try:
            self._writer.enqueue_critical(payload)
        except WriterOverflowError as exc:
            self._record_failure(exc)
            raise

    def _record_failure(self, error: WriterOverflowError) -> None:
        failed = self.failed
        if not failed.done():
            failed.set_exception(error)

    async def _run_filesystem(
        self,
        function: Any,
        *args: Any,
        on_abandoned: Callable[[Any], Any] | None = None,
    ) -> Any:
        return await _to_thread_safely(
            function,
            *args,
            tracker=self._blocking_tasks,
            on_abandoned=on_abandoned,
        )

    def _cancel_slot_worker(self, slot: _Slot) -> None:
        """Cancel a worker and clean even when cancellation wins before its first step."""

        task = slot.task
        if task is not None and not task.done():
            task.cancel()
        self._spawn(self._cleanup_cancelled_slot(slot, task))

    async def _cleanup_cancelled_slot(
        self,
        slot: _Slot,
        task: asyncio.Task[None] | None,
    ) -> None:
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        if self._slots.get(slot.slot_id) is slot:
            await self._cleanup_slot(slot)

    async def _reserve_destination(self, slot: _Slot) -> None:
        begin = slot.begin
        if begin is None or begin.dst_path is None:
            raise TransferOperationError(
                "workspace_invalid_request", "Transfer destination is missing"
            )
        try:
            paths, destination = await self._run_filesystem(
                _resolve_destination,
                slot.snapshot,
                begin.dst_path,
            )
        except ToolFailure as exc:
            raise TransferOperationError(exc.code, exc.args[0] if exc.args else exc.code) from exc
        except OSError as exc:
            raise TransferOperationError(
                "workspace_permission_denied", "Destination is unavailable"
            ) from exc
        stack = contextlib.AsyncExitStack()
        slot.lock_stack = stack
        await stack.enter_async_context(self._locks.hold(str(destination)))
        slot.destination = destination
        slot.destination_paths = paths
        try:
            reservation = await self._run_filesystem(
                _prepare_destination,
                paths,
                destination,
                slot.purpose,
                begin.if_match,
                begin.if_none_match,
                on_abandoned=_discard_destination_reservation,
            )
            slot.destination_initial_fingerprint = reservation.initial_fingerprint
            slot.destination_created = reservation.created
            slot.temporary = reservation.temporary
        except TransferOperationError:
            await stack.aclose()
            slot.lock_stack = None
            raise
        except (OSError, ToolFailure) as exc:
            await stack.aclose()
            slot.lock_stack = None
            code = exc.code if isinstance(exc, ToolFailure) else "workspace_permission_denied"
            raise TransferOperationError(code, "Destination is unavailable") from exc
        except BaseException:
            await stack.aclose()
            slot.lock_stack = None
            raise

    async def _receive_destination(self, slot: _Slot) -> None:
        assert slot.inbound is not None
        assert slot.temporary is not None
        assert slot.destination is not None
        file_handle = None
        try:
            if slot.destination_paths is None or slot.begin is None or slot.begin.dst_path is None:
                raise TransferOperationError(
                    "workspace_invalid_request", "Transfer destination is missing"
                )
            try:
                destination_unchanged = await self._run_filesystem(
                    _destination_parent_unchanged,
                    slot.destination_paths,
                    slot.begin.dst_path,
                    slot.destination,
                )
            except ToolFailure as exc:
                raise TransferOperationError(
                    exc.code, exc.args[0] if exc.args else exc.code
                ) from exc
            if not destination_unchanged:
                raise TransferOperationError(
                    "workspace_file_changed", "Destination parent changed during transfer"
                )
            file_handle, slot.temporary_identity = await self._run_filesystem(
                _open_temp,
                slot.temporary,
                on_abandoned=_close_open_temp_result,
            )
            idle_deadline = asyncio.get_running_loop().time() + self._idle_timeout
            while True:
                try:
                    remaining = idle_deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        raise TimeoutError
                    chunk = await asyncio.wait_for(slot.inbound.get(), remaining)
                except TimeoutError as exc:
                    raise TransferOperationError(
                        "workspace_transfer_timeout", "Transfer made no progress"
                    ) from exc
                if chunk is None:
                    slot.inbound.task_done()
                    break
                if chunk:
                    await self._run_filesystem(file_handle.write, chunk)
                    slot.received_bytes += len(chunk)
                    slot.received_digest.update(chunk)
                    slot.state = "STREAMING"
                    idle_deadline = asyncio.get_running_loop().time() + self._idle_timeout
                slot.inbound.task_done()
            await asyncio.wait_for(self._run_filesystem(file_handle.flush), self._idle_timeout)
            await asyncio.wait_for(
                self._run_filesystem(os.fsync, file_handle.fileno()), self._idle_timeout
            )
            open_identity = _identity(
                await self._run_filesystem(os.fstat, file_handle.fileno())
            )
            # Close before the rename so Windows does not reject replacing an
            # open destination handle; the finally block still owns cleanup
            # if closing itself fails.
            await self._run_filesystem(file_handle.close)
            file_handle = None
            digest = slot.received_digest.hexdigest()
            slot.temporary_identity = await self._run_filesystem(
                _identity_after_close,
                slot.temporary,
                open_identity,
                slot.received_bytes,
                digest,
            )
            declared_end = slot.remote_end
            if declared_end is None:
                raise TransferOperationError(
                    "workspace_transfer_timeout", "Transfer did not finish"
                )
            if not declared_end.ok:
                raise TransferOperationError(
                    declared_end.code or "workspace_transfer_integrity_failed",
                    "Sender aborted the transfer",
                )
            expected_bytes = slot.begin.total_bytes if slot.begin is not None else None
            if (
                (expected_bytes is not None and slot.received_bytes != expected_bytes)
                or declared_end.bytes_sent != slot.received_bytes
                or declared_end.sha256 != digest
            ):
                raise TransferOperationError(
                    "workspace_transfer_integrity_failed", "Transfer checksum or length mismatch"
                )
            if slot.begin is not None and slot.begin.sha256 not in {None, digest}:
                raise TransferOperationError(
                    "workspace_transfer_integrity_failed", "Transfer checksum or length mismatch"
                )
            if slot.destination_paths is None or slot.begin is None or slot.begin.dst_path is None:
                raise TransferOperationError(
                    "workspace_invalid_request", "Transfer destination is missing"
                )
            try:
                destination_unchanged = await self._run_filesystem(
                    _destination_parent_unchanged,
                    slot.destination_paths,
                    slot.begin.dst_path,
                    slot.destination,
                )
            except ToolFailure as exc:
                raise TransferOperationError(
                    exc.code, exc.args[0] if exc.args else exc.code
                ) from exc
            if not destination_unchanged:
                raise TransferOperationError(
                    "workspace_file_changed", "Destination parent changed during transfer"
                )
            if not await self._run_filesystem(
                _temporary_unchanged, slot.temporary, slot.temporary_identity
            ):
                raise TransferOperationError(
                    "workspace_file_changed", "Temporary destination changed during transfer"
                )
            current_fingerprint = await self._run_filesystem(
                _stat_fingerprint, slot.destination
            )
            if slot.purpose == "workspace_upload":
                if current_fingerprint != slot.destination_initial_fingerprint:
                    raise TransferOperationError(
                        "workspace_file_changed", "Destination changed during transfer"
                    )
                await self._run_filesystem(
                    _commit_replace,
                    slot.temporary,
                    slot.destination,
                    slot.temporary_identity,
                )
            else:
                if current_fingerprint is not None:
                    raise TransferOperationError(
                        "workspace_file_changed", "Destination changed during transfer"
                    )
                await self._run_filesystem(
                    _commit_no_replace,
                    slot.temporary,
                    slot.destination,
                    slot.temporary_identity,
                )
            slot.temporary = None
            slot.state = "COMMITTED"
            if slot.purpose == "workspace_upload":
                committed_etag = await self._run_filesystem(
                    _stat_fingerprint, slot.destination
                )
                if committed_etag is None or slot.destination_created is None:
                    raise TransferOperationError(
                        "workspace_storage_unavailable", "Committed destination is unavailable"
                    )
                self._enqueue_end(
                    slot,
                    ack=True,
                    ok=True,
                    bytes_sent=slot.received_bytes,
                    sha256=digest,
                    etag=committed_etag,
                    created=slot.destination_created,
                )
            else:
                self._enqueue_end(
                    slot,
                    ack=True,
                    ok=True,
                    bytes_sent=slot.received_bytes,
                    sha256=digest,
                )
            await self._finish_slot(slot)
        except asyncio.CancelledError:
            raise
        except TransferOperationError as exc:
            slot.state = "ABORTED"
            self._enqueue_end(
                slot,
                ack=slot.remote_end is not None,
                ok=False,
                code=exc.code,
            )
        except (OSError, ValueError, TypeError) as exc:
            slot.state = "ABORTED"
            self._enqueue_end(
                slot,
                ack=slot.remote_end is not None,
                ok=False,
                code="workspace_storage_unavailable",
            )
            del exc
        finally:
            if file_handle is not None:
                with contextlib.suppress(OSError):
                    await self._run_filesystem(file_handle.close)
            await self._cleanup_slot(slot)

    async def _await_rejection_ack(self, slot: _Slot) -> None:
        try:
            await asyncio.wait_for(slot.sender_ack.wait(), self._idle_timeout)
        except (TimeoutError, asyncio.CancelledError):
            pass
        finally:
            await self._cleanup_slot(slot)

    async def _send_requested(self, slot: _Slot) -> None:
        assert slot.request is not None
        request = slot.request
        fd: int | None = None
        try:
            try:
                source = await self._run_filesystem(
                    _resolve_source,
                    slot.snapshot,
                    request.src_path,
                )
            except ToolFailure as exc:
                raise TransferOperationError(exc.code, "Source path is unavailable") from exc
            source_lock = contextlib.AsyncExitStack()
            slot.lock_stack = source_lock
            await source_lock.enter_async_context(self._locks.hold(str(source)))
            fd, initial = await self._run_filesystem(
                _open_source,
                source,
                on_abandoned=_close_open_source_result,
            )
            slot.source_fd = fd
            slot.source_path = source
            slot.source_identity = initial
            self._writer.register_binary_lane(slot.slot_id)
            slot.state = "BEGUN"
            self._enqueue_critical(
                encode_frame(
                    TransferBegin(
                        id=slot.slot_id,
                        direction="client_to_server",
                        purpose=request.purpose,
                        src_device=slot.snapshot.device_name or None,
                        src_path=request.src_path,
                        dst_device="server" if request.purpose == "file_transfer" else None,
                        dst_path=request.dst_path,
                        total_bytes=initial[2],
                        etag=(
                            _fingerprint_from_identity(initial)
                            if request.purpose in {"file_transfer", "http_relay"}
                            else None
                        ),
                    )
                )
            )
            await asyncio.wait_for(slot.sender_ready.wait(), self._idle_timeout)
            hasher = hashlib.sha256()
            bytes_sent = 0
            while True:
                chunk = await self._run_filesystem(os.read, fd, TRANSFER_CHUNK_BYTES)
                if not chunk:
                    break
                hasher.update(chunk)
                bytes_sent += len(chunk)
                await self._writer.wait_enqueue_binary(slot.slot_id, chunk)
            await self._writer.drain_binary(slot.slot_id)
            if not await self._run_filesystem(_source_unchanged, source, fd, initial):
                raise TransferOperationError(
                    "workspace_file_changed", "Source changed during transfer"
                )
            digest = hasher.hexdigest()
            slot.state = "SENDER_ENDED"
            final_end = TransferEnd(
                id=slot.slot_id,
                ack=False,
                ok=True,
                bytes_sent=bytes_sent,
                sha256=digest,
            )
            self._enqueue_critical(encode_frame(final_end))
            slot.final_end = final_end
            await asyncio.wait_for(slot.sender_ack.wait(), self._idle_timeout)
            remote_end = slot.remote_end
            if remote_end is None or not remote_end.ok:
                if remote_end is not None and remote_end.ack:
                    slot.final_end = remote_end
                return
            if remote_end.bytes_sent != bytes_sent or remote_end.sha256 != digest:
                raise TransferOperationError(
                    "workspace_transfer_integrity_failed", "Receiver acknowledgement mismatched"
                )
            slot.final_end = remote_end
            await self._finish_slot(slot)
        except asyncio.CancelledError:
            raise
        except TransferOperationError as exc:
            had_begun = slot.source_fd is not None or slot.state != "REQUESTED"
            slot.state = "ABORTED"
            self._enqueue_end(slot, ok=False, code=exc.code, ack=False)
            await self._wait_for_failure_ack(slot, had_begun=had_begun)
        except (OSError, TimeoutError, WriterOverflowError) as exc:
            had_begun = slot.source_fd is not None or slot.state != "REQUESTED"
            slot.state = "ABORTED"
            code = (
                "workspace_transfer_timeout"
                if isinstance(exc, TimeoutError)
                else "workspace_storage_unavailable"
            )
            self._enqueue_end(slot, ok=False, code=code, ack=False)
            await self._wait_for_failure_ack(slot, had_begun=had_begun)
        except Exception as exc:
            had_begun = slot.source_fd is not None or slot.state != "REQUESTED"
            slot.state = "ABORTED"
            raw_code: object = getattr(exc, "code", None)
            code = (
                raw_code if isinstance(raw_code, str) and raw_code else "workspace_invalid_request"
            )
            self._enqueue_end(slot, ok=False, code=code, ack=False)
            await self._wait_for_failure_ack(slot, had_begun=had_begun)
        finally:
            if fd is not None:
                with contextlib.suppress(OSError):
                    await self._run_filesystem(os.close, fd)
            slot.source_fd = None
            if self._writer.has_binary_lane(slot.slot_id):
                with contextlib.suppress(WriterOverflowError):
                    await self._writer.drain_binary(slot.slot_id)
                    self._writer.unregister_binary_lane(slot.slot_id)
            await self._cleanup_slot(slot)

    def _enqueue_end(
        self,
        slot: _Slot,
        *,
        ok: bool,
        code: str | None = None,
        ack: bool,
        bytes_sent: int | None = None,
        sha256: str | None = None,
        etag: str | None = None,
        created: bool | None = None,
    ) -> None:
        end = TransferEnd(
            id=slot.slot_id,
            ack=ack,
            ok=ok,
            code=code,
            bytes_sent=bytes_sent,
            sha256=sha256,
            etag=etag,
            created=created,
        )
        self._enqueue_critical(encode_frame(end))
        slot.final_end = end

    async def _wait_for_failure_ack(self, slot: _Slot, *, had_begun: bool) -> None:
        """Keep a begun failed sender alive long enough to consume its ACK."""

        if not had_begun:
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(slot.sender_ack.wait(), self._idle_timeout)
        if slot.remote_end is not None and slot.remote_end.ack:
            slot.final_end = slot.remote_end

    def _send_terminal_rejection(self, slot_id: UUID, code: str) -> None:
        end = TransferEnd(id=slot_id, ack=False, ok=False, code=code)
        self._enqueue_critical(encode_frame(end))
        self._remember_tombstone(slot_id, end)

    async def _finish_slot(self, slot: _Slot) -> None:
        self._remember_tombstone(slot.slot_id, slot.final_end)

    async def _cleanup_slot(self, slot: _Slot) -> None:
        if slot.temporary is not None:
            await self._run_filesystem(_unlink_temporary, slot.temporary)
            slot.temporary = None
        if slot.lock_stack is not None:
            await slot.lock_stack.aclose()
            slot.lock_stack = None
        self._slots.pop(slot.slot_id, None)
        if slot.final_end is not None:
            self._remember_tombstone(slot.slot_id, slot.final_end)

    def _remember_tombstone(self, slot_id: UUID, end: TransferEnd | None) -> None:
        if end is None:
            return
        self._tombstones.pop(slot_id, None)
        self._tombstones[slot_id] = _Tombstone(
            ack=end.ack,
            ok=end.ok,
            code=end.code,
            bytes_sent=end.bytes_sent,
            sha256=end.sha256,
            etag=end.etag,
            created=end.created,
            expires_at=asyncio.get_running_loop().time() + TOMBSTONE_TTL_SECONDS,
        )
        while len(self._tombstones) > TOMBSTONE_MAX_ENTRIES:
            self._tombstones.pop(next(iter(self._tombstones)))

    def _purge_tombstones(self) -> None:
        now = asyncio.get_running_loop().time()
        for slot_id, tombstone in tuple(self._tombstones.items()):
            if tombstone.expires_at <= now:
                self._tombstones.pop(slot_id, None)

    @staticmethod
    def _same_terminal(tombstone: _Tombstone, end: TransferEnd) -> bool:
        exact = (
            tombstone.ack == end.ack
            and tombstone.ok == end.ok
            and tombstone.code == end.code
            and tombstone.bytes_sent == end.bytes_sent
            and tombstone.sha256 == end.sha256
            and tombstone.etag == end.etag
            and tombstone.created == end.created
        )
        if exact:
            return True
        # Either role may send the first failure terminal before cleaning its
        # slot.  Its peer returns the same payload as an ACK, which can arrive
        # after only the tombstone remains.
        return (
            not tombstone.ack
            and end.ack
            and tombstone.ok == end.ok
            and tombstone.code == end.code
            and tombstone.bytes_sent == end.bytes_sent
            and tombstone.sha256 == end.sha256
            and tombstone.etag == end.etag
            and tombstone.created == end.created
        )

    @staticmethod
    def _ack_matches_terminal(slot: _Slot, end: TransferEnd) -> bool:
        terminal = slot.final_end
        return (
            terminal is not None
            and terminal.ok == end.ok
            and terminal.code == end.code
            and terminal.bytes_sent == end.bytes_sent
            and terminal.sha256 == end.sha256
            and terminal.etag == end.etag
            and terminal.created == end.created
        )


def _resolve_destination(
    snapshot: TransferConfigSnapshot,
    dst_path: str,
) -> tuple[WorkspacePaths, Path]:
    paths = WorkspacePaths(
        snapshot.workspace_path,
        sandbox_mode=snapshot.sandbox_mode,
    )
    return paths, paths.resolve(dst_path, directory=None)


def _prepare_destination(
    paths: WorkspacePaths,
    destination: Path,
    purpose: str,
    if_match: str | None,
    if_none_match: bool | None,
) -> _DestinationReservation:
    initial_fingerprint = _stat_fingerprint(destination)
    if purpose == "workspace_upload":
        if if_match is not None and initial_fingerprint != if_match:
            raise TransferOperationError(
                "workspace_file_changed", "Destination does not match If-Match"
            )
        if if_none_match is True and initial_fingerprint is not None:
            raise TransferOperationError(
                "workspace_file_changed", "Destination already exists"
            )
    elif initial_fingerprint is not None:
        # Ordinary file_transfer remains create-without-overwrite.
        raise TransferOperationError("workspace_file_changed", "Destination already exists")
    paths.prepare_parent(destination)
    if not destination.parent.is_dir():
        raise TransferOperationError(
            "tool_not_a_directory", "Destination parent is not a directory"
        )
    return _DestinationReservation(
        initial_fingerprint=initial_fingerprint,
        created=initial_fingerprint is None,
        temporary=_create_temp(destination.parent, destination.name),
    )


def _resolve_source(snapshot: TransferConfigSnapshot, src_path: str) -> Path:
    paths = WorkspacePaths(
        snapshot.workspace_path,
        sandbox_mode=snapshot.sandbox_mode,
    )
    return paths.resolve(src_path, directory=False)


def _destination_parent_unchanged(
    paths: WorkspacePaths,
    path: str,
    destination: Path,
) -> bool:
    return paths.resolve(path, directory=None) == destination and destination.parent.is_dir()


def _discard_destination_reservation(reservation: _DestinationReservation) -> None:
    _unlink_temporary(reservation.temporary)


def _unlink_temporary(path: Path) -> None:
    """Remove a temporary entry, tolerating only transient Windows sharing errors."""

    for attempt in range(_TEMPORARY_UNLINK_ATTEMPTS):
        try:
            os.unlink(path)
            return
        except FileNotFoundError:
            return
        except OSError as exc:
            if getattr(exc, "winerror", None) not in {32, 33}:
                return
            if attempt + 1 == _TEMPORARY_UNLINK_ATTEMPTS:
                return
            time.sleep(_TEMPORARY_UNLINK_RETRY_SECONDS)


def _close_open_source_result(
    result: tuple[int, tuple[int, int, int, int, int]],
) -> None:
    with contextlib.suppress(OSError):
        os.close(result[0])


def _close_open_temp_result(
    result: tuple[Any, tuple[int, int, int, int, int]],
) -> None:
    with contextlib.suppress(BaseException):
        result[0].close()


def _lexists(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def _create_temp(parent: Path, name: str) -> Path:
    for _ in range(16):
        candidate = parent / f".{name}.openoctopus-{secrets.token_hex(12)}.tmp"
        try:
            fd = os.open(
                candidate,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | int(getattr(os, "O_NOFOLLOW", 0)),
                0o600,
            )
            os.close(fd)
            return candidate
        except FileExistsError:
            continue
    raise TransferOperationError(
        "workspace_storage_unavailable", "Temporary destination unavailable"
    )


def _open_temp(path: Path) -> tuple[Any, tuple[int, int, int, int, int]]:
    """Open the already-created temp by fd without following a replacement symlink."""

    flags = (
        os.O_WRONLY
        | os.O_TRUNC
        | int(getattr(os, "O_BINARY", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
    )
    fd: int | None = None
    try:
        fd = os.open(path, flags)
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise TransferOperationError(
                "workspace_storage_unavailable", "Temporary destination is not a file"
            )
        handle = os.fdopen(fd, "wb")
        fd = None
        return handle, _identity(info)
    except TransferOperationError:
        raise
    except OSError as exc:
        raise TransferOperationError(
            "workspace_storage_unavailable", "Temporary destination is unavailable"
        ) from exc
    finally:
        if fd is not None:
            with contextlib.suppress(OSError):
                os.close(fd)


def _temporary_unchanged(
    path: Path, expected: tuple[int, int, int, int, int] | None
) -> bool:
    if expected is None:
        return False
    try:
        info = os.lstat(path)
    except OSError:
        return False
    return stat.S_ISREG(info.st_mode) and _identity(info) == expected


def _identity_after_close(
    path: Path,
    open_identity: tuple[int, int, int, int, int],
    expected_bytes: int,
    expected_sha256: str,
) -> tuple[int, int, int, int, int]:
    flags = (
        os.O_RDONLY
        | int(getattr(os, "O_BINARY", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
        | int(getattr(os, "O_NONBLOCK", 0))
    )
    fd: int | None = None
    try:
        path_before_info = os.lstat(path)
        path_before = _identity(path_before_info)
        if (
            not stat.S_ISREG(path_before_info.st_mode)
            or path_before[:3] != open_identity[:3]
        ):
            raise TransferOperationError(
                "workspace_file_changed", "Temporary destination changed during transfer"
            )

        fd = os.open(path, flags)
        handle_before_info = os.fstat(fd)
        handle_before = _identity(handle_before_info)
        if (
            not stat.S_ISREG(handle_before_info.st_mode)
            or handle_before[:4] != path_before[:4]
        ):
            raise TransferOperationError(
                "workspace_file_changed", "Temporary destination changed during transfer"
            )

        digest = hashlib.sha256()
        bytes_read = 0
        while chunk := os.read(fd, TRANSFER_CHUNK_BYTES):
            digest.update(chunk)
            bytes_read += len(chunk)

        handle_after_info = os.fstat(fd)
        path_after_info = os.lstat(path)
        handle_after = _identity(handle_after_info)
        path_after = _identity(path_after_info)
        if (
            not stat.S_ISREG(handle_after_info.st_mode)
            or not stat.S_ISREG(path_after_info.st_mode)
            or handle_after != handle_before
            or path_after != path_before
            or path_after[:4] != handle_after[:4]
            or bytes_read != expected_bytes
            or digest.hexdigest() != expected_sha256
        ):
            raise TransferOperationError(
                "workspace_file_changed", "Temporary destination changed during transfer"
            )
        return path_after
    except TransferOperationError:
        raise
    except OSError as exc:
        raise TransferOperationError(
            "workspace_file_changed", "Temporary destination changed during transfer"
        ) from exc
    finally:
        if fd is not None:
            with contextlib.suppress(OSError):
                os.close(fd)


def _commit_no_replace(
    temporary: Path,
    destination: Path,
    expected_identity: tuple[int, int, int, int, int] | None = None,
) -> None:
    if not _temporary_unchanged(temporary, expected_identity):
        raise TransferOperationError(
            "workspace_file_changed", "Temporary destination changed during transfer"
        )
    linked = False
    try:
        os.link(temporary, destination, follow_symlinks=False)
        linked = True
    except FileExistsError as exc:
        raise TransferOperationError(
            "workspace_file_changed", "Destination already exists"
        ) from exc
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()
    if linked:
        _fsync_parent(destination.parent)


def _commit_replace(
    temporary: Path,
    destination: Path,
    expected_identity: tuple[int, int, int, int, int] | None = None,
) -> None:
    """Publish an upload atomically after the in-process checks complete.

    The path lock protects cooperating client work.  A non-cooperating process
    can still race the final check with ``os.replace`` on portable filesystems;
    the caller deliberately does not claim a cross-process transaction here.
    """

    if not _temporary_unchanged(temporary, expected_identity):
        raise TransferOperationError(
            "workspace_file_changed", "Temporary destination changed during transfer"
        )
    os.replace(temporary, destination)
    _fsync_parent(destination.parent)


def _fsync_parent(parent: Path) -> None:
    # Directory fsync makes the rename durable on POSIX.  Windows and some
    # filesystems reject opening directories; the atomic replace still holds.
    if os.name == "nt":
        return
    fd: int | None = None
    try:
        fd = os.open(parent, os.O_RDONLY | int(getattr(os, "O_DIRECTORY", 0)))
        os.fsync(fd)
    except OSError:
        return
    finally:
        if fd is not None:
            with contextlib.suppress(OSError):
                os.close(fd)


def _open_source(path: Path) -> tuple[int, tuple[int, int, int, int, int]]:
    try:
        initial_info = os.lstat(path)
    except FileNotFoundError as exc:
        raise TransferOperationError("workspace_not_found", "Source file was not found") from exc
    except OSError as exc:
        raise TransferOperationError(
            "workspace_permission_denied", "Source file is unavailable"
        ) from exc
    if stat.S_ISLNK(initial_info.st_mode):
        raise TransferOperationError("workspace_symlink_escape", "Source path is a symbolic link")
    if not stat.S_ISREG(initial_info.st_mode):
        raise TransferOperationError("workspace_blocked_path", "Source is not a regular file")
    flags = (
        os.O_RDONLY
        | int(getattr(os, "O_BINARY", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
        | int(getattr(os, "O_NONBLOCK", 0))
    )
    try:
        fd = os.open(path, flags)
    except FileNotFoundError as exc:
        raise TransferOperationError("workspace_not_found", "Source file was not found") from exc
    except OSError as exc:
        raise TransferOperationError(
            "workspace_permission_denied", "Source file is unavailable"
        ) from exc
    try:
        info = os.fstat(fd)
    except OSError:
        os.close(fd)
        raise
    if not stat.S_ISREG(info.st_mode):
        os.close(fd)
        raise TransferOperationError("workspace_blocked_path", "Source is not a regular file")
    return fd, _identity(info)


def _identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_size,
        info.st_mtime_ns,
        getattr(info, "st_ctime_ns", 0),
    )


def _fingerprint_from_identity(identity: tuple[int, int, int, int, int]) -> str:
    """Hash stat identity so inode/device numbers never cross the wire."""

    return opaque_stat_fingerprint(identity[:4])


def _stat_fingerprint(path: Path) -> str | None:
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise TransferOperationError(
            "workspace_permission_denied", "Destination is unavailable"
        ) from exc
    if stat.S_ISLNK(info.st_mode):
        raise TransferOperationError("workspace_symlink_escape", "Path is a symbolic link")
    if not stat.S_ISREG(info.st_mode):
        raise TransferOperationError("workspace_blocked_path", "Destination is not a regular file")
    return _fingerprint_from_identity(_identity(info))


def _source_unchanged(path: Path, fd: int, initial: tuple[int, int, int, int, int]) -> bool:
    try:
        handle_identity = _identity(os.fstat(fd))
        path_identity = _identity(os.stat(path, follow_symlinks=False))
        # Windows can expose a slightly different creation-time value through
        # an open descriptor and a path stat.  The descriptor still proves the
        # opened file's full identity, while the path check proves it was not
        # replaced; do not reject that representation-only ctime skew.
        return handle_identity == initial and path_identity[:4] == initial[:4]
    except OSError:
        return False


async def receive_chunks(chunks: AsyncIterator[bytes], manager: TransferManager) -> None:
    """Small adapter for HTTP/WebSocket bridges that already own a chunk stream."""

    async for chunk in chunks:
        await manager.handle_binary(chunk)


async def _to_thread_safely(
    function: Any,
    *args: Any,
    tracker: set[asyncio.Task[Any]] | None = None,
    on_abandoned: Callable[[Any], Any] | None = None,
) -> Any:
    """Bound cancellation while a worker-thread filesystem call is running."""

    task = asyncio.create_task(asyncio.to_thread(function, *args))
    abandoned = False
    cleanup_started = False

    def cleanup_abandoned_result(completed: asyncio.Task[Any]) -> None:
        nonlocal cleanup_started
        if (
            not abandoned
            or cleanup_started
            or on_abandoned is None
            or completed.cancelled()
        ):
            return
        cleanup_started = True
        try:
            result = completed.result()
        except BaseException:
            return
        cleanup_task = asyncio.create_task(asyncio.to_thread(on_abandoned, result))
        if tracker is None:
            cleanup_task.add_done_callback(_consume_thread_result)
        else:
            tracker.add(cleanup_task)
            cleanup_task.add_done_callback(
                lambda completed_cleanup: _finish_tracked_thread(
                    completed_cleanup, tracker
                )
            )

    if tracker is None:
        task.add_done_callback(_consume_thread_result)
        task.add_done_callback(cleanup_abandoned_result)
    else:
        tracker.add(task)

        def finish_tracked_thread(completed: asyncio.Task[Any]) -> None:
            tracker.discard(completed)
            _consume_thread_result(completed)
            cleanup_abandoned_result(completed)

        task.add_done_callback(finish_tracked_thread)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        abandoned = True
        if task.done():
            cleanup_abandoned_result(task)
        with contextlib.suppress(BaseException):
            await asyncio.wait_for(
                asyncio.shield(task), _TO_THREAD_CANCEL_GRACE_SECONDS
            )
        if task.done():
            cleanup_abandoned_result(task)
        raise


def _consume_future_exception(future: asyncio.Future[Any]) -> None:
    if future.cancelled():
        return
    with contextlib.suppress(BaseException):
        future.exception()


def _consume_thread_result(task: asyncio.Task[Any]) -> None:
    if task.cancelled():
        return
    with contextlib.suppress(BaseException):
        task.exception()


def _finish_tracked_thread(task: asyncio.Task[Any], tracker: set[asyncio.Task[Any]]) -> None:
    tracker.discard(task)
    _consume_thread_result(task)
