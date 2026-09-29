"""Device transfer API, direct transfer lifecycle, and shared shutdown coordination."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Literal
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

from .transfer_admission import FairTransferAdmission, TransferLease
from .transfer_bridge import BridgeTransfers
from .transfer_io import send_transfer_text
from .transfer_slots import TransferSlots
from .transfer_types import (
    DEFAULT_TOMBSTONE_TTL_SECONDS,
    TRANSFER_TIMEOUT_CODE,
    BridgeRole,
    CommitSink,
    DeleteBridgeSource,
    DeleteSource,
    SinkFactory,
    SourceFactory,
    TransferCommitResult,
    TransferCommittedAfterCancellation,
    TransferDisconnectedError,
    TransferError,
    TransferIntegrityError,
    TransferProtocolError,
    TransferResult,
    TransferRoute,
    TransferSource,
    TransferState,
    TransferTransport,
    TransferUnavailableError,
    _completed_transfer_result,
    _error_code,
    _fenced_transfer_error,
    _handle_identity,
    _same_handle,
    _source_etag,
    _TransferSlot,
)


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
        self._state = TransferSlots()
        self._source_cleanup_tasks: set[asyncio.Task[None]] = set()
        self._bridge = BridgeTransfers(
            transport,
            admission=admission,
            state=self._state,
            idle_timeout_seconds=idle_timeout_seconds,
            tombstone_ttl_seconds=tombstone_ttl_seconds,
        )

    @property
    def active_slots(self) -> int:
        return len(self._state.slots) + len(self._state.bridges)

    @property
    def slot_ids(self) -> tuple[UUID, ...]:
        return tuple(slot.slot_id for slot in self._state.slots.values()) + tuple(
            self._state.bridges
        )

    @property
    def idle_timeout_seconds(self) -> float:
        """Timeout shared by file slots and private directory controllers."""

        return self._idle_timeout_seconds

    async def acquire_operation(self, user_id: UUID) -> TransferLease:
        """Acquire one transfer credit owned by a multi-file coordinator."""

        return await self._admission.acquire(user_id)

    def fence_handle(self, handle: object) -> None:
        """Synchronously prevent a retired generation from making more progress."""

        for slot in tuple(self._state.slots.values()):
            if _same_handle(slot.handle, handle):
                self._fence_slot(slot)
        for bridge in tuple(self._state.bridges.values()):
            if _same_handle(bridge.source_route.handle, handle):
                self._bridge.fence(bridge, BridgeRole.SOURCE)
            if _same_handle(bridge.destination_route.handle, handle):
                self._bridge.fence(bridge, BridgeRole.DESTINATION)

    def fence_route(self, route: TransferRoute) -> None:
        """Fence only slots that have not crossed their initial send boundary."""

        for slot in tuple(self._state.slots.values()):
            if slot.route == route:
                self._fence_slot(slot)
        for bridge in tuple(self._state.bridges.values()):
            if bridge.source_route == route and not bridge.source_issued:
                self._bridge.fence(bridge, BridgeRole.SOURCE)
            if bridge.destination_route == route and not bridge.destination_issued:
                self._bridge.fence(bridge, BridgeRole.DESTINATION)

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
            _operation_lease.validate(
                self._admission,
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
                        raise TransferProtocolError("transfer source did not declare its size")
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
                etag=(slot.source_etag if purpose in {"file_transfer", "http_relay"} else None),
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
            if slot.state is TransferState.SENDER_ENDED and slot.worker is not None:
                try:
                    await await_future_cancellation_safe(slot.worker)
                except asyncio.CancelledError:
                    pass
            else:
                try:
                    await self._abort(slot, "cancelled", send_frame=True)
                except asyncio.CancelledError:
                    pass
            committed = _completed_transfer_result(slot.completion)
            if committed is not None:
                raise TransferCommittedAfterCancellation(committed) from None
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

    async def start_server_to_client_regular_admitted(
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
        mode: str,
        delete_source: DeleteSource | None,
        src_device: str = "server",
        dst_device: str | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> TransferResult:
        """Run one regular file while the caller retains operation admission."""

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
            mode=mode,
            delete_source=delete_source,
            src_device=src_device,
            dst_device=dst_device,
            on_issued=on_issued,
            _slot_id=slot_id,
            _operation_lease=operation_lease,
            _directory_child=False,
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
            _operation_lease.validate(
                self._admission,
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
            if not await send_transfer_text(
                self._transport,
                handle,
                request.model_dump_json(),
                route=slot.route,
                on_issued=self._initial_issue_callback(slot),
            ):
                raise TransferUnavailableError("device route was unavailable before send")
            slot.route = None
            return await asyncio.shield(slot.completion)
        except asyncio.CancelledError:
            try:
                await self._abort(slot, "cancelled", send_frame=True)
            except asyncio.CancelledError:
                pass
            committed = _completed_transfer_result(slot.completion)
            if committed is not None:
                raise TransferCommittedAfterCancellation(committed) from None
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

    async def start_client_to_server_regular_admitted(
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
        mode: str,
        delete_source: DeleteSource | None,
        on_issued: Callable[[], None] | None = None,
    ) -> TransferResult:
        """Run one regular file while the caller retains operation admission."""

        return await self.start_client_to_server(
            handle=handle,
            route=route,
            user_id=user_id,
            src_path=src_path,
            dst_path=dst_path,
            sink_factory=sink_factory,
            commit_sink=commit_sink,
            delete_source=delete_source,
            purpose=purpose,
            mode=mode,
            on_issued=on_issued,
            _slot_id=slot_id,
            _operation_lease=operation_lease,
            _directory_child=False,
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
        _expected_source_size: int | None = None,
        _expected_source_fingerprint: str | None = None,
    ) -> TransferResult:
        return await self._bridge.start_client_to_client(
            source_route=source_route,
            destination_route=destination_route,
            user_id=user_id,
            src_path=src_path,
            dst_path=dst_path,
            mode=mode,
            delete_source=delete_source,
            on_issued=on_issued,
            _slot_id=_slot_id,
            _operation_lease=_operation_lease,
            _directory_child=_directory_child,
            _expected_source_size=_expected_source_size,
            _expected_source_fingerprint=_expected_source_fingerprint,
        )

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
        expected_source_size: int,
        expected_source_fingerprint: str,
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
            _expected_source_size=expected_source_size,
            _expected_source_fingerprint=expected_source_fingerprint,
        )

    async def start_client_to_client_regular_admitted(
        self,
        *,
        source_route: TransferRoute,
        destination_route: TransferRoute,
        operation_lease: TransferLease,
        slot_id: UUID,
        user_id: UUID,
        src_path: str,
        dst_path: str,
        expected_source_size: int,
        expected_source_fingerprint: str,
        mode: Literal["copy", "move"],
        delete_source: DeleteBridgeSource | None,
        on_issued: Callable[[], None] | None,
    ) -> TransferResult:
        """Relay one regular file while the caller retains operation admission."""

        return await self.start_client_to_client(
            source_route=source_route,
            destination_route=destination_route,
            user_id=user_id,
            src_path=src_path,
            dst_path=dst_path,
            mode=mode,
            delete_source=delete_source,
            on_issued=on_issued,
            _slot_id=slot_id,
            _operation_lease=operation_lease,
            _directory_child=False,
            _expected_source_size=expected_source_size,
            _expected_source_fingerprint=expected_source_fingerprint,
        )

    async def handle_frame(self, handle: object, frame: object) -> None:
        """Route one already-validated client transfer control frame."""
        if await self._bridge.handle_frame(handle, frame):
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
        if await self._bridge.handle_binary(handle, slot_id, chunk):
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
        slots = [slot for slot in self._state.slots.values() if _same_handle(slot.handle, handle)]
        bridges = [
            bridge
            for bridge in self._state.bridges.values()
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
                self._bridge.disconnect(
                    bridge,
                    "peer_disconnected",
                )
                for bridge in bridges
            ),
            return_exceptions=True,
        )
        await self._bridge.wait_for_cleanup()

    async def close(self) -> None:
        slots = list(self._state.slots.values())
        bridges = list(self._state.bridges.values())
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
                self._bridge.disconnect(
                    bridge,
                    "server_shutdown",
                )
                for bridge in bridges
            ),
            return_exceptions=True,
        )
        await self._bridge.wait_for_cleanup()

    async def _send_server_source(self, slot: _TransferSlot) -> None:
        assert slot.source is not None
        assert slot.begin is not None
        assert slot.ready_future is not None
        assert slot.ack_future is not None
        try:
            if not await send_transfer_text(
                self._transport,
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
            if not await send_transfer_text(
                self._transport, slot.handle, end.model_dump_json(), route=slot.route
            ):
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
            if (
                slot.purpose != "workspace_upload"
                and not slot.directory_child
                and (ack.etag is not None or ack.created is not None)
            ):
                raise TransferProtocolError("transfer metadata is only valid for workspace_upload")
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
            if not await send_transfer_text(
                self._transport, slot.handle, ready.model_dump_json(), route=slot.route
            ):
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
                    else slot.begin.etag
                    if slot.purpose == "http_relay"
                    else None
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
                    ack_delivered = await send_transfer_text(
                        self._transport,
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
                    else slot.begin.etag
                    if slot.purpose == "http_relay"
                    else None
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
                    await send_transfer_text(
                        self._transport,
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
                await send_transfer_text(
                    self._transport,
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
                    await send_transfer_text(
                        self._transport,
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
            await send_transfer_text(
                self._transport, slot.handle, ack.model_dump_json(), route=slot.route
            )
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
            async with self._state.lock:
                self._state.expire_tombstones_locked()
                key = (device_id, generation, slot.slot_id)
                if self._state.key_in_use_locked(key):
                    raise TransferProtocolError("transfer slot id collided with an active slot")
                self._state.slots[key] = slot
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
        async with self._state.lock:
            self._state.expire_tombstones_locked()
            slot = self._state.slots.get(key)
            if slot is not None:
                if slot.fenced:
                    raise TransferDisconnectedError("device route was replaced")
                return slot
            if key in self._state.tombstones and terminal:
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
        async with self._state.lock:
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
                self._state.remember_tombstone_locked(
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
                await send_transfer_text(
                    self._transport,
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
        async with self._state.lock:
            self._state.slots.pop(key, None)
            if key not in self._state.tombstones:
                self._state.remember_tombstone_locked(
                    key,
                    (
                        time.monotonic() + self._tombstone_ttl_seconds,
                        slot.terminal_ack,
                        slot.direction == "client_to_server" and slot.committed_result is not None,
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

    async def _matches_tombstone(self, handle: object, frame: TransferEndFrame) -> bool:
        device_id, generation = _handle_identity(handle)
        key = (device_id, generation, frame.id)
        async with self._state.lock:
            self._state.expire_tombstones_locked()
            tombstone = self._state.tombstones.get(key)
            if tombstone is None:
                return False
            _, expected, _ = tombstone
            if expected == frame:
                self._state.acknowledged_failure_tombstones.add(key)
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
        async with self._state.lock:
            self._state.expire_tombstones_locked()
            slot = self._state.slots.get(key)
            if (
                slot is not None
                and slot.direction == "client_to_server"
                and slot.committed_result is not None
            ):
                return True, slot.route
            tombstone = self._state.tombstones.get(key)
            return tombstone is not None and tombstone[2], None

    async def _is_failed_tombstone(self, handle: object, slot_id: UUID) -> bool:
        device_id, generation = _handle_identity(handle)
        key = (device_id, generation, slot_id)
        async with self._state.lock:
            self._state.expire_tombstones_locked()
            tombstone = self._state.tombstones.get(key)
            if tombstone is None:
                return False
            _, expected_ack, _ = tombstone
            return (
                key not in self._state.acknowledged_failure_tombstones
                and expected_ack is not None
                and expected_ack.ack
                and not expected_ack.ok
            )
