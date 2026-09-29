"""Client-to-client relay state machine and late endpoint handling."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any, Literal
from uuid import UUID

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.devices.protocol import (
    MAX_BINARY_CHUNK_BYTES,
    TransferBeginFrame,
    TransferEndFrame,
    TransferProgressFrame,
    TransferReadyFrame,
    TransferRequestFrame,
    new_uuid7,
)

from .transfer_admission import FairTransferAdmission, TransferLease
from .transfer_io import send_transfer_text
from .transfer_slots import TransferSlots
from .transfer_types import (
    BRIDGE_SOURCE_DELETE_TIMEOUT_SECONDS,
    TRANSFER_TIMEOUT_CODE,
    BridgeRole,
    BridgeState,
    DeleteBridgeSource,
    SourceResolution,
    TransferCommittedAfterCancellation,
    TransferDisconnectedError,
    TransferError,
    TransferIntegrityError,
    TransferProtocolError,
    TransferResult,
    TransferRoute,
    TransferTransport,
    TransferUnavailableError,
    _ack_resolves_success_terminal,
    _BridgeSlot,
    _BridgeTombstone,
    _completed_transfer_result,
    _error_code,
    _handle_identity,
    _is_sender_timeout,
)


class BridgeTransfers:
    """Own relay transitions while sharing the manager's slot and admission budgets."""

    def __init__(
        self,
        transport: TransferTransport,
        *,
        admission: FairTransferAdmission,
        state: TransferSlots,
        idle_timeout_seconds: float,
        tombstone_ttl_seconds: float,
    ) -> None:
        self._transport = transport
        self._admission = admission
        self._state = state
        self._idle_timeout_seconds = idle_timeout_seconds
        self._tombstone_ttl_seconds = tombstone_ttl_seconds
        self._bridge_worker_cleanup_tasks: set[asyncio.Task[None]] = set()

    async def wait_for_cleanup(self) -> None:
        if self._bridge_worker_cleanup_tasks:
            await asyncio.gather(*tuple(self._bridge_worker_cleanup_tasks), return_exceptions=True)

    @staticmethod
    def fence(bridge: _BridgeSlot, role: BridgeRole) -> None:
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
        """Relay one file directly between two current device generations."""

        if mode not in {"copy", "move"}:
            raise ValueError("transfer mode must be copy or move")
        has_expected_source = (
            _expected_source_size is not None and _expected_source_fingerprint is not None
        )
        if (_expected_source_size is None) != (_expected_source_fingerprint is None) or (
            _directory_child and not has_expected_source
        ):
            raise ValueError("pre-probed bridge source requires expected size and fingerprint")
        if _expected_source_size is not None and _expected_source_size < 0:
            raise ValueError("pre-probed source size is invalid")
        if _expected_source_fingerprint is not None and (
            not 1 <= len(_expected_source_fingerprint) <= 512
            or not _expected_source_fingerprint.isascii()
            or any(not 0x21 <= ord(character) <= 0x7E for character in _expected_source_fingerprint)
        ):
            raise ValueError("pre-probed source fingerprint is invalid")
        source_identity = _handle_identity(source_route.handle)
        destination_identity = _handle_identity(destination_route.handle)
        if source_identity[0] == destination_identity[0]:
            raise ValueError("client bridge requires two distinct devices")

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
                expected_source_size=_expected_source_size,
                expected_source_fingerprint=_expected_source_fingerprint,
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
                try:
                    await await_future_cancellation_safe(bridge.worker)
                except asyncio.CancelledError:
                    pass
            else:
                try:
                    await self._abort_bridge(
                        bridge,
                        "cancelled",
                        error=TransferError("cancelled"),
                    )
                except asyncio.CancelledError:
                    pass
            committed = _completed_transfer_result(bridge.completion)
            if committed is not None:
                raise TransferCommittedAfterCancellation(committed) from None
            raise

    async def disconnect(self, bridge: _BridgeSlot, code: str) -> None:
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
        expected_source_size: int | None = None,
        expected_source_fingerprint: str | None = None,
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
            expected_source_size=expected_source_size,
            expected_source_fingerprint=expected_source_fingerprint,
            directory_child=directory_child,
        )
        source_device, source_generation = _handle_identity(source_route.handle)
        destination_device, destination_generation = _handle_identity(destination_route.handle)
        source_key = (source_device, source_generation, slot_id)
        destination_key = (destination_device, destination_generation, slot_id)
        async with self._state.lock:
            self._state.expire_tombstones_locked()
            if (
                slot_id in self._state.bridges
                or self._state.key_in_use_locked(source_key)
                or self._state.key_in_use_locked(destination_key)
            ):
                raise TransferProtocolError("transfer slot id collided with an active slot")
            self._state.reserve_tombstone_credits_locked(2)
            bridge.tombstone_credits = 2
            self._state.bridges[slot_id] = bridge
            self._state.bridge_endpoints[source_key] = (bridge, BridgeRole.SOURCE)
            self._state.bridge_endpoints[destination_key] = (bridge, BridgeRole.DESTINATION)
        return bridge

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
        async with self._state.lock:
            return self._state.bridge_endpoints.get((device_id, generation, slot_id))

    async def _handle_bridge_tombstone_frame(self, handle: object, frame: object) -> bool:
        frame_id = getattr(frame, "id", None)
        if not isinstance(frame_id, UUID):
            return False
        device_id, generation = _handle_identity(handle)
        key = (device_id, generation, frame_id)
        timeout_ack: TransferEndFrame | None = None
        simultaneous_failure_ack: TransferEndFrame | None = None
        async with self._state.lock:
            self._state.expire_tombstones_locked()
            tombstone = self._state.bridge_tombstones.get(key)
            if tombstone is None:
                return False
            if isinstance(frame, TransferProgressFrame):
                if (
                    tombstone.role is BridgeRole.SOURCE
                    and tombstone.failed
                    and (
                        tombstone.source_ready_issued
                        or (tombstone.bridge is not None and tombstone.bridge.source_ready_issued)
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
                delivered = await send_transfer_text(
                    self._transport,
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
        async with self._state.lock:
            current = self._state.bridge_tombstones.get(key)
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
        async with self._state.lock:
            current = self._state.bridge_tombstones.get(key)
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
        async with self._state.lock:
            self._state.expire_tombstones_locked()
            tombstone = self._state.bridge_tombstones.get(key)
            if tombstone is None:
                return False
            if (
                tombstone.role is BridgeRole.SOURCE
                and tombstone.failed
                and (
                    tombstone.source_ready_issued
                    or (tombstone.bridge is not None and tombstone.bridge.source_ready_issued)
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
            if not await send_transfer_text(
                self._transport,
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
            if not await send_transfer_text(
                self._transport,
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
            if not await send_transfer_text(
                self._transport,
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
            if not await send_transfer_text(
                self._transport,
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
                source_ack = destination_ack.model_copy(update={"etag": None, "created": None})
            bridge.source_ack_delivered = await send_transfer_text(
                self._transport,
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
                    error = TransferUnavailableError("device route was unavailable before send")
                await self._abort_bridge(
                    bridge,
                    (error.code if bridge.abort_event.is_set() else "cancelled"),
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
                    send_transfer_text(
                        self._transport,
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
            or (
                frame.src_device is not None and frame.src_device != bridge.source_route.device_name
            )
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
        if (
            bridge.expected_source_size is not None
            and frame.total_bytes != bridge.expected_source_size
        ) or (
            bridge.expected_source_fingerprint is not None
            and frame.etag != bridge.expected_source_fingerprint
        ):
            bridge.source_begin_future.set_exception(TransferError("workspace_file_changed"))
            return
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
            if not await send_transfer_text(
                self._transport,
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
            declared = bridge.source_begin.total_bytes if bridge.source_begin is not None else None
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
                    error=TransferIntegrityError("source terminal did not match relayed bytes"),
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
                raise TransferProtocolError(
                    "destination terminal arrived in an invalid bridge state"
                )
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
            declared = bridge.source_begin.total_bytes if bridge.source_begin is not None else None
            if (
                role is BridgeRole.SOURCE
                and bridge.source_ready_issued
                and chunk
                and declared is not None
                and bridge.bytes_received + bridge.late_binary_bytes + len(chunk) <= declared
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
                error=TransferIntegrityError("source bytes exceeded the declared transfer size"),
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
                ack_task = asyncio.create_task(self._send_bridge_source_timeout_ack(bridge, frame))
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
                delivered = await send_transfer_text(
                    self._transport,
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
        async with self._state.lock:
            tombstone = self._state.bridge_tombstones.get(
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
            if not await send_transfer_text(
                self._transport,
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
            if not await send_transfer_text(
                self._transport,
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
                            etag=(destination_ack.etag if bridge.directory_child else None),
                            created=(destination_ack.created if bridge.directory_child else None),
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
                    await send_transfer_text(
                        self._transport,
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
                    await send_transfer_text(
                        self._transport,
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
                    await send_transfer_text(
                        self._transport,
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
                    await send_transfer_text(
                        self._transport,
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
                    send_transfer_text(
                        self._transport,
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
                    send_transfer_text(
                        self._transport,
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
                await asyncio.gather(*(asyncio.shield(future) for future in acknowledgements))
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
        async with self._state.lock:
            self._state.bridges.pop(bridge.slot_id, None)
            self._state.bridge_endpoints.pop(
                (source_device, source_generation, bridge.slot_id),
                None,
            )
            self._state.bridge_endpoints.pop(
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
        async with self._state.lock:
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
                if bridge.tombstone_credits <= 0 or self._state.reserved_tombstone_credits <= 0:
                    raise RuntimeError("bridge tombstone reservation was lost")
                self._state.reserved_tombstone_credits -= 1
                bridge.tombstone_credits -= 1
                self._state.bridge_tombstones[key] = self._bridge_tombstone_for(
                    bridge,
                    role,
                    pinned=True,
                    error=None,
                )
            if bridge.tombstone_credits:
                self._state.reserved_tombstone_credits -= bridge.tombstone_credits
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
            current = self._state.bridge_tombstones.get(key)
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
                current.source_timeout_ack_attempted or finalized.source_timeout_ack_attempted
            )
            finalized.source_timeout_ack_in_flight = (
                current.source_timeout_ack_in_flight or finalized.source_timeout_ack_in_flight
            )
            finalized.failure_issued = current.failure_issued or finalized.failure_issued
            finalized.simultaneous_failure_ack_in_flight = (
                current.simultaneous_failure_ack_in_flight
                or finalized.simultaneous_failure_ack_in_flight
            )
            finalized.simultaneous_failure_ack_sent = (
                current.simultaneous_failure_ack_sent or finalized.simultaneous_failure_ack_sent
            )
            finalized.late_destination_ack = current.late_destination_ack
            finalized.late_source_success_terminal = (
                current.late_source_success_terminal or finalized.late_source_success_terminal
            )
            self._state.bridge_tombstones[key] = finalized

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
            expires_at=(None if pinned else time.monotonic() + self._tombstone_ttl_seconds),
            expected_terminals=tuple(expected),
            sender_success_terminal=(
                bridge.source_end
                if role is BridgeRole.DESTINATION
                and bridge.source_end is not None
                and bridge.source_end.ok
                else None
            ),
            source_resolution=(bridge.source_resolution if role is BridgeRole.SOURCE else None),
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
                bridge.late_progress_remaining if failed and role is BridgeRole.SOURCE else 0
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

    async def handle_frame(self, handle: object, frame: object) -> bool:
        frame_id = getattr(frame, "id", None)
        if isinstance(frame_id, UUID):
            if await self._handle_bridge_tombstone_frame(handle, frame):
                return True
            endpoint = await self._find_bridge_endpoint(handle, frame_id)
            if await self._handle_bridge_tombstone_frame(handle, frame):
                return True
            if endpoint is not None:
                bridge, role = endpoint
                await self._handle_bridge_frame(bridge, role, frame)
                return True
        return False

    async def handle_binary(self, handle: object, slot_id: UUID, chunk: bytes) -> bool:
        if await self._handle_bridge_tombstone_binary(handle, slot_id, chunk):
            return True
        endpoint = await self._find_bridge_endpoint(handle, slot_id)
        if await self._handle_bridge_tombstone_binary(handle, slot_id, chunk):
            return True
        if endpoint is not None:
            bridge, role = endpoint
            await self._handle_bridge_binary(bridge, role, chunk)
            return True
        return False
