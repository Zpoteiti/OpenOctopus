from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from fastapi import APIRouter, Depends, WebSocket
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from starlette.websockets import WebSocketDisconnect

from openctopus_server.db.engine import get_engine
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.devices.protocol import (
    MAX_BINARY_CHUNK_BYTES,
    MAX_TEXT_FRAME_BYTES,
    PROTOCOL_VERSION,
    DeviceConfigFrame,
    ErrorFrame,
    HelloAckFrame,
    HelloFrame,
    PingFrame,
    PongFrame,
    ToolResultFrame,
    TransferBeginFrame,
    TransferEndFrame,
    TransferProgressFrame,
    TransferReadyFrame,
    new_uuid7,
    parse_client_frame,
)
from openctopus_server.devices.registry import (
    UNAUTHORIZED_CLOSE_REASON,
    ConnectionHandle,
    DeviceProtocolError,
    DeviceRegistry,
)
from openctopus_server.devices.transfer import TransferProtocolError
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.services import devices

router = APIRouter()

HELLO_TIMEOUT_SECONDS = 10.0
PING_INTERVAL_SECONDS = 30.0
LIVENESS_TIMEOUT_SECONDS = 70.0

_VERSION_REASON = '{"code":"version_unsupported","protocol_version":"2"}'


class _WebSocketLike(Protocol):
    @property
    def headers(self) -> Mapping[str, str]: ...

    async def accept(self) -> None: ...

    async def receive(self) -> Mapping[str, Any]: ...

    async def send_text(self, payload: str) -> None: ...

    async def send_bytes(self, payload: bytes) -> None: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...


class _FrameError(ValueError):
    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code


CRITICAL_QUEUE_MAX = 16
NORMAL_QUEUE_MAX = 64
NORMAL_QUEUE_MAX_BYTES = 32 * 1024 * 1024
BULK_QUEUE_MAX = 4
OUTBOUND_IO_TIMEOUT_SECONDS = 5.0


class _TransportClosedError(RuntimeError):
    pass


class _TransportQueueFullError(RuntimeError):
    pass


@dataclass(slots=True)
class _Outbound:
    kind: str
    payload: str | bytes
    future: asyncio.Future[None]
    slot_id: UUID | None
    size: int


class WebSocketTransport:
    """Bounded single writer for one WebSocket generation.

    Text control frames and binary transfer chunks never call the underlying
    WebSocket concurrently.  Critical control has a reserved lane; normal
    control and each transfer slot are bounded independently so a slow file
    cannot retain an unbounded amount of server memory.
    """

    def __init__(
        self,
        websocket: _WebSocketLike,
        *,
        io_timeout_seconds: float = OUTBOUND_IO_TIMEOUT_SECONDS,
    ) -> None:
        if io_timeout_seconds <= 0:
            raise ValueError("WebSocket I/O timeout must be positive")
        self._websocket = websocket
        self._io_timeout_seconds = io_timeout_seconds
        self._condition = asyncio.Condition()
        self._critical: deque[_Outbound] = deque()
        self._normal: deque[_Outbound] = deque()
        self._bulk: dict[UUID, deque[_Outbound]] = {}
        self._bulk_round_robin: deque[UUID] = deque()
        self._normal_bytes = 0
        self._normal_turn = True
        self._writer: asyncio.Task[None] | None = None
        self._closed = False
        self._close_requested = False
        self._close_code = 1000
        self._close_reason = ""
        self._close_future: asyncio.Future[None] | None = None

    async def send_text(self, payload: str) -> None:
        if len(payload.encode("utf-8")) > MAX_TEXT_FRAME_BYTES:
            raise ValueError("text frame exceeds its size limit")
        await self._enqueue(
            kind="text",
            payload=payload,
            critical=_is_critical_text(payload),
            slot_id=None,
        )

    async def send_binary(self, payload: bytes) -> None:
        if len(payload) < 16 or len(payload) > 16 + MAX_BINARY_CHUNK_BYTES:
            raise ValueError("binary transfer frame exceeds its size limit")
        slot_id = UUID(bytes=payload[:16])
        if slot_id.version != 7:
            raise ValueError("binary transfer frame has an invalid slot ID")
        await self._enqueue(
            kind="binary",
            payload=payload,
            critical=False,
            slot_id=slot_id,
        )

    async def close(self, code: int, reason: str) -> None:
        async with self._condition:
            if self._closed:
                return
            if not self._close_requested:
                self._close_requested = True
                self._close_code = code
                self._close_reason = reason
            if self._close_future is None:
                self._close_future = asyncio.get_running_loop().create_future()
            close_future = self._close_future
            self._ensure_writer_locked()
            self._condition.notify_all()
        await asyncio.shield(close_future)

    async def _enqueue(
        self,
        *,
        kind: str,
        payload: str | bytes,
        critical: bool,
        slot_id: UUID | None,
    ) -> None:
        future = asyncio.get_running_loop().create_future()
        size = len(payload.encode("utf-8")) if isinstance(payload, str) else len(payload)
        item = _Outbound(kind, payload, future, slot_id, size)
        async with self._condition:
            self._ensure_writer_locked()
            while not self._closed and not self._close_requested:
                if critical:
                    if len(self._critical) >= CRITICAL_QUEUE_MAX:
                        self._request_close_locked(1011, "writer_queue_full")
                        raise _TransportQueueFullError("critical WebSocket queue is full")
                    self._critical.append(item)
                    break
                if slot_id is not None:
                    queue = self._bulk.setdefault(slot_id, deque())
                    if len(queue) < BULK_QUEUE_MAX:
                        queue.append(item)
                        if slot_id not in self._bulk_round_robin:
                            self._bulk_round_robin.append(slot_id)
                        break
                elif (
                    len(self._normal) < NORMAL_QUEUE_MAX
                    and self._normal_bytes + size <= NORMAL_QUEUE_MAX_BYTES
                ):
                    self._normal.append(item)
                    self._normal_bytes += size
                    break
                await self._condition.wait()
            else:
                raise _TransportClosedError("WebSocket generation is closed")
            self._condition.notify_all()
        await future

    def _ensure_writer_locked(self) -> None:
        if self._writer is None or self._writer.done():
            self._writer = asyncio.create_task(self._writer_loop())

    def _request_close_locked(self, code: int, reason: str) -> None:
        if not self._close_requested:
            self._close_requested = True
            self._close_code = code
            self._close_reason = reason
        if self._close_future is None:
            self._close_future = asyncio.get_running_loop().create_future()
        self._condition.notify_all()

    async def _writer_loop(self) -> None:
        while True:
            async with self._condition:
                while (
                    not self._critical
                    and not self._normal
                    and not self._bulk_round_robin
                    and not self._close_requested
                ):
                    await self._condition.wait()
                if self._close_requested:
                    code = self._close_code
                    reason = self._close_reason
                    close_future = self._close_future
                    self._closed = True
                    self._fail_queued_locked(
                        _TransportClosedError("WebSocket generation is closed")
                    )
                    self._condition.notify_all()
                    break
                item = self._pop_next_locked()
                self._condition.notify_all()
            if item.future.cancelled():
                continue
            try:
                async with asyncio.timeout(self._io_timeout_seconds):
                    if item.kind == "text":
                        await self._websocket.send_text(item.payload)  # type: ignore[arg-type]
                    else:
                        await self._websocket.send_bytes(item.payload)  # type: ignore[arg-type]
            except BaseException as exc:
                if not item.future.done():
                    item.future.set_exception(exc)
                await self._handle_writer_failure(exc)
                return
            if not item.future.done():
                item.future.set_result(None)
        try:
            async with asyncio.timeout(self._io_timeout_seconds):
                await self._websocket.close(code=code, reason=reason)
        except Exception:
            pass
        if close_future is not None and not close_future.done():
            close_future.set_result(None)

    async def _handle_writer_failure(self, error: BaseException) -> None:
        async with self._condition:
            self._closed = True
            self._close_requested = True
            self._close_code = 1011
            self._close_reason = "transport_error"
            self._fail_queued_locked(_TransportClosedError("WebSocket writer failed"))
            close_future = self._close_future
            self._condition.notify_all()
        try:
            async with asyncio.timeout(self._io_timeout_seconds):
                await self._websocket.close(code=1011, reason="transport_error")
        except Exception:
            pass
        if close_future is not None and not close_future.done():
            close_future.set_result(None)

    def _pop_next_locked(self) -> _Outbound:
        if self._critical:
            return self._critical.popleft()
        if self._normal and self._bulk_round_robin and self._normal_turn:
            self._normal_turn = False
            item = self._normal.popleft()
            self._normal_bytes -= item.size
            return item
        if self._bulk_round_robin:
            self._normal_turn = True
            slot_id = self._bulk_round_robin.popleft()
            queue = self._bulk[slot_id]
            item = queue.popleft()
            if queue:
                self._bulk_round_robin.append(slot_id)
            else:
                self._bulk.pop(slot_id, None)
            return item
        self._normal_turn = True
        item = self._normal.popleft()
        self._normal_bytes -= item.size
        return item

    def _fail_queued_locked(self, error: BaseException) -> None:
        queued = [*self._critical, *self._normal]
        for queue in self._bulk.values():
            queued.extend(queue)
        self._critical.clear()
        self._normal.clear()
        self._bulk.clear()
        self._bulk_round_robin.clear()
        self._normal_bytes = 0
        for item in queued:
            if not item.future.done():
                item.future.set_exception(error)


def _is_critical_text(payload: str) -> bool:
    try:
        frame_type = json.loads(payload).get("type")
    except (TypeError, json.JSONDecodeError):
        return False
    return frame_type in {"error", "ping", "pong", "transfer_ready", "transfer_end"}


@router.websocket("/ws/device")
async def device_websocket(
    websocket: WebSocket,
    registry: DeviceRegistry = Depends(get_device_registry),
) -> None:
    await serve_device_socket(websocket, registry, get_engine())


async def serve_device_socket(
    websocket: _WebSocketLike,
    registry: DeviceRegistry,
    engine: AsyncEngine,
) -> None:
    """Authenticate and serve one device connection without retaining a DB session."""
    await websocket.accept()
    transport = WebSocketTransport(websocket)
    token = _bearer_token(websocket)
    if token is None:
        await transport.close(4401, UNAUTHORIZED_CLOSE_REASON)
        return

    try:
        device = await _find_device_by_token(engine, token)
    except Exception:
        await transport.close(1013, '{"code":"io_error"}')
        return
    if device is None:
        await transport.close(4401, UNAUTHORIZED_CLOSE_REASON)
        return
    registration_epoch = await registry.registration_epoch(device.id)

    try:
        hello = await _receive_hello(websocket)
    except WebSocketDisconnect:
        return
    except _FrameError as exc:
        await _send_error(transport, exc.code, str(exc))
        await transport.close(1002, "")
        return
    except _VersionMismatchError:
        await transport.close(4409, _VERSION_REASON)
        return

    handle: ConnectionHandle | None = None
    try:
        # Serialize the authoritative config read, registration, and hello_ack
        # with REST config commit-and-push.  Otherwise a PATCH can commit while
        # the device is not yet online and leave this connection on stale config.
        async with registry.config_update_lock(
            user_id=device.user_id,
            device_name=device.name,
        ):
            try:
                # Do not let a token rotated or revoked while waiting for hello register.
                device = await _find_device_by_token(engine, token)
            except Exception:
                await transport.close(1013, '{"code":"io_error"}')
                return
            if device is None:
                await transport.close(4401, UNAUTHORIZED_CLOSE_REASON)
                return

            handle = await registry.register(
                device_id=device.id,
                user_id=device.user_id,
                device_name=device.name,
                transport=transport,
                expected_revocation_epoch=registration_epoch,
                ready=False,
            )
            if handle is None:
                await transport.close(4401, UNAUTHORIZED_CLOSE_REASON)
                return
            ack = HelloAckFrame(
                id=hello.id,
                device_name=device.name,
                config=DeviceConfigFrame(
                    workspace_path=device.workspace_path,
                    sandbox_mode=device.sandbox_mode,
                    ssrf_denylist=device.ssrf_denylist,
                    shell_timeout_max=device.shell_timeout_max,
                    env_allowlist=device.env_allowlist,
                ),
            )
            if not await registry.activate(handle, ack.model_dump_json()):
                return
        stop_event = asyncio.Event()
        heartbeat = asyncio.create_task(
            _heartbeat(registry, handle, transport, stop_event=stop_event)
        )
        try:
            await _route_frames(websocket, registry, handle, transport, stop_event=stop_event)
        finally:
            heartbeat.cancel()
            try:
                await heartbeat
            except asyncio.CancelledError:
                pass
    finally:
        if handle is not None:
            try:
                await registry.unregister(handle)
            finally:
                # A peer disconnect removes the registry entry but otherwise
                # leaves the bounded writer waiting forever on its condition.
                await transport.close(1000, "")


async def _find_device_by_token(engine: AsyncEngine, token: str) -> devices.DeviceSnapshot | None:
    async with AsyncSession(engine, expire_on_commit=False) as db:
        return await devices.find_by_token(db, token)


def _bearer_token(websocket: _WebSocketLike) -> str | None:
    authorization = websocket.headers.get("authorization")
    if not isinstance(authorization, str):
        return None
    scheme, separator, token = authorization.partition(" ")
    if scheme != "Bearer" or not separator or not token or token.strip() != token:
        return None
    return token


async def _receive_hello(websocket: _WebSocketLike) -> HelloFrame:
    try:
        async with asyncio.timeout(HELLO_TIMEOUT_SECONDS):
            payload = await _receive_text(websocket)
    except TimeoutError as exc:
        raise _FrameError(ErrorCode.PROTOCOL_MALFORMED_FRAME, "Timed out waiting for hello") from exc
    frame = _parse_frame(payload)
    if not isinstance(frame, HelloFrame):
        raise _FrameError(ErrorCode.PROTOCOL_MALFORMED_FRAME, "Expected hello as first frame")
    return frame


async def _route_frames(
    websocket: _WebSocketLike,
    registry: DeviceRegistry,
    handle: ConnectionHandle,
    transport: WebSocketTransport,
    *,
    stop_event: asyncio.Event | None = None,
) -> None:
    while True:
        try:
            message = await _receive_message_or_stop(websocket, stop_event)
            if message is None:
                return
            binary_payload = message.get("bytes")
            if isinstance(binary_payload, bytes):
                if not await registry.handle_transfer_binary(handle, binary_payload):
                    return
                continue
            payload = message.get("text")
            if not isinstance(payload, str):
                raise _FrameError(ErrorCode.PROTOCOL_MALFORMED_FRAME, "Expected a text frame")
            frame = _parse_frame(payload)
        except WebSocketDisconnect:
            return
        except TransferProtocolError as exc:
            error_code = (
                ErrorCode.PROTOCOL_TRANSFER_UNKNOWN_ID
                if exc.code == "protocol_transfer_unknown_id"
                else ErrorCode.PROTOCOL_MALFORMED_FRAME
            )
            if await registry.send_text(handle, _error_payload(error_code, str(exc))):
                await transport.close(1002, "protocol_error")
            return
        except _FrameError as exc:
            if await registry.send_text(
                handle,
                _error_payload(exc.code, str(exc)),
            ):
                await transport.close(1002, "protocol_error")
            return
        except _VersionMismatchError:
            if await registry.send_text(
                handle,
                _error_payload(ErrorCode.PROTOCOL_VERSION_MISMATCH, "Protocol version is unsupported"),
            ):
                await transport.close(4409, _VERSION_REASON)
            return

        if isinstance(frame, PongFrame):
            if not await registry.mark_pong(handle, frame.id):
                if await registry.send_text(
                    handle,
                    _error_payload(ErrorCode.PROTOCOL_MALFORMED_FRAME, "Unexpected pong ID"),
                ):
                    await transport.close(1002, "protocol_error")
                return
        elif isinstance(frame, ToolResultFrame):
            try:
                resolved = await registry.resolve_tool_result(
                    handle,
                    frame,
                    encoded_bytes=len(payload.encode("utf-8")),
                )
            except DeviceProtocolError:
                if await registry.send_text(
                    handle,
                    _error_payload(
                        ErrorCode.PROTOCOL_MALFORMED_FRAME,
                        "Tool result exceeded its response credit",
                    ),
                ):
                    await transport.close(1002, "protocol_error")
                return
            if not resolved:
                if not await registry.is_current(handle):
                    return
                if not await registry.send_text(
                    handle,
                    _error_payload(ErrorCode.PROTOCOL_MALFORMED_FRAME, "Unknown tool result ID"),
                ):
                    return
                await transport.close(1002, "protocol_error")
                return
        elif isinstance(frame, HelloFrame):
            if await registry.send_text(
                handle,
                _error_payload(ErrorCode.PROTOCOL_MALFORMED_FRAME, "hello is only valid as first frame"),
            ):
                await transport.close(1002, "protocol_error")
            return
        elif isinstance(
            frame,
            (TransferBeginFrame, TransferReadyFrame, TransferProgressFrame, TransferEndFrame),
        ):
            try:
                if not await registry.handle_transfer_frame(handle, frame):
                    return
            except TransferProtocolError as exc:
                error_code = (
                    ErrorCode.PROTOCOL_TRANSFER_UNKNOWN_ID
                    if exc.code == "protocol_transfer_unknown_id"
                    else ErrorCode.PROTOCOL_MALFORMED_FRAME
                )
                if await registry.send_text(handle, _error_payload(error_code, str(exc))):
                    await transport.close(1002, "protocol_error")
                return
        # A device-originated error is informational and does not affect routing.


async def _heartbeat(
    registry: DeviceRegistry,
    handle: ConnectionHandle,
    transport: WebSocketTransport,
    *,
    ping_interval_seconds: float = PING_INTERVAL_SECONDS,
    liveness_timeout_seconds: float = LIVENESS_TIMEOUT_SECONDS,
    stop_event: asyncio.Event | None = None,
) -> None:
    next_ping = time.monotonic() + ping_interval_seconds
    while True:
        last_pong = await registry.last_pong(handle)
        if last_pong is None:
            if stop_event is not None:
                stop_event.set()
            return
        deadline = last_pong + liveness_timeout_seconds
        await asyncio.sleep(max(0.0, min(next_ping, deadline) - time.monotonic()))

        last_pong = await registry.last_pong(handle)
        if last_pong is None:
            if stop_event is not None:
                stop_event.set()
            return
        now = time.monotonic()
        if now >= last_pong + liveness_timeout_seconds:
            await registry.unregister(handle)
            await transport.close(4408, "")
            if stop_event is not None:
                stop_event.set()
            return
        if now >= next_ping:
            ping = PingFrame(id=new_uuid7())
            try:
                sent = await registry.send_ping(handle, ping.id, ping.model_dump_json())
            except Exception:
                await registry.unregister(handle)
                try:
                    await transport.close(1011, "transport_error")
                except Exception:
                    pass
                if stop_event is not None:
                    stop_event.set()
                return
            if not sent:
                if stop_event is not None:
                    stop_event.set()
                return
            next_ping += ping_interval_seconds


async def _receive_message_or_stop(
    websocket: _WebSocketLike,
    stop_event: asyncio.Event | None,
) -> Mapping[str, Any] | None:
    if stop_event is None:
        return await _receive_message(websocket)
    receive_task = asyncio.create_task(_receive_message(websocket))
    stop_task = asyncio.create_task(stop_event.wait())
    done, pending = await asyncio.wait(
        {receive_task, stop_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    if stop_task in done:
        receive_task.cancel()
        await asyncio.gather(receive_task, return_exceptions=True)
        return None
    stop_task.cancel()
    await asyncio.gather(stop_task, return_exceptions=True)
    return await receive_task


async def _receive_message(websocket: _WebSocketLike) -> Mapping[str, Any]:
    message = await websocket.receive()
    if message.get("type") == "websocket.disconnect":
        raise WebSocketDisconnect(code=int(message.get("code", 1000)))
    payload = message.get("text")
    if isinstance(payload, str) and len(payload.encode("utf-8")) > MAX_TEXT_FRAME_BYTES:
        raise _FrameError(ErrorCode.PROTOCOL_MALFORMED_FRAME, "Text frame exceeds the size limit")
    binary_payload = message.get("bytes")
    if isinstance(binary_payload, bytes) and len(binary_payload) > 16 + 64 * 1024:
        raise _FrameError(ErrorCode.PROTOCOL_MALFORMED_FRAME, "Binary frame exceeds the size limit")
    return message


async def _receive_text(websocket: _WebSocketLike) -> str:
    message = await _receive_message(websocket)
    payload = message.get("text")
    if not isinstance(payload, str):
        raise _FrameError(ErrorCode.PROTOCOL_MALFORMED_FRAME, "Expected a text frame")
    return payload


def _parse_frame(
    payload: str,
) -> (
    HelloFrame
    | ToolResultFrame
    | PongFrame
    | ErrorFrame
    | TransferBeginFrame
    | TransferReadyFrame
    | TransferProgressFrame
    | TransferEndFrame
):
    try:
        raw = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise _FrameError(ErrorCode.PROTOCOL_MALFORMED_FRAME, "Malformed JSON frame") from exc
    if not isinstance(raw, dict):
        raise _FrameError(ErrorCode.PROTOCOL_MALFORMED_FRAME, "Frame must be a JSON object")
    if raw.get("type") == "hello" and raw.get("version") != PROTOCOL_VERSION:
        raise _VersionMismatchError()
    try:
        return parse_client_frame(payload)
    except ValidationError as exc:
        if raw.get("type") not in {
            "hello",
            "tool_result",
            "pong",
            "error",
            "transfer_begin",
            "transfer_ready",
            "transfer_progress",
            "transfer_end",
        }:
            raise _FrameError(ErrorCode.PROTOCOL_UNKNOWN_TYPE, "Unknown frame type") from exc
        raise _FrameError(ErrorCode.PROTOCOL_MALFORMED_FRAME, "Malformed protocol frame") from exc


class _VersionMismatchError(Exception):
    pass


async def _send_error(transport: WebSocketTransport, code: ErrorCode, message: str) -> None:
    await transport.send_text(_error_payload(code, message))


def _error_payload(code: ErrorCode, message: str) -> str:
    return ErrorFrame(code=code.value, message=message).model_dump_json()
