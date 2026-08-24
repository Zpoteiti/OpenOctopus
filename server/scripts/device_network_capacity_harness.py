#!/usr/bin/env python3
"""Real PostgreSQL/Uvicorn source-mode device and bridge capacity evidence.

This opt-in harness opens real ``/ws/device`` WebSockets and authenticates
Bearer tokens through the production PostgreSQL ``devices.token_hash`` query.
The device side is a bounded, lightweight Protocol v3 peer task in this process:
it is not a PyInstaller bundle and it does not run provider turns.

The existing ``device_capacity_harness.py`` remains the fast in-memory
registry probe and honestly reports ``network_exercised=false``.  For the
manual 500-connection run, from ``server/`` with a real PostgreSQL ``.env``::

    conda run --no-capture-output -n oo python scripts/device_network_capacity_harness.py \
      --connections 500 --users 100 --sessions 500 --dispatch-concurrency 64

The report is JSON.  Temporary rows have unique IDs and are deleted by exact
ID in ``finally``; this script never truncates a table.  It raises the
``RLIMIT_NOFILE`` soft limit only up to the hard limit when necessary.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import resource
import socket
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, replace
from typing import Any, cast
from uuid import UUID, uuid4

import uvicorn
from device_capacity_harness import _MetricsSampler, _process_sample
from fastapi import FastAPI
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from openctopus_server.api import device_ws
from openctopus_server.db.engine import get_engine
from openctopus_server.db.models import Device, User
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.devices.mcp_catalog import canonical_json_bytes, with_catalog_digest
from openctopus_server.devices.mcp_models import (
    PersistedMcpCatalog,
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
)
from openctopus_server.devices.mcp_routes import (
    OwnerMcpDevice,
    build_owner_mcp_snapshot,
)
from openctopus_server.devices.protocol import (
    PROTOCOL_VERSION,
    ConfigAppliedAckFrame,
    ConfigAppliedFrame,
    ConfigUpdateFrame,
    DeviceCapabilities,
    HelloAckFrame,
    HelloFrame,
    PingFrame,
    PongFrame,
    ShellMetadata,
    ToolCallFrame,
    ToolResultFrame,
    TransferBeginFrame,
    TransferEndFrame,
    TransferProgressFrame,
    TransferReadyFrame,
    TransferRequestFrame,
    decode_binary_chunk,
    encode_binary_chunk,
    new_uuid7,
    parse_server_frame,
)
from openctopus_server.devices.registry import (
    ConnectionHandle,
    DeviceRegistry,
    DeviceUnavailableError,
)
from openctopus_server.devices.transfer import (
    TRANSFER_QUEUE_CHUNKS,
    TransferBusyError,
    TransferManager,
    TransferResult,
)
from openctopus_server.services.devices import parse_stored_mcp_catalog, token_digest, token_hint

_DEFAULT_CONNECTIONS = 8
_DEFAULT_DISPATCH_CONCURRENCY = 32
_DEFAULT_PENDING_PER_USER = 8
_DEFAULT_QUEUE_CAPACITY = 8
_DEFAULT_CALL_DELAY = 0.002
_DEFAULT_SLOW_DELAY = 0.75
_DEFAULT_PING_INTERVAL = 0.5
_DEFAULT_LIVENESS_TIMEOUT = 10.0
_DEFAULT_SAMPLE_INTERVAL = 0.01
_MAX_TEXT_FRAME_BYTES = 12 * 1024 * 1024
_TRANSFER_BYTES = 64 * 1024
_TRANSFER_CHUNK_BYTES = 8 * 1024
_TRANSFER_CHUNK_DELAY = 0.025
_BRIDGE_CHUNK_DELAY = 0.005
_BRIDGE_DESTINATION_WRITE_DELAY = 0.02
_TRANSFER_MAX_CONCURRENCY = 32
_BRIDGE_JOBS_PER_USER = 5
_CLIENT_LOCAL_TRANSFER_SLOTS = 2


@dataclass(frozen=True, slots=True)
class NetworkHarnessConfig:
    connections: int = _DEFAULT_CONNECTIONS
    users: int | None = None
    sessions: int | None = None
    dispatch_concurrency: int = _DEFAULT_DISPATCH_CONCURRENCY
    mode: str = "source"

    def normalized(self) -> NetworkHarnessConfig:
        users = self.users if self.users is not None else min(100, self.connections // 2)
        sessions = self.sessions if self.sessions is not None else self.connections
        if self.mode != "source" or self.connections < 4:
            raise ValueError("source mode requires at least four connections")
        if not 2 <= users <= self.connections // 2:
            raise ValueError("capacity bridge requires two connected devices per user")
        if self.connections >= 500 and users < 100:
            raise ValueError("500 connections require at least 100 users")
        if sessions != self.connections or self.dispatch_concurrency < 1:
            raise ValueError("sessions must equal connections and dispatch must be positive")
        return replace(self, users=users, sessions=sessions)


@dataclass(frozen=True, slots=True)
class _Identity:
    user_id: UUID
    device_id: UUID
    device_name: str
    token: str


@dataclass(frozen=True, slots=True)
class _Rows:
    user_ids: tuple[UUID, ...]
    device_ids: tuple[UUID, ...]
    offline_device_ids: tuple[UUID, ...]
    identities: tuple[_Identity, ...]


@dataclass(frozen=True, slots=True)
class _Outcome:
    latency: float
    result: ToolResultFrame | None
    error: Exception | None


@dataclass(frozen=True, slots=True)
class _BridgeJob:
    source: _Identity
    destination: _Identity
    sequence: int


@dataclass(frozen=True, slots=True)
class _BridgeOutcome:
    user_id: UUID
    latency: float
    result: TransferResult | None
    error: Exception | None
    completed_at: float


@dataclass(slots=True)
class _InboundTransfer:
    transfer_id: UUID
    expected_bytes: int
    expected_sha256: str | None
    received_bytes: int = 0
    digest: Any = None
    bridge: bool = False
    ready_task: asyncio.Task[None] | None = None

    def __post_init__(self) -> None:
        self.digest = hashlib.sha256()


@dataclass(slots=True)
class _OutboundTransfer:
    transfer_id: UUID
    payload: bytes
    digest: str
    send_task: asyncio.Task[None] | None = None


@dataclass(slots=True)
class _BridgeMetrics:
    active_high_water: int = 0
    endpoint_high_water: int = 0
    queue_high_water: int = 0
    tombstone_high_water: int = 0
    pinned_tombstone_high_water: int = 0
    reserved_tombstone_high_water: int = 0
    admission_active_high_water: int = 0
    admission_waiting_high_water: int = 0
    per_user_active_high_water: int = 0
    task_high_water: int = 0
    local_slot_high_water: int = 0

    def record(self, manager: TransferManager, peers: list[_SourcePeer]) -> None:
        bridges = tuple(manager._bridges.values())  # noqa: SLF001
        tombstones = tuple(manager._bridge_tombstones.values())  # noqa: SLF001
        admission = manager._admission  # noqa: SLF001
        self.active_high_water = max(self.active_high_water, len(bridges))
        self.endpoint_high_water = max(
            self.endpoint_high_water,
            len(manager._bridge_endpoints),  # noqa: SLF001
        )
        self.queue_high_water = max(
            self.queue_high_water,
            max((bridge.queue.qsize() for bridge in bridges), default=0),
        )
        self.tombstone_high_water = max(self.tombstone_high_water, len(tombstones))
        self.pinned_tombstone_high_water = max(
            self.pinned_tombstone_high_water,
            sum(tombstone.pinned for tombstone in tombstones),
        )
        self.reserved_tombstone_high_water = max(
            self.reserved_tombstone_high_water,
            manager._reserved_tombstone_credits,  # noqa: SLF001
        )
        self.admission_active_high_water = max(
            self.admission_active_high_water,
            admission.active_count,
        )
        self.admission_waiting_high_water = max(
            self.admission_waiting_high_water,
            admission.waiting_count,
        )
        self.per_user_active_high_water = max(
            self.per_user_active_high_water,
            max(admission.active_by_user.values(), default=0),
        )
        self.task_high_water = max(
            self.task_high_water,
            sum(
                task is not None and not task.done()
                for bridge in bridges
                for task in (bridge.worker, bridge.relay_task, bridge.finish_task)
            ),
        )
        self.local_slot_high_water = max(
            self.local_slot_high_water,
            max((peer.local_active_slots for peer in peers), default=0),
        )

    @staticmethod
    def current(manager: TransferManager, peers: list[_SourcePeer]) -> dict[str, int]:
        bridges = tuple(manager._bridges.values())  # noqa: SLF001
        tombstones = tuple(manager._bridge_tombstones.values())  # noqa: SLF001
        return {
            "bridge_slots": len(bridges),
            "bridge_endpoints": len(manager._bridge_endpoints),  # noqa: SLF001
            "bridge_tombstones": len(tombstones),
            "bridge_pinned_tombstones": sum(
                tombstone.pinned for tombstone in tombstones
            ),
            "bridge_reserved_tombstones": manager._reserved_tombstone_credits,  # noqa: SLF001
            "bridge_tasks": sum(
                task is not None and not task.done()
                for bridge in bridges
                for task in (bridge.worker, bridge.relay_task, bridge.finish_task)
            ),
            "client_local_slots": sum(peer.local_active_slots for peer in peers),
            "transfer_admission_active": manager._admission.active_count,  # noqa: SLF001
            "transfer_admission_waiting": manager._admission.waiting_count,  # noqa: SLF001
        }


@dataclass(slots=True)
class _MemorySource:
    payload: bytes
    offset: int = 0
    closed: bool = False

    @property
    def size(self) -> int:
        return len(self.payload)

    async def read(self) -> bytes:
        await asyncio.sleep(_TRANSFER_CHUNK_DELAY)
        if self.offset >= len(self.payload):
            return b""
        end = min(self.offset + _TRANSFER_CHUNK_BYTES, len(self.payload))
        chunk = self.payload[self.offset:end]
        self.offset = end
        return chunk

    async def aclose(self) -> None:
        self.closed = True


def _capacity_transfer_payload() -> bytes:
    seed = b"openoctopus-capacity-transfer\n"
    return (seed * (_TRANSFER_BYTES // len(seed) + 1))[:_TRANSFER_BYTES]


class _SourcePeer:
    """Bounded transfer peer used instead of a client subprocess."""

    def __init__(
        self,
        identity: _Identity,
        delay: float,
        queue_capacity: int,
        *,
        bridge_ready_delay: float = 0.0,
    ) -> None:
        self.identity = identity
        self.delay = delay
        self.bridge_ready_delay = bridge_ready_delay
        self.websocket: ClientConnection | None = None
        self.reader: asyncio.Task[None] | None = None
        self.worker: asyncio.Task[None] | None = None
        self.queue: asyncio.Queue[ToolCallFrame] = asyncio.Queue(maxsize=queue_capacity)
        self.queue_high_water = 0
        self.ping_count = 0
        self.pong_count = 0
        self.error: str | None = None
        self.unexpected_disconnect = False
        self._normal_close_started = False
        self._inbound_transfers: dict[UUID, _InboundTransfer] = {}
        self._outbound_transfers: dict[UUID, _OutboundTransfer] = {}
        self._transfer_tasks: set[asyncio.Task[None]] = set()
        self.transfer_count = 0
        self.transfer_bytes_received = 0
        self.bridge_source_count = 0
        self.bridge_destination_count = 0
        self.bridge_bytes_received = 0
        self.local_slot_high_water = 0
        self.local_busy_rejections = 0
        self.send_lock = asyncio.Lock()

    @property
    def local_active_slots(self) -> int:
        return len(self._inbound_transfers) + len(self._outbound_transfers)

    def _has_local_capacity(self) -> bool:
        if self.local_active_slots >= _CLIENT_LOCAL_TRANSFER_SLOTS:
            self.local_busy_rejections += 1
            return False
        self.local_slot_high_water = max(
            self.local_slot_high_water,
            self.local_active_slots + 1,
        )
        return True

    @staticmethod
    def _hello_frame() -> HelloFrame:
        return HelloFrame(
            id=new_uuid7(),
            version=PROTOCOL_VERSION,
            client_version="capacity-source-peer",
            os="linux",
            caps=DeviceCapabilities(),
            shells=ShellMetadata(default="sh", available=["sh"]),
        )

    async def connect(self, base_url: str, timeout: float) -> None:
        url = base_url.replace("http://", "ws://", 1) + "/ws/device"
        self.websocket = await connect(url, additional_headers={"Authorization": f"Bearer {self.identity.token}"}, compression=None, max_size=_MAX_TEXT_FRAME_BYTES, open_timeout=timeout, close_timeout=timeout, ping_interval=None, proxy=None)
        hello = self._hello_frame()
        await self.websocket.send(hello.model_dump_json())
        raw = await asyncio.wait_for(self.websocket.recv(), timeout=timeout)
        if not isinstance(raw, str):
            raise RuntimeError("hello_ack was not text")
        frame = parse_server_frame(raw)
        if not isinstance(frame, HelloAckFrame) or frame.device_name != self.identity.device_name:
            raise RuntimeError("hello_ack identity mismatch")
        await self.websocket.send(
            ConfigAppliedFrame(
                id=frame.id,
                config_revision=frame.config_revision,
            ).model_dump_json()
        )
        raw_ack = await asyncio.wait_for(self.websocket.recv(), timeout=timeout)
        if not isinstance(raw_ack, str):
            raise RuntimeError("config_applied_ack was not text")
        ack = parse_server_frame(raw_ack)
        if (
            not isinstance(ack, ConfigAppliedAckFrame)
            or ack.id != frame.id
            or ack.config_revision != frame.config_revision
        ):
            raise RuntimeError("config_applied_ack mismatch")
        self.reader = asyncio.create_task(self.read_frames())
        self.worker = asyncio.create_task(self.write_results())

    async def close(self) -> None:
        self._normal_close_started = True
        if self.websocket is not None:
            with contextlib.suppress(Exception):
                await self.websocket.close()
        tasks = [
            task
            for task in (
                self.reader,
                self.worker,
                *self._transfer_tasks,
            )
            if task is not None
        ]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def read_frames(self) -> None:
        assert self.websocket is not None
        try:
            async for raw in self.websocket:
                if isinstance(raw, bytes):
                    await self._receive_binary(raw)
                    continue
                frame = parse_server_frame(raw)
                if isinstance(frame, PingFrame):
                    self.ping_count += 1
                    await self.send(PongFrame(id=frame.id).model_dump_json())
                    self.pong_count += 1
                elif isinstance(frame, ToolCallFrame):
                    await self.queue.put(frame)
                    self.queue_high_water = max(self.queue_high_water, self.queue.qsize())
                elif isinstance(frame, ConfigUpdateFrame):
                    await self.send(
                        ConfigAppliedFrame(
                            id=frame.id,
                            config_revision=frame.config_revision,
                        ).model_dump_json()
                    )
                elif isinstance(frame, ConfigAppliedAckFrame):
                    continue
                elif isinstance(frame, TransferBeginFrame):
                    await self._begin_transfer(frame)
                elif isinstance(frame, TransferRequestFrame):
                    await self._begin_source_transfer(frame)
                elif isinstance(frame, TransferReadyFrame):
                    await self._ready_source_transfer(frame)
                elif isinstance(frame, TransferProgressFrame):
                    continue
                elif isinstance(frame, TransferEndFrame):
                    await self._end_transfer(frame)
        except ConnectionClosed as exc:
            if not self._normal_close_started:
                close_code = exc.rcvd.code if exc.rcvd is not None else 1006
                self.unexpected_disconnect = True
                self.error = f"connection closed unexpectedly: {close_code}"
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"
        else:
            if not self._normal_close_started:
                self.unexpected_disconnect = True
                self.error = "connection ended before harness shutdown"

    async def _begin_transfer(self, frame: TransferBeginFrame) -> None:
        if (
            frame.direction != "server_to_client"
            or frame.total_bytes is None
            or frame.id in self._inbound_transfers
            or frame.id in self._outbound_transfers
        ):
            raise RuntimeError("invalid capacity transfer begin")
        if not self._has_local_capacity():
            await self.send(
                TransferEndFrame(
                    id=frame.id,
                    ack=False,
                    ok=False,
                    code="tool_device_busy",
                ).model_dump_json()
            )
            return
        transfer = _InboundTransfer(
            transfer_id=frame.id,
            expected_bytes=frame.total_bytes,
            expected_sha256=frame.sha256,
            bridge=frame.src_device not in {None, "server"},
        )
        self._inbound_transfers[frame.id] = transfer

        async def send_ready() -> None:
            if transfer.bridge and self.bridge_ready_delay:
                await asyncio.sleep(self.bridge_ready_delay)
            if self._inbound_transfers.get(frame.id) is transfer:
                await self.send(TransferReadyFrame(id=frame.id).model_dump_json())

        transfer.ready_task = self._track_transfer_task(send_ready())

    async def _receive_binary(self, raw: bytes) -> None:
        transfer_id, chunk = decode_binary_chunk(raw)
        transfer = self._inbound_transfers.get(transfer_id)
        if transfer is None:
            raise RuntimeError("binary chunk did not match an active transfer")
        transfer.received_bytes += len(chunk)
        transfer.digest.update(chunk)

    async def _end_transfer(self, frame: TransferEndFrame) -> None:
        if frame.ack:
            outbound = self._outbound_transfers.pop(frame.id, None)
            if outbound is None:
                raise RuntimeError("capacity source acknowledgement had no active transfer")
            if frame.ok and (
                frame.bytes_sent != len(outbound.payload) or frame.sha256 != outbound.digest
            ):
                raise RuntimeError("capacity source acknowledgement mismatched")
            if frame.ok:
                self.bridge_source_count += 1
            return

        inbound = self._inbound_transfers.pop(frame.id, None)
        if inbound is None:
            outbound = self._outbound_transfers.pop(frame.id, None)
            if outbound is None or frame.ok:
                raise RuntimeError("invalid capacity transfer end")
            await self.send(frame.model_copy(update={"ack": True}).model_dump_json())
            return
        if inbound.ready_task is not None and not inbound.ready_task.done():
            inbound.ready_task.cancel()
            await asyncio.gather(inbound.ready_task, return_exceptions=True)
        if not frame.ok:
            await self.send(frame.model_copy(update={"ack": True}).model_dump_json())
            return
        digest = inbound.digest.hexdigest()
        ok = (
            frame.bytes_sent == inbound.received_bytes == inbound.expected_bytes
            and frame.sha256 == digest
            and (inbound.expected_sha256 is None or inbound.expected_sha256 == digest)
        )
        if not ok:
            await self.send(
                TransferEndFrame(
                    id=frame.id,
                    ack=True,
                    ok=False,
                    code="workspace_transfer_integrity_failed",
                ).model_dump_json()
            )
            raise RuntimeError("capacity transfer integrity mismatch")
        await self.send(
            TransferEndFrame(
                id=frame.id,
                ack=True,
                ok=True,
                bytes_sent=inbound.received_bytes,
                sha256=digest,
            ).model_dump_json()
        )
        if inbound.bridge:
            self.bridge_destination_count += 1
            self.bridge_bytes_received += inbound.received_bytes
        else:
            self.transfer_count += 1
            self.transfer_bytes_received += inbound.received_bytes

    async def _begin_source_transfer(self, frame: TransferRequestFrame) -> None:
        if (
            frame.purpose != "file_transfer"
            or frame.dst_path is None
            or frame.id in self._inbound_transfers
            or frame.id in self._outbound_transfers
        ):
            raise RuntimeError("invalid capacity transfer request")
        if not self._has_local_capacity():
            await self.send(
                TransferEndFrame(
                    id=frame.id,
                    ack=False,
                    ok=False,
                    code="tool_device_busy",
                ).model_dump_json()
            )
            return
        payload = _capacity_transfer_payload()
        digest = hashlib.sha256(payload).hexdigest()
        self._outbound_transfers[frame.id] = _OutboundTransfer(
            transfer_id=frame.id,
            payload=payload,
            digest=digest,
        )
        await self.send(
            TransferBeginFrame(
                id=frame.id,
                direction="client_to_server",
                purpose="file_transfer",
                src_device=self.identity.device_name,
                src_path=frame.src_path,
                dst_device="server",
                dst_path=frame.dst_path,
                total_bytes=len(payload),
                sha256=digest,
                etag=f"capacity-{frame.id}",
            ).model_dump_json()
        )

    async def _ready_source_transfer(self, frame: TransferReadyFrame) -> None:
        transfer = self._outbound_transfers.get(frame.id)
        if transfer is None or transfer.send_task is not None:
            raise RuntimeError("capacity transfer ready had no waiting source")
        transfer.send_task = self._track_transfer_task(self._send_source_transfer(transfer))

    async def _send_source_transfer(self, transfer: _OutboundTransfer) -> None:
        for offset in range(0, len(transfer.payload), _TRANSFER_CHUNK_BYTES):
            await asyncio.sleep(_BRIDGE_CHUNK_DELAY)
            await self.send(
                encode_binary_chunk(
                    transfer.transfer_id,
                    transfer.payload[offset : offset + _TRANSFER_CHUNK_BYTES],
                )
            )
        await self.send(
            TransferEndFrame(
                id=transfer.transfer_id,
                ack=False,
                ok=True,
                bytes_sent=len(transfer.payload),
                sha256=transfer.digest,
            ).model_dump_json()
        )

    def _track_transfer_task(
        self,
        coroutine: Coroutine[Any, Any, None],
    ) -> asyncio.Task[None]:
        task = asyncio.create_task(coroutine)
        self._transfer_tasks.add(task)

        def completed(done: asyncio.Task[None]) -> None:
            self._transfer_tasks.discard(done)
            if done.cancelled():
                return
            error = done.exception()
            if error is not None:
                self.error = f"{type(error).__name__}: {error}"

        task.add_done_callback(completed)
        return task

    async def write_results(self) -> None:
        while True:
            frame = await self.queue.get()
            try:
                await asyncio.sleep(self.delay)
                sequence = frame.args.get("sequence")
                sequence = sequence if isinstance(sequence, int) else -1
                session_id = str(frame.args.get("session_id", ""))
                result = ToolResultFrame(id=frame.id, content=f"user={self.identity.user_id};device={self.identity.device_id};session={session_id};sequence={sequence}", is_error=False)
                await self.send(result.model_dump_json())
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.error = f"{type(exc).__name__}: {exc}"
                return
            finally:
                self.queue.task_done()

    async def send(self, payload: str | bytes) -> None:
        assert self.websocket is not None
        async with self.send_lock:
            await self.websocket.send(payload)


async def _start_server(app: FastAPI, connections: int) -> tuple[uvicorn.Server, asyncio.Task[None], str, socket.socket]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(max(512, connections + 16))
    listener.setblocking(False)
    port = int(listener.getsockname()[1])
    config = uvicorn.Config(app, host="127.0.0.1", port=port, ws="websockets-sansio", ws_max_size=_MAX_TEXT_FRAME_BYTES, ws_max_queue=1, ws_ping_interval=None, ws_per_message_deflate=False, lifespan="off", log_config=None, access_log=False)
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve(sockets=[listener]))
    for _ in range(500):
        if server.started:
            return server, task, f"http://127.0.0.1:{port}", listener
        if task.done():
            task.result()
        await asyncio.sleep(0.01)
    server.should_exit = True
    with contextlib.suppress(Exception):
        await asyncio.wait_for(task, timeout=10)
    listener.close()
    raise RuntimeError("Uvicorn did not start")


async def _stop_server(server: uvicorn.Server | None, task: asyncio.Task[None] | None, listener: socket.socket | None) -> None:
    if server is not None:
        server.should_exit = True
    if task is not None and not task.done():
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(task, timeout=15)
    if task is not None and not task.done():
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    if listener is not None:
        listener.close()


async def _delete_rows(engine: AsyncEngine, rows: _Rows | None) -> None:
    if rows is None:
        return
    async with engine.begin() as connection:
        for model, ids in ((Device, rows.device_ids), (User, rows.user_ids)):
            await connection.execute(delete(model).where(model.id.in_(ids)))


async def _create_rows(
    engine: AsyncEngine,
    config: NetworkHarnessConfig,
    *,
    sample: Callable[[], None],
) -> _Rows:
    assert config.users is not None
    prefix = f"py7-cap-{uuid4().hex[:16]}"
    users = [
        User(
            id=uuid4(),
            email=f"{prefix}-u{index}@example.com",
            password_hash="capacity-harness-password-hash",
            name=f"Capacity Harness User {index}",
        )
        for index in range(config.users)
    ]
    user_ids = tuple(user.id for user in users)
    identities: list[_Identity] = []
    devices: list[Device] = []
    for index in range(config.connections):
        user_id = user_ids[index % config.users]
        device_id = uuid4()
        name = f"{prefix}-d{index}"
        token = f"{prefix}-token-{index}-{uuid4().hex}"
        identities.append(_Identity(user_id, device_id, name, token))
        devices.append(
            Device(
                id=device_id,
                user_id=user_id,
                name=name,
                token_hash=token_digest(token),
                token_hint=token_hint(token),
                workspace_path="/tmp/openoctopus-capacity-harness",
                restrict_to_workspace=True,
                ssrf_denylist=[],
            )
        )
    offline_devices: list[Device] = []
    for index in range(config.connections):
        user_id = user_ids[index % config.users]
        device_id = uuid4()
        catalog = with_catalog_digest(
            PersistedMcpCatalog(
                version=1,
                digest="0" * 64,
                servers=[
                    PersistedMcpServerCatalog(
                        name="offline",
                        entries=[
                            PersistedMcpCatalogEntry(
                                entry_id=new_uuid7(),
                                server="offline",
                                surface="tool",
                                raw_name="probe",
                                invocation_identity="probe",
                                final_name="mcp_offline_probe",
                                provider_description="Probe one bounded offline MCP catalog.",
                                input_schema={
                                    "type": "object",
                                    "properties": {},
                                    "additionalProperties": False,
                                },
                                enabled=True,
                            )
                        ],
                    )
                ],
            )
        )
        offline_devices.append(
            Device(
                id=device_id,
                user_id=user_id,
                name=f"{prefix}-offline-{index}",
                token_hash=token_digest(f"{prefix}-offline-token-{index}"),
                token_hint="capacity-offline",
                workspace_path="/tmp/openoctopus-capacity-harness",
                restrict_to_workspace=True,
                ssrf_denylist=[],
                mcp_servers=[
                    {
                        "name": "offline",
                        "transport": "stdio",
                        "command": "offline-capacity-mcp",
                        "args": [],
                        "cwd": None,
                        "env": {},
                        "enabled_capabilities": None,
                    }
                ],
                mcp_catalog=catalog.model_dump(mode="json"),
            )
        )
    all_devices = [*devices, *offline_devices]
    sample()
    rows = _Rows(
        user_ids,
        tuple(device.id for device in all_devices),
        tuple(device.id for device in offline_devices),
        tuple(identities),
    )
    try:
        async with AsyncSession(engine, expire_on_commit=False) as db:
            db.add_all(users)
            await db.commit()
            sample()
            db.add_all(all_devices)
            await db.commit()
            sample()
    except Exception:
        await _delete_rows(engine, rows)
        raise
    return rows


async def _offline_catalog_metrics(
    engine: AsyncEngine,
    rows: _Rows,
    *,
    sample: Callable[[], None],
) -> dict[str, int]:
    async with AsyncSession(engine, expire_on_commit=False) as db:
        devices = list(
            (
                await db.scalars(
                    select(Device).where(Device.id.in_(rows.offline_device_ids))
                )
            ).all()
        )
    sample()
    by_user: dict[UUID, list[OwnerMcpDevice]] = {}
    for device in devices:
        catalog = parse_stored_mcp_catalog(device.mcp_catalog)
        by_user.setdefault(device.user_id, []).append(
            OwnerMcpDevice(
                device_id=device.id,
                name=device.name,
                config_revision=device.config_revision,
                catalog=catalog,
            )
        )
    sample()
    schema_high_water = 0
    route_high_water = 0
    bytes_high_water = 0
    for owner_devices in by_user.values():
        snapshot = build_owner_mcp_snapshot(owner_devices)
        schema_high_water = max(schema_high_water, len(snapshot.schemas))
        route_high_water = max(route_high_water, len(snapshot.routes))
        bytes_high_water = max(
            bytes_high_water,
            len(canonical_json_bytes(snapshot.schemas)),
        )
        sample()
    return {
        "devices": len(devices),
        "owners": len(by_user),
        "schemas_high_water": schema_high_water,
        "routes_high_water": route_high_water,
        "schema_bytes_high_water": bytes_high_water,
    }


async def _verify_hashes(engine: AsyncEngine, rows: _Rows) -> bool:
    async with AsyncSession(engine, expire_on_commit=False) as db:
        devices = list((await db.scalars(select(Device).where(Device.id.in_(rows.device_ids)))).all())
    by_id = {device.id: device for device in devices}
    return all(
        identity.device_id in by_id
        and by_id[identity.device_id].token_hash == token_digest(identity.token)
        and identity.token not in repr(by_id[identity.device_id])
        for identity in rows.identities
    )


async def _dispatch(
    registry: DeviceRegistry,
    *,
    device_id: UUID,
    user_id: UUID,
    device_name: str,
    sequence: int,
    timeout: float,
) -> _Outcome:
    started = time.perf_counter()
    try:
        result = await registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="read_file",
            args={"path": f"capacity/{uuid4()}.txt", "session_id": str(uuid4()), "sequence": sequence},
            max_result_bytes=4096,
            timeout_seconds=timeout,
            expected_device_name=device_name,
        )
        return _Outcome(time.perf_counter() - started, result, None)
    except Exception as exc:
        return _Outcome(time.perf_counter() - started, None, exc)


def _raise_nofile(connections: int) -> dict[str, int]:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    requested = max(soft, 2 * connections + 512)
    effective = min(requested, hard)
    if effective > soft:
        resource.setrlimit(resource.RLIMIT_NOFILE, (effective, hard))
    return {"before_soft": soft, "hard": hard, "requested": requested, "effective_soft": effective}


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


async def _run_transfer_batch[T](
    count: int,
    *,
    max_concurrency: int,
    transfer: Callable[[int], Awaitable[T]],
) -> list[T]:
    semaphore = asyncio.Semaphore(max_concurrency)

    async def one(index: int) -> T:
        async with semaphore:
            return await transfer(index)

    return list(await asyncio.gather(*(one(index) for index in range(count))))


def _bridge_jobs(identities: tuple[_Identity, ...]) -> list[_BridgeJob]:
    by_user: dict[UUID, list[_Identity]] = {}
    for identity in identities:
        by_user.setdefault(identity.user_id, []).append(identity)
    if any(len(owner_devices) < 2 for owner_devices in by_user.values()):
        raise ValueError("capacity bridge requires two connected devices per user")

    owners = list(by_user)
    jobs_by_owner: dict[UUID, list[_BridgeJob]] = {}
    sequence = 0
    for owner in owners:
        first, second = by_user[owner][:2]
        jobs: list[_BridgeJob] = []
        for index in range(_BRIDGE_JOBS_PER_USER):
            source, destination = (first, second) if index % 2 == 0 else (second, first)
            jobs.append(_BridgeJob(source, destination, sequence))
            sequence += 1
        jobs_by_owner[owner] = jobs

    # Admit two slow-owner bridges first to prove the per-user ceiling, then
    # spread the remaining work across owners so other users can progress.
    first_owner = owners[0]
    ordered = jobs_by_owner[first_owner][:2]
    for job_index in range(_BRIDGE_JOBS_PER_USER):
        for owner in owners:
            if owner == first_owner and job_index < 2:
                continue
            ordered.append(jobs_by_owner[owner][job_index])
    return ordered


async def _run_bridge_job(
    registry: DeviceRegistry,
    job: _BridgeJob,
    *,
    on_issued: Callable[[_BridgeJob], None] | None = None,
) -> _BridgeOutcome:
    started = time.perf_counter()
    try:
        routes = await registry.get_bridge_route_pair(
            user_id=job.source.user_id,
            source_device_id=job.source.device_id,
            source_device_name=job.source.device_name,
            destination_device_id=job.destination.device_id,
            destination_device_name=job.destination.device_name,
        )
        if routes is None:
            raise DeviceUnavailableError("capacity bridge route was unavailable")
        result = await registry.transfers.start_client_to_client(
            source_route=routes.source,
            destination_route=routes.destination,
            user_id=job.source.user_id,
            src_path=f"capacity/bridge-source-{job.sequence}.bin",
            dst_path=f"capacity/bridge-destination-{job.sequence}.bin",
            mode="copy",
            delete_source=None,
            on_issued=(lambda: on_issued(job)) if on_issued is not None else None,
        )
        expected_digest = hashlib.sha256(_capacity_transfer_payload()).hexdigest()
        if (
            result.bytes_transferred != _TRANSFER_BYTES
            or result.sha256 != expected_digest
        ):
            raise RuntimeError("capacity bridge result mismatched source bytes")
        return _BridgeOutcome(
            job.source.user_id,
            time.perf_counter() - started,
            result,
            None,
            time.perf_counter(),
        )
    except Exception as exc:
        return _BridgeOutcome(
            job.source.user_id,
            time.perf_counter() - started,
            None,
            exc,
            time.perf_counter(),
        )


async def run_network_harness(config: NetworkHarnessConfig = NetworkHarnessConfig()) -> dict[str, object]:
    """Run one real network/PG pass and return JSON-compatible evidence."""

    config = config.normalized()
    nofile = _raise_nofile(config.connections)
    baseline = _process_sample()
    engine = get_engine()
    registry = DeviceRegistry(
        pending_calls_max=max(config.connections, _DEFAULT_PENDING_PER_USER * 2),
        pending_calls_max_per_user=_DEFAULT_PENDING_PER_USER,
        pending_bytes_max=max(config.connections * 8192, 1 << 20),
        pending_bytes_max_per_user=max(_DEFAULT_PENDING_PER_USER * 8192, 1 << 20),
        transfer_max_concurrency=_TRANSFER_MAX_CONCURRENCY,
    )
    rows: _Rows | None = None
    peers: list[_SourcePeer] = []
    server: uvicorn.Server | None = None
    server_task: asyncio.Task[None] | None = None
    listener: socket.socket | None = None
    failure: str | None = None
    auth_hashes = False
    cross_user_rejected = False
    cross_user_result_errors = 0
    bulk: list[_Outcome] = []
    bridge_probe: list[_Outcome] = []
    bridge_outcomes: list[_BridgeOutcome] = []
    bridge_jobs: list[_BridgeJob] = []
    bridge_issue_order: list[_BridgeJob] = []
    bridge_phase_started = False
    bridge_wall_time = 0.0
    bridge_metrics = _BridgeMetrics()
    bridge_after_cleanup = _BridgeMetrics.current(registry.transfers, peers)
    slow_bridge_owner: UUID | None = None
    latencies: list[float] = []
    errors: Counter[str] = Counter()
    dispatch_baseline = baseline
    online_connections_before_shutdown = 0
    live_peer_readers_before_shutdown = 0
    offline_catalog_metrics = {
        "devices": 0,
        "owners": 0,
        "schemas_high_water": 0,
        "routes_high_water": 0,
        "schema_bytes_high_water": 0,
    }
    started = time.perf_counter()
    original_heartbeat = device_ws._heartbeat

    async def short_heartbeat(
        heartbeat_registry: DeviceRegistry,
        handle: ConnectionHandle,
        transport: device_ws.WebSocketTransport,
        *,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        await original_heartbeat(
            heartbeat_registry,
            handle,
            transport,
            ping_interval_seconds=_DEFAULT_PING_INTERVAL,
            liveness_timeout_seconds=_DEFAULT_LIVENESS_TIMEOUT,
            stop_event=stop_event,
        )

    device_ws._heartbeat = cast(Any, short_heartbeat)

    def sample_peer_queue() -> int:
        bridge_metrics.record(registry.transfers, peers)
        return max((peer.queue_high_water for peer in peers), default=0)

    sampler = _MetricsSampler(
        _DEFAULT_SAMPLE_INTERVAL,
        sample_peer_queue,
        lambda: registry.pending_count,
        lambda: registry.transfers.active_slots,
    )
    try:
        await sampler.start()
        rows = await _create_rows(engine, config, sample=sampler._record)  # noqa: SLF001
        auth_hashes = await _verify_hashes(engine, rows)
        offline_catalog_metrics = await _offline_catalog_metrics(
            engine,
            rows,
            sample=sampler._record,  # noqa: SLF001
        )
        app = FastAPI()
        app.include_router(device_ws.router)
        app.dependency_overrides[get_device_registry] = lambda: registry
        server, server_task, base_url, listener = await _start_server(app, config.connections)
        slow_bridge_owner = rows.user_ids[0]
        peers = [
            _SourcePeer(
                identity,
                _DEFAULT_SLOW_DELAY if index == 0 else _DEFAULT_CALL_DELAY,
                _DEFAULT_QUEUE_CAPACITY,
                bridge_ready_delay=(
                    _DEFAULT_SLOW_DELAY
                    if identity.user_id == slow_bridge_owner
                    else 0.0
                ),
            )
            for index, identity in enumerate(rows.identities)
        ]
        await asyncio.gather(*(peer.connect(base_url, 15.0) for peer in peers))
        dispatch_baseline = _process_sample()

        semaphore = asyncio.Semaphore(config.dispatch_concurrency)

        async def one(index: int) -> _Outcome:
            async with semaphore:
                identity = rows.identities[index % len(rows.identities)]
                return await _dispatch(
                    registry,
                    device_id=identity.device_id,
                    user_id=identity.user_id,
                    device_name=identity.device_name,
                    sequence=index,
                    timeout=max(_DEFAULT_LIVENESS_TIMEOUT * 4, 2.0),
                )

        transfer_payload = _capacity_transfer_payload()
        transfer_digest = hashlib.sha256(transfer_payload).hexdigest()

        async def one_transfer(index: int) -> object:
            identity = rows.identities[index]
            route = await registry.get_route_snapshot(
                identity.device_id,
                user_id=identity.user_id,
                expected_device_name=identity.device_name,
            )
            if route is None:
                raise DeviceUnavailableError("capacity device disconnected before transfer")
            return await registry.transfers.start_server_to_client(
                handle=route.handle,
                route=route,
                user_id=identity.user_id,
                src_path=f"capacity/source-{index}.bin",
                dst_path=f"capacity/destination-{index}.bin",
                source=_MemorySource(transfer_payload),
                total_bytes=len(transfer_payload),
                sha256=transfer_digest,
                src_device="server",
                dst_device=identity.device_name,
            )

        bulk, _ = await asyncio.gather(
            asyncio.gather(*(one(index) for index in range(config.sessions or 0))),
            _run_transfer_batch(
                len(rows.identities),
                max_concurrency=_TRANSFER_MAX_CONCURRENCY,
                transfer=one_transfer,
            ),
        )
        for index, outcome in enumerate(bulk):
            if outcome.error is not None:
                errors[type(outcome.error).__name__] += 1
                continue
            assert rows is not None
            identity = rows.identities[index % len(rows.identities)]
            expected_session = "session="
            content = outcome.result.content if outcome.result is not None else None
            if not isinstance(content, str) or not content.startswith(
                f"user={identity.user_id};device={identity.device_id};{expected_session}"
            ):
                cross_user_result_errors += 1
            latencies.append(outcome.latency)

        bridge_jobs = _bridge_jobs(rows.identities)
        bridge_phase_started = True
        bridge_started = time.perf_counter()
        slow_destination_ids = {
            identity.device_id
            for identity in rows.identities
            if identity.user_id == slow_bridge_owner
        }
        original_send_binary = registry.send_binary

        async def delayed_bridge_send_binary(
            handle: ConnectionHandle,
            payload: bytes,
            *,
            expected_device_name: str | None = None,
            expected_config_epoch: int | None = None,
        ) -> bool:
            if handle.device_id in slow_destination_ids:
                await asyncio.sleep(_BRIDGE_DESTINATION_WRITE_DELAY)
            return await original_send_binary(
                handle,
                payload,
                expected_device_name=expected_device_name,
                expected_config_epoch=expected_config_epoch,
            )

        setattr(registry, "send_binary", delayed_bridge_send_binary)

        async def bridge_tool_probe(index: int) -> _Outcome:
            identity = rows.identities[index]
            return await _dispatch(
                registry,
                device_id=identity.device_id,
                user_id=identity.user_id,
                device_name=identity.device_name,
                sequence=(config.sessions or 0) + index,
                timeout=max(_DEFAULT_LIVENESS_TIMEOUT * 4, 2.0),
            )

        try:
            bridge_probe, bridge_outcomes = await asyncio.gather(
                asyncio.gather(
                    *(bridge_tool_probe(index) for index in range(len(rows.identities)))
                ),
                asyncio.gather(
                    *(
                        _run_bridge_job(
                            registry,
                            job,
                            on_issued=bridge_issue_order.append,
                        )
                        for job in bridge_jobs
                    )
                ),
            )
        finally:
            setattr(registry, "send_binary", original_send_binary)
        bridge_metrics.record(registry.transfers, peers)
        bridge_after_cleanup = _BridgeMetrics.current(registry.transfers, peers)
        bridge_wall_time = time.perf_counter() - bridge_started

        if len(peers) > 1:
            first = rows.identities[0]
            second = rows.identities[1]
            wrong = await _dispatch(
                registry,
                device_id=first.device_id,
                user_id=second.user_id,
                device_name=first.device_name,
                sequence=-1,
                timeout=2.0,
            )
            cross_user_rejected = isinstance(wrong.error, DeviceUnavailableError)
            if wrong.error is not None:
                errors[type(wrong.error).__name__] += 1

        await asyncio.sleep(max(_DEFAULT_PING_INTERVAL * 6, 0.5))
        online_connections_before_shutdown = sum(
            await asyncio.gather(
                *(
                    registry.is_online(identity.device_id, user_id=identity.user_id)
                    for identity in rows.identities
                )
            )
        )
        live_peer_readers_before_shutdown = sum(
            peer.reader is not None and not peer.reader.done() for peer in peers
        )
    except Exception as exc:
        failure = f"{type(exc).__name__}: {exc}"
        cross_user_rejected = False
    finally:
        await asyncio.gather(*(peer.close() for peer in peers), return_exceptions=True)
        await _stop_server(server, server_task, listener)
        with contextlib.suppress(Exception):
            await registry.close()
        await sampler.stop()
        device_ws._heartbeat = original_heartbeat
        await _delete_rows(engine, rows)
        await engine.dispose()
        get_engine.cache_clear()

    online_connections_after_cleanup = sum(
        await asyncio.gather(
            *(
                registry.is_online(identity.device_id, user_id=identity.user_id)
                for identity in (rows.identities if rows is not None else ())
            )
        )
    )
    after = _process_sample()
    elapsed = time.perf_counter() - started
    successful = sum(outcome.error is None for outcome in bulk)
    documented = all(
        outcome.error is None or type(outcome.error).__name__ in {"DeviceBusyError", "DeviceUnavailableError"}
        for outcome in bulk
    )
    rss_growth = None
    if dispatch_baseline.rss_bytes is not None and after.rss_bytes is not None:
        rss_growth = max(0, after.rss_bytes - dispatch_baseline.rss_bytes)
    rss_plateau = rss_growth is None or rss_growth <= max(
        8 * 1024 * 1024, (sampler.peak_rss_bytes or 0) // 10
    )
    ping_count = sum(peer.ping_count for peer in peers)
    pong_count = sum(peer.pong_count for peer in peers)
    heartbeat_peers = sum(peer.pong_count > 0 for peer in peers)
    successful_transfers = sum(peer.transfer_count for peer in peers)
    transfer_bytes_received = sum(peer.transfer_bytes_received for peer in peers)
    successful_client_bridges = sum(
        outcome.error is None for outcome in bridge_outcomes
    )
    busy_client_bridges = sum(
        isinstance(outcome.error, TransferBusyError) for outcome in bridge_outcomes
    )
    bridge_errors = Counter(
        type(outcome.error).__name__
        for outcome in bridge_outcomes
        if outcome.error is not None
    )
    bridge_warnings = Counter(
        warning
        for outcome in bridge_outcomes
        if outcome.result is not None
        for warning in (outcome.result.warnings or ("none",))
    )
    bridge_latencies = [
        outcome.latency for outcome in bridge_outcomes if outcome.error is None
    ]
    bridge_source_count = sum(peer.bridge_source_count for peer in peers)
    bridge_destination_count = sum(peer.bridge_destination_count for peer in peers)
    bridge_bytes_received = sum(peer.bridge_bytes_received for peer in peers)
    bridge_local_busy_rejections = sum(peer.local_busy_rejections for peer in peers)
    expected_bridge_active = min(
        _TRANSFER_MAX_CONCURRENCY,
        (config.users or 0) * _CLIENT_LOCAL_TRANSFER_SLOTS,
    )
    slow_completion = min(
        (
            outcome.completed_at
            for outcome in bridge_outcomes
            if outcome.error is None and outcome.user_id == slow_bridge_owner
        ),
        default=None,
    )
    other_completion = min(
        (
            outcome.completed_at
            for outcome in bridge_outcomes
            if outcome.error is None and outcome.user_id != slow_bridge_owner
        ),
        default=None,
    )
    cross_user_bridge_progress = (
        slow_completion is not None
        and other_completion is not None
        and other_completion < slow_completion
    )
    bridge_fifo = all(
        [job.sequence for job in bridge_issue_order if job.source.user_id == owner]
        == [job.sequence for job in bridge_jobs if job.source.user_id == owner][
            : sum(job.source.user_id == owner for job in bridge_issue_order)
        ]
        for owner in {job.source.user_id for job in bridge_jobs}
    )
    fd_returns_to_baseline = (
        baseline.fd_count is None
        or after.fd_count is None
        or after.fd_count <= baseline.fd_count + 2
    )
    in_flight_pings = ping_count - pong_count
    checks: dict[str, bool] = {
        "authenticated_connections": len(peers) == config.connections
        and online_connections_before_shutdown == config.connections
        and live_peer_readers_before_shutdown == config.connections
        and all(peer.error is None and not peer.unexpected_disconnect for peer in peers),
        "postgres_token_hash_lookup": auth_hashes,
        "minimum_users": rows is not None
        and len(rows.user_ids) >= (100 if config.connections >= 500 else (2 if config.connections > 1 else 1)),
        "independent_sessions": len(bulk) == config.connections and successful == len(bulk),
        "no_cross_user_result_or_slot_delivery": cross_user_result_errors == 0,
        "cross_user_dispatch_rejected": cross_user_rejected or len(peers) < 2,
        "source_peer_queue_is_bounded": sampler.peak_queue_high_water <= _DEFAULT_QUEUE_CAPACITY,
        # Shutdown may observe the final ping after the peer has counted it but
        # before its serialized send records the matching pong.  Prove every
        # connection made heartbeat progress and bound that final in-flight
        # edge to at most one ping per peer.
        "ping_pong_under_load": heartbeat_peers == len(peers)
        and 0 <= in_flight_pings <= len(peers),
        "transfer_under_load": successful_transfers == config.connections
        and transfer_bytes_received == config.connections * _TRANSFER_BYTES
        and sampler.peak_transfer_count > 0,
        "client_bridge_capacity": len(bridge_outcomes)
        == (config.users or 0) * _BRIDGE_JOBS_PER_USER
        and successful_client_bridges > 0
        and busy_client_bridges > 0
        and successful_client_bridges + busy_client_bridges == len(bridge_outcomes)
        and set(bridge_errors) <= {"TransferBusyError"},
        "client_bridge_one_logical_permit": bridge_metrics.active_high_water
        == bridge_metrics.admission_active_high_water
        == expected_bridge_active
        and bridge_metrics.endpoint_high_water
        == 2 * bridge_metrics.active_high_water,
        "client_bridge_per_user_and_local_limits": bridge_metrics.per_user_active_high_water
        == _CLIENT_LOCAL_TRANSFER_SLOTS
        and bridge_metrics.local_slot_high_water == _CLIENT_LOCAL_TRANSFER_SLOTS
        and bridge_local_busy_rejections == 0,
        "client_bridge_queue_is_bounded": 0
        < bridge_metrics.queue_high_water
        <= TRANSFER_QUEUE_CHUNKS,
        "client_bridge_fair_progress": cross_user_bridge_progress,
        "client_bridge_per_user_fifo": bridge_fifo,
        "client_bridge_bytes_match": bridge_source_count
        == bridge_destination_count
        == successful_client_bridges
        and bridge_bytes_received == successful_client_bridges * _TRANSFER_BYTES,
        "other_device_tools_progress_under_bridge_load": len(bridge_probe)
        == config.connections
        and all(outcome.error is None for outcome in bridge_probe),
        "client_bridge_cleanup": bridge_after_cleanup["bridge_slots"] == 0
        and bridge_after_cleanup["bridge_endpoints"] == 0
        and bridge_after_cleanup["bridge_pinned_tombstones"] == 0
        and bridge_after_cleanup["bridge_reserved_tombstones"] == 0
        and bridge_after_cleanup["bridge_tasks"] == 0
        and bridge_after_cleanup["client_local_slots"] == 0
        and bridge_after_cleanup["transfer_admission_active"] == 0
        and bridge_after_cleanup["transfer_admission_waiting"] == 0,
        "bulk_calls_complete_or_documented": successful == len(bulk) or documented,
        "offline_catalog_projection_is_bounded": offline_catalog_metrics["devices"]
        == config.connections
        and offline_catalog_metrics["owners"] == config.users
        and offline_catalog_metrics["schemas_high_water"] == 1
        and offline_catalog_metrics["routes_high_water"] <= config.connections,
        "registry_pending_and_transfers_clean": registry.pending_count == 0
        and registry.transfers.active_slots == 0
        and registry.transfers._admission.waiting_count == 0,  # noqa: SLF001
        "task_count_returns_to_baseline": after.task_count <= baseline.task_count + 2,
        "fd_count_returns_to_baseline": fd_returns_to_baseline,
        "queue_high_water_is_bounded": sampler.peak_queue_high_water <= _DEFAULT_QUEUE_CAPACITY,
        "rss_plateau": rss_plateau,
    }
    if failure is not None:
        checks["harness_completed"] = False
    return {
        "ok": all(checks.values()),
        "failure": failure,
        "mode": config.mode,
        "transport": "real_fastapi_uvicorn_websocket",
        "network_exercised": True,
        "transfers_exercised": True,
        "client_bridges_exercised": bridge_phase_started,
        "offline_catalogs_exercised": True,
        "authentication": "PostgreSQL devices.token_hash lookup in /ws/device",
        "client_kind": "lightweight source-protocol peers; not PyInstaller bundles",
        "provider_turns_exercised": False,
        "limitations": [
            "Peers implement Protocol v3 config acknowledgement, ping/pong, bounded read_file, server-to-client transfer, and client-to-client bridge roles.",
            "No frozen client processes or provider turns are part of this evidence.",
            "MCP runtime high-water is zero here; separate native/frozen Client E2E starts real runtimes.",
            "FIFO and busy/unreachable edge probes remain covered by the in-memory harness.",
        ],
        "connections": config.connections,
        "authenticated_connections": online_connections_before_shutdown,
        "live_peer_readers_before_shutdown": live_peer_readers_before_shutdown,
        "users": len(rows.user_ids) if rows is not None else 0,
        "independent_sessions": config.sessions,
        "successful_bulk_dispatches": successful,
        "dispatch_errors": dict(errors),
        "cross_user_result_errors": cross_user_result_errors,
        "cross_user_dispatch_rejected": cross_user_rejected,
        "ping_count": ping_count,
        "pong_count": pong_count,
        "heartbeat_peers": heartbeat_peers,
        "successful_transfers": successful_transfers,
        "requested_client_bridges": len(bridge_outcomes),
        "successful_client_bridges": successful_client_bridges,
        "busy_client_bridges": busy_client_bridges,
        "bridge_errors": dict(bridge_errors),
        "bridge_warnings": dict(bridge_warnings),
        "bridge_bytes_received": bridge_bytes_received,
        "bridge_issued_sequence": [job.sequence for job in bridge_issue_order],
        "offline_catalog_devices": offline_catalog_metrics["devices"],
        "transfer_bytes_received": transfer_bytes_received,
        "in_flight_pings_at_shutdown": in_flight_pings,
        "peer_errors": [peer.error for peer in peers if peer.error is not None],
        "metrics": {
            "wall_time_seconds": round(elapsed, 6),
            "dispatch_latency_ms": {"count": len(latencies), "p50": round((_percentile(latencies, 0.5) or 0) * 1000, 3), "p95": round((_percentile(latencies, 0.95) or 0) * 1000, 3)},
            "peak_rss_bytes": sampler.peak_rss_bytes, "peak_open_file_descriptors": sampler.peak_fd_count,
            "peak_task_count": sampler.peak_task_count,
            "process_metrics_source": "procfs_and_resource",
            "registry_pending_high_water": sampler.peak_pending_count, "source_peer_queue_high_water": sampler.peak_queue_high_water,
            "transfer_active_high_water": sampler.peak_transfer_count,
            "transfer_bytes_received": transfer_bytes_received,
            "bridge_latency_ms": {
                "count": len(bridge_latencies),
                "p50": round((_percentile(bridge_latencies, 0.5) or 0) * 1000, 3),
                "p95": round((_percentile(bridge_latencies, 0.95) or 0) * 1000, 3),
            },
            "bridge_wall_time_seconds": round(bridge_wall_time, 6),
            "bridge_effective_global_limit": expected_bridge_active,
            "bridge_active_high_water": bridge_metrics.active_high_water,
            "bridge_admission_active_high_water": bridge_metrics.admission_active_high_water,
            "bridge_admission_waiting_high_water": bridge_metrics.admission_waiting_high_water,
            "bridge_per_user_active_high_water": bridge_metrics.per_user_active_high_water,
            "bridge_endpoint_high_water": bridge_metrics.endpoint_high_water,
            "bridge_tombstone_high_water": bridge_metrics.tombstone_high_water,
            "bridge_pinned_tombstone_high_water": bridge_metrics.pinned_tombstone_high_water,
            "bridge_reserved_tombstone_high_water": bridge_metrics.reserved_tombstone_high_water,
            "bridge_task_high_water": bridge_metrics.task_high_water,
            "bridge_queue_high_water": bridge_metrics.queue_high_water,
            "bridge_queue_capacity": TRANSFER_QUEUE_CHUNKS,
            "client_local_slot_high_water": bridge_metrics.local_slot_high_water,
            "client_local_slot_capacity": _CLIENT_LOCAL_TRANSFER_SLOTS,
            "cross_user_bridge_progress": cross_user_bridge_progress,
            "offline_catalog_routes_high_water": offline_catalog_metrics[
                "routes_high_water"
            ],
            "offline_catalog_schemas_high_water": offline_catalog_metrics[
                "schemas_high_water"
            ],
            "offline_catalog_schema_bytes_high_water": offline_catalog_metrics[
                "schema_bytes_high_water"
            ],
            # These lightweight protocol peers intentionally do not start MCP
            # runtimes. Frozen/native Client E2E is the non-zero runtime gate.
            "mcp_runtime_high_water": 0,
            "online_connections_before_shutdown": online_connections_before_shutdown,
            "online_connections_after_cleanup": online_connections_after_cleanup,
            "source_peer_queue_capacity": _DEFAULT_QUEUE_CAPACITY, "rss_before_bytes": baseline.rss_bytes,
            "rss_before_dispatch_bytes": dispatch_baseline.rss_bytes, "rss_after_cleanup_bytes": after.rss_bytes,
            "rss_growth_after_cleanup_bytes": rss_growth,
            "rss_plateau": rss_plateau,
            "baseline": {"rss_bytes": baseline.rss_bytes, "fd_count": baseline.fd_count, "task_count": baseline.task_count},
            "after_cleanup": {"rss_bytes": after.rss_bytes, "fd_count": after.fd_count, "task_count": after.task_count, "connections": online_connections_after_cleanup, "pending_calls": registry.pending_count, "transfer_slots": registry.transfers.active_slots, "transfer_waiters": registry.transfers._admission.waiting_count, **bridge_after_cleanup},  # noqa: SLF001
            "rlimit_nofile": nofile,
        },
        "checks": checks,
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("source",), default="source")
    parser.add_argument("--connections", type=int, default=_DEFAULT_CONNECTIONS)
    parser.add_argument("--users", type=int, default=None)
    parser.add_argument("--sessions", type=int, default=None)
    parser.add_argument("--dispatch-concurrency", type=int, default=_DEFAULT_DISPATCH_CONCURRENCY)
    parser.add_argument("--indent", type=int, default=2)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = NetworkHarnessConfig(
        mode=args.mode,
        connections=args.connections,
        users=args.users,
        sessions=args.sessions,
        dispatch_concurrency=args.dispatch_concurrency,
    )
    try:
        result = asyncio.run(run_network_harness(config))
    except (OSError, RuntimeError, ValueError) as exc:
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}))
        return 2
    print(json.dumps(result, indent=args.indent, sort_keys=True))
    return 0 if result["ok"] is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
