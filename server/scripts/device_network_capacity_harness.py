#!/usr/bin/env python3
"""Real PostgreSQL/Uvicorn source-mode device capacity evidence.

This opt-in harness opens real ``/ws/device`` WebSockets and authenticates
Bearer tokens through the production PostgreSQL ``devices.token_hash`` query.
The device side is a bounded, lightweight source-protocol peer task in this
process: it is not a PyInstaller bundle and it does not run provider turns.

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
from collections.abc import Awaitable, Callable
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
from openctopus_server.devices.protocol import (
    DeviceCapabilities,
    HelloAckFrame,
    HelloFrame,
    PingFrame,
    PongFrame,
    ToolCallFrame,
    ToolResultFrame,
    TransferBeginFrame,
    TransferEndFrame,
    TransferReadyFrame,
    decode_binary_chunk,
    new_uuid7,
    parse_server_frame,
)
from openctopus_server.devices.registry import (
    ConnectionHandle,
    DeviceRegistry,
    DeviceUnavailableError,
)
from openctopus_server.services.devices import token_digest, token_hint

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
_TRANSFER_MAX_CONCURRENCY = 32


@dataclass(frozen=True, slots=True)
class NetworkHarnessConfig:
    connections: int = _DEFAULT_CONNECTIONS
    users: int | None = None
    sessions: int | None = None
    dispatch_concurrency: int = _DEFAULT_DISPATCH_CONCURRENCY
    mode: str = "source"

    def normalized(self) -> NetworkHarnessConfig:
        users = self.users if self.users is not None else min(100, self.connections)
        sessions = self.sessions if self.sessions is not None else self.connections
        if self.mode != "source" or self.connections < 1:
            raise ValueError("source mode requires positive connections")
        if not 1 <= users <= self.connections or (self.connections > 1 and users < 2):
            raise ValueError("users must be between 2 and connections")
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
    identities: tuple[_Identity, ...]


@dataclass(frozen=True, slots=True)
class _Outcome:
    latency: float
    result: ToolResultFrame | None
    error: Exception | None


@dataclass(slots=True)
class _InboundTransfer:
    transfer_id: UUID
    expected_bytes: int
    expected_sha256: str | None
    received_bytes: int = 0
    digest: Any = None

    def __post_init__(self) -> None:
        self.digest = hashlib.sha256()


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


class _SourcePeer:
    """Bounded source-protocol peer used instead of a client subprocess."""

    def __init__(self, identity: _Identity, delay: float, queue_capacity: int) -> None:
        self.identity = identity
        self.delay = delay
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
        self._inbound_transfer: _InboundTransfer | None = None
        self.transfer_count = 0
        self.transfer_bytes_received = 0
        self.send_lock = asyncio.Lock()

    async def connect(self, base_url: str, timeout: float) -> None:
        url = base_url.replace("http://", "ws://", 1) + "/ws/device"
        self.websocket = await connect(url, additional_headers={"Authorization": f"Bearer {self.identity.token}"}, compression=None, max_size=_MAX_TEXT_FRAME_BYTES, open_timeout=timeout, close_timeout=timeout, ping_interval=None, proxy=None)
        hello = HelloFrame(id=new_uuid7(), version="1", client_version="capacity-source-peer", os="linux", caps=DeviceCapabilities())
        await self.websocket.send(hello.model_dump_json())
        raw = await asyncio.wait_for(self.websocket.recv(), timeout=timeout)
        if not isinstance(raw, str):
            raise RuntimeError("hello_ack was not text")
        frame = parse_server_frame(raw)
        if not isinstance(frame, HelloAckFrame) or frame.device_name != self.identity.device_name:
            raise RuntimeError("hello_ack identity mismatch")
        self.reader = asyncio.create_task(self.read_frames())
        self.worker = asyncio.create_task(self.write_results())

    async def close(self) -> None:
        self._normal_close_started = True
        if self.websocket is not None:
            with contextlib.suppress(Exception):
                await self.websocket.close()
        tasks = [task for task in (self.reader, self.worker) if task is not None]
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
                elif isinstance(frame, TransferBeginFrame):
                    await self._begin_transfer(frame)
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
            or self._inbound_transfer is not None
        ):
            raise RuntimeError("invalid capacity transfer begin")
        self._inbound_transfer = _InboundTransfer(
            transfer_id=frame.id,
            expected_bytes=frame.total_bytes,
            expected_sha256=frame.sha256,
        )
        await self.send(TransferReadyFrame(id=frame.id).model_dump_json())

    async def _receive_binary(self, raw: bytes) -> None:
        transfer_id, chunk = decode_binary_chunk(raw)
        transfer = self._inbound_transfer
        if transfer is None or transfer.transfer_id != transfer_id:
            raise RuntimeError("binary chunk did not match an active transfer")
        transfer.received_bytes += len(chunk)
        transfer.digest.update(chunk)

    async def _end_transfer(self, frame: TransferEndFrame) -> None:
        transfer = self._inbound_transfer
        if frame.ack or transfer is None or transfer.transfer_id != frame.id:
            raise RuntimeError("invalid capacity transfer end")
        digest = transfer.digest.hexdigest()
        ok = (
            frame.ok
            and frame.bytes_sent == transfer.received_bytes == transfer.expected_bytes
            and frame.sha256 == digest
            and (transfer.expected_sha256 is None or transfer.expected_sha256 == digest)
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
                bytes_sent=transfer.received_bytes,
                sha256=digest,
            ).model_dump_json()
        )
        self.transfer_count += 1
        self.transfer_bytes_received += transfer.received_bytes
        self._inbound_transfer = None

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

    async def send(self, payload: str) -> None:
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


async def _create_rows(engine: AsyncEngine, config: NetworkHarnessConfig) -> _Rows:
    assert config.users is not None
    prefix = f"py5-cap-{uuid4().hex[:16]}"
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
                sandbox_mode=True,
                ssrf_denylist=[],
            )
        )
    rows = _Rows(user_ids, tuple(device.id for device in devices), tuple(identities))
    try:
        async with AsyncSession(engine, expire_on_commit=False) as db:
            db.add_all(users)
            await db.commit()
            db.add_all(devices)
            await db.commit()
    except Exception:
        await _delete_rows(engine, rows)
        raise
    return rows


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
    latencies: list[float] = []
    errors: Counter[str] = Counter()
    dispatch_baseline = baseline
    online_connections_before_shutdown = 0
    live_peer_readers_before_shutdown = 0
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
    sampler = _MetricsSampler(
        _DEFAULT_SAMPLE_INTERVAL,
        lambda: max((peer.queue_high_water for peer in peers), default=0),
        lambda: registry.pending_count,
        lambda: registry.transfers.active_slots,
    )
    try:
        rows = await _create_rows(engine, config)
        auth_hashes = await _verify_hashes(engine, rows)
        app = FastAPI()
        app.include_router(device_ws.router)
        app.dependency_overrides[get_device_registry] = lambda: registry
        server, server_task, base_url, listener = await _start_server(app, config.connections)
        await sampler.start()
        peers = [
            _SourcePeer(
                identity,
                _DEFAULT_SLOW_DELAY if index == 0 else _DEFAULT_CALL_DELAY,
                _DEFAULT_QUEUE_CAPACITY,
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

        transfer_payload = b"openoctopus-capacity-transfer\n" * (
            _TRANSFER_BYTES // len(b"openoctopus-capacity-transfer\n") + 1
        )
        transfer_payload = transfer_payload[:_TRANSFER_BYTES]
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
        "bulk_calls_complete_or_documented": successful == len(bulk) or documented,
        "registry_pending_and_transfers_clean": registry.pending_count == 0
        and registry.transfers.active_slots == 0
        and registry.transfers._admission.waiting_count == 0,  # noqa: SLF001
        "task_count_returns_to_baseline": after.task_count <= baseline.task_count + 2,
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
        "authentication": "PostgreSQL devices.token_hash lookup in /ws/device",
        "client_kind": "lightweight source-protocol peers; not PyInstaller bundles",
        "provider_turns_exercised": False,
        "limitations": [
            "Peers implement hello, ping/pong, bounded read_file, and bounded server-to-client transfer.",
            "No frozen client processes or provider turns are part of this evidence.",
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
            "online_connections_before_shutdown": online_connections_before_shutdown,
            "online_connections_after_cleanup": online_connections_after_cleanup,
            "source_peer_queue_capacity": _DEFAULT_QUEUE_CAPACITY, "rss_before_bytes": baseline.rss_bytes,
            "rss_before_dispatch_bytes": dispatch_baseline.rss_bytes, "rss_after_cleanup_bytes": after.rss_bytes,
            "rss_growth_after_cleanup_bytes": rss_growth,
            "rss_plateau": rss_plateau,
            "baseline": {"rss_bytes": baseline.rss_bytes, "fd_count": baseline.fd_count, "task_count": baseline.task_count},
            "after_cleanup": {"rss_bytes": after.rss_bytes, "fd_count": after.fd_count, "task_count": after.task_count, "connections": online_connections_after_cleanup, "pending_calls": registry.pending_count, "transfer_slots": registry.transfers.active_slots, "transfer_waiters": registry.transfers._admission.waiting_count},  # noqa: SLF001
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
