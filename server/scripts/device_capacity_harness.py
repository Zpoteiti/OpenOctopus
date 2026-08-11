#!/usr/bin/env python3
"""Run the repeatable Py5 source-mode device capacity harness.

This harness deliberately exercises the real :class:`DeviceRegistry`, but its
device side is an in-memory transport.  It is therefore a server/registry
capacity run, not a real FastAPI WebSocket or packaged-client E2E run.  The
JSON result says so explicitly.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import resource
import sys
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast
from uuid import UUID, uuid5

from openctopus_server.api import device_ws
from openctopus_server.devices.protocol import (
    PingFrame,
    ServerFrame,
    ToolCallFrame,
    ToolResultFrame,
    parse_server_frame,
)
from openctopus_server.devices.registry import (
    ConnectionHandle,
    DeviceBusyError,
    DeviceRegistry,
    DeviceUnavailableError,
)

_HARNESS_NAMESPACE = UUID("4d1cf25e-2f20-4bf0-b0e7-4b24ed8df5c4")
_DEFAULT_CONNECTIONS = 16
_DEFAULT_DISPATCH_CONCURRENCY = 32
_DEFAULT_QUEUE_CAPACITY = 8
_DEFAULT_PENDING_PER_USER = 8
_DEFAULT_CALL_DELAY_SECONDS = 0.002
_DEFAULT_SLOW_DELAY_SECONDS = 0.075
_DEFAULT_PING_INTERVAL_SECONDS = 0.02
_DEFAULT_LIVENESS_TIMEOUT_SECONDS = 0.2
_DEFAULT_SAMPLE_INTERVAL_SECONDS = 0.01


@dataclass(frozen=True, slots=True)
class HarnessConfig:
    """Configuration shared by the CLI and the opt-in smoke test."""

    connections: int = _DEFAULT_CONNECTIONS
    users: int | None = None
    sessions: int | None = None
    dispatch_concurrency: int = _DEFAULT_DISPATCH_CONCURRENCY
    queue_capacity: int = _DEFAULT_QUEUE_CAPACITY
    pending_calls_per_user: int = _DEFAULT_PENDING_PER_USER
    call_delay_seconds: float = _DEFAULT_CALL_DELAY_SECONDS
    slow_delay_seconds: float = _DEFAULT_SLOW_DELAY_SECONDS
    ping_interval_seconds: float = _DEFAULT_PING_INTERVAL_SECONDS
    liveness_timeout_seconds: float = _DEFAULT_LIVENESS_TIMEOUT_SECONDS
    sample_interval_seconds: float = _DEFAULT_SAMPLE_INTERVAL_SECONDS
    mode: str = "source"

    def normalized(self) -> HarnessConfig:
        users = self.users if self.users is not None else min(100, self.connections)
        sessions = self.sessions if self.sessions is not None else self.connections
        if self.mode != "source":
            raise ValueError("only --mode source is implemented; it uses an in-memory transport")
        if self.connections < 1:
            raise ValueError("connections must be positive")
        if users < 1 or users > self.connections:
            raise ValueError("users must be between 1 and connections")
        if self.connections > 1 and users < 2:
            raise ValueError("at least two users are required for cross-user probes")
        if sessions < 1:
            raise ValueError("sessions must be positive")
        if self.connections >= 500 and users < 100:
            raise ValueError("500 connections require at least 100 users")
        if self.dispatch_concurrency < 1:
            raise ValueError("dispatch concurrency must be positive")
        if self.queue_capacity < 2:
            raise ValueError("queue capacity must be at least two for the FIFO probe")
        if self.pending_calls_per_user < 2:
            raise ValueError("pending calls per user must be at least two for the busy probe")
        for name, value in (
            ("call delay", self.call_delay_seconds),
            ("slow delay", self.slow_delay_seconds),
            ("ping interval", self.ping_interval_seconds),
            ("liveness timeout", self.liveness_timeout_seconds),
            ("sample interval", self.sample_interval_seconds),
        ):
            if value < 0:
                raise ValueError(f"{name} must not be negative")
        if self.ping_interval_seconds <= 0:
            raise ValueError("ping interval must be positive")
        if self.liveness_timeout_seconds <= self.ping_interval_seconds:
            raise ValueError("liveness timeout must be greater than ping interval")
        return HarnessConfig(
            connections=self.connections,
            users=users,
            sessions=sessions,
            dispatch_concurrency=self.dispatch_concurrency,
            queue_capacity=self.queue_capacity,
            pending_calls_per_user=self.pending_calls_per_user,
            call_delay_seconds=self.call_delay_seconds,
            slow_delay_seconds=self.slow_delay_seconds,
            ping_interval_seconds=self.ping_interval_seconds,
            liveness_timeout_seconds=self.liveness_timeout_seconds,
            sample_interval_seconds=self.sample_interval_seconds,
            mode=self.mode,
        )


@dataclass(frozen=True, slots=True)
class _ProcessSample:
    rss_bytes: int | None
    fd_count: int | None
    task_count: int


@dataclass(slots=True)
class _WorkStats:
    active: int = 0
    max_active: int = 0
    started: int = 0


def _process_sample() -> _ProcessSample:
    rss_bytes: int | None = None
    fd_count: int | None = None
    try:
        with open("/proc/self/statm", encoding="ascii") as statm:
            resident_pages = int(statm.read().split()[1])
        rss_bytes = resident_pages * os.sysconf("SC_PAGE_SIZE")
    except (FileNotFoundError, IndexError, OSError, ValueError):
        pass
    try:
        fd_count = len(os.listdir("/proc/self/fd"))
    except (FileNotFoundError, OSError):
        pass
    try:
        peak_rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        # Linux reports KiB; macOS reports bytes.  /proc is the normal path in
        # the supported Linux server environment, so only use this fallback
        # when /proc did not provide a current RSS value.
        if rss_bytes is None:
            rss_bytes = peak_rss if sys.platform == "darwin" else peak_rss * 1024
    except (OSError, ValueError):
        pass
    return _ProcessSample(
        rss_bytes=rss_bytes,
        fd_count=fd_count,
        task_count=len(asyncio.all_tasks()),
    )


class _MetricsSampler:
    def __init__(
        self,
        interval_seconds: float,
        queue_high_water: Callable[[], int],
        pending_count: Callable[[], int],
    ) -> None:
        self._interval_seconds = interval_seconds
        self._queue_high_water = queue_high_water
        self._pending_count = pending_count
        self.samples: list[_ProcessSample] = []
        self.peak_queue_high_water = 0
        self.peak_pending_count = 0
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._record()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self._task = None
        self._record()

    def _record(self) -> None:
        self.samples.append(_process_sample())
        self.peak_queue_high_water = max(self.peak_queue_high_water, self._queue_high_water())
        self.peak_pending_count = max(self.peak_pending_count, self._pending_count())

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval_seconds)
            self._record()

    @property
    def peak_rss_bytes(self) -> int | None:
        values = [sample.rss_bytes for sample in self.samples if sample.rss_bytes is not None]
        return max(values) if values else None

    @property
    def peak_fd_count(self) -> int | None:
        values = [sample.fd_count for sample in self.samples if sample.fd_count is not None]
        return max(values) if values else None

    @property
    def peak_task_count(self) -> int:
        return max((sample.task_count for sample in self.samples), default=0)


class _MemoryDeviceTransport:
    """Bounded in-memory source-mode device transport.

    A single worker consumes the queue, which gives each device FIFO handling;
    different transport workers can run concurrently.  The token check and
    registry registration happen in ``_SourceHarness._connect``.
    """

    def __init__(
        self,
        *,
        user_id: UUID,
        device_id: UUID,
        device_name: str,
        delay_seconds: float,
        queue_capacity: int,
        work_stats: _WorkStats,
    ) -> None:
        self.user_id = user_id
        self.device_id = device_id
        self.device_name = device_name
        self.delay_seconds = delay_seconds
        self._queue: asyncio.Queue[ToolCallFrame | None] = asyncio.Queue(maxsize=queue_capacity)
        self._work_stats = work_stats
        self._registry: DeviceRegistry | None = None
        self._handle: ConnectionHandle | None = None
        self._worker: asyncio.Task[None] | None = None
        self._closed = False
        self.sent_tool_ids: list[UUID] = []
        self.completed_tool_ids: list[UUID] = []
        self.ping_count = 0
        self.pong_count = 0
        self.max_queue_size = 0
        self.first_call_started = asyncio.Event()
        self._hold_next_call = False
        self.held_call_started = asyncio.Event()
        self.release_held_call = asyncio.Event()

    async def attach(self, registry: DeviceRegistry, handle: ConnectionHandle) -> None:
        self._registry = registry
        self._handle = handle
        self._worker = asyncio.create_task(self._worker_loop())

    def hold_next_call(self) -> None:
        self._hold_next_call = True
        self.held_call_started.clear()
        self.release_held_call.clear()

    async def send_text(self, payload: str) -> None:
        if self._closed:
            raise OSError("in-memory device transport is closed")
        frame: ServerFrame = parse_server_frame(payload)
        if isinstance(frame, PingFrame):
            self.ping_count += 1
            if self._registry is not None and self._handle is not None:
                if await self._registry.mark_pong(self._handle, frame.id):
                    self.pong_count += 1
            return
        if not isinstance(frame, ToolCallFrame):
            return
        self.sent_tool_ids.append(frame.id)
        await self._queue.put(frame)
        self.max_queue_size = max(self.max_queue_size, self._queue.qsize())

    async def send_binary(self, payload: bytes) -> None:
        del payload
        raise NotImplementedError("source harness does not exercise binary transfers")

    async def close(self, code: int, reason: str) -> None:
        del code, reason
        if self._closed:
            return
        self._closed = True
        worker = self._worker
        if worker is not None and not worker.done():
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)

    async def _worker_loop(self) -> None:
        while True:
            frame = await self._queue.get()
            if frame is None:
                return
            self._work_stats.active += 1
            self._work_stats.started += 1
            self._work_stats.max_active = max(
                self._work_stats.max_active,
                self._work_stats.active,
            )
            self.first_call_started.set()
            try:
                if self._hold_next_call:
                    self._hold_next_call = False
                    self.held_call_started.set()
                    await self.release_held_call.wait()
                await asyncio.sleep(self.delay_seconds)
                result = ToolResultFrame(
                    id=frame.id,
                    content=(
                        f"user={self.user_id};device={self.device_id};"
                        f"session={frame.args.get('session_id', '')}"
                    ),
                    is_error=False,
                )
                if self._registry is not None and self._handle is not None:
                    resolved = await self._registry.resolve_tool_result(self._handle, result)
                    if resolved:
                        self.completed_tool_ids.append(frame.id)
            finally:
                self._work_stats.active -= 1
                self._queue.task_done()


@dataclass(frozen=True, slots=True)
class _DeviceIdentity:
    user_id: UUID
    device_id: UUID
    device_name: str
    token: str


@dataclass(frozen=True, slots=True)
class _DispatchOutcome:
    index: int
    latency_seconds: float
    result: ToolResultFrame | None
    error: Exception | None


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def _sample_to_dict(sample: _ProcessSample) -> dict[str, int | None]:
    return {
        "rss_bytes": sample.rss_bytes,
        "fd_count": sample.fd_count,
        "task_count": sample.task_count,
    }


class _SourceHarness:
    def __init__(self, config: HarnessConfig) -> None:
        self.config = config.normalized()
        assert self.config.users is not None
        assert self.config.sessions is not None
        self.user_ids = [
            uuid5(_HARNESS_NAMESPACE, f"user-{index}")
            for index in range(self.config.users)
        ]
        self.identities: list[_DeviceIdentity] = []
        self.transports: list[_MemoryDeviceTransport] = []
        self.handles: list[ConnectionHandle] = []
        self.heartbeat_tasks: list[asyncio.Task[None]] = []
        self.registry = DeviceRegistry(
            pending_calls_max=max(
                self.config.dispatch_concurrency * 2,
                self.config.pending_calls_per_user * 2,
            ),
            pending_calls_max_per_user=self.config.pending_calls_per_user,
        )
        self.work_stats = _WorkStats()
        self.dispatch_latencies: list[float] = []
        self.error_counts: Counter[str] = Counter()
        self.cross_user_result_errors = 0
        self.cross_user_slot_errors = 0
        self.slot_spoof_rejected = False
        self.slot_spoof_future_survived = False
        self.slow_user_isolated = False
        self.fifo_ok = False
        self.busy_observed = False
        self.unreachable_observed = False
        self.authenticated_connections = 0

    def _queue_high_water(self) -> int:
        return max((transport.max_queue_size for transport in self.transports), default=0)

    async def connect_all(self) -> None:
        for index in range(self.config.connections):
            user_id = self.user_ids[index % len(self.user_ids)]
            device_id = uuid5(_HARNESS_NAMESPACE, f"device-{index}")
            identity = _DeviceIdentity(
                user_id=user_id,
                device_id=device_id,
                device_name=f"capacity-device-{index}",
                token=f"source-bearer-{index:04d}",
            )
            self.identities.append(identity)
            transport = _MemoryDeviceTransport(
                user_id=user_id,
                device_id=device_id,
                device_name=identity.device_name,
                delay_seconds=(
                    self.config.slow_delay_seconds
                    if user_id == self.user_ids[0]
                    else self.config.call_delay_seconds
                ),
                queue_capacity=self.config.queue_capacity,
                work_stats=self.work_stats,
            )
            self.transports.append(transport)
            handle = await self._connect(identity.token)
            self.handles.append(handle)
            await transport.attach(self.registry, handle)
            self.heartbeat_tasks.append(
                asyncio.create_task(
                    device_ws._heartbeat(
                        self.registry,
                        handle,
                        cast(Any, transport),
                        ping_interval_seconds=self.config.ping_interval_seconds,
                        liveness_timeout_seconds=self.config.liveness_timeout_seconds,
                    )
                )
            )

    async def _connect(self, token: str) -> ConnectionHandle:
        identity = next((item for item in self.identities if item.token == token), None)
        if identity is None:
            raise DeviceUnavailableError("source bearer token is not recognized")
        transport = self.transports[-1]
        handle = await self.registry.register(
            device_id=identity.device_id,
            user_id=identity.user_id,
            device_name=identity.device_name,
            transport=transport,
        )
        if handle is None:
            raise DeviceUnavailableError("source device registration was rejected")
        self.authenticated_connections += 1
        return handle

    async def _dispatch(
        self,
        *,
        index: int,
        device_index: int,
        session_id: UUID,
        record_latency: bool,
    ) -> _DispatchOutcome:
        identity = self.identities[device_index]
        started = time.perf_counter()
        try:
            result = await self.registry.dispatch_tool(
                device_id=identity.device_id,
                user_id=identity.user_id,
                name="read_file",
                args={
                    "path": f"capacity/{session_id}.txt",
                    "session_id": str(session_id),
                },
                max_result_bytes=1024,
                timeout_seconds=max(self.config.liveness_timeout_seconds * 4, 1.0),
                expected_device_name=identity.device_name,
            )
        except Exception as exc:
            self.error_counts[type(exc).__name__] += 1
            return _DispatchOutcome(
                index=index,
                latency_seconds=time.perf_counter() - started,
                result=None,
                error=exc,
            )
        latency = time.perf_counter() - started
        if record_latency:
            self.dispatch_latencies.append(latency)
        expected = (
            f"user={identity.user_id};device={identity.device_id};session={session_id}"
        )
        if result.content != expected:
            self.cross_user_result_errors += 1
            self.cross_user_slot_errors += 1
        return _DispatchOutcome(
            index=index,
            latency_seconds=latency,
            result=result,
            error=None,
        )

    async def _wait_for_pending(self, _user_id: UUID, expected: int) -> None:
        for _ in range(2_000):
            if self.registry.pending_count >= expected:
                return
            await asyncio.sleep(0)
        raise AssertionError(f"pending call count did not reach {expected}")

    async def run_probes(self) -> None:
        if len(self.identities) == 1:
            return
        await self._run_slot_spoof_probe()
        slow_tasks = [
            asyncio.create_task(
                self._dispatch(
                    index=-1 - index,
                    device_index=0,
                    session_id=uuid5(_HARNESS_NAMESPACE, f"slow-probe-{index}"),
                    record_latency=False,
                )
            )
            for index in range(self.config.pending_calls_per_user)
        ]
        await self.transports[0].first_call_started.wait()
        await self._wait_for_pending(self.user_ids[0], self.config.pending_calls_per_user)
        fast_id = uuid5(_HARNESS_NAMESPACE, "fast-probe")
        fast_task = asyncio.create_task(
            self._dispatch(
                index=-2,
                device_index=1,
                session_id=fast_id,
                record_latency=False,
            )
        )
        fast_outcome = await fast_task
        self.slow_user_isolated = (
            any(not task.done() for task in slow_tasks) and fast_outcome.error is None
        )
        await asyncio.gather(*slow_tasks)

        fifo_device_index = 2 if len(self.identities) > 2 else 1
        transport = self.transports[fifo_device_index]
        sent_before = len(transport.sent_tool_ids)
        completed_before = len(transport.completed_tool_ids)
        fifo_tasks = [
            asyncio.create_task(
                self._dispatch(
                    index=-100 - index,
                    device_index=fifo_device_index,
                    session_id=uuid5(_HARNESS_NAMESPACE, f"fifo-{index}"),
                    record_latency=False,
                )
            )
            for index in range(min(self.config.queue_capacity, 8))
        ]
        fifo_outcomes = await asyncio.gather(*fifo_tasks)
        sent = transport.sent_tool_ids[sent_before:]
        completed = transport.completed_tool_ids[completed_before:]
        self.fifo_ok = (
            len(sent) == len(completed) == len(fifo_outcomes)
            and sent == completed
            and all(outcome.error is None for outcome in fifo_outcomes)
        )

        busy_tasks = [
            asyncio.create_task(
                self._dispatch(
                    index=-200 - index,
                    device_index=0,
                    session_id=uuid5(_HARNESS_NAMESPACE, f"busy-{index}"),
                    record_latency=False,
                )
            )
            for index in range(self.config.pending_calls_per_user)
        ]
        await self._wait_for_pending(self.user_ids[0], self.config.pending_calls_per_user)
        busy_outcome = await self._dispatch(
            index=-299,
            device_index=0,
            session_id=uuid5(_HARNESS_NAMESPACE, "busy-overflow"),
            record_latency=False,
        )
        self.busy_observed = isinstance(busy_outcome.error, DeviceBusyError)
        await asyncio.gather(*busy_tasks)
        unreachable_id = uuid5(_HARNESS_NAMESPACE, "unreachable-device")
        unreachable_user = uuid5(_HARNESS_NAMESPACE, "unreachable-user")
        try:
            await self.registry.dispatch_tool(
                device_id=unreachable_id,
                user_id=unreachable_user,
                name="read_file",
                args={"path": "missing"},
                max_result_bytes=1024,
                timeout_seconds=1,
            )
        except DeviceUnavailableError:
            self.unreachable_observed = True
            self.error_counts[DeviceUnavailableError.__name__] += 1

    async def _run_slot_spoof_probe(self) -> None:
        """Reject B/stale-generation delivery while A's future stays live."""
        transport_a = self.transports[0]
        transport_a.hold_next_call()
        pending = asyncio.create_task(
            self._dispatch(
                index=-400,
                device_index=0,
                session_id=uuid5(_HARNESS_NAMESPACE, "slot-spoof"),
                record_latency=False,
            )
        )
        await transport_a.held_call_started.wait()
        call_id = transport_a.sent_tool_ids[-1]
        spoof = ToolResultFrame(
            id=call_id,
            content="spoofed-by-other-device",
            is_error=False,
        )
        from_other_device = await self.registry.resolve_tool_result(self.handles[1], spoof)
        stale_handle = ConnectionHandle(
            device_id=self.handles[0].device_id,
            generation=max(0, self.handles[0].generation - 1),
        )
        from_stale_generation = await self.registry.resolve_tool_result(stale_handle, spoof)
        self.slot_spoof_rejected = not from_other_device and not from_stale_generation
        self.slot_spoof_future_survived = not pending.done()
        transport_a.release_held_call.set()
        outcome = await pending
        self.slot_spoof_future_survived = self.slot_spoof_future_survived and outcome.error is None

    async def run_sessions(self) -> list[_DispatchOutcome]:
        assert self.config.sessions is not None
        semaphore = asyncio.Semaphore(self.config.dispatch_concurrency)

        async def bounded(index: int) -> _DispatchOutcome:
            async with semaphore:
                return await self._dispatch(
                    index=index,
                    device_index=index % len(self.identities),
                    session_id=uuid5(_HARNESS_NAMESPACE, f"session-{index}"),
                    record_latency=True,
                )

        tasks = [asyncio.create_task(bounded(index)) for index in range(self.config.sessions)]
        return await asyncio.gather(*tasks)

    async def disconnect_pending_probe(self) -> bool:
        pending_task = asyncio.create_task(
            self._dispatch(
                index=-300,
                device_index=0,
                session_id=uuid5(_HARNESS_NAMESPACE, "disconnect-pending"),
                record_latency=False,
            )
        )
        await self._wait_for_pending(self.user_ids[0], 1)
        unregistered = await self.registry.unregister(self.handles[0])
        outcome = await pending_task
        return unregistered and isinstance(outcome.error, DeviceUnavailableError)

    async def close(self) -> None:
        await self.registry.close()
        for task in self.heartbeat_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.heartbeat_tasks, return_exceptions=True)


async def run_harness(config: HarnessConfig = HarnessConfig()) -> dict[str, object]:
    """Run one source-mode capacity pass and return JSON-compatible evidence."""

    harness = _SourceHarness(config)
    baseline_sample = _process_sample()
    sampler = _MetricsSampler(
        harness.config.sample_interval_seconds,
        harness._queue_high_water,
        lambda: harness.registry.pending_count,
    )
    started = time.perf_counter()
    bulk_outcomes: list[_DispatchOutcome] = []
    disconnect_ok = False
    failure: str | None = None
    dispatch_baseline_sample = baseline_sample
    await sampler.start()
    try:
        await harness.connect_all()
        await harness.run_probes()
        # Connection objects and heartbeat workers are an intentional fixed
        # footprint.  Compare the post-run RSS with this point to detect
        # growth attributable to completed dispatches, not setup itself.
        dispatch_baseline_sample = _process_sample()
        bulk_outcomes = await harness.run_sessions()
        # Keep heartbeats alive for at least two scheduling intervals so the
        # ping/pong assertion is meaningful even when the smoke workload is tiny.
        await asyncio.sleep(max(harness.config.ping_interval_seconds * 2, 0.01))
        disconnect_ok = await harness.disconnect_pending_probe()
    except Exception as exc:
        failure = f"{type(exc).__name__}: {exc}"
    finally:
        await harness.close()
        await sampler.stop()
    after_cleanup = _process_sample()
    elapsed = time.perf_counter() - started
    all_ping_count = sum(transport.ping_count for transport in harness.transports)
    all_pong_count = sum(transport.pong_count for transport in harness.transports)
    successful_bulk = sum(outcome.error is None for outcome in bulk_outcomes)
    documented_errors = {
        "DeviceBusyError",
        "DeviceUnavailableError",
    }
    bulk_errors_are_documented = all(
        outcome.error is None or type(outcome.error).__name__ in documented_errors
        for outcome in bulk_outcomes
    )
    final_connections = sum(
        await asyncio.gather(
            *(harness.registry.is_current(handle) for handle in harness.handles),
        )
    )
    final_pending = harness.registry.pending_count
    final_transfer_slots = harness.registry.transfers.active_slots
    final_transfer_waiters = harness.registry.transfers._admission.waiting_count  # noqa: SLF001
    peak_rss = sampler.peak_rss_bytes
    p50_seconds = _percentile(harness.dispatch_latencies, 0.50)
    p95_seconds = _percentile(harness.dispatch_latencies, 0.95)
    rss_growth = None
    if dispatch_baseline_sample.rss_bytes is not None and after_cleanup.rss_bytes is not None:
        rss_growth = max(0, after_cleanup.rss_bytes - dispatch_baseline_sample.rss_bytes)
    rss_plateau = rss_growth is None or rss_growth <= max(4 * 1024 * 1024, (peak_rss or 0) // 10)
    baseline_tasks = baseline_sample.task_count
    task_baseline_ok = after_cleanup.task_count <= baseline_tasks + 1
    required_users = 100 if harness.config.connections >= 500 else 1
    checks = {
        "authenticated_connections": harness.authenticated_connections
        == harness.config.connections,
        "minimum_users": len(harness.user_ids) >= required_users,
        "no_cross_user_result_or_slot_delivery": harness.cross_user_result_errors == 0
        and harness.cross_user_slot_errors == 0,
        "cross_user_and_stale_generation_slots_rejected": harness.slot_spoof_rejected
        and harness.slot_spoof_future_survived,
        "same_device_fifo": harness.fifo_ok,
        "cross_device_concurrency": harness.work_stats.max_active >= 2
        if harness.config.connections > 1
        else True,
        "slow_user_does_not_block_other_user": harness.slow_user_isolated,
        "ping_pong_under_load": all_ping_count > 0 and all_ping_count == all_pong_count,
        "busy_result_is_documented": harness.busy_observed,
        "unreachable_result_is_documented": harness.unreachable_observed,
        "bulk_calls_complete_or_documented": successful_bulk == len(bulk_outcomes)
        or bulk_errors_are_documented,
        "disconnect_cleans_pending_and_limiter": disconnect_ok
        and final_pending == 0
        and final_transfer_slots == 0
        and final_transfer_waiters == 0,
        "task_count_returns_to_baseline": task_baseline_ok,
        "queue_high_water_is_bounded": sampler.peak_queue_high_water
        <= harness.config.queue_capacity,
        "rss_plateau": rss_plateau,
    }
    if failure is not None:
        checks["harness_completed"] = False
    result: dict[str, object] = {
        "ok": all(checks.values()),
        "failure": failure,
        "mode": harness.config.mode,
        "transport": "in_memory_device_transport",
        "network_exercised": False,
        "authentication": "synthetic_bearer_token_registry_lookup",
        "limitations": [
            "Does not exercise FastAPI WebSocket framing, PostgreSQL token lookup, or a packaged client.",
            "Run a real WebSocket/client E2E separately before making network-capacity claims.",
        ],
        "connections": harness.config.connections,
        "authenticated_connections": harness.authenticated_connections,
        "users": len(harness.user_ids),
        "independent_sessions": harness.config.sessions,
        "read_only_tool": "read_file",
        "dispatch_concurrency": harness.config.dispatch_concurrency,
        "successful_bulk_dispatches": successful_bulk,
        "dispatch_errors": dict(harness.error_counts),
        "cross_user_result_errors": harness.cross_user_result_errors,
        "cross_user_slot_errors": harness.cross_user_slot_errors,
        "slot_spoof_rejected": harness.slot_spoof_rejected,
        "slot_spoof_future_survived": harness.slot_spoof_future_survived,
        "max_cross_device_active_calls": harness.work_stats.max_active,
        "device_calls_started": harness.work_stats.started,
        "ping_count": all_ping_count,
        "pong_count": all_pong_count,
        "metrics": {
            "wall_time_seconds": round(elapsed, 6),
            "dispatch_latency_ms": {
                "count": len(harness.dispatch_latencies),
                "p50": _round_or_none(p50_seconds * 1000) if p50_seconds is not None else None,
                "p95": _round_or_none(p95_seconds * 1000) if p95_seconds is not None else None,
            },
            "peak_rss_bytes": peak_rss,
            "peak_open_file_descriptors": sampler.peak_fd_count,
            "peak_task_count": sampler.peak_task_count,
            "process_metrics_source": "procfs_and_resource",
            "device_queue_high_water": sampler.peak_queue_high_water,
            "registry_pending_high_water": sampler.peak_pending_count,
            "queue_capacity": harness.config.queue_capacity,
            "rss_before_bytes": baseline_sample.rss_bytes,
            "rss_before_dispatch_bytes": dispatch_baseline_sample.rss_bytes,
            "rss_connection_setup_growth_bytes": (
                max(0, dispatch_baseline_sample.rss_bytes - baseline_sample.rss_bytes)
                if dispatch_baseline_sample.rss_bytes is not None
                and baseline_sample.rss_bytes is not None
                else None
            ),
            "rss_after_cleanup_bytes": after_cleanup.rss_bytes,
            "rss_growth_after_cleanup_bytes": rss_growth,
            "rss_plateau": rss_plateau,
            "baseline": _sample_to_dict(baseline_sample),
            "after_cleanup": {
                **_sample_to_dict(after_cleanup),
                "connections": final_connections,
                "pending_calls": final_pending,
                "transfer_slots": final_transfer_slots,
                "transfer_waiters": final_transfer_waiters,
            },
        },
        "checks": checks,
    }
    return result


def _round_or_none(value: float | None) -> float | None:
    return round(value, 3) if value is not None else None


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("source",), default="source")
    parser.add_argument("--connections", type=int, default=_DEFAULT_CONNECTIONS)
    parser.add_argument("--users", type=int, default=None)
    parser.add_argument("--sessions", type=int, default=None)
    parser.add_argument("--dispatch-concurrency", type=int, default=_DEFAULT_DISPATCH_CONCURRENCY)
    parser.add_argument("--queue-capacity", type=int, default=_DEFAULT_QUEUE_CAPACITY)
    parser.add_argument("--pending-calls-per-user", type=int, default=_DEFAULT_PENDING_PER_USER)
    parser.add_argument("--call-delay-ms", type=float, default=_DEFAULT_CALL_DELAY_SECONDS * 1000)
    parser.add_argument("--slow-delay-ms", type=float, default=_DEFAULT_SLOW_DELAY_SECONDS * 1000)
    parser.add_argument("--ping-interval-ms", type=float, default=_DEFAULT_PING_INTERVAL_SECONDS * 1000)
    parser.add_argument(
        "--liveness-timeout-ms",
        type=float,
        default=_DEFAULT_LIVENESS_TIMEOUT_SECONDS * 1000,
    )
    parser.add_argument(
        "--sample-interval-ms",
        type=float,
        default=_DEFAULT_SAMPLE_INTERVAL_SECONDS * 1000,
    )
    parser.add_argument("--indent", type=int, default=2)
    return parser.parse_args(argv)


def _config_from_args(args: argparse.Namespace) -> HarnessConfig:
    return HarnessConfig(
        mode=args.mode,
        connections=args.connections,
        users=args.users,
        sessions=args.sessions,
        dispatch_concurrency=args.dispatch_concurrency,
        queue_capacity=args.queue_capacity,
        pending_calls_per_user=args.pending_calls_per_user,
        call_delay_seconds=args.call_delay_ms / 1000,
        slow_delay_seconds=args.slow_delay_ms / 1000,
        ping_interval_seconds=args.ping_interval_ms / 1000,
        liveness_timeout_seconds=args.liveness_timeout_ms / 1000,
        sample_interval_seconds=args.sample_interval_ms / 1000,
    )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        config = _config_from_args(args).normalized()
        result = asyncio.run(run_harness(config))
    except (ValueError, OSError) as exc:
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}))
        return 2
    print(json.dumps(result, indent=args.indent, sort_keys=True))
    return 0 if result["ok"] is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
