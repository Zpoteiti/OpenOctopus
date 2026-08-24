"""Fast unit checks for the real-network capacity peer lifecycle."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest
from websockets.exceptions import ConnectionClosedError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from device_network_capacity_harness import (  # noqa: E402
    _BRIDGE_JOBS_PER_USER,
    NetworkHarnessConfig,
    _bridge_jobs,
    _BridgeMetrics,
    _Identity,
    _run_transfer_batch,
    _SourcePeer,
)

from openctopus_server.devices.protocol import PROTOCOL_VERSION


def test_network_harness_reserves_two_devices_per_bridge_owner() -> None:
    assert NetworkHarnessConfig(connections=8).normalized().users == 4

    with pytest.raises(ValueError, match="two connected devices per user"):
        NetworkHarnessConfig(connections=8, users=5).normalized()


def test_bridge_jobs_are_same_owner_distinct_device_and_fill_the_busy_probe() -> None:
    first_owner = uuid4()
    second_owner = uuid4()
    first_a = _Identity(first_owner, uuid4(), "first-a", "token-a")
    first_b = _Identity(first_owner, uuid4(), "first-b", "token-b")
    second_a = _Identity(second_owner, uuid4(), "second-a", "token-c")
    second_b = _Identity(second_owner, uuid4(), "second-b", "token-d")

    jobs = _bridge_jobs((first_a, second_a, first_b, second_b))

    assert len(jobs) == 2 * _BRIDGE_JOBS_PER_USER
    assert jobs[0].source == first_a
    assert jobs[0].destination == first_b
    assert jobs[1].source == first_b
    assert jobs[1].destination == first_a
    assert all(job.source.user_id == job.destination.user_id for job in jobs)
    assert all(job.source.device_id != job.destination.device_id for job in jobs)


def test_bridge_metrics_track_logical_slots_endpoints_queue_and_cleanup() -> None:
    owner = uuid4()
    first = SimpleNamespace(
        queue=asyncio.Queue(maxsize=4),
        user_id=owner,
        worker=SimpleNamespace(done=lambda: False),
        relay_task=SimpleNamespace(done=lambda: False),
        finish_task=None,
    )
    first.queue.put_nowait(b"one")
    second = SimpleNamespace(
        queue=asyncio.Queue(maxsize=4),
        user_id=owner,
        worker=SimpleNamespace(done=lambda: False),
        relay_task=None,
        finish_task=None,
    )
    manager = SimpleNamespace(
        _bridges={uuid4(): first, uuid4(): second},
        _bridge_endpoints={uuid4(): object() for _ in range(4)},
        _bridge_tombstones={uuid4(): SimpleNamespace(pinned=True)},
        _reserved_tombstone_credits=4,
        _admission=SimpleNamespace(
            active_count=2,
            waiting_count=3,
            active_by_user={owner: 2},
        ),
    )
    peer = SimpleNamespace(local_active_slots=2)
    metrics = _BridgeMetrics()

    metrics.record(cast(Any, manager), cast(Any, [peer]))

    assert metrics.active_high_water == 2
    assert metrics.endpoint_high_water == 4
    assert metrics.queue_high_water == 1
    assert metrics.admission_active_high_water == 2
    assert metrics.admission_waiting_high_water == 3
    assert metrics.per_user_active_high_water == 2
    assert metrics.task_high_water == 3
    assert metrics.local_slot_high_water == 2
    assert metrics.pinned_tombstone_high_water == 1
    assert metrics.reserved_tombstone_high_water == 4


class _EndedSocket:
    def __aiter__(self) -> _EndedSocket:
        return self

    async def __anext__(self) -> str:
        raise StopAsyncIteration


class _DroppedSocket:
    def __aiter__(self) -> _DroppedSocket:
        return self

    async def __anext__(self) -> str:
        raise ConnectionClosedError(None, None, None)


def test_source_peer_hello_tracks_the_current_protocol_contract() -> None:
    peer = _SourcePeer(
        _Identity(uuid4(), uuid4(), "device", "token"),
        delay=0,
        queue_capacity=2,
    )

    hello = peer._hello_frame()

    assert hello.version == PROTOCOL_VERSION
    assert hello.shells.default == "sh"
    assert hello.shells.available == ["sh"]


@pytest.mark.asyncio
async def test_source_peer_records_unexpected_connection_end() -> None:
    peer = _SourcePeer(
        _Identity(uuid4(), uuid4(), "device", "token"),
        delay=0,
        queue_capacity=2,
    )
    peer.websocket = cast(Any, _EndedSocket())

    await peer.read_frames()

    assert peer.unexpected_disconnect is True
    assert peer.error == "connection ended before harness shutdown"


@pytest.mark.asyncio
async def test_source_peer_records_an_abnormal_websocket_drop() -> None:
    peer = _SourcePeer(
        _Identity(uuid4(), uuid4(), "device", "token"),
        delay=0,
        queue_capacity=2,
    )
    peer.websocket = cast(Any, _DroppedSocket())

    await peer.read_frames()

    assert peer.unexpected_disconnect is True
    assert peer.error == "connection closed unexpectedly: 1006"


@pytest.mark.asyncio
async def test_transfer_batch_bounds_launches_below_admission_queue_capacity() -> None:
    max_concurrency = 2
    connection_count = max_concurrency * 2 + 1
    active = 0
    active_high_water = 0

    async def transfer(index: int) -> int:
        nonlocal active, active_high_water
        active += 1
        active_high_water = max(active_high_water, active)
        try:
            await asyncio.sleep(0)
            return index
        finally:
            active -= 1

    results = await _run_transfer_batch(
        connection_count,
        max_concurrency=max_concurrency,
        transfer=transfer,
    )

    assert results == list(range(connection_count))
    assert active_high_water == max_concurrency
